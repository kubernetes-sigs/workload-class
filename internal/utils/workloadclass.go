/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

// NamespaceEnforcedWorkloadClass returns the WorkloadClass that the Namespace enforces on all of its Pods through the
// workloads.x-k8s.io/enforced-class label (EnforcedWorkloadClassLabel).
//
// enforced reports whether the Namespace has a non-empty enforced-class label. When it does, the label overrides
// PodSelector matching for every Pod in the Namespace, even if the referenced WorkloadClass does not exist or is being
// deleted; wc is nil in that case, so no WorkloadClass applies to the Namespace's Pods.
//
// If the Namespace does not exist, it returns (nil, false, nil). If the Namespace or the WorkloadClass cannot be read,
// it returns the error along with whether the Namespace was found to enforce a class.
func NamespaceEnforcedWorkloadClass(
	ctx context.Context, c client.Reader, namespace string,
) (wc *workloadsv1alpha1.WorkloadClass, enforced bool, err error) {
	ns := &corev1.Namespace{}
	if err := c.Get(ctx, client.ObjectKey{Name: namespace}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to get Namespace %s: %w", namespace, err)
	}

	name := ns.Labels[workloadsv1alpha1.EnforcedWorkloadClassLabel]
	if name == "" {
		return nil, false, nil
	}

	wc = &workloadsv1alpha1.WorkloadClass{}
	if err := c.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, wc); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, true, nil
		}
		return nil, true, fmt.Errorf("failed to get enforced WorkloadClass %s/%s: %w", namespace, name, err)
	}
	if !wc.DeletionTimestamp.IsZero() {
		return nil, true, nil
	}
	return wc, true, nil
}

// BestMatchWorkloadClassForPod returns the WorkloadClass that applies to the Pod, evaluated in memory from the result
// of NamespaceEnforcedWorkloadClass for the Pod's Namespace and the WorkloadClasses in it, or nil if none applies.
//
// If the Namespace enforces a WorkloadClass (enforced is true), enforcedWC applies to the Pod, or no WorkloadClass
// does if enforcedWC is nil. Otherwise, the best match by PodSelector is returned (see SelectorBestMatch).
func BestMatchWorkloadClassForPod(
	pod *corev1.Pod, enforcedWC *workloadsv1alpha1.WorkloadClass, enforced bool, wcs []workloadsv1alpha1.WorkloadClass,
) *workloadsv1alpha1.WorkloadClass {
	if pod == nil {
		return nil
	}
	if enforced {
		if enforcedWC == nil || enforcedWC.Namespace != pod.Namespace || !enforcedWC.DeletionTimestamp.IsZero() {
			return nil
		}
		return enforcedWC
	}
	return SelectorBestMatch(pod, wcs)
}

// SelectorBestMatch returns the WorkloadClass in the Pod's namespace that governs the Pod by PodSelector, without
// considering the Namespace's enforced WorkloadClass.
//
// WorkloadClasses in other namespaces, being deleted, or with an invalid or nil PodSelector (which matches no Pods,
// like a policy/v1 PodDisruptionBudget selector) are skipped. If more than one WorkloadClass matches the Pod, the
// oldest one wins (see OlderWorkloadClass), so the result does not depend on the order of wcs and creating a
// WorkloadClass never takes a Pod away from the WorkloadClass that already governs it.
func SelectorBestMatch(pod *corev1.Pod, wcs []workloadsv1alpha1.WorkloadClass) *workloadsv1alpha1.WorkloadClass {
	if pod == nil {
		return nil
	}

	var bestMatch *workloadsv1alpha1.WorkloadClass
	podLabels := labels.Set(pod.Labels)
	for i := range wcs {
		wc := &wcs[i]
		if wc.Namespace != pod.Namespace || !wc.DeletionTimestamp.IsZero() || wc.Spec.PodSelector == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil || !selector.Matches(podLabels) {
			continue
		}
		if bestMatch == nil || OlderWorkloadClass(wc, bestMatch) {
			bestMatch = wc
		}
	}
	return bestMatch
}

// OlderWorkloadClass reports whether a was created before b, breaking ties on equal creationTimestamp by name.
func OlderWorkloadClass(a, b *workloadsv1alpha1.WorkloadClass) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.Name < b.Name
}
