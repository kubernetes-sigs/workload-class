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

package webhook

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

// bestMatchWorkloadClass finds the WorkloadClass that applies to a given Pod.
//
// If the Pod's Namespace has the workloads.x-k8s.io/enforced-class label set, that enforced
// WorkloadClass applies to all Pods in the namespace and overrides PodSelector matching on any
// other WorkloadClasses in the namespace.
//
// If no enforced class is defined on the Namespace, the active WorkloadClass in the Pod's
// namespace whose PodSelector matches the Pod is selected (a nil PodSelector matches no Pods,
// matching policy/v1 PDB selector semantics). WorkloadClasses marked with the SelectorOverlap
// condition are skipped.
//
// If no matching or active enforced WorkloadClass is found, it returns nil.
func (v *DisruptionWebhook) bestMatchWorkloadClass(ctx context.Context, pod *corev1.Pod) (*workloadsv1alpha1.WorkloadClass, error) {
	// If the namespace defines an enforced WorkloadClass, it overrides PodSelector matching for the namespace.
	if bestMatch, enforced := v.namespaceDefaultWorkloadClass(ctx, pod); enforced {
		return bestMatch, nil
	}

	wcs := &workloadsv1alpha1.WorkloadClassList{}
	if err := v.Client.List(ctx, wcs, client.InNamespace(pod.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list WorkloadClasses: %w", err)
	}

	for i := range wcs.Items {
		wc := &wcs.Items[i]
		if !wc.DeletionTimestamp.IsZero() || hasSelectorOverlap(wc) {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil {
			continue
		}
		if selector.Matches(labels.Set(pod.Labels)) {
			return wc, nil
		}
	}

	return nil, nil
}

func hasSelectorOverlap(wc *workloadsv1alpha1.WorkloadClass) bool {
	for _, cond := range wc.Status.Conditions {
		if cond.Type == workloadsv1alpha1.ConditionTypeValidated &&
			cond.Status == metav1.ConditionFalse &&
			cond.Reason == workloadsv1alpha1.ReasonSelectorOverlap {
			return true
		}
	}
	return false
}

func (v *DisruptionWebhook) namespaceDefaultWorkloadClass(ctx context.Context, pod *corev1.Pod) (*workloadsv1alpha1.WorkloadClass, bool) {
	ns := &corev1.Namespace{}
	if err := v.Client.Get(ctx, client.ObjectKey{Name: pod.Namespace}, ns); err == nil {
		if defaultClass := ns.Labels[workloadsv1alpha1.EnforcedWorkloadClassLabel]; defaultClass != "" {
			wc := &workloadsv1alpha1.WorkloadClass{}
			if err := v.Client.Get(ctx, client.ObjectKey{Name: defaultClass, Namespace: pod.Namespace}, wc); err != nil {
				logf.FromContext(ctx).Error(err, "Failed to get namespace enforced WorkloadClass", "namespace", pod.Namespace, "workloadClass", defaultClass)
			} else if wc.DeletionTimestamp.IsZero() {
				return wc, true
			}
			return nil, true
		}
	}
	return nil, false
}
