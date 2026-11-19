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
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
	"sigs.k8s.io/workload-class/internal/utils"
)

// bestMatchWorkloadClass finds the WorkloadClass that applies to a given Pod.
//
// If the Pod's Namespace has the workloads.x-k8s.io/enforced-class label set, that enforced
// WorkloadClass applies to all Pods in the namespace and overrides PodSelector matching on any
// other WorkloadClasses in the namespace.
//
// If no enforced class is defined on the Namespace, the active WorkloadClass in the Pod's
// namespace whose PodSelector matches the Pod is selected (a nil PodSelector matches no Pods,
// matching policy/v1 PDB selector semantics). If more than one WorkloadClass matches, the oldest
// one wins (by creationTimestamp, then name), so creating a WorkloadClass never takes a Pod away
// from the WorkloadClass that already governs it. Overlap is reported separately through the
// informational PodSelectorOverlap condition.
//
// The selection rules are shared with the WorkloadClass controller (see utils.SelectorBestMatch), so that the
// WorkloadClass enforced here is the one that owns the Pod's PodDisruptionBudget.
//
// If no matching or active enforced WorkloadClass is found, it returns nil.
func (v *DisruptionWebhook) bestMatchWorkloadClass(ctx context.Context, pod *corev1.Pod) (*workloadsv1alpha1.WorkloadClass, error) {
	// If the namespace defines an enforced WorkloadClass, it overrides PodSelector matching for the namespace.
	if bestMatch, enforced := v.namespaceEnforcedWorkloadClass(ctx, pod); enforced {
		return bestMatch, nil
	}

	wcs := &workloadsv1alpha1.WorkloadClassList{}
	if err := v.Client.List(ctx, wcs, client.InNamespace(pod.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list WorkloadClasses: %w", err)
	}

	return utils.SelectorBestMatch(pod, wcs.Items), nil
}

// namespaceEnforcedWorkloadClass returns the WorkloadClass enforced by the Pod's Namespace (see
// utils.NamespaceEnforcedWorkloadClass) and whether the Namespace enforces one.
//
// Errors are logged rather than returned: if the Namespace cannot be read, PodSelector matching is used; if the
// enforced WorkloadClass cannot be read, no WorkloadClass applies to the Pod.
func (v *DisruptionWebhook) namespaceEnforcedWorkloadClass(ctx context.Context, pod *corev1.Pod) (*workloadsv1alpha1.WorkloadClass, bool) {
	wc, enforced, err := utils.NamespaceEnforcedWorkloadClass(ctx, v.Client, pod.Namespace)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Failed to get namespace enforced WorkloadClass", "namespace", pod.Namespace)
		return nil, enforced
	}
	return wc, enforced
}
