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
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
	"sigs.k8s.io/workload-class/internal/utils"
)

// bestMatchWorkloadClass finds the most suitable WorkloadClass for a given Pod.
//
// If the Pod's Namespace has the workloads.x-k8s.io/enforced-class label set, that enforced
// WorkloadClass applies to all Pods in the namespace and overrides PodSelector matching on any
// other WorkloadClasses in the namespace.
//
// If no enforced class is defined on the Namespace, the WorkloadClass in the Pod's namespace
// that has the highest number of matching labels and expressions against the Pod is selected
// (a nil PodSelector matches no Pods, matching policy/v1 PDB selector semantics).
// If two or more WorkloadClasses match in specificity to the Pod, the oldest
// WorkloadClass takes precedence (breaking ties on equal CreationTimestamp by Name).
//
// The selection rules are shared with the WorkloadClass controller (see utils.SelectorBestMatch), so that the
// WorkloadClass enforced here is the one that owns the Pod's PodDisruptionBudget.
//
// If no matching or active enforced WorkloadClass is found, it returns nil.
func (v *DisruptionWebhook) bestMatchWorkloadClass(ctx context.Context, req admission.Request, pod *corev1.Pod) (bestMatch *workloadsv1alpha1.WorkloadClass, err error) {
	// If the namespace defines an enforced WorkloadClass, it overrides PodSelector matching for the namespace.
	if bestMatch, enforced := v.namespaceDefaultWorkloadClass(ctx, pod); enforced {
		return bestMatch, nil
	}

	wcs := &workloadsv1alpha1.WorkloadClassList{}
	if err := v.Client.List(ctx, wcs, client.InNamespace(pod.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list WorkloadClasses: %w", err)
	}

	bestMatch, otherMatches, maxSpecificity := utils.SelectorBestMatch(pod, wcs.Items)

	// Emit warning message for WorkloadClasses that matched, but are ignored
	v.emitWarning(ctx, req, pod, bestMatch, otherMatches, maxSpecificity)

	return bestMatch, nil
}

func (v *DisruptionWebhook) emitWarning(ctx context.Context, req admission.Request, pod *corev1.Pod, bestMatch *workloadsv1alpha1.WorkloadClass, matches map[string]int, maxSpecificity int) {
	if len(matches) == 0 {
		return
	}

	log := logf.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace, "user", req.UserInfo.Username)
	log.Info("Multiple WorkloadClasses matched Pod, but were not the best match", "pod", pod.Name, "otherMatches", matches)

	var matchesWithMaxSpecificity []string
	for m, s := range matches {
		if s == maxSpecificity {
			matchesWithMaxSpecificity = append(matchesWithMaxSpecificity, m)
		}
	}

	slices.Sort(matchesWithMaxSpecificity)

	// Emit a warning specifically for those with max specificity that were not selected
	if len(matchesWithMaxSpecificity) != 0 && v.Recorder != nil {
		warning := fmt.Sprintf("the following WorkloadClasses match pods with the same specificity as the best match, but were not selected: %s", strings.Join(matchesWithMaxSpecificity, ", "))
		v.Recorder.Eventf(
			bestMatch,
			nil,
			corev1.EventTypeWarning,
			"AmbiguousMatch",
			"SelectWorkloadClass",
			"%s",
			warning,
		)
	}
}

// namespaceDefaultWorkloadClass returns the WorkloadClass enforced by the Pod's Namespace (see
// utils.NamespaceEnforcedWorkloadClass) and whether the Namespace enforces one.
//
// Errors are logged rather than returned: if the Namespace cannot be read, PodSelector matching is used; if the
// enforced WorkloadClass cannot be read, no WorkloadClass applies to the Pod.
func (v *DisruptionWebhook) namespaceDefaultWorkloadClass(ctx context.Context, pod *corev1.Pod) (*workloadsv1alpha1.WorkloadClass, bool) {
	wc, enforced, err := utils.NamespaceEnforcedWorkloadClass(ctx, v.Client, pod.Namespace)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Failed to get namespace enforced WorkloadClass", "namespace", pod.Namespace)
		return nil, enforced
	}
	return wc, enforced
}
