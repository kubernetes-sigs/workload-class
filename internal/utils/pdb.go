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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

const (
	PDBNamePrefix = "workload-"

	openValue   = "100%" // Allows 100% of Pods to be unavailable
	closedValue = 0      // Allows 0 Pods to be unavailable
)

// PDBName returns the name of the PDB based on the WorkloadClass name.
// PDBs made for WorkloadClasses will have the prefix `workload-`.
func PDBName(wcName string) string {
	return PDBNamePrefix + wcName
}

// PDBBase returns a basic PDB, configured based on the WorkloadClass.
func PDBBase(wc *workloadsv1alpha1.WorkloadClass) *policyv1.PodDisruptionBudget {
	if wc == nil {
		return nil
	}
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PDBName(wc.Name),
			Namespace: wc.Namespace,
		},
	}
}

// IsEnforcedWorkloadClass reports whether ns enforces wc across all Pods in the namespace
// via the workloads.x-k8s.io/enforced-class label.
func IsEnforcedWorkloadClass(ns *corev1.Namespace, wc *workloadsv1alpha1.WorkloadClass) bool {
	if ns == nil || wc == nil || !wc.DeletionTimestamp.IsZero() {
		return false
	}
	return ns.Labels[workloadsv1alpha1.EnforcedWorkloadClassLabel] == wc.Name
}

// SyncPDBWithWorkloadClass configures the PDB to match the WorkloadClass' disruption policy.
//
// When enforced is true (i.e. the Namespace's workloads.x-k8s.io/enforced-class label names wc),
// the enforced WorkloadClass overrides PodSelector matching and applies to every Pod in the namespace,
// so pdb.Spec.Selector is set to an empty LabelSelector (&metav1.LabelSelector{}) to match all Pods
// in the namespace. Under an enforced-class Namespace, the controller must only maintain a PDB for
// the enforced WorkloadClass and delete managed PDBs for all other WorkloadClasses in that namespace
// so that no Pod is covered by multiple class PDBs.
//
// Otherwise, pdb.Spec.Selector is a copy of wc.Spec.PodSelector (which in policy/v1 matches no Pods
// when nil); overlapping PodSelectors across WorkloadClasses in the same namespace fail validation
// (ReasonSelectorOverlap) so their PDBs are not active simultaneously.
//
// In both modes, if a user-managed PodDisruptionBudget in the namespace overlaps the WorkloadClass's
// selected Pods (including any user PDB in an enforced-class namespace), the controller skips creating
// (or deletes) the managed class PDB and relies on DisruptionWebhook enforcement so that no Pod is
// covered by more than one PDB.
func SyncPDBWithWorkloadClass(wc *workloadsv1alpha1.WorkloadClass, pdb *policyv1.PodDisruptionBudget, enforced ...bool) error {
	if wc == nil {
		return fmt.Errorf("failed to sync PDB with WorkloadClass, WorkloadClass is nil")
	}

	if pdb == nil {
		return fmt.Errorf("failed to sync PDB with WorkloadClass, PDB is nil")
	}

	if pdb.Name == "" {
		pdb.Name = PDBName(wc.Name)
	}
	if pdb.Namespace == "" {
		pdb.Namespace = wc.Namespace
	}

	if len(enforced) > 0 && enforced[0] {
		pdb.Spec.Selector = &metav1.LabelSelector{}
	} else {
		pdb.Spec.Selector = wc.Spec.PodSelector.DeepCopy()
	}

	// AlwaysAllow allows evicting unhealthy (unready) Pods even when maxUnavailable is 0
	// so broken Pods (e.g. in CrashLoopBackOff) do not hold up node drains or upgrades.
	policy := policyv1.AlwaysAllow
	pdb.Spec.UnhealthyPodEvictionPolicy = &policy

	open := intstr.FromString(openValue)
	closed := intstr.FromInt32(closedValue)

	// If the WorkloadClass failed validation, do not block evictions via the PDB.
	for _, cond := range wc.Status.Conditions {
		if cond.Type == workloadsv1alpha1.ConditionTypeValidated && cond.Status == metav1.ConditionFalse {
			pdb.Spec.MaxUnavailable = &open
			return nil
		}
	}

	maxUnavailableMap := map[workloadsv1alpha1.MaintenanceReadiness]*intstr.IntOrString{
		workloadsv1alpha1.ReadinessReady:    &open,
		workloadsv1alpha1.ReadinessNotReady: &closed,
		workloadsv1alpha1.ReadinessOverdue:  &open,
	}

	// Set MaxUnavailable based on the WorkloadClass' MaintenanceReadiness
	if val, ok := maxUnavailableMap[wc.Status.MaintenanceReadiness]; ok {
		pdb.Spec.MaxUnavailable = val
	} else {
		pdb.Spec.MaxUnavailable = &closed
	}

	return nil
}
