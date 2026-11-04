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
	"crypto/sha256"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

const (
	// PDBNamePrefix is the prefix of the name of every PodDisruptionBudget managed by a WorkloadClass.
	PDBNamePrefix = "workload-"

	// workloadClassKind is the Kind of the controller OwnerReference of a PodDisruptionBudget managed by a WorkloadClass.
	workloadClassKind = "WorkloadClass"

	// MaxPDBNameLength is the maximum length of a PodDisruptionBudget name (a DNS-1123 subdomain).
	MaxPDBNameLength = 253

	// pdbNameHashLength is the number of bytes of the SHA-256 hash of the Pod name that are hex-encoded and appended
	// to a truncated PodDisruptionBudget name.
	pdbNameHashLength = 4

	openValue   = "100%" // Allows 100% of Pods to be unavailable
	closedValue = 0      // Allows 0 Pods to be unavailable
)

// PDBName returns the name of the PodDisruptionBudget of a Pod governed by a WorkloadClass: the Pod name with the
// prefix `workload-`.
//
// If the prefixed name exceeds MaxPDBNameLength, it is truncated and suffixed with a hyphen and the first 8 hex
// characters of the SHA-256 hash of the Pod name, so that the name stays unique and deterministic.
func PDBName(podName string) string {
	name := PDBNamePrefix + podName
	if len(name) <= MaxPDBNameLength {
		return name
	}
	sum := sha256.Sum256([]byte(podName))
	suffix := fmt.Sprintf("-%x", sum[:pdbNameHashLength])
	// A DNS-1123 subdomain must end with an alphanumeric character, so trim separators left by the truncation.
	prefix := strings.TrimRight(name[:MaxPDBNameLength-len(suffix)], "-.")
	return prefix + suffix
}

// PodNameFromPDBName returns the name of the Pod that a PodDisruptionBudget managed by a WorkloadClass was created
// for, by removing the `workload-` prefix.
//
// The result is not the Pod name if PDBName truncated the name; callers can detect that case by the PDB name being
// MaxPDBNameLength characters long and compare PDBName(pod.Name) against the PDB name instead.
func PodNameFromPDBName(pdbName string) string {
	return strings.TrimPrefix(pdbName, PDBNamePrefix)
}

// PDBBase returns a base PodDisruptionBudget for the Pod: its name (see PDBName), the Pod's namespace, and the
// PodUIDLabel label set to the Pod's UID. It returns nil if the Pod is nil.
func PDBBase(pod *corev1.Pod) *policyv1.PodDisruptionBudget {
	if pod == nil {
		return nil
	}
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PDBName(pod.Name),
			Namespace: pod.Namespace,
			Labels: map[string]string{
				workloadsv1alpha1.PodUIDLabel: string(pod.UID),
			},
		},
	}
}

// SyncPDBWithWorkloadClass configures the PodDisruptionBudget of a single Pod to match the disruption policy of the
// WorkloadClass that governs the Pod.
//
// The PDB selects only the PodUIDLabel label with the Pod's UID, so it covers exactly this Pod. Because each Pod has
// its own PDB, a Pod is never covered by more than one WorkloadClass-managed PDB, which the eviction API rejects.
//
// protects reports whether the WorkloadClass currently protects the Pods it governs; the caller decides it from the
// WorkloadClass' Validated condition. If protects is false (e.g. the WorkloadClass failed validation), the PDB is
// opened (MaxUnavailable = 100%) so that it does not block evictions. Otherwise, MaxUnavailable follows the
// WorkloadClass' MaintenanceReadiness:
//   - Ready or Overdue: 100% (disruptions are allowed).
//   - NotReady, or any other value including unset: 0 (disruptions are blocked), so that the PDB fails safe before
//     the controller has calculated readiness or when readiness cannot be determined.
//
// UnhealthyPodEvictionPolicy is AlwaysAllow, so that a Pod that is not Ready (e.g. in CrashLoopBackOff) does not hold
// up node drains or upgrades while MaxUnavailable is 0.
func SyncPDBWithWorkloadClass(
	wc *workloadsv1alpha1.WorkloadClass, pod *corev1.Pod, pdb *policyv1.PodDisruptionBudget, protects bool,
) error {
	if wc == nil {
		return fmt.Errorf("failed to sync PDB with WorkloadClass, WorkloadClass is nil")
	}
	if pod == nil {
		return fmt.Errorf("failed to sync PDB with WorkloadClass, Pod is nil")
	}
	if pdb == nil {
		return fmt.Errorf("failed to sync PDB with WorkloadClass, PDB is nil")
	}

	if pdb.Name == "" {
		pdb.Name = PDBName(pod.Name)
	}
	if pdb.Namespace == "" {
		pdb.Namespace = pod.Namespace
	}
	if pdb.Labels == nil {
		pdb.Labels = map[string]string{}
	}
	pdb.Labels[workloadsv1alpha1.PodUIDLabel] = string(pod.UID)

	pdb.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: map[string]string{
			workloadsv1alpha1.PodUIDLabel: string(pod.UID),
		},
	}

	policy := policyv1.AlwaysAllow
	pdb.Spec.UnhealthyPodEvictionPolicy = &policy

	open := intstr.FromString(openValue)
	closed := intstr.FromInt32(closedValue)

	if !protects {
		pdb.Spec.MaxUnavailable = &open
		return nil
	}

	switch wc.Status.MaintenanceReadiness {
	case workloadsv1alpha1.ReadinessReady, workloadsv1alpha1.ReadinessOverdue:
		pdb.Spec.MaxUnavailable = &open
	default:
		pdb.Spec.MaxUnavailable = &closed
	}
	return nil
}

// IsWorkloadClassManagedPDB reports whether the PodDisruptionBudget is managed by a WorkloadClass, i.e. its
// controller OwnerReference is a WorkloadClass of the workloads.x-k8s.io group (any version).
//
// Every other PDB is user-created. The WorkloadClass controller never modifies a user-created PDB, even one named
// like a managed PDB (see PDBName).
func IsWorkloadClassManagedPDB(pdb *policyv1.PodDisruptionBudget) bool {
	if pdb == nil {
		return false
	}
	owner := metav1.GetControllerOf(pdb)
	if owner == nil || owner.Kind != workloadClassKind {
		return false
	}
	gv, err := schema.ParseGroupVersion(owner.APIVersion)
	return err == nil && gv.Group == workloadsv1alpha1.GroupVersion.Group
}

// IsPodCoveredByUserPDB reports whether any user-created PodDisruptionBudget in pdbs (see IsWorkloadClassManagedPDB)
// is in the Pod's namespace and selects the Pod. PDBs managed by a WorkloadClass are ignored, so callers can pass
// every PDB in the namespace.
//
// User-created PDBs take precedence: the eviction API rejects evicting a Pod that is covered by more than one PDB,
// so the WorkloadClass controller does not create a PDB for a Pod that a user-created PDB covers, and relies on the
// eviction webhook to enforce the WorkloadClass' disruption policy for it.
//
// Selectors follow policy/v1 semantics: a nil selector selects no Pods, and an empty selector ({}) selects every Pod
// in the namespace. PDBs with an invalid selector are ignored.
func IsPodCoveredByUserPDB(pod *corev1.Pod, pdbs []policyv1.PodDisruptionBudget) bool {
	if pod == nil {
		return false
	}
	podLabels := labels.Set(pod.Labels)
	for i := range pdbs {
		pdb := &pdbs[i]
		if pdb.Namespace != pod.Namespace || pdb.Spec.Selector == nil || IsWorkloadClassManagedPDB(pdb) {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			continue
		}
		if selector.Matches(podLabels) {
			return true
		}
	}
	return false
}
