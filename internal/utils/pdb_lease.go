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
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

const (
	BypassPod            = "workloads.x-k8s.io/bypass-pod"
	BypassExpiration     = "workloads.x-k8s.io/bypass-expiration"
	ExpirationFormat     = "2006-01-02 15:04:05.999999999 -0700 MST"
	DefaultLeaseDuration = 5 * time.Second
)

// AllowLease evaluates the PDB and determines whether or not to allow the disruption webhook to start a new lease.
// A new lease is allowed if any of the following are true:
//   - There is no PDB
//   - There is no ongoing lease (including leases for other Pods)
//   - There is an ongoing lease but it is invalid: an annotation is missing/empty OR the expiration time cannot be parsed
//   - The lease has expired
func AllowLease(pdb *policyv1.PodDisruptionBudget) bool {
	// If the PDB is nil or there are no annotations, there cannot be an ongoing lease
	if pdb == nil || len(pdb.Annotations) == 0 {
		return true
	}

	bypassPod := pdb.Annotations[BypassPod]
	expirationString := pdb.Annotations[BypassExpiration]

	// If an annotation is missing or empty, there is either no lease or an ongoing lease that is invalid
	if bypassPod == "" || expirationString == "" {
		return true
	}

	expirationTime, err := time.Parse(ExpirationFormat, expirationString)
	if err != nil {
		// If the time cannot be parsed, the lease is invalid
		return true
	}

	return time.Now().Compare(expirationTime) >= 0
}

// PDBWithLease configures a PDB with annotations to represent a temporary lease and sets MaxUnavailable to 100%.
//
// Lease protocol:
//   - Setting the lease: When DisruptionWebhook handles an eviction for a NotReady WorkloadClass from a caller
//     matching allowedDisruptionsOutsideOfWindow, it checks AllowLease(pdb) (or verifies that an active lease
//     already targets pod.Name) and updates the PDB via PDBWithLease. Because the disruption controller must
//     observe the new PDB generation and update status.disruptionsAllowed before the eviction API allows the
//     eviction through, the initial eviction request may receive 429 TooManyRequests and succeed on retry within
//     the lease window.
//   - Keeping other Pods out while active: Every pods/eviction request passes through DisruptionWebhook before
//     the eviction API evaluates the PDB. While MaintenanceReadiness is NotReady and a lease is active, the
//     webhook denies evictions for any Pod other than pdb.Annotations[BypassPod].
//   - Clearing the lease: WorkloadClassReconciler watches owned PDBs; while a lease is active it requeues at the
//     remaining lease duration, and once the lease expires (or the leased Pod terminates) it clears the bypass
//     annotations and restores Spec.MaxUnavailable via SyncPDBWithWorkloadClass.
func PDBWithLease(pdb *policyv1.PodDisruptionBudget, wc *workloadsv1alpha1.WorkloadClass, pod *corev1.Pod, enforced ...bool) error {
	if wc == nil {
		return fmt.Errorf("failed to update PDB with lease, WorkloadClass is nil")
	}

	if pdb == nil {
		return fmt.Errorf("failed to update PDB with lease, PDB is nil")
	}

	if pod == nil {
		return fmt.Errorf("failed to update PDB with lease, Pod is nil")
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

	// AlwaysAllow allows evicting unhealthy (unready) Pods even when maxUnavailable is 0.
	policy := policyv1.AlwaysAllow
	pdb.Spec.UnhealthyPodEvictionPolicy = &policy

	// Disable the PDB - set maxUnavailable to 100%
	open := intstr.FromString(openValue)
	pdb.Spec.MaxUnavailable = &open

	// Add lease annotations
	if pdb.Annotations == nil {
		pdb.Annotations = map[string]string{}
	}

	pdb.Annotations[BypassPod] = pod.Name
	pdb.Annotations[BypassExpiration] = time.Now().Add(DefaultLeaseDuration).Format(ExpirationFormat)

	return nil
}
