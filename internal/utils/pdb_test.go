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
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

const (
	testWCName    = "test-wc"
	testNamespace = "default"
	testPodName   = "web-7f9c4d-abcde"
	testPodUID    = types.UID("3f0c8a52-1d7e-4b8e-9c2a-5e6f7a8b9c0d")
)

func TestPDBName(t *testing.T) {
	// Pod names are DNS-1123 subdomains of up to 253 characters, so the prefixed name can exceed the limit.
	maxShortPodName := strings.Repeat("a", MaxPDBNameLength-len(PDBNamePrefix))
	longPodName := strings.Repeat("b", 253)
	// Truncating this name at the cut point leaves trailing separators, which must be trimmed.
	separatorPodName := strings.Repeat("c", MaxPDBNameLength-len(PDBNamePrefix)-12) + "-.-.-.-.-.-." + strings.Repeat("d", 20)

	tests := []struct {
		name     string
		podName  string
		want     string
		wantHash bool
	}{
		{name: "short_name_is_prefixed", podName: "my-pod", want: "workload-my-pod"},
		{name: "name_at_limit_is_not_truncated", podName: maxShortPodName, want: PDBNamePrefix + maxShortPodName},
		{name: "long_name_is_truncated_with_hash", podName: longPodName, wantHash: true},
		{name: "truncation_trims_trailing_separators", podName: separatorPodName, wantHash: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PDBName(tt.podName)
			if errs := validation.IsDNS1123Subdomain(got); len(errs) > 0 {
				t.Errorf("PDBName(%q) = %q is not a valid DNS-1123 subdomain: %v", tt.podName, got, errs)
			}
			if !tt.wantHash {
				if got != tt.want {
					t.Errorf("PDBName(%q) = %q, want %q", tt.podName, got, tt.want)
				}
				return
			}
			if !strings.HasPrefix(got, PDBNamePrefix) {
				t.Errorf("PDBName(%q) = %q, want prefix %q", tt.podName, got, PDBNamePrefix)
			}
			if got != PDBName(tt.podName) {
				t.Errorf("PDBName(%q) is not deterministic", tt.podName)
			}
			if strings.Contains(got, ".-") || strings.Contains(got, "--") {
				t.Errorf("PDBName(%q) = %q, want trailing separators trimmed before the hash", tt.podName, got)
			}
		})
	}

	// Two long Pod names that share the truncated prefix must not map to the same PDB.
	a := strings.Repeat("e", 252) + "1"
	b := strings.Repeat("e", 252) + "2"
	if PDBName(a) == PDBName(b) {
		t.Errorf("PDBName() returned the same name %q for different Pods", PDBName(a))
	}
}

func TestPodNameFromPDBName(t *testing.T) {
	if got, want := PodNameFromPDBName(PDBName(testPodName)), testPodName; got != want {
		t.Errorf("PodNameFromPDBName() = %q, want %q", got, want)
	}
	if got, want := PodNameFromPDBName("user-pdb"), "user-pdb"; got != want {
		t.Errorf("PodNameFromPDBName() = %q, want %q", got, want)
	}
}

func TestPDBBase(t *testing.T) {
	if got := PDBBase(nil); got != nil {
		t.Errorf("PDBBase(nil) = %v, want nil", got)
	}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: testPodName, Namespace: "test-ns", UID: testPodUID}}
	pdb := PDBBase(pod)
	if pdb == nil || pdb.Name != PDBName(testPodName) || pdb.Namespace != "test-ns" {
		t.Fatalf("PDBBase() = %v, want test-ns/%s", pdb, PDBName(testPodName))
	}
	if got := pdb.Labels[workloadsv1alpha1.PodUIDLabel]; got != string(testPodUID) {
		t.Errorf("PDBBase() label %s = %q, want %q", workloadsv1alpha1.PodUIDLabel, got, testPodUID)
	}
}

func TestSyncPDBWithWorkloadClass(t *testing.T) {
	alwaysAllow := policyv1.AlwaysAllow
	open := intstr.FromString(openValue)
	closed := intstr.FromInt32(closedValue)
	wantSelector := &metav1.LabelSelector{MatchLabels: map[string]string{workloadsv1alpha1.PodUIDLabel: string(testPodUID)}}

	newWC := func(readiness workloadsv1alpha1.MaintenanceReadiness) *workloadsv1alpha1.WorkloadClass {
		return &workloadsv1alpha1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: testWCName, Namespace: testNamespace},
			Spec: workloadsv1alpha1.WorkloadClassSpec{
				PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			},
			Status: workloadsv1alpha1.WorkloadClassStatus{MaintenanceReadiness: readiness},
		}
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: testPodName, Namespace: testNamespace, UID: testPodUID}}

	tests := []struct {
		name               string
		wc                 *workloadsv1alpha1.WorkloadClass
		pod                *corev1.Pod
		pdb                *policyv1.PodDisruptionBudget
		protects           bool
		wantErr            bool
		wantName           string
		wantLabels         map[string]string
		wantMaxUnavailable *intstr.IntOrString
	}{
		{
			name:    "workload_class_is_nil",
			pod:     pod,
			pdb:     &policyv1.PodDisruptionBudget{},
			wantErr: true,
		},
		{
			name:    "pod_is_nil",
			wc:      newWC(workloadsv1alpha1.ReadinessReady),
			pdb:     &policyv1.PodDisruptionBudget{},
			wantErr: true,
		},
		{
			name:    "pdb_is_nil",
			wc:      newWC(workloadsv1alpha1.ReadinessReady),
			pod:     pod,
			wantErr: true,
		},
		{
			name:               "ready_opens_and_populates_empty_metadata",
			wc:                 newWC(workloadsv1alpha1.ReadinessReady),
			pod:                pod,
			pdb:                &policyv1.PodDisruptionBudget{},
			protects:           true,
			wantName:           PDBName(testPodName),
			wantLabels:         map[string]string{workloadsv1alpha1.PodUIDLabel: string(testPodUID)},
			wantMaxUnavailable: &open,
		},
		{
			name:               "overdue_opens",
			wc:                 newWC(workloadsv1alpha1.ReadinessOverdue),
			pod:                pod,
			pdb:                PDBBase(pod),
			protects:           true,
			wantName:           PDBName(testPodName),
			wantLabels:         map[string]string{workloadsv1alpha1.PodUIDLabel: string(testPodUID)},
			wantMaxUnavailable: &open,
		},
		{
			name:               "not_ready_closes",
			wc:                 newWC(workloadsv1alpha1.ReadinessNotReady),
			pod:                pod,
			pdb:                PDBBase(pod),
			protects:           true,
			wantName:           PDBName(testPodName),
			wantLabels:         map[string]string{workloadsv1alpha1.PodUIDLabel: string(testPodUID)},
			wantMaxUnavailable: &closed,
		},
		{
			name:               "unset_readiness_fails_closed",
			wc:                 newWC(""),
			pod:                pod,
			pdb:                PDBBase(pod),
			protects:           true,
			wantName:           PDBName(testPodName),
			wantLabels:         map[string]string{workloadsv1alpha1.PodUIDLabel: string(testPodUID)},
			wantMaxUnavailable: &closed,
		},
		{
			name:               "unrecognized_readiness_fails_closed",
			wc:                 newWC("Unknown"),
			pod:                pod,
			pdb:                PDBBase(pod),
			protects:           true,
			wantName:           PDBName(testPodName),
			wantLabels:         map[string]string{workloadsv1alpha1.PodUIDLabel: string(testPodUID)},
			wantMaxUnavailable: &closed,
		},
		{
			name:               "not_protecting_opens_even_when_not_ready",
			wc:                 newWC(workloadsv1alpha1.ReadinessNotReady),
			pod:                pod,
			pdb:                PDBBase(pod),
			protects:           false,
			wantName:           PDBName(testPodName),
			wantLabels:         map[string]string{workloadsv1alpha1.PodUIDLabel: string(testPodUID)},
			wantMaxUnavailable: &open,
		},
		{
			name: "preserves_existing_metadata_and_replaces_stale_pod_uid",
			wc:   newWC(workloadsv1alpha1.ReadinessNotReady),
			pod:  pod,
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:      PDBName(testPodName),
					Namespace: testNamespace,
					Labels:    map[string]string{"team": "a", workloadsv1alpha1.PodUIDLabel: "uid-of-an-older-pod"},
				},
				Spec: policyv1.PodDisruptionBudgetSpec{
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{workloadsv1alpha1.PodUIDLabel: "uid-of-an-older-pod"}},
				},
			},
			protects:           true,
			wantName:           PDBName(testPodName),
			wantLabels:         map[string]string{"team": "a", workloadsv1alpha1.PodUIDLabel: string(testPodUID)},
			wantMaxUnavailable: &closed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SyncPDBWithWorkloadClass(tt.wc, tt.pod, tt.pdb, tt.protects)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SyncPDBWithWorkloadClass() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if tt.pdb.Name != tt.wantName || tt.pdb.Namespace != testNamespace {
				t.Errorf("SyncPDBWithWorkloadClass() metadata = %s/%s, want %s/%s", tt.pdb.Namespace, tt.pdb.Name, testNamespace, tt.wantName)
			}
			if !reflect.DeepEqual(tt.pdb.Labels, tt.wantLabels) {
				t.Errorf("SyncPDBWithWorkloadClass() Labels = %v, want %v", tt.pdb.Labels, tt.wantLabels)
			}
			if !reflect.DeepEqual(tt.pdb.Spec.Selector, wantSelector) {
				t.Errorf("SyncPDBWithWorkloadClass() Selector = %v, want %v", tt.pdb.Spec.Selector, wantSelector)
			}
			if !reflect.DeepEqual(tt.pdb.Spec.UnhealthyPodEvictionPolicy, &alwaysAllow) {
				t.Errorf("SyncPDBWithWorkloadClass() UnhealthyPodEvictionPolicy = %v, want %v", tt.pdb.Spec.UnhealthyPodEvictionPolicy, alwaysAllow)
			}
			if !reflect.DeepEqual(tt.pdb.Spec.MaxUnavailable, tt.wantMaxUnavailable) {
				t.Errorf("SyncPDBWithWorkloadClass() MaxUnavailable = %v, want %v", tt.pdb.Spec.MaxUnavailable, tt.wantMaxUnavailable)
			}
		})
	}
}
