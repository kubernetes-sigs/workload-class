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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

const testPodName = "test-pod"

func TestAllowLease(t *testing.T) {
	pdbWithAnnotations := func(annotations map[string]string) *policyv1.PodDisruptionBudget {
		return &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Annotations: annotations}}
	}
	futureExp := time.Now().Add(time.Hour).Format(ExpirationFormat)
	pastExp := time.Now().Add(-time.Hour).Format(ExpirationFormat)

	tests := []struct {
		name string
		pdb  *policyv1.PodDisruptionBudget
		want bool
	}{
		{name: "pdb_is_nil", pdb: nil, want: true},
		{name: "no_annotations", pdb: pdbWithAnnotations(nil), want: true},
		{name: "missing_bypass_annotation", pdb: pdbWithAnnotations(map[string]string{BypassExpiration: futureExp}), want: true},
		{name: "empty_bypass_annotation", pdb: pdbWithAnnotations(map[string]string{BypassPod: "", BypassExpiration: futureExp}), want: true},
		{name: "missing_expiration_annotation", pdb: pdbWithAnnotations(map[string]string{BypassPod: testPodName}), want: true},
		{name: "empty_expiration_annotation", pdb: pdbWithAnnotations(map[string]string{BypassPod: testPodName, BypassExpiration: ""}), want: true},
		{name: "invalid_expiration_format", pdb: pdbWithAnnotations(map[string]string{BypassPod: testPodName, BypassExpiration: "invalid"}), want: true},
		{name: "expired_lease", pdb: pdbWithAnnotations(map[string]string{BypassPod: testPodName, BypassExpiration: pastExp}), want: true},
		{name: "valid_active_lease", pdb: pdbWithAnnotations(map[string]string{BypassPod: testPodName, BypassExpiration: futureExp}), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AllowLease(tt.pdb); got != tt.want {
				t.Errorf("AllowLease() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPDBWithLease(t *testing.T) {
	testSelector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}}
	emptySelector := &metav1.LabelSelector{}
	baseWC := &workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: testWCName, Namespace: testNamespace},
		Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: testSelector},
	}
	basePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: testPodName}}

	tests := []struct {
		name         string
		wc           *workloadsv1alpha1.WorkloadClass
		pdb          *policyv1.PodDisruptionBudget
		pod          *corev1.Pod
		enforced     []bool
		wantSelector *metav1.LabelSelector
		wantExisting bool
		wantErr      bool
	}{
		{name: "workload_class_is_nil", wc: nil, pdb: &policyv1.PodDisruptionBudget{}, pod: basePod, wantErr: true},
		{name: "pdb_is_nil", wc: baseWC, pdb: nil, pod: basePod, wantErr: true},
		{name: "pod_is_nil", wc: baseWC, pdb: &policyv1.PodDisruptionBudget{}, pod: nil, wantErr: true},
		{
			name:         "populates_empty_metadata_and_initializes_annotations",
			wc:           baseWC,
			pdb:          &policyv1.PodDisruptionBudget{},
			pod:          basePod,
			wantSelector: testSelector,
		},
		{
			name: "preserves_existing_metadata_and_annotations",
			wc:   baseWC,
			pdb: &policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:        PDBName(testWCName),
					Namespace:   testNamespace,
					Annotations: map[string]string{"existing": "annotation"},
				},
			},
			pod:          basePod,
			wantSelector: testSelector,
			wantExisting: true,
		},
		{
			name:         "enforced_class_selects_all_pods_in_namespace",
			wc:           baseWC,
			pdb:          &policyv1.PodDisruptionBudget{},
			pod:          basePod,
			enforced:     []bool{true},
			wantSelector: emptySelector,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := PDBWithLease(tt.pdb, tt.wc, tt.pod, tt.enforced...)
			if (err != nil) != tt.wantErr {
				t.Fatalf("PDBWithLease() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if tt.pdb.Name != PDBName(testWCName) || tt.pdb.Namespace != testNamespace {
					t.Errorf("PDBWithLease() metadata = %s/%s, want %s/%s", tt.pdb.Namespace, tt.pdb.Name, testNamespace, PDBName(testWCName))
				}
				if !reflect.DeepEqual(tt.pdb.Spec.Selector, tt.wantSelector) {
					t.Errorf("PDBWithLease() Selector = %v, want %v", tt.pdb.Spec.Selector, tt.wantSelector)
				}
				wantPolicy := policyv1.AlwaysAllow
				if tt.pdb.Spec.UnhealthyPodEvictionPolicy == nil || *tt.pdb.Spec.UnhealthyPodEvictionPolicy != wantPolicy {
					t.Errorf("PDBWithLease() UnhealthyPodEvictionPolicy = %v, want %v", tt.pdb.Spec.UnhealthyPodEvictionPolicy, wantPolicy)
				}
				wantMaxUnavailable := intstr.FromString(openValue)
				if !reflect.DeepEqual(tt.pdb.Spec.MaxUnavailable, &wantMaxUnavailable) {
					t.Errorf("PDBWithLease() MaxUnavailable = %v, want %v", tt.pdb.Spec.MaxUnavailable, &wantMaxUnavailable)
				}
				if tt.pdb.Annotations[BypassPod] != tt.pod.Name {
					t.Errorf("PDBWithLease() BypassPod annotation = %v, want %v", tt.pdb.Annotations[BypassPod], tt.pod.Name)
				}
				if _, err := time.Parse(ExpirationFormat, tt.pdb.Annotations[BypassExpiration]); err != nil {
					t.Errorf("PDBWithLease() BypassExpiration annotation invalid: %v", err)
				}
				if AllowLease(tt.pdb) {
					t.Errorf("AllowLease() = true after PDBWithLease(), want false for active lease")
				}
				if tt.wantExisting && tt.pdb.Annotations["existing"] != "annotation" {
					t.Errorf("PDBWithLease() failed to preserve existing annotations")
				}
			}
		})
	}
}
