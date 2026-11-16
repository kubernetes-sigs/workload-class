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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

const (
	testNamespace = "namespace"
	testLabelKeyA = "labelA"
	testLabelValA = "valueA"
	testLabelKeyB = "labelB"
	testLabelValB = "valueB"
)

func TestNamespaceDefaultWorkloadClass(t *testing.T) {
	now := metav1.Now()
	wc := &workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: testNamespace},
		Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{testLabelKeyA: testLabelValA}}},
	}
	wcDeleting := &workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: testNamespace, DeletionTimestamp: &now},
		Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{testLabelKeyA: testLabelValA}}},
	}
	nsDefault := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: testNamespace, Labels: map[string]string{workloadsv1alpha1.EnforcedWorkloadClassLabel: "wc"}},
	}
	nsNoDefault := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: testNamespace, Labels: map[string]string{"something-else": "true"}},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace}}

	testCases := []struct {
		name          string
		getNSError    error
		getNSResult   *corev1.Namespace
		getWCError    error
		getWCResult   *workloadsv1alpha1.WorkloadClass
		wantBestMatch *workloadsv1alpha1.WorkloadClass
		wantEnforced  bool
	}{
		{name: "error_getting_namespace", getNSError: fmt.Errorf("error getting Namespace"), wantEnforced: false},
		{name: "no_default_class_with_namespace", getNSResult: nsNoDefault, wantEnforced: false},
		{name: "error_getting_wc", getNSResult: nsDefault, getWCError: fmt.Errorf("error getting WorkloadClass"), wantEnforced: true},
		{name: "terminating_default_wc_is_skipped", getNSResult: nsDefault, getWCResult: wcDeleting, wantEnforced: true},
		{name: "success_getting_namespace_default", getNSResult: nsDefault, getWCResult: wc, wantBestMatch: wc, wantEnforced: true},
	}

	ctx := t.Context()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v := &DisruptionWebhook{Client: createClient(tc.getNSError, tc.getWCError, nil, nil, tc.getNSResult, tc.getWCResult, nil, nil)}
			gotWC, gotEnforced := v.namespaceDefaultWorkloadClass(ctx, pod)
			if gotEnforced != tc.wantEnforced {
				t.Errorf("namespaceDefaultWorkloadClass() enforced = %v, want %v", gotEnforced, tc.wantEnforced)
			}
			if (gotWC != nil) != (tc.wantBestMatch != nil) {
				t.Fatalf("namespaceDefaultWorkloadClass() = %v, want %v", gotWC, tc.wantBestMatch)
			}
			if gotWC != nil && gotWC.Name != tc.wantBestMatch.Name {
				t.Errorf("namespaceDefaultWorkloadClass() = %v, want %v", gotWC, tc.wantBestMatch)
			}
		})
	}
}

func TestBestMatchWorkloadClass(t *testing.T) {
	now := metav1.Now()
	wc := workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: testNamespace},
		Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{testLabelKeyA: testLabelValA}}},
	}
	wcDeleting := workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc-deleting", Namespace: testNamespace, DeletionTimestamp: &now},
		Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{testLabelKeyA: testLabelValA, testLabelKeyB: testLabelValB}}},
	}
	wcSelectorOverlap := workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc-overlap", Namespace: testNamespace},
		Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{testLabelKeyA: testLabelValA}}},
		Status: workloadsv1alpha1.WorkloadClassStatus{
			Conditions: []metav1.Condition{
				{
					Type:   workloadsv1alpha1.ConditionTypeValidated,
					Status: metav1.ConditionFalse,
					Reason: workloadsv1alpha1.ReasonSelectorOverlap,
				},
			},
		},
	}
	wcNilSelector := workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc-nil-selector", Namespace: testNamespace},
		Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: nil},
	}
	wcDefault := workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc-default", Namespace: testNamespace},
		Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{testLabelKeyB: testLabelValB}}},
	}
	nsNoDefault := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace, Labels: map[string]string{"something-else": "true"}}}
	nsWithDefault := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace, Labels: map[string]string{workloadsv1alpha1.EnforcedWorkloadClassLabel: "wc-default"}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Labels: map[string]string{testLabelKeyA: testLabelValA, testLabelKeyB: testLabelValB}}}

	testCases := []struct {
		name          string
		listWCError   error
		listWCResult  *workloadsv1alpha1.WorkloadClassList
		getNSError    error
		getNSResult   *corev1.Namespace
		getWCError    error
		getWCResult   *workloadsv1alpha1.WorkloadClass
		wantBestMatch *workloadsv1alpha1.WorkloadClass
		wantErr       bool
	}{
		{
			name: "error_listing_wcs", listWCError: fmt.Errorf("error listing WorkloadClasses"),
			listWCResult: &workloadsv1alpha1.WorkloadClassList{}, getNSResult: nsNoDefault, wantErr: true,
		},
		{
			name:         "success_getting_namespace_default",
			listWCResult: &workloadsv1alpha1.WorkloadClassList{Items: []workloadsv1alpha1.WorkloadClass{wc, wcDefault}},
			getNSResult:  nsWithDefault, getWCResult: &wcDefault, wantBestMatch: &wcDefault,
		},
		{
			name:         "enforced_class_missing_overrides_pod_selector_matching",
			listWCResult: &workloadsv1alpha1.WorkloadClassList{Items: []workloadsv1alpha1.WorkloadClass{wc}},
			getNSResult:  nsWithDefault, getWCError: fmt.Errorf("not found"), wantBestMatch: nil,
		},
		{
			name:         "success_no_namespace_default_skips_terminating_wc",
			listWCResult: &workloadsv1alpha1.WorkloadClassList{Items: []workloadsv1alpha1.WorkloadClass{wcDeleting, wc}},
			getNSResult:  nsNoDefault, wantBestMatch: &wc,
		},
		{
			name:         "skips_wc_with_selector_overlap_condition",
			listWCResult: &workloadsv1alpha1.WorkloadClassList{Items: []workloadsv1alpha1.WorkloadClass{wcSelectorOverlap}},
			getNSResult:  nsNoDefault, wantBestMatch: nil,
		},
		{
			name:         "nil_pod_selector_matches_nothing",
			listWCResult: &workloadsv1alpha1.WorkloadClassList{Items: []workloadsv1alpha1.WorkloadClass{wcNilSelector}},
			getNSResult:  nsNoDefault, wantBestMatch: nil,
		},
	}

	ctx := t.Context()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v := &DisruptionWebhook{Client: createClient(tc.getNSError, tc.getWCError, tc.listWCError, nil, tc.getNSResult, tc.getWCResult, tc.listWCResult, nil)}
			gotBestMatch, err := v.bestMatchWorkloadClass(ctx, pod)
			if (err != nil) != tc.wantErr {
				t.Fatalf("bestMatchWorkloadClass() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if (gotBestMatch != nil) != (tc.wantBestMatch != nil) {
				t.Fatalf("bestMatchWorkloadClass() = %v, want %v", gotBestMatch, tc.wantBestMatch)
			}
			if gotBestMatch != nil && gotBestMatch.Name != tc.wantBestMatch.Name {
				t.Errorf("bestMatchWorkloadClass() = %v, want %v", gotBestMatch, tc.wantBestMatch)
			}
		})
	}
}

func TestInjectDecoder(t *testing.T) {
	v := &DisruptionWebhook{}
	if err := v.InjectDecoder(nil); err != nil {
		t.Errorf("InjectDecoder() unexpected error: %v", err)
	}
}

type fakeClient struct {
	client.Client
	getPodError            error
	getPodResult           *corev1.Pod
	listWCError            error
	listWCResult           *workloadsv1alpha1.WorkloadClassList
	getNamespaceError      error
	getNamespaceResult     *corev1.Namespace
	getWorkloadClassError  error
	getWorkloadClassResult *workloadsv1alpha1.WorkloadClass
}

func createClient(getNSErr, getWCErr, listWCErr, getPodErr error, getNSResult *corev1.Namespace, getWCResult *workloadsv1alpha1.WorkloadClass, listWCResult *workloadsv1alpha1.WorkloadClassList, getPodResult *corev1.Pod) fakeClient {
	return fakeClient{
		getNamespaceError:      getNSErr,
		getNamespaceResult:     getNSResult,
		getWorkloadClassError:  getWCErr,
		getPodError:            getPodErr,
		getWorkloadClassResult: getWCResult,
		listWCError:            listWCErr,
		listWCResult:           listWCResult,
		getPodResult:           getPodResult,
	}
}

func (fc fakeClient) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	fc.listWCResult.DeepCopyInto(list.(*workloadsv1alpha1.WorkloadClassList))
	return fc.listWCError
}

func (fc fakeClient) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	switch typedObj := obj.(type) {
	case *corev1.Namespace:
		if fc.getNamespaceError != nil {
			return fc.getNamespaceError
		}
		fc.getNamespaceResult.DeepCopyInto(typedObj)
	case *workloadsv1alpha1.WorkloadClass:
		if fc.getWorkloadClassError != nil {
			return fc.getWorkloadClassError
		}
		fc.getWorkloadClassResult.DeepCopyInto(typedObj)
	case *corev1.Pod:
		if fc.getPodError != nil {
			return fc.getPodError
		}
		fc.getPodResult.DeepCopyInto(typedObj)
	default:
		return fmt.Errorf("unknown object type")
	}
	return nil
}
