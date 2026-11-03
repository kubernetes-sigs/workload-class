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
	"maps"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

const (
	testNamespace = "namespace"
	testLabelKeyA = "labelA"
	testLabelValA = "valueA"
	testLabelKeyB = "labelB"
	testLabelValB = "valueB"
)

func TestGetSpecificity(t *testing.T) {
	t.Run("nil_selector", func(t *testing.T) {
		if got := getSpecificity(nil); got != 0 {
			t.Errorf("getSpecificity(nil) = %d, want 0", got)
		}
	})

	exprs := []metav1.LabelSelectorRequirement{
		{Key: "k1", Operator: "op1", Values: []string{"v1"}},
		{Key: "k2", Operator: "op2", Values: []string{"v2"}},
	}
	testCases := []struct {
		name             string
		matchLabels      map[string]string
		matchExpressions []metav1.LabelSelectorRequirement
		want             int
	}{
		{name: "empty_selector", want: 0},
		{name: "only_match_expressions", matchExpressions: exprs, want: 2},
		{name: "only_match_labels", matchLabels: map[string]string{"k1": "v1", "k2": "v2"}, want: 2},
		{
			name:             "both_not_nil",
			matchLabels:      map[string]string{"k1": "v1", "k2": "v2"},
			matchExpressions: append(exprs, metav1.LabelSelectorRequirement{Key: "k3", Operator: "op3", Values: []string{"v3"}}),
			want:             5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			selector := &metav1.LabelSelector{MatchLabels: tc.matchLabels, MatchExpressions: tc.matchExpressions}
			if got := getSpecificity(selector); got != tc.want {
				t.Errorf("getSpecificity() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestUpdateBestMatch(t *testing.T) {
	latest := time.Now()
	oldest := latest.AddDate(0, 0, -1)
	newMatchWC := func(name string, created time.Time, matchLabels map[string]string) *workloadsv1alpha1.WorkloadClass {
		return &workloadsv1alpha1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace, CreationTimestamp: metav1.Time{Time: created}},
			Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: &metav1.LabelSelector{MatchLabels: matchLabels}},
		}
	}
	wcEmpty := newMatchWC("wc-empty", latest, nil)
	wc1 := newMatchWC("wc1", latest, map[string]string{testLabelKeyA: testLabelValA})
	wc2 := newMatchWC("wc2", latest, map[string]string{testLabelKeyA: testLabelValA, testLabelKeyB: testLabelValB})
	wc2A := newMatchWC("wc2-a", latest, map[string]string{testLabelKeyA: testLabelValA, testLabelKeyB: testLabelValB})
	wc22 := newMatchWC("wc22", oldest, map[string]string{testLabelKeyA: testLabelValA, testLabelKeyB: testLabelValB})

	testCases := []struct {
		name             string
		wc               *workloadsv1alpha1.WorkloadClass
		bm               *workloadsv1alpha1.WorkloadClass
		maxSpec          int
		wantBM           *workloadsv1alpha1.WorkloadClass
		wantMaxSpec      int
		wantOtherMatches map[string]int
	}{
		{
			name: "nil_best_match_update_to_new_best_match", wc: wc1, maxSpec: -1,
			wantBM: wc1, wantMaxSpec: 1, wantOtherMatches: map[string]int{},
		},
		{
			name: "nil_best_match_with_zero_specificity_does_not_panic", wc: wcEmpty, maxSpec: 0,
			wantBM: wcEmpty, wantMaxSpec: 0, wantOtherMatches: map[string]int{},
		},
		{
			name: "higher_specificity_updates_best_match", wc: wc2, bm: wc1, maxSpec: 1,
			wantBM: wc2, wantMaxSpec: 2, wantOtherMatches: map[string]int{"namespace/wc1": 1},
		},
		{
			name: "equal_specificity_older_wc_updates_best_match", wc: wc22, bm: wc2, maxSpec: 2,
			wantBM: wc22, wantMaxSpec: 2, wantOtherMatches: map[string]int{"namespace/wc2": 2},
		},
		{
			name: "equal_specificity_newer_wc_no_update", wc: wc2, bm: wc22, maxSpec: 2,
			wantBM: wc22, wantMaxSpec: 2, wantOtherMatches: map[string]int{"namespace/wc2": 2},
		},
		{
			name: "equal_specificity_and_timestamp_smaller_name_updates_best_match", wc: wc2, bm: wc2A, maxSpec: 2,
			wantBM: wc2, wantMaxSpec: 2, wantOtherMatches: map[string]int{"namespace/wc2-a": 2},
		},
		{
			name: "equal_specificity_and_timestamp_larger_name_no_update", wc: wc2A, bm: wc2, maxSpec: 2,
			wantBM: wc2, wantMaxSpec: 2, wantOtherMatches: map[string]int{"namespace/wc2-a": 2},
		},
		{
			name: "lower_specificity_no_update", wc: wc1, bm: wc2, maxSpec: 2,
			wantBM: wc2, wantMaxSpec: 2, wantOtherMatches: map[string]int{"namespace/wc1": 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			otherMatches := map[string]int{}
			gotBM, gotOtherMatches := updateBestMatch(tc.wc, tc.bm, &tc.maxSpec, otherMatches)
			if gotBM.Name != tc.wantBM.Name {
				t.Errorf("updateBestMatch() bestMatch = %v, want %v", gotBM, tc.wantBM)
			}
			if tc.maxSpec != tc.wantMaxSpec {
				t.Errorf("updateBestMatch() maxSpecificity = %d, want %d", tc.maxSpec, tc.wantMaxSpec)
			}
			if !maps.Equal(gotOtherMatches, tc.wantOtherMatches) {
				t.Errorf("updateBestMatch() otherMatches = %v, want %v", gotOtherMatches, tc.wantOtherMatches)
			}
		})
	}
}

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
	}

	ctx := t.Context()
	req := admission.Request{}
	req.Name = "name"
	req.Namespace = testNamespace
	req.UserInfo.Username = "test-user"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v := &DisruptionWebhook{Client: createClient(tc.getNSError, tc.getWCError, tc.listWCError, nil, tc.getNSResult, tc.getWCResult, tc.listWCResult, nil)}
			gotBestMatch, err := v.bestMatchWorkloadClass(ctx, req, pod)
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

func TestBestMatchWorkloadClass_EmitEvent(t *testing.T) {
	now := time.Now()
	wc1 := workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc1", Namespace: testNamespace, CreationTimestamp: metav1.Time{Time: now.Add(-10 * time.Minute)}},
		Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{testLabelKeyA: testLabelValA}}},
	}
	wc2 := workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc2", Namespace: testNamespace, CreationTimestamp: metav1.Time{Time: now}},
		Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{testLabelKeyA: testLabelValA}}},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "my-pod", Namespace: testNamespace, Labels: map[string]string{testLabelKeyA: testLabelValA}}}
	listWCResult := &workloadsv1alpha1.WorkloadClassList{Items: []workloadsv1alpha1.WorkloadClass{wc1, wc2}}
	nsNoDefault := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace, Labels: map[string]string{"something-else": "true"}}}

	req := admission.Request{}
	req.Name = pod.Name
	req.Namespace = testNamespace
	req.UserInfo.Username = "test-user"

	fakeRecorder := events.NewFakeRecorder(10)
	v := &DisruptionWebhook{
		Client:   createClient(nil, nil, nil, nil, nsNoDefault, &wc1, listWCResult, nil),
		Recorder: fakeRecorder,
	}

	gotBestMatch, err := v.bestMatchWorkloadClass(t.Context(), req, pod)
	if err != nil {
		t.Fatalf("bestMatchWorkloadClass returned unexpected error: %v", err)
	}
	if gotBestMatch.Name != "wc1" {
		t.Errorf("Expected wc1 to be best match, got: %s", gotBestMatch.Name)
	}
	select {
	case event := <-fakeRecorder.Events:
		expected := "Warning AmbiguousMatch the following WorkloadClasses match pods with the same specificity as the best match, but were not selected: namespace/wc2"
		if event != expected {
			t.Errorf("Expected event: %q, got: %q", expected, event)
		}
	default:
		t.Error("Expected AmbiguousMatch event to be emitted, but none was found")
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
