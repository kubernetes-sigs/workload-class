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
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

const (
	matchNamespace = "match-ns"
	matchKeyA      = "labelA"
	matchValA      = "valueA"
	matchKeyB      = "labelB"
	matchValB      = "valueB"
)

func newMatchWC(name string, created time.Time, selector *metav1.LabelSelector) *workloadsv1alpha1.WorkloadClass {
	return &workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: matchNamespace, CreationTimestamp: metav1.Time{Time: created}},
		Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: selector},
	}
}

func matchLabels(kv ...string) *metav1.LabelSelector {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return &metav1.LabelSelector{MatchLabels: m}
}

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
	wcEmpty := newMatchWC("wc-empty", latest, &metav1.LabelSelector{})
	wc1 := newMatchWC("wc1", latest, matchLabels(matchKeyA, matchValA))
	wc2 := newMatchWC("wc2", latest, matchLabels(matchKeyA, matchValA, matchKeyB, matchValB))
	wc2A := newMatchWC("wc2-a", latest, matchLabels(matchKeyA, matchValA, matchKeyB, matchValB))
	wc22 := newMatchWC("wc22", oldest, matchLabels(matchKeyA, matchValA, matchKeyB, matchValB))
	key := func(name string) string { return matchNamespace + "/" + name }

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
			wantBM: wc2, wantMaxSpec: 2, wantOtherMatches: map[string]int{key("wc1"): 1},
		},
		{
			name: "equal_specificity_older_wc_updates_best_match", wc: wc22, bm: wc2, maxSpec: 2,
			wantBM: wc22, wantMaxSpec: 2, wantOtherMatches: map[string]int{key("wc2"): 2},
		},
		{
			name: "equal_specificity_newer_wc_no_update", wc: wc2, bm: wc22, maxSpec: 2,
			wantBM: wc22, wantMaxSpec: 2, wantOtherMatches: map[string]int{key("wc2"): 2},
		},
		{
			name: "equal_specificity_and_timestamp_smaller_name_updates_best_match", wc: wc2, bm: wc2A, maxSpec: 2,
			wantBM: wc2, wantMaxSpec: 2, wantOtherMatches: map[string]int{key("wc2-a"): 2},
		},
		{
			name: "equal_specificity_and_timestamp_larger_name_no_update", wc: wc2A, bm: wc2, maxSpec: 2,
			wantBM: wc2, wantMaxSpec: 2, wantOtherMatches: map[string]int{key("wc2-a"): 2},
		},
		{
			name: "lower_specificity_no_update", wc: wc1, bm: wc2, maxSpec: 2,
			wantBM: wc2, wantMaxSpec: 2, wantOtherMatches: map[string]int{key("wc1"): 1},
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

func TestSelectorBestMatch(t *testing.T) {
	now := time.Now()
	deleting := metav1.NewTime(now)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "pod", Namespace: matchNamespace, Labels: map[string]string{matchKeyA: matchValA, matchKeyB: matchValB},
	}}

	broad := newMatchWC("broad", now, matchLabels(matchKeyA, matchValA))
	specific := newMatchWC("specific", now, matchLabels(matchKeyA, matchValA, matchKeyB, matchValB))
	specificTwin := newMatchWC("specific-twin", now, matchLabels(matchKeyA, matchValA, matchKeyB, matchValB))
	everything := newMatchWC("everything", now, &metav1.LabelSelector{})
	nilSelector := newMatchWC("nil-selector", now.Add(-time.Hour), nil)
	nonMatching := newMatchWC("non-matching", now, matchLabels(matchKeyA, "other"))
	invalid := newMatchWC("invalid", now, &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: matchKeyA, Operator: "Bogus"}}})
	terminating := newMatchWC("terminating", now.Add(-time.Hour), matchLabels(matchKeyA, matchValA, matchKeyB, matchValB))
	terminating.DeletionTimestamp = &deleting
	otherNamespace := newMatchWC("other-namespace", now.Add(-time.Hour), matchLabels(matchKeyA, matchValA, matchKeyB, matchValB))
	otherNamespace.Namespace = "other"

	testCases := []struct {
		name        string
		pod         *corev1.Pod
		wcs         []workloadsv1alpha1.WorkloadClass
		wantBM      string
		wantOthers  map[string]int
		wantMaxSpec int
		wantNoMatch bool
	}{
		{name: "nil_pod", pod: nil, wcs: []workloadsv1alpha1.WorkloadClass{*broad}, wantNoMatch: true, wantMaxSpec: -1, wantOthers: map[string]int{}},
		{name: "no_classes", pod: pod, wantNoMatch: true, wantMaxSpec: -1, wantOthers: map[string]int{}},
		{
			name: "most_specific_wins", pod: pod, wcs: []workloadsv1alpha1.WorkloadClass{*everything, *broad, *specific},
			wantBM: "specific", wantMaxSpec: 2,
			wantOthers: map[string]int{matchNamespace + "/everything": 0, matchNamespace + "/broad": 1},
		},
		{
			name: "tie_broken_by_name_regardless_of_order", pod: pod, wcs: []workloadsv1alpha1.WorkloadClass{*specificTwin, *specific},
			wantBM: "specific", wantMaxSpec: 2, wantOthers: map[string]int{matchNamespace + "/specific-twin": 2},
		},
		{
			name: "skips_nil_invalid_non_matching_terminating_and_other_namespace", pod: pod,
			wcs:    []workloadsv1alpha1.WorkloadClass{*nilSelector, *invalid, *nonMatching, *terminating, *otherNamespace, *broad},
			wantBM: "broad", wantMaxSpec: 1, wantOthers: map[string]int{},
		},
		{
			name: "nothing_matches", pod: pod, wcs: []workloadsv1alpha1.WorkloadClass{*nilSelector, *nonMatching},
			wantNoMatch: true, wantMaxSpec: -1, wantOthers: map[string]int{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotBM, gotOthers, gotMaxSpec := SelectorBestMatch(tc.pod, tc.wcs)
			if tc.wantNoMatch {
				if gotBM != nil {
					t.Errorf("SelectorBestMatch() bestMatch = %s, want nil", gotBM.Name)
				}
			} else if gotBM == nil || gotBM.Name != tc.wantBM {
				t.Errorf("SelectorBestMatch() bestMatch = %v, want %s", gotBM, tc.wantBM)
			}
			if gotMaxSpec != tc.wantMaxSpec {
				t.Errorf("SelectorBestMatch() maxSpecificity = %d, want %d", gotMaxSpec, tc.wantMaxSpec)
			}
			if !maps.Equal(gotOthers, tc.wantOthers) {
				t.Errorf("SelectorBestMatch() otherMatches = %v, want %v", gotOthers, tc.wantOthers)
			}
		})
	}
}

func TestBestMatchWorkloadClassForPod(t *testing.T) {
	now := time.Now()
	deleting := metav1.NewTime(now)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: matchNamespace, Labels: map[string]string{matchKeyA: matchValA}}}
	bySelector := newMatchWC("by-selector", now, matchLabels(matchKeyA, matchValA))
	enforcedWC := newMatchWC("enforced", now, nil)
	enforcedDeleting := enforcedWC.DeepCopy()
	enforcedDeleting.DeletionTimestamp = &deleting
	enforcedOtherNamespace := enforcedWC.DeepCopy()
	enforcedOtherNamespace.Namespace = "other"
	wcs := []workloadsv1alpha1.WorkloadClass{*bySelector, *enforcedWC}

	testCases := []struct {
		name       string
		pod        *corev1.Pod
		enforcedWC *workloadsv1alpha1.WorkloadClass
		enforced   bool
		want       string
	}{
		{name: "nil_pod", pod: nil, enforcedWC: enforcedWC, enforced: true, want: ""},
		{name: "not_enforced_uses_selectors", pod: pod, want: "by-selector"},
		{name: "enforced_class_overrides_selectors", pod: pod, enforcedWC: enforcedWC, enforced: true, want: "enforced"},
		{name: "enforced_missing_class_does_not_fall_back", pod: pod, enforced: true, want: ""},
		{name: "enforced_deleting_class_does_not_fall_back", pod: pod, enforcedWC: enforcedDeleting, enforced: true, want: ""},
		{name: "enforced_class_from_other_namespace_is_ignored", pod: pod, enforcedWC: enforcedOtherNamespace, enforced: true, want: ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := BestMatchWorkloadClassForPod(tc.pod, tc.enforcedWC, tc.enforced, wcs)
			gotName := ""
			if got != nil {
				gotName = got.Name
			}
			if gotName != tc.want {
				t.Errorf("BestMatchWorkloadClassForPod() = %q, want %q", gotName, tc.want)
			}
		})
	}
}

func TestIsPodActive(t *testing.T) {
	deleting := metav1.Now()
	testCases := []struct {
		name string
		pod  *corev1.Pod
		want bool
	}{
		{name: "nil_pod", pod: nil, want: false},
		{name: "pending", pod: &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending}}, want: true},
		{name: "running", pod: &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}}, want: true},
		{name: "unset_phase", pod: &corev1.Pod{}, want: true},
		{name: "succeeded", pod: &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}, want: false},
		{name: "failed", pod: &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed}}, want: false},
		{
			name: "terminating",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &deleting},
				Status:     corev1.PodStatus{Phase: corev1.PodRunning},
			},
			want: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsPodActive(tc.pod); got != tc.want {
				t.Errorf("IsPodActive() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsValidated(t *testing.T) {
	validated := func(status metav1.ConditionStatus, reason string) *metav1.Condition {
		return &metav1.Condition{Type: workloadsv1alpha1.ConditionTypeValidated, Status: status, Reason: reason}
	}
	testCases := []struct {
		name string
		cond *metav1.Condition
		want bool
	}{
		{name: "nil_condition", cond: nil, want: false},
		{name: "validation_passed", cond: validated(metav1.ConditionTrue, workloadsv1alpha1.ReasonValidationPassed), want: true},
		{name: "no_guardrails", cond: validated(metav1.ConditionTrue, workloadsv1alpha1.ReasonNoGuardrails), want: true},
		{name: "selector_overlap", cond: validated(metav1.ConditionFalse, workloadsv1alpha1.ReasonSelectorOverlap), want: true},
		{name: "validation_failed", cond: validated(metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed), want: false},
		{name: "unknown_status", cond: validated(metav1.ConditionUnknown, ""), want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsValidated(tc.cond); got != tc.want {
				t.Errorf("IsValidated() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNamespaceEnforcedWorkloadClass(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := workloadsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	namespace := func(enforcedClass string) *corev1.Namespace {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: matchNamespace}}
		if enforcedClass != "" {
			ns.Labels = map[string]string{workloadsv1alpha1.EnforcedWorkloadClassLabel: enforcedClass}
		}
		return ns
	}
	wc := newMatchWC("enforced", time.Now(), nil)
	wcDeleting := newMatchWC("deleting", time.Now(), nil)
	wcDeleting.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	wcDeleting.Finalizers = []string{"test/keep"}
	getError := errors.New("get failed")
	failGet := func(kind client.Object) interceptor.Funcs {
		return interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Namespace); ok && kind == nil {
				return getError
			}
			if _, ok := obj.(*workloadsv1alpha1.WorkloadClass); ok && kind != nil {
				return getError
			}
			return c.Get(ctx, key, obj, opts...)
		}}
	}

	testCases := []struct {
		name         string
		objs         []client.Object
		funcs        *interceptor.Funcs
		wantWC       string
		wantEnforced bool
		wantErr      bool
	}{
		{name: "namespace_not_found", wantEnforced: false},
		{name: "no_label", objs: []client.Object{namespace(""), wc}, wantEnforced: false},
		{name: "enforced_class", objs: []client.Object{namespace("enforced"), wc}, wantWC: "enforced", wantEnforced: true},
		{name: "enforced_class_not_found", objs: []client.Object{namespace("missing"), wc}, wantEnforced: true},
		{name: "enforced_class_deleting", objs: []client.Object{namespace("deleting"), wcDeleting}, wantEnforced: true},
		{name: "namespace_get_error", objs: []client.Object{namespace("enforced"), wc}, funcs: ptrTo(failGet(nil)), wantErr: true},
		{
			name: "workload_class_get_error", objs: []client.Object{namespace("enforced"), wc},
			funcs: ptrTo(failGet(&workloadsv1alpha1.WorkloadClass{})), wantEnforced: true, wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objs...)
			if tc.funcs != nil {
				builder = builder.WithInterceptorFuncs(*tc.funcs)
			}
			gotWC, gotEnforced, err := NamespaceEnforcedWorkloadClass(context.Background(), builder.Build(), matchNamespace)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NamespaceEnforcedWorkloadClass() error = %v, wantErr %v", err, tc.wantErr)
			}
			if gotEnforced != tc.wantEnforced {
				t.Errorf("NamespaceEnforcedWorkloadClass() enforced = %v, want %v", gotEnforced, tc.wantEnforced)
			}
			gotName := ""
			if gotWC != nil {
				gotName = gotWC.Name
			}
			if gotName != tc.wantWC {
				t.Errorf("NamespaceEnforcedWorkloadClass() wc = %q, want %q", gotName, tc.wantWC)
			}
		})
	}
}

func ptrTo[T any](v T) *T { return &v }
