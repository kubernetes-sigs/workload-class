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

func TestOlderWorkloadClass(t *testing.T) {
	latest := time.Now()
	oldest := latest.AddDate(0, 0, -1)
	older := newMatchWC("wc-b", oldest, nil)
	newer := newMatchWC("wc-a", latest, nil)
	sameAgeA := newMatchWC("wc-a", oldest, nil)

	testCases := []struct {
		name string
		a, b *workloadsv1alpha1.WorkloadClass
		want bool
	}{
		{name: "older_creation_timestamp_wins", a: older, b: newer, want: true},
		{name: "newer_creation_timestamp_loses", a: newer, b: older, want: false},
		{name: "equal_creation_timestamp_smaller_name_wins", a: sameAgeA, b: older, want: true},
		{name: "equal_creation_timestamp_larger_name_loses", a: older, b: sameAgeA, want: false},
		{name: "same_class_is_not_older", a: older, b: older, want: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := OlderWorkloadClass(tc.a, tc.b); got != tc.want {
				t.Errorf("OlderWorkloadClass(%s, %s) = %t, want %t", tc.a.Name, tc.b.Name, got, tc.want)
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

	oldBroad := newMatchWC("old-broad", now.Add(-2*time.Hour), matchLabels(matchKeyA, matchValA))
	newSpecific := newMatchWC("new-specific", now, matchLabels(matchKeyA, matchValA, matchKeyB, matchValB))
	twinA := newMatchWC("twin-a", now.Add(-time.Hour), matchLabels(matchKeyB, matchValB))
	twinB := newMatchWC("twin-b", now.Add(-time.Hour), matchLabels(matchKeyA, matchValA))
	everything := newMatchWC("everything", now.Add(-time.Minute), &metav1.LabelSelector{})
	nilSelector := newMatchWC("nil-selector", now.Add(-3*time.Hour), nil)
	nonMatching := newMatchWC("non-matching", now.Add(-3*time.Hour), matchLabels(matchKeyA, "other"))
	invalid := newMatchWC("invalid", now.Add(-3*time.Hour), &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: matchKeyA, Operator: "Bogus"}}})
	terminating := newMatchWC("terminating", now.Add(-3*time.Hour), matchLabels(matchKeyA, matchValA, matchKeyB, matchValB))
	terminating.DeletionTimestamp = &deleting
	otherNamespace := newMatchWC("other-namespace", now.Add(-3*time.Hour), matchLabels(matchKeyA, matchValA, matchKeyB, matchValB))
	otherNamespace.Namespace = "other"

	testCases := []struct {
		name   string
		pod    *corev1.Pod
		wcs    []workloadsv1alpha1.WorkloadClass
		wantBM string
	}{
		{name: "nil_pod", pod: nil, wcs: []workloadsv1alpha1.WorkloadClass{*oldBroad}},
		{name: "no_classes", pod: pod},
		{
			name: "oldest_wins_over_more_specific", pod: pod,
			wcs:    []workloadsv1alpha1.WorkloadClass{*everything, *newSpecific, *oldBroad},
			wantBM: "old-broad",
		},
		{
			name: "tie_broken_by_name_regardless_of_order", pod: pod,
			wcs:    []workloadsv1alpha1.WorkloadClass{*twinB, *twinA},
			wantBM: "twin-a",
		},
		{
			name: "skips_nil_invalid_non_matching_terminating_and_other_namespace", pod: pod,
			wcs:    []workloadsv1alpha1.WorkloadClass{*nilSelector, *invalid, *nonMatching, *terminating, *otherNamespace, *newSpecific},
			wantBM: "new-specific",
		},
		{name: "nothing_matches", pod: pod, wcs: []workloadsv1alpha1.WorkloadClass{*nilSelector, *nonMatching}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := SelectorBestMatch(tc.pod, tc.wcs)
			gotName := ""
			if got != nil {
				gotName = got.Name
			}
			if gotName != tc.wantBM {
				t.Errorf("SelectorBestMatch() = %q, want %q", gotName, tc.wantBM)
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
