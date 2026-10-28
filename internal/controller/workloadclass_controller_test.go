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

package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

var _ = Describe("WorkloadClass Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			workloadClassName = "test-workloadclass"
			guardrailName     = "test-workloadclass-guardrail"
			namespace         = "default"
		)

		ctx := context.Background()
		typeNamespacedName := types.NamespacedName{Name: workloadClassName, Namespace: namespace}
		podSelector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}}

		var controllerReconciler *WorkloadClassReconciler

		newWorkloadClass := func(name string) *workloadsv1alpha1.WorkloadClass {
			return &workloadsv1alpha1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec: workloadsv1alpha1.WorkloadClassSpec{
					PodSelector: podSelector.DeepCopy(),
					DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 1,
					},
				},
			}
		}

		window := func(name string, days ...string) workloadsv1alpha1.DisruptionWindow {
			return workloadsv1alpha1.DisruptionWindow{
				Name:       name,
				DaysOfWeek: days,
				TimeZone:   "UTC",
				StartTime:  "22:00",
				EndTime:    "04:00",
			}
		}

		createGuardrail := func(name string, disruption workloadsv1alpha1.Disruption) {
			guardrail := &workloadsv1alpha1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1alpha1.Constraints{Disruption: disruption},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())
		}

		createDefaultGuardrail := func() {
			createGuardrail(guardrailName, workloadsv1alpha1.Disruption{
				DaysOfWeek:                   []string{"Sunday", "Monday", "Tuesday"},
				MaxAllowedWindows:            2,
				MaxNonDisruptionDurationDays: 3,
			})
		}

		updateWorkloadClass := func(mutate func(wc *workloadsv1alpha1.WorkloadClass)) {
			wc := &workloadsv1alpha1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
			mutate(wc)
			Expect(k8sClient.Update(ctx, wc)).To(Succeed())
		}

		reconcileAndGetValidated := func(name string) *metav1.Condition {
			key := types.NamespacedName{Name: name, Namespace: namespace}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			wc := &workloadsv1alpha1.WorkloadClass{}
			Expect(k8sClient.Get(ctx, key, wc)).To(Succeed())
			cond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1alpha1.ConditionTypeValidated)
			Expect(cond).NotTo(BeNil())
			Expect(cond.ObservedGeneration).To(Equal(wc.Generation))
			return cond
		}

		BeforeEach(func() {
			controllerReconciler = &WorkloadClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			By("creating the custom resource for the Kind WorkloadClass")
			err := k8sClient.Get(ctx, typeNamespacedName, &workloadsv1alpha1.WorkloadClass{})
			if err != nil && errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, newWorkloadClass(workloadClassName))).To(Succeed())
			}
		})

		AfterEach(func() {
			By("cleaning up all WorkloadClasses in the namespace")
			Expect(k8sClient.DeleteAllOf(ctx, &workloadsv1alpha1.WorkloadClass{}, client.InNamespace(namespace))).To(Succeed())

			By("cleaning up the WorkloadClassGuardrails")
			for _, name := range []string{guardrailName, guardrailName + "-2"} {
				g := &workloadsv1alpha1.WorkloadClassGuardrail{ObjectMeta: metav1.ObjectMeta{Name: name}}
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, g))).To(Succeed())
			}
		})

		It("should not error when the WorkloadClass no longer exists", func() {
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "does-not-exist", Namespace: namespace},
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should be validated with reason NoGuardrails when there are no guardrails", func() {
			cond := reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonNoGuardrails))
		})

		It("should pass validation when the WorkloadClass adheres to the guardrail", func() {
			createDefaultGuardrail()
			updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
				wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{
					window("monday", "Monday"),
				}
			})

			cond := reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonValidationPassed))
			Expect(cond.Message).To(Equal(validationPassedMsg))
		})

		It("should fail validation if the number of windows exceeds MaxAllowedWindows", func() {
			createDefaultGuardrail()
			updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
				wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{
					window("one", "Sunday"),
					window("two", "Monday"),
					window("three", "Tuesday"),
				}
			})

			cond := reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonValidationFailed))
			Expect(cond.Message).To(ContainSubstring(
				"`allowedDisruptionWindows` must have at most 2 items (guardrail limit), got 3"))
		})

		It("should fail validation if DaysOfWeek is not a subset of the guardrail DaysOfWeek", func() {
			createDefaultGuardrail()
			updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
				wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{
					window("midweek", "Monday", "Wednesday"),
				}
			})

			cond := reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonValidationFailed))
			Expect(cond.Message).To(ContainSubstring(
				"`daysOfWeek` of disruption window 'midweek' must contain only 'Sunday', 'Monday', 'Tuesday' (guardrail limit), got 'Monday', 'Wednesday'"))
		})

		It("should fail validation if a window with no DaysOfWeek is used with a guardrail that restricts days", func() {
			createDefaultGuardrail()
			updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
				wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{
					window("every-day"),
				}
			})

			cond := reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonValidationFailed))
			Expect(cond.Message).To(ContainSubstring("`daysOfWeek` of disruption window 'every-day' must be specified"))
		})

		It("should fail validation if allowedDisruptionWindows is omitted with a guardrail that restricts days", func() {
			createDefaultGuardrail()

			cond := reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonValidationFailed))
			Expect(cond.Message).To(ContainSubstring("`allowedDisruptionWindows` must be specified"))
		})

		It("should fail validation if MaxNonDisruptionDurationDays exceeds the guardrail limit", func() {
			createDefaultGuardrail()
			updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
				wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = 5
			})

			cond := reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonValidationFailed))
			Expect(cond.Message).To(ContainSubstring("`maxNonDisruptionDurationDays` must be between 1 and 3, inclusive (guardrail limit), got 5"))
		})

		It("should validate against every guardrail", func() {
			createDefaultGuardrail()
			createGuardrail(guardrailName+"-2", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 2})
			updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
				wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{
					window("monday", "Monday"),
				}
				wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = 3
			})

			cond := reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Message).To(Equal(
				"`maxNonDisruptionDurationDays` must be between 1 and 2, inclusive (guardrail limit), got 3 (0 means no limit)"))
		})

		It("should fail validation if a window has an invalid time zone, even without guardrails", func() {
			updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
				w := window("mars", "Monday")
				w.TimeZone = "Mars/Phobos"
				wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{w}
			})

			cond := reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonValidationFailed))
			Expect(cond.Message).To(ContainSubstring(
				"`timeZone` of disruption window 'mars' must be a valid IANA time zone name other than 'Local', got 'Mars/Phobos'"))
		})

		It("should fail validation if a window has an invalid day of week", func() {
			updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
				wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{
					window("funday", "Funday"),
				}
			})

			cond := reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonValidationFailed))
			Expect(cond.Message).To(ContainSubstring("disruption window 'funday': `daysOfWeek` must only contain"))
		})

		It("should surface SelectorOverlap when another WorkloadClass has an overlapping PodSelector", func() {
			const otherName = "test-workloadclass-overlapping-selector"
			other := newWorkloadClass(otherName)
			other.Spec.PodSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test", "tier": "web"}}
			Expect(k8sClient.Create(ctx, other)).To(Succeed())

			By("marking both WorkloadClasses with SelectorOverlap")
			cond := reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonSelectorOverlap))
			Expect(cond.Message).To(Equal(fmt.Sprintf(
				"`podSelector` must not select the same Pods as another WorkloadClass in the namespace, "+
					"but it overlaps with the `podSelector` of '%s'", otherName)))

			cond = reconcileAndGetValidated(otherName)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonSelectorOverlap))

			By("clearing SelectorOverlap once the selectors no longer overlap")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: otherName, Namespace: namespace}, other)).To(Succeed())
			other.Spec.PodSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "other"}}
			Expect(k8sClient.Update(ctx, other)).To(Succeed())

			cond = reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonNoGuardrails))
		})

		It("should report guardrail violations and selector overlap together as ValidationFailed", func() {
			createDefaultGuardrail()
			Expect(k8sClient.Create(ctx, newWorkloadClass("test-workloadclass-same-selector"))).To(Succeed())
			updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
				wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = 5
			})

			cond := reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonValidationFailed))
			Expect(cond.Message).To(ContainSubstring("`maxNonDisruptionDurationDays` must be between 1 and 3, inclusive (guardrail limit), got 5"))
			Expect(cond.Message).To(ContainSubstring("overlaps with the `podSelector` of 'test-workloadclass-same-selector'"))
		})
	})
})

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(workloadsv1alpha1.AddToScheme(scheme))
	return scheme
}

func testWorkloadClass(name, namespace string, matchLabels map[string]string) *workloadsv1alpha1.WorkloadClass {
	return &workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: workloadsv1alpha1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: matchLabels},
		},
	}
}

func TestOverlappingWorkloadClasses(t *testing.T) {
	const ns = "test-namespace"
	nginx := map[string]string{"app": "nginx"}
	wcCurrent := testWorkloadClass("wc-current", ns, nginx)

	invalid := testWorkloadClass("wc-invalid", ns, nil)
	invalid.Spec.PodSelector.MatchExpressions = []metav1.LabelSelectorRequirement{
		{Key: "app", Operator: metav1.LabelSelectorOpIn},
	}

	now := metav1.Now()
	deleting := testWorkloadClass("wc-deleting", ns, nginx)
	deleting.Finalizers = []string{"workloads.x-k8s.io/test"}
	deleting.DeletionTimestamp = &now

	testCases := []struct {
		name      string
		existing  []client.Object
		wantNames []string
	}{
		{
			name:     "no_other_workload_classes",
			existing: []client.Object{wcCurrent},
		},
		{
			name:     "other_disjoint_selector",
			existing: []client.Object{wcCurrent, testWorkloadClass("wc-different", ns, map[string]string{"app": "redis"})},
		},
		{
			name:      "other_same_selector",
			existing:  []client.Object{wcCurrent, testWorkloadClass("wc-same", ns, nginx)},
			wantNames: []string{"wc-same"},
		},
		{
			name:      "other_superset_selector",
			existing:  []client.Object{wcCurrent, testWorkloadClass("wc-superset", ns, map[string]string{"app": "nginx", "tier": "web"})},
			wantNames: []string{"wc-superset"},
		},
		{
			name:     "same_selector_different_namespace",
			existing: []client.Object{wcCurrent, testWorkloadClass("wc-same-other-ns", "another-namespace", nginx)},
		},
		{
			name:     "other_invalid_selector_is_skipped",
			existing: []client.Object{wcCurrent, invalid},
		},
		{
			name:     "other_deleting_is_skipped",
			existing: []client.Object{wcCurrent, deleting},
		},
		{
			name: "multiple_overlapping",
			existing: []client.Object{
				wcCurrent,
				testWorkloadClass("wc-same-1", ns, nginx),
				testWorkloadClass("wc-same-2", ns, nginx),
			},
			wantNames: []string{"wc-same-1", "wc-same-2"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			r := &WorkloadClassReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.existing...).Build(),
				Scheme: scheme,
			}

			got, err := r.overlappingWorkloadClasses(context.Background(), wcCurrent)
			if err != nil {
				t.Fatalf("overlappingWorkloadClasses() returned unexpected error: %v", err)
			}

			gotNames := make([]string, 0, len(got))
			for _, wc := range got {
				gotNames = append(gotNames, wc.Name)
			}
			slices.Sort(gotNames)
			if !slices.Equal(gotNames, tc.wantNames) {
				t.Errorf("overlappingWorkloadClasses() = %v, want %v", gotNames, tc.wantNames)
			}
		})
	}
}

func testGuardrail(name string, disruption workloadsv1alpha1.Disruption) workloadsv1alpha1.WorkloadClassGuardrail {
	return workloadsv1alpha1.WorkloadClassGuardrail{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{
			Constraints: workloadsv1alpha1.Constraints{Disruption: disruption},
		},
	}
}

func TestValidateAgainstGuardrails(t *testing.T) {
	wc := &workloadsv1alpha1.WorkloadClass{
		Spec: workloadsv1alpha1.WorkloadClassSpec{
			DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{MaxNonDisruptionDurationDays: 40},
		},
	}

	testCases := []struct {
		name       string
		guardrails []workloadsv1alpha1.WorkloadClassGuardrail
		want       []string
	}{
		{
			name: "no_guardrails",
		},
		{
			name: "compliant_with_all_guardrails",
			guardrails: []workloadsv1alpha1.WorkloadClassGuardrail{
				testGuardrail("a", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 40}),
				testGuardrail("b", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 60}),
			},
		},
		{
			name: "effective_strictest_limits_across_guardrails_without_guardrail_names",
			guardrails: []workloadsv1alpha1.WorkloadClassGuardrail{
				testGuardrail("lax", workloadsv1alpha1.Disruption{
					DaysOfWeek:                   []string{"Friday", "Saturday", "Sunday"},
					MaxNonDisruptionDurationDays: 30,
				}),
				testGuardrail("strict", workloadsv1alpha1.Disruption{
					DaysOfWeek:                   []string{"Saturday", "Sunday", "Monday"},
					MaxNonDisruptionDurationDays: 5,
				}),
			},
			want: []string{
				"`allowedDisruptionWindows` must be specified, with `daysOfWeek` containing only 'Saturday', 'Sunday'",
				"`maxNonDisruptionDurationDays` must be between 1 and 5, inclusive",
			},
		},
		{
			name: "identical_violations_from_several_guardrails_are_reported_once",
			guardrails: []workloadsv1alpha1.WorkloadClassGuardrail{
				testGuardrail("strict-1", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 5}),
				testGuardrail("strict-2", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 5}),
			},
			want: []string{
				"`maxNonDisruptionDurationDays` must be between 1 and 5, inclusive",
			},
		},
		{
			name: "disjoint_days_of_week_constraints_are_both_reported",
			guardrails: []workloadsv1alpha1.WorkloadClassGuardrail{
				testGuardrail("saturday", workloadsv1alpha1.Disruption{
					DaysOfWeek:                   []string{"Saturday"},
					MaxNonDisruptionDurationDays: 60,
				}),
				testGuardrail("sunday", workloadsv1alpha1.Disruption{
					DaysOfWeek:                   []string{"Sunday"},
					MaxNonDisruptionDurationDays: 60,
				}),
			},
			want: []string{
				"`allowedDisruptionWindows` must be specified, with `daysOfWeek` containing only 'Saturday'",
				"`allowedDisruptionWindows` must be specified, with `daysOfWeek` containing only 'Sunday'",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := validateAgainstGuardrails(wc, tc.guardrails)
			if len(got) != len(tc.want) {
				t.Fatalf("validateAgainstGuardrails() = %q, want %d violation(s) starting with %q", got, len(tc.want), tc.want)
			}
			for i, want := range tc.want {
				if !strings.HasPrefix(got[i], want) {
					t.Errorf("violation[%d] = %q, want it to start with %q", i, got[i], want)
				}
				for _, g := range tc.guardrails {
					if strings.Contains(got[i], "'"+g.Name+"'") {
						t.Errorf("violation[%d] = %q must not contain guardrail name %q", i, got[i], g.Name)
					}
				}
			}
		})
	}
}

func TestValidatedCondition(t *testing.T) {
	wc := testWorkloadClass("wc", "ns", map[string]string{"app": "nginx"})
	wc.Generation = 3
	wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = 10
	invalidSelectorWC := wc.DeepCopy()
	invalidSelectorWC.Spec.PodSelector.MatchExpressions = []metav1.LabelSelectorRequirement{
		{Key: "tier", Operator: metav1.LabelSelectorOpIn},
	}
	overlapping := []workloadsv1alpha1.WorkloadClass{*testWorkloadClass("wc-same", "ns", map[string]string{"app": "nginx"})}
	permissive := testGuardrail("permissive", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 30})
	restrictive := testGuardrail("restrictive", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 5})

	testCases := []struct {
		name        string
		wc          *workloadsv1alpha1.WorkloadClass
		guardrails  []workloadsv1alpha1.WorkloadClassGuardrail
		overlapping []workloadsv1alpha1.WorkloadClass
		wantStatus  metav1.ConditionStatus
		wantReason  string
	}{
		{
			name:       "no_guardrails",
			wc:         wc,
			wantStatus: metav1.ConditionTrue,
			wantReason: workloadsv1alpha1.ReasonNoGuardrails,
		},
		{
			name:       "guardrails_pass",
			wc:         wc,
			guardrails: []workloadsv1alpha1.WorkloadClassGuardrail{permissive},
			wantStatus: metav1.ConditionTrue,
			wantReason: workloadsv1alpha1.ReasonValidationPassed,
		},
		{
			name:       "guardrails_fail",
			wc:         wc,
			guardrails: []workloadsv1alpha1.WorkloadClassGuardrail{restrictive},
			wantStatus: metav1.ConditionFalse,
			wantReason: workloadsv1alpha1.ReasonValidationFailed,
		},
		{
			name:       "invalid_pod_selector",
			wc:         invalidSelectorWC,
			wantStatus: metav1.ConditionFalse,
			wantReason: workloadsv1alpha1.ReasonValidationFailed,
		},
		{
			name:        "selector_overlap_only",
			wc:          wc,
			guardrails:  []workloadsv1alpha1.WorkloadClassGuardrail{permissive},
			overlapping: overlapping,
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1alpha1.ReasonSelectorOverlap,
		},
		{
			name:        "selector_overlap_without_guardrails",
			wc:          wc,
			overlapping: overlapping,
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1alpha1.ReasonSelectorOverlap,
		},
		{
			name:        "guardrail_violation_takes_precedence_over_overlap",
			wc:          wc,
			guardrails:  []workloadsv1alpha1.WorkloadClassGuardrail{restrictive},
			overlapping: overlapping,
			wantStatus:  metav1.ConditionFalse,
			wantReason:  workloadsv1alpha1.ReasonValidationFailed,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := validatedCondition(tc.wc, tc.guardrails, tc.overlapping)
			if got.Type != workloadsv1alpha1.ConditionTypeValidated {
				t.Errorf("Type = %q, want %q", got.Type, workloadsv1alpha1.ConditionTypeValidated)
			}
			if got.Status != tc.wantStatus || got.Reason != tc.wantReason {
				t.Errorf("got (%s, %s), want (%s, %s); message: %q", got.Status, got.Reason, tc.wantStatus, tc.wantReason, got.Message)
			}
			if got.ObservedGeneration != tc.wc.Generation {
				t.Errorf("ObservedGeneration = %d, want %d", got.ObservedGeneration, tc.wc.Generation)
			}
		})
	}
}

func TestWorkloadClassMapFuncs(t *testing.T) {
	scheme := newTestScheme(t)
	labels := map[string]string{"app": "nginx"}
	wcA := testWorkloadClass("wc-a", "ns-1", labels)
	wcB := testWorkloadClass("wc-b", "ns-1", labels)
	wcC := testWorkloadClass("wc-c", "ns-2", labels)
	r := &WorkloadClassReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(wcA, wcB, wcC).Build(),
		Scheme: scheme,
	}

	names := func(reqs []reconcile.Request) []string {
		out := make([]string, 0, len(reqs))
		for _, req := range reqs {
			out = append(out, req.String())
		}
		slices.Sort(out)
		return out
	}

	t.Run("all_workload_classes", func(t *testing.T) {
		got := names(r.allWorkloadClasses(context.Background(), &workloadsv1alpha1.WorkloadClassGuardrail{}))
		want := []string{"ns-1/wc-a", "ns-1/wc-b", "ns-2/wc-c"}
		if !slices.Equal(got, want) {
			t.Errorf("allWorkloadClasses() = %v, want %v", got, want)
		}
	})

	t.Run("other_workload_classes_in_namespace", func(t *testing.T) {
		got := names(r.workloadClassesInNamespace(context.Background(), wcA))
		want := []string{"ns-1/wc-b"}
		if !slices.Equal(got, want) {
			t.Errorf("workloadClassesInNamespace() = %v, want %v", got, want)
		}
	})
}

func TestSelectorOverlapMessageCapsItems(t *testing.T) {
	var overlapping []workloadsv1alpha1.WorkloadClass
	for i := 1; i <= 7; i++ {
		overlapping = append(overlapping, *testWorkloadClass(fmt.Sprintf("wc-%d", i), "ns", nil))
	}

	got := selectorOverlapMessage(overlapping)
	want := "`podSelector` must not select the same Pods as another WorkloadClass in the namespace, " +
		"but it overlaps with the `podSelector` of 'wc-1', 'wc-2', 'wc-3', 'wc-4', 'wc-5', and 2 more"
	if got != want {
		t.Errorf("selectorOverlapMessage() = %q, want %q", got, want)
	}
}

func TestTruncateMessage(t *testing.T) {
	if got := truncateMessage("short", 10); got != "short" {
		t.Errorf("truncateMessage(\"short\", 10) = %q, want \"short\"", got)
	}
	if got := truncateMessage("0123456789abcdef", 10); got != "0123456..." {
		t.Errorf("truncateMessage(\"0123456789abcdef\", 10) = %q, want \"0123456...\"", got)
	}
	// Multi-byte UTF-8 rune ("é" is 2 bytes at indices 6..7) must not be split mid-rune.
	if got := truncateMessage("123456é890", 10); got != "123456..." {
		t.Errorf("truncateMessage(\"123456é890\", 10) = %q, want \"123456...\"", got)
	}
}
