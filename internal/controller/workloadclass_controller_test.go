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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
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

			By("clearing SelectorOverlap when the overlapping WorkloadClass is deleted")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: otherName, Namespace: namespace}, other)).To(Succeed())
			other.Spec.PodSelector = podSelector.DeepCopy()
			Expect(k8sClient.Update(ctx, other)).To(Succeed())

			cond = reconcileAndGetValidated(workloadClassName)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(workloadsv1alpha1.ReasonSelectorOverlap))

			Expect(k8sClient.Delete(ctx, other)).To(Succeed())
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

		Context("maintenance readiness", func() {
			reconcileAt := func(now time.Time) (ctrl.Result, *workloadsv1alpha1.WorkloadClass) {
				controllerReconciler.Clock = clocktesting.NewFakePassiveClock(now)
				result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
				Expect(err).NotTo(HaveOccurred())

				wc := &workloadsv1alpha1.WorkloadClass{}
				Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
				return result, wc
			}

			expectReadiness := func(
				wc *workloadsv1alpha1.WorkloadClass,
				readiness workloadsv1alpha1.MaintenanceReadiness,
				status metav1.ConditionStatus,
				reason string,
			) {
				GinkgoHelper()
				Expect(wc.Status.MaintenanceReadiness).To(Equal(readiness))
				cond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1alpha1.ConditionTypeMaintenanceAllowed)
				Expect(cond).NotTo(BeNil())
				Expect(cond.Status).To(Equal(status))
				Expect(cond.Reason).To(Equal(reason))
				Expect(cond.ObservedGeneration).To(Equal(wc.Generation))
			}

			createPod := func(name string, finalizers ...string) *corev1.Pod {
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:       name,
						Namespace:  namespace,
						Labels:     podSelector.MatchLabels,
						Finalizers: finalizers,
					},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
				}
				Expect(k8sClient.Create(ctx, pod)).To(Succeed())
				DeferCleanup(func() {
					current := &corev1.Pod{}
					if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
						Expect(errors.IsNotFound(err)).To(BeTrue())
						return
					}
					if len(current.Finalizers) > 0 {
						current.Finalizers = nil
						Expect(k8sClient.Update(ctx, current)).To(Succeed())
					}
					Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, current, client.GracePeriodSeconds(0)))).To(Succeed())
				})
				return pod
			}

			It("should transition between NotReady, Ready, and Overdue, requeueing at each transition", func() {
				wc := &workloadsv1alpha1.WorkloadClass{}
				Expect(k8sClient.Get(ctx, typeNamespacedName, wc)).To(Succeed())
				created := wc.CreationTimestamp.Time
				t0 := created.UTC().Truncate(time.Minute)
				updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
					wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{{
						Name:      "daily",
						TimeZone:  "UTC",
						StartTime: t0.Add(time.Hour).Format("15:04"),
						EndTime:   t0.Add(3 * time.Hour).Format("15:04"),
					}}
					wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = 1
				})

				By("being NotReady before the window opens, and requeueing when it opens")
				now := t0.Add(30 * time.Minute)
				result, wc := reconcileAt(now)
				expectReadiness(wc, workloadsv1alpha1.ReadinessNotReady, metav1.ConditionFalse,
					workloadsv1alpha1.ReasonOutsideDisruptionWindow)
				Expect(result.RequeueAfter).To(Equal(30*time.Minute + transitionRequeueDelay))

				By("being Ready once the window opens, and requeueing when it closes")
				now = now.Add(result.RequeueAfter)
				result, wc = reconcileAt(now)
				expectReadiness(wc, workloadsv1alpha1.ReadinessReady, metav1.ConditionTrue,
					workloadsv1alpha1.ReasonWithinDisruptionWindow)
				Expect(result.RequeueAfter).To(Equal(2 * time.Hour))

				By("being NotReady once the window closes, and requeueing when it becomes overdue before the next window")
				now = now.Add(result.RequeueAfter)
				result, wc = reconcileAt(now)
				expectReadiness(wc, workloadsv1alpha1.ReadinessNotReady, metav1.ConditionFalse,
					workloadsv1alpha1.ReasonOutsideDisruptionWindow)
				Expect(result.RequeueAfter).To(Equal(created.Add(24*time.Hour).Sub(now) + transitionRequeueDelay))

				By("being Overdue once it has remained undisrupted for MaxNonDisruptionDurationDays")
				now = now.Add(result.RequeueAfter)
				result, wc = reconcileAt(now)
				expectReadiness(wc, workloadsv1alpha1.ReadinessOverdue, metav1.ConditionTrue,
					workloadsv1alpha1.ReasonMaintenanceOverdue)
				Expect(result.RequeueAfter).To(BeZero())

				By("no longer being Overdue once a disruption is observed")
				wc.Status.LastDisruptionTime = &metav1.Time{Time: now}
				Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())
				result, wc = reconcileAt(now)
				expectReadiness(wc, workloadsv1alpha1.ReadinessNotReady, metav1.ConditionFalse,
					workloadsv1alpha1.ReasonOutsideDisruptionWindow)
				Expect(result.RequeueAfter).To(Equal(t0.Add(25*time.Hour).Sub(now) + transitionRequeueDelay))
			})

			It("should be Unknown when the WorkloadClass fails validation", func() {
				updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
					w := window("mars", "Monday")
					w.TimeZone = "Mars/Phobos"
					wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{w}
				})

				result, wc := reconcileAt(time.Now())
				expectReadiness(wc, workloadsv1alpha1.ReadinessUnknown, metav1.ConditionUnknown,
					workloadsv1alpha1.ReasonValidationFailed)
				Expect(result.RequeueAfter).To(BeZero())
			})

			It("should be Ready when EmergencyOverride is set, even outside of disruption windows", func() {
				now := time.Now().UTC()
				updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
					wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{{
						Name:      "later",
						StartTime: now.Add(2 * time.Hour).Format("15:04"),
						EndTime:   now.Add(3 * time.Hour).Format("15:04"),
					}}
					wc.Spec.DisruptionPolicy.EmergencyOverride = true
				})

				result, wc := reconcileAt(now)
				expectReadiness(wc, workloadsv1alpha1.ReadinessReady, metav1.ConditionTrue,
					workloadsv1alpha1.ReasonEmergencyOverride)
				Expect(result.RequeueAfter).To(BeZero())
			})

			It("should be NotReady until selected Pods have run for MinInitialRunDurationDays", func() {
				updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
					wc.Spec.DisruptionPolicy.MinInitialRunDurationDays = 1
					wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = 7
				})
				pod := createPod("young-pod")

				By("being NotReady, and requeueing when the Pod has run long enough")
				now := pod.CreationTimestamp.Add(time.Hour)
				result, wc := reconcileAt(now)
				expectReadiness(wc, workloadsv1alpha1.ReadinessNotReady, metav1.ConditionFalse,
					workloadsv1alpha1.ReasonMinInitialRunDurationNotMet)
				Expect(result.RequeueAfter).To(Equal(23*time.Hour + transitionRequeueDelay))

				By("being Ready once the Pod has run long enough, and requeueing when it becomes overdue")
				now = now.Add(result.RequeueAfter)
				result, wc = reconcileAt(now)
				expectReadiness(wc, workloadsv1alpha1.ReadinessReady, metav1.ConditionTrue,
					workloadsv1alpha1.ReasonWithinDisruptionWindow)
				Expect(result.RequeueAfter).To(Equal(wc.CreationTimestamp.Add(7*24*time.Hour).Sub(now) + transitionRequeueDelay))
			})

			It("should be NotReady while selected Pods terminate within GraceTerminationDurationSeconds", func() {
				updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
					wc.Spec.DisruptionPolicy.GraceTerminationDurationSeconds = 60
				})
				pod := createPod("terminating-pod", "workloads.x-k8s.io/test")
				Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
				Expect(pod.DeletionTimestamp).NotTo(BeNil())

				By("being NotReady, and requeueing when the grace period passes")
				now := pod.DeletionTimestamp.Add(10 * time.Second)
				result, wc := reconcileAt(now)
				expectReadiness(wc, workloadsv1alpha1.ReadinessNotReady, metav1.ConditionFalse,
					workloadsv1alpha1.ReasonGraceTerminationInProgress)
				Expect(result.RequeueAfter).To(Equal(50*time.Second + transitionRequeueDelay))

				By("being Ready once the grace period passes")
				_, wc = reconcileAt(now.Add(result.RequeueAfter))
				expectReadiness(wc, workloadsv1alpha1.ReadinessReady, metav1.ConditionTrue,
					workloadsv1alpha1.ReasonWithinDisruptionWindow)
			})
		})
	})

	Context("When running with the controller manager", func() {
		const (
			namespace     = "default"
			guardrailName = "test-mgr-guardrail"
		)

		var (
			mgrCtx    context.Context
			mgrCancel context.CancelFunc
			mgrDone   chan error
		)

		BeforeEach(func() {
			mgrCtx, mgrCancel = context.WithCancel(context.Background())
			mgrDone = make(chan error, 1)

			mgr, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme:  k8sClient.Scheme(),
				Metrics: metricsserver.Options{BindAddress: "0"},
				Controller: config.Controller{
					SkipNameValidation: ptr.To(true),
				},
			})
			Expect(err).NotTo(HaveOccurred())

			reconciler := &WorkloadClassReconciler{
				Client: mgr.GetClient(),
				Scheme: mgr.GetScheme(),
			}
			Expect(reconciler.SetupWithManager(mgr)).To(Succeed())

			go func() {
				mgrDone <- mgr.Start(mgrCtx)
			}()
		})

		AfterEach(func() {
			mgrCancel()
			Eventually(mgrDone).Should(Receive(BeNil()))

			Expect(k8sClient.DeleteAllOf(ctx, &workloadsv1alpha1.WorkloadClass{}, client.InNamespace(namespace))).To(Succeed())
			g := &workloadsv1alpha1.WorkloadClassGuardrail{ObjectMeta: metav1.ObjectMeta{Name: guardrailName}}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, g))).To(Succeed())
		})

		eventuallyCondition := func(name string, status metav1.ConditionStatus, reason string) {
			key := types.NamespacedName{Name: name, Namespace: namespace}
			Eventually(func(g Gomega) {
				wc := &workloadsv1alpha1.WorkloadClass{}
				g.Expect(k8sClient.Get(ctx, key, wc)).To(Succeed())
				cond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1alpha1.ConditionTypeValidated)
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.ObservedGeneration).To(Equal(wc.Generation))
				g.Expect(cond.Status).To(Equal(status))
				g.Expect(cond.Reason).To(Equal(reason))
			}).Should(Succeed())
		}

		It("should not re-queue WorkloadClasses on a guardrail status-only update, and should re-queue on a spec update", func() {
			guardrail := &workloadsv1alpha1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: guardrailName},
				Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1alpha1.Constraints{
						Disruption: workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 10},
					},
				},
			}
			Expect(k8sClient.Create(ctx, guardrail)).To(Succeed())

			wcName := "wc-watched-by-guardrail"
			wc := &workloadsv1alpha1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: wcName, Namespace: namespace},
				Spec: workloadsv1alpha1.WorkloadClassSpec{
					PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "watched"}},
					DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 7,
					},
				},
			}
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())
			eventuallyCondition(wcName, metav1.ConditionTrue, workloadsv1alpha1.ReasonValidationPassed)

			By("clearing the WorkloadClass status condition without bumping its generation")
			key := types.NamespacedName{Name: wcName, Namespace: namespace}
			Expect(k8sClient.Get(ctx, key, wc)).To(Succeed())
			wc.Status.Conditions = nil
			Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())

			By("updating only the guardrail status and verifying the WorkloadClass is not re-queued")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: guardrailName}, guardrail)).To(Succeed())
			meta.SetStatusCondition(&guardrail.Status.Conditions, metav1.Condition{
				Type:               workloadsv1alpha1.ConditionTypeValidated,
				Status:             metav1.ConditionTrue,
				Reason:             workloadsv1alpha1.ReasonValidationPassed,
				Message:            "Guardrail is valid",
				ObservedGeneration: guardrail.Generation,
			})
			Expect(k8sClient.Status().Update(ctx, guardrail)).To(Succeed())

			Consistently(func(g Gomega) {
				current := &workloadsv1alpha1.WorkloadClass{}
				g.Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
				g.Expect(current.Status.Conditions).To(BeEmpty())
			}, "300ms", "50ms").Should(Succeed())

			By("updating the guardrail spec and verifying the WorkloadClass is re-queued and re-validated")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: guardrailName}, guardrail)).To(Succeed())
			guardrail.Spec.Constraints.Disruption.MaxNonDisruptionDurationDays = 5
			Expect(k8sClient.Update(ctx, guardrail)).To(Succeed())

			eventuallyCondition(wcName, metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
		})

		It("should clear SelectorOverlap on the remaining WorkloadClass when the overlapping WorkloadClass is deleted", func() {
			for _, name := range []string{"wc-overlap-a", "wc-overlap-b"} {
				wc := &workloadsv1alpha1.WorkloadClass{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
					Spec: workloadsv1alpha1.WorkloadClassSpec{
						PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "shared"}},
						DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{
							MaxNonDisruptionDurationDays: 7,
						},
					},
				}
				Expect(k8sClient.Create(ctx, wc)).To(Succeed())
			}

			eventuallyCondition("wc-overlap-a", metav1.ConditionFalse, workloadsv1alpha1.ReasonSelectorOverlap)
			eventuallyCondition("wc-overlap-b", metav1.ConditionFalse, workloadsv1alpha1.ReasonSelectorOverlap)

			By("deleting wc-overlap-b and verifying the sibling watch clears SelectorOverlap on wc-overlap-a")
			Expect(k8sClient.Delete(ctx, &workloadsv1alpha1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "wc-overlap-b", Namespace: namespace},
			})).To(Succeed())

			eventuallyCondition("wc-overlap-a", metav1.ConditionTrue, workloadsv1alpha1.ReasonNoGuardrails)
		})

		It("should re-calculate maintenance readiness when a selected Pod is created", func() {
			const wcName = "wc-watched-pods"
			podLabels := map[string]string{"app": "readiness-watched"}
			wc := &workloadsv1alpha1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: wcName, Namespace: namespace},
				Spec: workloadsv1alpha1.WorkloadClassSpec{
					PodSelector: &metav1.LabelSelector{MatchLabels: podLabels},
					DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 7,
						MinInitialRunDurationDays:    1,
					},
				},
			}
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())

			eventuallyReadiness := func(readiness workloadsv1alpha1.MaintenanceReadiness, reason string) {
				GinkgoHelper()
				Eventually(func(g Gomega) {
					current := &workloadsv1alpha1.WorkloadClass{}
					g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: wcName, Namespace: namespace}, current)).To(Succeed())
					g.Expect(current.Status.MaintenanceReadiness).To(Equal(readiness))
					cond := meta.FindStatusCondition(current.Status.Conditions, workloadsv1alpha1.ConditionTypeMaintenanceAllowed)
					g.Expect(cond).NotTo(BeNil())
					g.Expect(cond.Reason).To(Equal(reason))
				}).Should(Succeed())
			}
			eventuallyReadiness(workloadsv1alpha1.ReadinessReady, workloadsv1alpha1.ReasonWithinDisruptionWindow)

			By("creating a selected Pod that has not run for MinInitialRunDurationDays")
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "readiness-watched-pod", Namespace: namespace, Labels: podLabels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0)))).To(Succeed())
			})
			eventuallyReadiness(workloadsv1alpha1.ReadinessNotReady, workloadsv1alpha1.ReasonMinInitialRunDurationNotMet)

			By("relabeling the Pod so that it is no longer selected")
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
			pod.Labels = map[string]string{"app": "something-else"}
			Expect(k8sClient.Update(ctx, pod)).To(Succeed())
			eventuallyReadiness(workloadsv1alpha1.ReadinessReady, workloadsv1alpha1.ReasonWithinDisruptionWindow)
		})
	})
})

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(workloadsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
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
			name: "multiple_overlapping_sorted_by_name",
			existing: []client.Object{
				wcCurrent,
				testWorkloadClass("wc-same-3", ns, nginx),
				testWorkloadClass("wc-same-1", ns, nginx),
				testWorkloadClass("wc-same-2", ns, nginx),
			},
			wantNames: []string{"wc-same-1", "wc-same-2", "wc-same-3"},
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
				"`allowedDisruptionWindows` must be specified, with `daysOfWeek` containing only 'Sunday', 'Saturday'",
				"`maxNonDisruptionDurationDays` must be between 1 and 5, inclusive",
			},
		},
		{
			name: "effective_days_order_is_deterministic_regardless_of_guardrail_order",
			guardrails: []workloadsv1alpha1.WorkloadClassGuardrail{
				testGuardrail("strict", workloadsv1alpha1.Disruption{
					DaysOfWeek:                   []string{"Saturday", "Sunday", "Monday"},
					MaxNonDisruptionDurationDays: 5,
				}),
				testGuardrail("lax", workloadsv1alpha1.Disruption{
					DaysOfWeek:                   []string{"Friday", "Saturday", "Sunday"},
					MaxNonDisruptionDurationDays: 30,
				}),
			},
			want: []string{
				"`allowedDisruptionWindows` must be specified, with `daysOfWeek` containing only 'Sunday', 'Saturday'",
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
			name: "disjoint_days_of_week_constraints_report_actionable_conflict_message",
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
				disjointDaysOfWeekMsg,
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
	order := []int{7, 2, 5, 1, 6, 3, 4}
	overlapping := make([]workloadsv1alpha1.WorkloadClass, 0, len(order))
	for _, i := range order {
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

func testPod(name string, podLabels map[string]string, created time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "ns",
			Labels:            podLabels,
			CreationTimestamp: metav1.NewTime(created),
		},
	}
}

func terminatingTestPod(name string, podLabels map[string]string, created, deleted time.Time) *corev1.Pod {
	pod := testPod(name, podLabels, created)
	pod.Finalizers = []string{"workloads.x-k8s.io/test"}
	pod.DeletionTimestamp = ptr.To(metav1.NewTime(deleted))
	return pod
}

func TestOverdue(t *testing.T) {
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name           string
		maxDays        int32
		created        time.Time
		lastDisruption *time.Time
		wantOverdue    bool
		wantRemaining  time.Duration
	}{
		{
			name:    "max_non_disruption_duration_unset_is_never_overdue",
			created: now.AddDate(-1, 0, 0),
		},
		{
			name:          "never_disrupted_within_max_duration_since_creation",
			maxDays:       2,
			created:       now.Add(-24 * time.Hour),
			wantRemaining: 24 * time.Hour,
		},
		{
			name:        "never_disrupted_beyond_max_duration_since_creation",
			maxDays:     2,
			created:     now.Add(-72 * time.Hour),
			wantOverdue: true,
		},
		{
			name:           "disrupted_within_max_duration",
			maxDays:        2,
			created:        now.AddDate(0, 0, -10),
			lastDisruption: ptr.To(now.Add(-36 * time.Hour)),
			wantRemaining:  12 * time.Hour,
		},
		{
			name:           "disrupted_beyond_max_duration",
			maxDays:        2,
			created:        now.AddDate(0, 0, -10),
			lastDisruption: ptr.To(now.Add(-72 * time.Hour)),
			wantOverdue:    true,
		},
		{
			name:        "exactly_at_max_duration",
			maxDays:     2,
			created:     now.Add(-48 * time.Hour),
			wantOverdue: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := &workloadsv1alpha1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(tc.created)},
				Spec: workloadsv1alpha1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{MaxNonDisruptionDurationDays: tc.maxDays},
				},
			}
			if tc.lastDisruption != nil {
				wc.Status.LastDisruptionTime = ptr.To(metav1.NewTime(*tc.lastDisruption))
			}

			gotOverdue, gotRemaining := overdue(wc, now)
			if gotOverdue != tc.wantOverdue || gotRemaining != tc.wantRemaining {
				t.Errorf("overdue() = (%v, %v), want (%v, %v)", gotOverdue, gotRemaining, tc.wantOverdue, tc.wantRemaining)
			}
		})
	}
}

func TestMinInitialRunPassed(t *testing.T) {
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	wc := &workloadsv1alpha1.WorkloadClass{
		Spec: workloadsv1alpha1.WorkloadClassSpec{
			DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{MinInitialRunDurationDays: 2},
		},
	}

	testCases := []struct {
		name          string
		created       time.Time
		wantPassed    bool
		wantRemaining time.Duration
	}{
		{name: "just_created", created: now, wantRemaining: 48 * time.Hour},
		{name: "running_for_one_day", created: now.Add(-24 * time.Hour), wantRemaining: 24 * time.Hour},
		{name: "running_for_exactly_min_duration", created: now.Add(-48 * time.Hour), wantPassed: true},
		{name: "running_for_longer_than_min_duration", created: now.AddDate(0, 0, -10), wantPassed: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotPassed, gotRemaining := minInitialRunPassed(wc, testPod("pod", nil, tc.created), now)
			if gotPassed != tc.wantPassed || gotRemaining != tc.wantRemaining {
				t.Errorf("minInitialRunPassed() = (%v, %v), want (%v, %v)", gotPassed, gotRemaining, tc.wantPassed, tc.wantRemaining)
			}
		})
	}
}

func TestGracePeriodPassed(t *testing.T) {
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name          string
		grace         int32
		deleted       *time.Time
		wantPassed    bool
		wantRemaining time.Duration
	}{
		{name: "not_terminating", grace: 30, wantPassed: true},
		{name: "terminating_within_grace_period", grace: 30, deleted: ptr.To(now), wantRemaining: 30 * time.Second},
		{
			name:          "terminating_partway_through_grace_period",
			grace:         30,
			deleted:       ptr.To(now.Add(-10 * time.Second)),
			wantRemaining: 20 * time.Second,
		},
		{name: "terminating_beyond_grace_period", grace: 86400, deleted: ptr.To(now.AddDate(0, 0, -2)), wantPassed: true},
		{name: "terminating_without_grace_period", grace: 0, deleted: ptr.To(now), wantPassed: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := &workloadsv1alpha1.WorkloadClass{
				Spec: workloadsv1alpha1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{GraceTerminationDurationSeconds: tc.grace},
				},
			}
			pod := testPod("pod", nil, now.AddDate(0, 0, -10))
			if tc.deleted != nil {
				pod.DeletionTimestamp = ptr.To(metav1.NewTime(*tc.deleted))
			}

			gotPassed, gotRemaining := gracePeriodPassed(wc, pod, now)
			if gotPassed != tc.wantPassed || gotRemaining != tc.wantRemaining {
				t.Errorf("gracePeriodPassed() = (%v, %v), want (%v, %v)", gotPassed, gotRemaining, tc.wantPassed, tc.wantRemaining)
			}
		})
	}
}

func TestEvaluatePodGracePeriod(t *testing.T) {
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	wc := &workloadsv1alpha1.WorkloadClass{
		Spec: workloadsv1alpha1.WorkloadClassSpec{
			DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{GraceTerminationDurationSeconds: 3600},
		},
	}

	testCases := []struct {
		name          string
		deleted       time.Time
		initialPassed bool
		initialMax    time.Duration
		wantPassed    bool
		wantMax       time.Duration
	}{
		{
			name:          "passed_and_previous_passed",
			deleted:       now.Add(-2 * time.Hour),
			initialPassed: true,
			wantPassed:    true,
		},
		{
			name:          "not_passed_and_previous_passed",
			deleted:       now.Add(-15 * time.Minute),
			initialPassed: true,
			wantMax:       45 * time.Minute,
		},
		{
			name:       "passed_and_previous_not_passed_keeps_previous_remaining",
			deleted:    now.Add(-2 * time.Hour),
			initialMax: 30 * time.Second,
			wantMax:    30 * time.Second,
		},
		{
			name:       "not_passed_with_longer_remaining_than_previous",
			deleted:    now.Add(-15 * time.Minute),
			initialMax: 30 * time.Second,
			wantMax:    45 * time.Minute,
		},
		{
			name:       "not_passed_with_shorter_remaining_than_previous",
			deleted:    now.Add(-15 * time.Minute),
			initialMax: 50 * time.Minute,
			wantMax:    50 * time.Minute,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pod := terminatingTestPod("pod", nil, now.AddDate(0, 0, -10), tc.deleted)
			gotPassed, gotMax := evaluatePodGracePeriod(wc, pod, now, tc.initialPassed, tc.initialMax)
			if gotPassed != tc.wantPassed || gotMax != tc.wantMax {
				t.Errorf("evaluatePodGracePeriod() = (%v, %v), want (%v, %v)", gotPassed, gotMax, tc.wantPassed, tc.wantMax)
			}
		})
	}
}

func TestCalculateReadiness(t *testing.T) {
	// 2026-04-20 is a Monday.
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	selected := map[string]string{"app": "nginx"}
	validated := metav1.Condition{Type: workloadsv1alpha1.ConditionTypeValidated, Status: metav1.ConditionTrue}
	notValidated := metav1.Condition{Type: workloadsv1alpha1.ConditionTypeValidated, Status: metav1.ConditionFalse}
	// The window opens 2h before now and closes 3h after now.
	openWindow := workloadsv1alpha1.DisruptionWindow{Name: "open", StartTime: "10:00", EndTime: "15:00"}
	// The window opens 10h after now.
	closedWindow := workloadsv1alpha1.DisruptionWindow{Name: "closed", StartTime: "22:00", EndTime: "23:00"}

	newWC := func(mutate func(*workloadsv1alpha1.DisruptionPolicy)) *workloadsv1alpha1.WorkloadClass {
		wc := testWorkloadClass("wc", "ns", selected)
		wc.CreationTimestamp = metav1.NewTime(now.AddDate(0, 0, -1))
		mutate(&wc.Spec.DisruptionPolicy)
		return wc
	}

	testCases := []struct {
		name          string
		wc            *workloadsv1alpha1.WorkloadClass
		validated     metav1.Condition
		pods          []client.Object
		wantReadiness workloadsv1alpha1.MaintenanceReadiness
		wantReason    string
		wantMessage   string
		wantRequeue   time.Duration
	}{
		{
			name: "validation_failed_is_unknown",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.EmergencyOverride = true
			}),
			validated:     notValidated,
			wantReadiness: workloadsv1alpha1.ReadinessUnknown,
			wantReason:    workloadsv1alpha1.ReasonValidationFailed,
			wantMessage:   notValidatedMsg,
		},
		{
			name: "emergency_override_is_ready_outside_of_windows",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{closedWindow}
				p.EmergencyOverride = true
			}),
			validated:     validated,
			wantReadiness: workloadsv1alpha1.ReadinessReady,
			wantReason:    workloadsv1alpha1.ReasonEmergencyOverride,
			wantMessage:   emergencyOverrideMsg,
		},
		{
			name: "overdue_outside_of_windows",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{closedWindow}
				p.MaxNonDisruptionDurationDays = 1
			}),
			validated:     validated,
			wantReadiness: workloadsv1alpha1.ReadinessOverdue,
			wantReason:    workloadsv1alpha1.ReasonMaintenanceOverdue,
			wantMessage:   overdueMsg,
		},
		{
			name: "overdue_takes_precedence_over_young_pods",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.MaxNonDisruptionDurationDays = 1
				p.MinInitialRunDurationDays = 2
			}),
			validated:     validated,
			pods:          []client.Object{testPod("young", selected, now)},
			wantReadiness: workloadsv1alpha1.ReadinessOverdue,
			wantReason:    workloadsv1alpha1.ReasonMaintenanceOverdue,
			wantMessage:   overdueMsg,
		},
		{
			name: "outside_of_windows_requeues_when_the_next_window_opens",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{closedWindow}
				p.MaxNonDisruptionDurationDays = 7
			}),
			validated:     validated,
			wantReadiness: workloadsv1alpha1.ReadinessNotReady,
			wantReason:    workloadsv1alpha1.ReasonOutsideDisruptionWindow,
			wantMessage:   outsideWindowMsg,
			wantRequeue:   10*time.Hour + transitionRequeueDelay,
		},
		{
			name: "outside_of_windows_requeues_when_overdue_if_sooner",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{
					{Name: "wednesday", DaysOfWeek: []string{"Wednesday"}, StartTime: "10:00", EndTime: "11:00"},
				}
				p.MaxNonDisruptionDurationDays = 2
			}),
			validated:     validated,
			wantReadiness: workloadsv1alpha1.ReadinessNotReady,
			wantReason:    workloadsv1alpha1.ReasonOutsideDisruptionWindow,
			wantMessage:   outsideWindowMsg,
			wantRequeue:   24*time.Hour + transitionRequeueDelay,
		},
		{
			name: "within_window_requeues_when_the_window_closes",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{openWindow}
			}),
			validated:     validated,
			wantReadiness: workloadsv1alpha1.ReadinessReady,
			wantReason:    workloadsv1alpha1.ReasonWithinDisruptionWindow,
			wantMessage:   withinWindowMsg,
			wantRequeue:   3*time.Hour + transitionRequeueDelay,
		},
		{
			name: "no_windows_requeues_when_overdue",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.MaxNonDisruptionDurationDays = 7
			}),
			validated:     validated,
			wantReadiness: workloadsv1alpha1.ReadinessReady,
			wantReason:    workloadsv1alpha1.ReasonWithinDisruptionWindow,
			wantMessage:   noWindowsMsg,
			wantRequeue:   6*24*time.Hour + transitionRequeueDelay,
		},
		{
			name:          "no_windows_and_no_max_non_disruption_duration_does_not_requeue",
			wc:            newWC(func(*workloadsv1alpha1.DisruptionPolicy) {}),
			validated:     validated,
			wantReadiness: workloadsv1alpha1.ReadinessReady,
			wantReason:    workloadsv1alpha1.ReasonWithinDisruptionWindow,
			wantMessage:   noWindowsMsg,
		},
		{
			name: "young_pods_requeue_when_the_youngest_has_run_long_enough",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.MinInitialRunDurationDays = 1
			}),
			validated: validated,
			pods: []client.Object{
				testPod("young", selected, now.Add(-20*time.Hour)),
				testPod("younger", selected, now.Add(-19*time.Hour)),
				testPod("old", selected, now.AddDate(0, 0, -5)),
			},
			wantReadiness: workloadsv1alpha1.ReadinessNotReady,
			wantReason:    workloadsv1alpha1.ReasonMinInitialRunDurationNotMet,
			wantMessage:   minInitialRunMsg,
			wantRequeue:   5*time.Hour + transitionRequeueDelay,
		},
		{
			name: "young_pods_requeue_when_the_window_closes_if_sooner",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.AllowedDisruptionWindows = []workloadsv1alpha1.DisruptionWindow{openWindow}
				p.MinInitialRunDurationDays = 1
			}),
			validated:     validated,
			pods:          []client.Object{testPod("young", selected, now)},
			wantReadiness: workloadsv1alpha1.ReadinessNotReady,
			wantReason:    workloadsv1alpha1.ReasonMinInitialRunDurationNotMet,
			wantMessage:   minInitialRunMsg,
			wantRequeue:   3*time.Hour + transitionRequeueDelay,
		},
		{
			name: "young_pods_that_are_not_selected_do_not_block",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.MinInitialRunDurationDays = 1
			}),
			validated: validated,
			pods: []client.Object{
				testPod("other-app", map[string]string{"app": "redis"}, now),
				func() client.Object {
					pod := testPod("other-namespace", selected, now)
					pod.Namespace = "other"
					return pod
				}(),
			},
			wantReadiness: workloadsv1alpha1.ReadinessReady,
			wantReason:    workloadsv1alpha1.ReasonWithinDisruptionWindow,
			wantMessage:   noWindowsMsg,
		},
		{
			name: "young_pods_in_a_terminal_phase_do_not_block",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.MinInitialRunDurationDays = 1
			}),
			validated: validated,
			pods: []client.Object{
				func() client.Object {
					pod := testPod("succeeded", selected, now)
					pod.Status.Phase = corev1.PodSucceeded
					return pod
				}(),
				func() client.Object {
					pod := testPod("failed", selected, now)
					pod.Status.Phase = corev1.PodFailed
					return pod
				}(),
			},
			wantReadiness: workloadsv1alpha1.ReadinessReady,
			wantReason:    workloadsv1alpha1.ReasonWithinDisruptionWindow,
			wantMessage:   noWindowsMsg,
		},
		{
			name: "nil_pod_selector_selects_no_pods",
			wc: func() *workloadsv1alpha1.WorkloadClass {
				wc := newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
					p.MinInitialRunDurationDays = 1
				})
				wc.Spec.PodSelector = nil
				return wc
			}(),
			validated:     validated,
			pods:          []client.Object{testPod("young", selected, now)},
			wantReadiness: workloadsv1alpha1.ReadinessReady,
			wantReason:    workloadsv1alpha1.ReasonWithinDisruptionWindow,
			wantMessage:   noWindowsMsg,
		},
		{
			name: "terminating_pods_within_grace_period_requeue_when_the_last_grace_period_passes",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.GraceTerminationDurationSeconds = 600
			}),
			validated: validated,
			pods: []client.Object{
				terminatingTestPod("terminating", selected, now.AddDate(0, 0, -5), now.Add(-8*time.Minute)),
				terminatingTestPod("just-terminating", selected, now.AddDate(0, 0, -5), now.Add(-time.Minute)),
			},
			wantReadiness: workloadsv1alpha1.ReadinessNotReady,
			wantReason:    workloadsv1alpha1.ReasonGraceTerminationInProgress,
			wantMessage:   graceTerminationMsg,
			wantRequeue:   9*time.Minute + transitionRequeueDelay,
		},
		{
			name: "terminating_pods_beyond_grace_period_do_not_block",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.GraceTerminationDurationSeconds = 600
			}),
			validated:     validated,
			pods:          []client.Object{terminatingTestPod("terminated", selected, now.AddDate(0, 0, -5), now.Add(-time.Hour))},
			wantReadiness: workloadsv1alpha1.ReadinessReady,
			wantReason:    workloadsv1alpha1.ReasonWithinDisruptionWindow,
			wantMessage:   noWindowsMsg,
		},
		{
			name: "terminating_young_pods_are_only_subject_to_the_grace_period",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.MinInitialRunDurationDays = 1
			}),
			validated:     validated,
			pods:          []client.Object{terminatingTestPod("young-terminating", selected, now, now)},
			wantReadiness: workloadsv1alpha1.ReadinessReady,
			wantReason:    workloadsv1alpha1.ReasonWithinDisruptionWindow,
			wantMessage:   noWindowsMsg,
		},
		{
			name: "young_and_terminating_pods_report_min_initial_run_until_it_passes",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.MinInitialRunDurationDays = 1
				p.GraceTerminationDurationSeconds = 7200
			}),
			validated: validated,
			pods: []client.Object{
				testPod("young", selected, now.Add(-23*time.Hour)),
				terminatingTestPod("terminating", selected, now.AddDate(0, 0, -5), now),
			},
			wantReadiness: workloadsv1alpha1.ReadinessNotReady,
			wantReason:    workloadsv1alpha1.ReasonMinInitialRunDurationNotMet,
			wantMessage:   minInitialRunMsg,
			wantRequeue:   time.Hour + transitionRequeueDelay,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			r := &WorkloadClassReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.pods...).Build(),
				Scheme: scheme,
				Clock:  clocktesting.NewFakePassiveClock(now),
			}

			got, err := r.calculateReadiness(t.Context(), tc.wc, tc.validated)
			if err != nil {
				t.Fatalf("calculateReadiness() returned unexpected error: %v", err)
			}
			want := readinessResult{
				readiness:    tc.wantReadiness,
				reason:       tc.wantReason,
				message:      tc.wantMessage,
				requeueAfter: tc.wantRequeue,
			}
			if got != want {
				t.Errorf("calculateReadiness() = %+v, want %+v", got, want)
			}
		})
	}
}

func TestReadinessResultCondition(t *testing.T) {
	testCases := []struct {
		readiness  workloadsv1alpha1.MaintenanceReadiness
		wantStatus metav1.ConditionStatus
	}{
		{readiness: workloadsv1alpha1.ReadinessReady, wantStatus: metav1.ConditionTrue},
		{readiness: workloadsv1alpha1.ReadinessOverdue, wantStatus: metav1.ConditionTrue},
		{readiness: workloadsv1alpha1.ReadinessNotReady, wantStatus: metav1.ConditionFalse},
		{readiness: workloadsv1alpha1.ReadinessUnknown, wantStatus: metav1.ConditionUnknown},
	}
	for _, tc := range testCases {
		t.Run(string(tc.readiness), func(t *testing.T) {
			result := readinessResult{readiness: tc.readiness, reason: "SomeReason", message: "some message", requeueAfter: time.Hour}
			got := result.condition(3)
			want := metav1.Condition{
				Type:               workloadsv1alpha1.ConditionTypeMaintenanceAllowed,
				Status:             tc.wantStatus,
				Reason:             "SomeReason",
				Message:            "some message",
				ObservedGeneration: 3,
			}
			if got != want {
				t.Errorf("condition() = %+v, want %+v", got, want)
			}
		})
	}
}

func TestPodReadinessPredicate(t *testing.T) {
	base := testPod("pod", map[string]string{"app": "nginx"}, time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC))
	withChange := func(mutate func(*corev1.Pod)) *corev1.Pod {
		pod := base.DeepCopy()
		mutate(pod)
		return pod
	}

	testCases := []struct {
		name   string
		newPod *corev1.Pod
		want   bool
	}{
		{
			name:   "unrelated_change",
			newPod: withChange(func(p *corev1.Pod) { p.Annotations = map[string]string{"foo": "bar"} }),
			want:   false,
		},
		{
			name:   "label_change",
			newPod: withChange(func(p *corev1.Pod) { p.Labels = map[string]string{"app": "redis"} }),
			want:   true,
		},
		{
			name:   "starts_terminating",
			newPod: withChange(func(p *corev1.Pod) { p.DeletionTimestamp = ptr.To(metav1.Now()) }),
			want:   true,
		},
		{
			name:   "reaches_terminal_phase",
			newPod: withChange(func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded }),
			want:   true,
		},
		{
			name:   "becomes_running",
			newPod: withChange(func(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning }),
			want:   false,
		},
	}
	p := podReadinessPredicate()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: tc.newPod}); got != tc.want {
				t.Errorf("Update() = %v, want %v", got, tc.want)
			}
		})
	}

	if !p.Create(event.CreateEvent{Object: base}) {
		t.Error("Create() = false, want true")
	}
	if !p.Delete(event.DeleteEvent{Object: base}) {
		t.Error("Delete() = false, want true")
	}
	if p.Generic(event.GenericEvent{Object: base}) {
		t.Error("Generic() = true, want false")
	}
}

func TestWorkloadClassesSelectingPod(t *testing.T) {
	scheme := newTestScheme(t)
	nilSelector := testWorkloadClass("wc-nil-selector", "ns", nil)
	nilSelector.Spec.PodSelector = nil
	invalid := testWorkloadClass("wc-invalid", "ns", nil)
	invalid.Spec.PodSelector.MatchExpressions = []metav1.LabelSelectorRequirement{
		{Key: "app", Operator: metav1.LabelSelectorOpIn},
	}
	r := &WorkloadClassReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(
			testWorkloadClass("wc-nginx", "ns", map[string]string{"app": "nginx"}),
			testWorkloadClass("wc-nginx-web", "ns", map[string]string{"app": "nginx", "tier": "web"}),
			testWorkloadClass("wc-everything", "ns", nil),
			testWorkloadClass("wc-redis", "ns", map[string]string{"app": "redis"}),
			testWorkloadClass("wc-other-namespace", "other", map[string]string{"app": "nginx"}),
			nilSelector,
			invalid,
		).Build(),
		Scheme: scheme,
	}

	pod := testPod("pod", map[string]string{"app": "nginx"}, time.Now())
	reqs := r.workloadClassesSelectingPod(t.Context(), pod)
	got := make([]string, 0, len(reqs))
	for _, req := range reqs {
		got = append(got, req.String())
	}
	slices.Sort(got)
	want := []string{"ns/wc-everything", "ns/wc-nginx"}
	if !slices.Equal(got, want) {
		t.Errorf("workloadClassesSelectingPod() = %v, want %v", got, want)
	}
}

func TestMinPositiveDuration(t *testing.T) {
	testCases := []struct {
		durations []time.Duration
		want      time.Duration
	}{
		{durations: nil, want: 0},
		{durations: []time.Duration{0, 0}, want: 0},
		{durations: []time.Duration{0, time.Hour}, want: time.Hour},
		{durations: []time.Duration{2 * time.Hour, -time.Hour, time.Hour}, want: time.Hour},
	}
	for _, tc := range testCases {
		if got := minPositiveDuration(tc.durations...); got != tc.want {
			t.Errorf("minPositiveDuration(%v) = %v, want %v", tc.durations, got, tc.want)
		}
	}
}
