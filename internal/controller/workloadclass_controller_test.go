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
	stderrors "errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
	"sigs.k8s.io/workload-class/internal/utils"
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

			By("cleaning up all Pods in the namespace")
			deleteAllPods(namespace)

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

			It("should be NotReady for GraceTerminationDurationSeconds after a selected Pod starts terminating, even once the Pod is gone", func() {
				updateWorkloadClass(func(wc *workloadsv1alpha1.WorkloadClass) {
					wc.Spec.DisruptionPolicy.GraceTerminationDurationSeconds = 60
				})
				pod := createPod("terminating-pod", "workloads.x-k8s.io/test")
				Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
				Expect(pod.DeletionTimestamp).NotTo(BeNil())
				terminationStart := podTerminationStart(pod)

				By("recording when the Pod started terminating, being NotReady, and requeueing when the grace period passes")
				now := terminationStart.Add(10 * time.Second)
				result, wc := reconcileAt(now)
				Expect(wc.Status.LastPodTerminationTime).NotTo(BeNil())
				Expect(wc.Status.LastPodTerminationTime.Time).To(BeTemporally("==", terminationStart))
				expectReadiness(wc, workloadsv1alpha1.ReadinessNotReady, metav1.ConditionFalse,
					workloadsv1alpha1.ReasonGraceTerminationInProgress)
				Expect(result.RequeueAfter).To(Equal(50*time.Second + transitionRequeueDelay))

				By("staying NotReady after the Pod is gone")
				pod.Finalizers = nil
				Expect(k8sClient.Update(ctx, pod)).To(Succeed())
				Eventually(func() error {
					return k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
				}).Should(MatchError(errors.IsNotFound, "IsNotFound"))
				result, wc = reconcileAt(now.Add(20 * time.Second))
				expectReadiness(wc, workloadsv1alpha1.ReadinessNotReady, metav1.ConditionFalse,
					workloadsv1alpha1.ReasonGraceTerminationInProgress)
				Expect(result.RequeueAfter).To(Equal(30*time.Second + transitionRequeueDelay))

				By("being Ready once the grace period passes")
				_, wc = reconcileAt(terminationStart.Add(60 * time.Second))
				expectReadiness(wc, workloadsv1alpha1.ReadinessReady, metav1.ConditionTrue,
					workloadsv1alpha1.ReasonWithinDisruptionWindow)
			})

			It("should be Unknown with reason SelectorOverlap when the PodSelector overlaps with another WorkloadClass", func() {
				Expect(k8sClient.Create(ctx, newWorkloadClass("test-workloadclass-overlapping"))).To(Succeed())

				result, wc := reconcileAt(time.Now())
				expectReadiness(wc, workloadsv1alpha1.ReadinessUnknown, metav1.ConditionUnknown,
					workloadsv1alpha1.ReasonSelectorOverlap)
				Expect(result.RequeueAfter).To(BeZero())
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
			deleteAllPods(namespace)
			// envtest runs no garbage collector, so the PDBs of the deleted WorkloadClasses are deleted explicitly.
			Expect(k8sClient.DeleteAllOf(ctx, &policyv1.PodDisruptionBudget{}, client.InNamespace(namespace))).To(Succeed())
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

		It("should re-calculate maintenance readiness when lastDisruptionTime changes", func() {
			const wcName = "wc-watched-disruption"
			key := types.NamespacedName{Name: wcName, Namespace: namespace}
			wc := &workloadsv1alpha1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: wcName, Namespace: namespace},
				Spec: workloadsv1alpha1.WorkloadClassSpec{
					PodSelector:      &metav1.LabelSelector{MatchLabels: map[string]string{"app": "disruption-watched"}},
					DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{MaxNonDisruptionDurationDays: 7},
				},
			}
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())

			eventuallyReadiness := func(readiness workloadsv1alpha1.MaintenanceReadiness) {
				GinkgoHelper()
				Eventually(func(g Gomega) {
					current := &workloadsv1alpha1.WorkloadClass{}
					g.Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
					g.Expect(current.Status.MaintenanceReadiness).To(Equal(readiness))
				}).Should(Succeed())
			}
			eventuallyReadiness(workloadsv1alpha1.ReadinessReady)

			By("recording a disruption older than MaxNonDisruptionDurationDays")
			Expect(k8sClient.Get(ctx, key, wc)).To(Succeed())
			patch := client.MergeFrom(wc.DeepCopy())
			wc.Status.LastDisruptionTime = &metav1.Time{Time: time.Now().AddDate(0, 0, -10)}
			Expect(k8sClient.Status().Patch(ctx, wc, patch)).To(Succeed())
			eventuallyReadiness(workloadsv1alpha1.ReadinessOverdue)

			By("recording a recent disruption")
			Expect(k8sClient.Get(ctx, key, wc)).To(Succeed())
			patch = client.MergeFrom(wc.DeepCopy())
			wc.Status.LastDisruptionTime = &metav1.Time{Time: time.Now()}
			Expect(k8sClient.Status().Patch(ctx, wc, patch)).To(Succeed())
			eventuallyReadiness(workloadsv1alpha1.ReadinessReady)
		})

		It("should re-reconcile the namespace's WorkloadClasses when its enforced class is set, changed, or removed", func() {
			const ns = "wc-enforced-class"
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())
			DeferCleanup(func() {
				Expect(k8sClient.DeleteAllOf(ctx, &workloadsv1alpha1.WorkloadClass{}, client.InNamespace(ns))).To(Succeed())
				deleteAllPods(ns)
			})

			for name, matchLabels := range map[string]map[string]string{
				"wc-broad": {"app": "shared"},
				"wc-web":   {"app": "shared", "tier": "web"},
			} {
				Expect(k8sClient.Create(ctx, &workloadsv1alpha1.WorkloadClass{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
					Spec: workloadsv1alpha1.WorkloadClassSpec{
						PodSelector: &metav1.LabelSelector{MatchLabels: matchLabels},
						DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{
							MaxNonDisruptionDurationDays: 7,
							MinInitialRunDurationDays:    1,
						},
					},
				})).To(Succeed())
			}
			// Neither Pod has run for MinInitialRunDurationDays, and no PodSelector selects young-other.
			for name, podLabels := range map[string]map[string]string{
				"young-web":   {"app": "shared", "tier": "web"},
				"young-other": {"app": "other"},
			} {
				Expect(k8sClient.Create(ctx, &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: podLabels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
				})).To(Succeed())
			}

			eventuallyStatus := func(name, validated string, readiness workloadsv1alpha1.MaintenanceReadiness, msg string) {
				GinkgoHelper()
				Eventually(func(g Gomega) {
					wc := &workloadsv1alpha1.WorkloadClass{}
					g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, wc)).To(Succeed())
					cond := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1alpha1.ConditionTypeValidated)
					g.Expect(cond).NotTo(BeNil())
					g.Expect(cond.Reason).To(Equal(validated))
					g.Expect(wc.Status.MaintenanceReadiness).To(Equal(readiness))
					cond = meta.FindStatusCondition(wc.Status.Conditions, workloadsv1alpha1.ConditionTypeMaintenanceAllowed)
					g.Expect(cond).NotTo(BeNil())
					g.Expect(cond.Message).To(ContainSubstring(msg))
				}).Should(Succeed())
			}
			setEnforcedClass := func(name string) {
				GinkgoHelper()
				namespace := &corev1.Namespace{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ns}, namespace)).To(Succeed())
				patch := client.MergeFrom(namespace.DeepCopy())
				if name == "" {
					delete(namespace.Labels, workloadsv1alpha1.EnforcedWorkloadClassLabel)
				} else {
					metav1.SetMetaDataLabel(&namespace.ObjectMeta, workloadsv1alpha1.EnforcedWorkloadClassLabel, name)
				}
				Expect(k8sClient.Patch(ctx, namespace, patch)).To(Succeed())
			}
			const bothPods = "2 selected Pods"

			By("reporting SelectorOverlap on both WorkloadClasses without an enforced class")
			eventuallyStatus("wc-broad", workloadsv1alpha1.ReasonSelectorOverlap, workloadsv1alpha1.ReadinessUnknown, "")
			eventuallyStatus("wc-web", workloadsv1alpha1.ReasonSelectorOverlap, workloadsv1alpha1.ReadinessUnknown, "")

			By("counting every Pod for the enforced class only, and not reporting overlap, once the label is set")
			setEnforcedClass("wc-broad")
			eventuallyStatus("wc-broad", workloadsv1alpha1.ReasonNoGuardrails, workloadsv1alpha1.ReadinessNotReady, bothPods)
			eventuallyStatus("wc-web", workloadsv1alpha1.ReasonNoGuardrails, workloadsv1alpha1.ReadinessReady, "")

			By("moving the Pods to the new enforced class when the label changes")
			setEnforcedClass("wc-web")
			eventuallyStatus("wc-broad", workloadsv1alpha1.ReasonNoGuardrails, workloadsv1alpha1.ReadinessReady, "")
			eventuallyStatus("wc-web", workloadsv1alpha1.ReasonNoGuardrails, workloadsv1alpha1.ReadinessNotReady, bothPods)

			By("counting the Pods for no WorkloadClass when the label names a missing WorkloadClass")
			setEnforcedClass("wc-missing")
			eventuallyStatus("wc-broad", workloadsv1alpha1.ReasonNoGuardrails, workloadsv1alpha1.ReadinessReady, "")
			eventuallyStatus("wc-web", workloadsv1alpha1.ReasonNoGuardrails, workloadsv1alpha1.ReadinessReady, "")

			By("reporting SelectorOverlap again once the label is removed")
			setEnforcedClass("")
			eventuallyStatus("wc-broad", workloadsv1alpha1.ReasonSelectorOverlap, workloadsv1alpha1.ReadinessUnknown, "")
			eventuallyStatus("wc-web", workloadsv1alpha1.ReasonSelectorOverlap, workloadsv1alpha1.ReadinessUnknown, "")
		})

		It("should create, update, and restore the Per-Pod PDBs of a valid WorkloadClass, and open them when it fails validation", func() {
			const wcName = "wc-pdbs"
			key := types.NamespacedName{Name: wcName, Namespace: namespace}
			podLabels := map[string]string{"app": "pdbs"}
			Expect(k8sClient.Create(ctx, &workloadsv1alpha1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: guardrailName},
				Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{Constraints: workloadsv1alpha1.Constraints{
					Disruption: workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 10},
				}},
			})).To(Succeed())
			// The window is never open during the test, so the WorkloadClass is NotReady.
			now := time.Now().UTC()
			laterWindow := []workloadsv1alpha1.DisruptionWindow{{
				Name:      "later",
				TimeZone:  "UTC",
				StartTime: now.Add(2 * time.Hour).Format("15:04"),
				EndTime:   now.Add(3 * time.Hour).Format("15:04"),
			}}
			Expect(k8sClient.Create(ctx, &workloadsv1alpha1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: wcName, Namespace: namespace},
				Spec: workloadsv1alpha1.WorkloadClassSpec{
					PodSelector: &metav1.LabelSelector{MatchLabels: podLabels},
					DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 7,
						AllowedDisruptionWindows:     laterWindow,
					},
				},
			})).To(Succeed())
			for _, name := range []string{"pdbs-1", "pdbs-2"} {
				Expect(k8sClient.Create(ctx, &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: podLabels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
				})).To(Succeed())
			}

			pdbKey := func(podName string) types.NamespacedName {
				return types.NamespacedName{Name: utils.PDBName(podName), Namespace: namespace}
			}
			eventuallyPDBs := func(maxUnavailable string) {
				GinkgoHelper()
				wc := &workloadsv1alpha1.WorkloadClass{}
				Expect(k8sClient.Get(ctx, key, wc)).To(Succeed())
				Eventually(func(g Gomega) {
					for _, podName := range []string{"pdbs-1", "pdbs-2"} {
						pod := &corev1.Pod{}
						g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: podName, Namespace: namespace}, pod)).To(Succeed())
						g.Expect(pod.Labels).To(HaveKeyWithValue(workloadsv1alpha1.PodUIDLabel, string(pod.UID)))

						pdb := &policyv1.PodDisruptionBudget{}
						g.Expect(k8sClient.Get(ctx, pdbKey(podName), pdb)).To(Succeed())
						owner := metav1.GetControllerOf(pdb)
						g.Expect(owner).NotTo(BeNil())
						g.Expect(owner.UID).To(Equal(wc.UID))
						g.Expect(owner.BlockOwnerDeletion).To(HaveValue(BeTrue()))
						g.Expect(pdb.Spec.Selector.MatchLabels).To(Equal(map[string]string{workloadsv1alpha1.PodUIDLabel: string(pod.UID)}))
						g.Expect(pdb.Spec.UnhealthyPodEvictionPolicy).To(HaveValue(Equal(policyv1.AlwaysAllow)))
						g.Expect(pdb.Spec.MaxUnavailable.String()).To(Equal(maxUnavailable))
					}
				}).Should(Succeed())
			}
			updatePolicy := func(mutate func(*workloadsv1alpha1.DisruptionPolicy)) {
				GinkgoHelper()
				wc := &workloadsv1alpha1.WorkloadClass{}
				Expect(k8sClient.Get(ctx, key, wc)).To(Succeed())
				mutate(&wc.Spec.DisruptionPolicy)
				Expect(k8sClient.Update(ctx, wc)).To(Succeed())
			}

			By("labeling the Pods and creating closed PDBs while the WorkloadClass is NotReady")
			eventuallyPDBs("0")

			By("restoring a deleted PDB")
			deleted := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, pdbKey("pdbs-1"), deleted)).To(Succeed())
			Expect(k8sClient.Delete(ctx, deleted)).To(Succeed())
			Eventually(func(g Gomega) {
				restored := &policyv1.PodDisruptionBudget{}
				g.Expect(k8sClient.Get(ctx, pdbKey("pdbs-1"), restored)).To(Succeed())
				g.Expect(restored.UID).NotTo(Equal(deleted.UID))
			}).Should(Succeed())

			By("restoring an edited PDB")
			edited := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, pdbKey("pdbs-2"), edited)).To(Succeed())
			edited.Spec.MaxUnavailable = ptr.To(intstr.FromString("100%"))
			Expect(k8sClient.Update(ctx, edited)).To(Succeed())
			eventuallyPDBs("0")

			By("opening the PDBs when the WorkloadClass becomes Ready because its window changes")
			updatePolicy(func(p *workloadsv1alpha1.DisruptionPolicy) { p.AllowedDisruptionWindows = nil })
			eventuallyPDBs("100%")

			By("closing the PDBs when the WorkloadClass becomes NotReady again")
			updatePolicy(func(p *workloadsv1alpha1.DisruptionPolicy) { p.AllowedDisruptionWindows = laterWindow })
			eventuallyPDBs("0")

			By("opening the PDBs when the WorkloadClass fails validation")
			updatePolicy(func(p *workloadsv1alpha1.DisruptionPolicy) { p.MaxNonDisruptionDurationDays = 20 })
			eventuallyCondition(wcName, metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
			eventuallyPDBs("100%")
		})

		It("should delete the Per-Pod PDBs of deleted Pods and of Pods relabeled out of the WorkloadClass", func() {
			podLabels := map[string]string{"app": "cleanup"}
			Expect(k8sClient.Create(ctx, &workloadsv1alpha1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: guardrailName},
				Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{Constraints: workloadsv1alpha1.Constraints{
					Disruption: workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 10},
				}},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, &workloadsv1alpha1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: "wc-cleanup", Namespace: namespace},
				Spec: workloadsv1alpha1.WorkloadClassSpec{
					PodSelector:      &metav1.LabelSelector{MatchLabels: podLabels},
					DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{MaxNonDisruptionDurationDays: 7},
				},
			})).To(Succeed())
			pods := map[string]*corev1.Pod{}
			for _, name := range []string{"cleanup-1", "cleanup-2", "cleanup-3"} {
				pods[name] = &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: podLabels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
				}
				Expect(k8sClient.Create(ctx, pods[name])).To(Succeed())
			}
			eventuallyPDBsFor := func(podNames ...string) {
				GinkgoHelper()
				want := make([]string, 0, len(podNames))
				for _, name := range podNames {
					want = append(want, utils.PDBName(name))
				}
				Eventually(func(g Gomega) {
					pdbs := &policyv1.PodDisruptionBudgetList{}
					g.Expect(k8sClient.List(ctx, pdbs, client.InNamespace(namespace))).To(Succeed())
					var got []string
					for _, pdb := range pdbs.Items {
						if strings.HasPrefix(pdb.Name, utils.PDBName("cleanup-")) {
							got = append(got, pdb.Name)
						}
					}
					g.Expect(got).To(ConsistOf(want))
				}).Should(Succeed())
			}

			By("creating a PDB for each Pod")
			eventuallyPDBsFor("cleanup-1", "cleanup-2", "cleanup-3")

			By("deleting the PDB of a deleted Pod, leaving no stale PDB after a scale down")
			Expect(k8sClient.Delete(ctx, pods["cleanup-3"], client.GracePeriodSeconds(0))).To(Succeed())
			eventuallyPDBsFor("cleanup-1", "cleanup-2")

			By("deleting the PDB of a Pod relabeled out of the PodSelector")
			relabeled := pods["cleanup-2"]
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(relabeled), relabeled)).To(Succeed())
			patch := client.MergeFrom(relabeled.DeepCopy())
			relabeled.Labels["app"] = "elsewhere"
			Expect(k8sClient.Patch(ctx, relabeled, patch)).To(Succeed())
			eventuallyPDBsFor("cleanup-1")
		})
	})
})

// deleteAllPods removes the finalizers of every Pod in the namespace and deletes the Pods immediately.
func deleteAllPods(namespace string) {
	GinkgoHelper()
	pods := &corev1.PodList{}
	Expect(k8sClient.List(ctx, pods, client.InNamespace(namespace))).To(Succeed())
	for i := range pods.Items {
		pod := &pods.Items[i]
		if len(pod.Finalizers) > 0 {
			pod.Finalizers = nil
			Expect(client.IgnoreNotFound(k8sClient.Update(ctx, pod))).To(Succeed())
		}
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0)))).To(Succeed())
	}
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.List(ctx, pods, client.InNamespace(namespace))).To(Succeed())
		g.Expect(pods.Items).To(BeEmpty())
	}).Should(Succeed())
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(workloadsv1alpha1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(policyv1.AddToScheme(scheme))
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

// testEnforcingNamespace returns a Namespace whose workloads.x-k8s.io/enforced-class label names enforcedClass.
func testEnforcingNamespace(name, enforcedClass string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: map[string]string{workloadsv1alpha1.EnforcedWorkloadClassLabel: enforcedClass},
	}}
}

// namespaceGetError returns interceptor functions that fail every Get of a Namespace with err.
func namespaceGetError(err error) interceptor.Funcs {
	return interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Namespace); ok {
				return err
			}
			return c.Get(ctx, key, obj, opts...)
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
		{
			name:     "enforced_namespace_does_not_report_overlap",
			existing: []client.Object{wcCurrent, testWorkloadClass("wc-same", ns, nginx), testEnforcingNamespace(ns, "wc-current")},
		},
		{
			name:     "enforced_label_naming_a_missing_class_does_not_report_overlap",
			existing: []client.Object{wcCurrent, testWorkloadClass("wc-same", ns, nginx), testEnforcingNamespace(ns, "wc-missing")},
		},
		{
			name:      "empty_enforced_label_reports_overlap",
			existing:  []client.Object{wcCurrent, testWorkloadClass("wc-same", ns, nginx), testEnforcingNamespace(ns, "")},
			wantNames: []string{"wc-same"},
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

	t.Run("every_workload_class_in_a_namespace", func(t *testing.T) {
		got := names(r.namespaceWorkloadClasses(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns-1"}}))
		want := []string{"ns-1/wc-a", "ns-1/wc-b"}
		if !slices.Equal(got, want) {
			t.Errorf("namespaceWorkloadClasses() = %v, want %v", got, want)
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
		started       *time.Time
		wantPassed    bool
		wantRemaining time.Duration
	}{
		{name: "just_created", created: now, wantRemaining: 48 * time.Hour},
		{name: "pending_for_one_day", created: now.Add(-24 * time.Hour), wantRemaining: 24 * time.Hour},
		{name: "pending_for_exactly_min_duration", created: now.Add(-48 * time.Hour), wantPassed: true},
		{name: "pending_for_longer_than_min_duration", created: now.AddDate(0, 0, -10), wantPassed: true},
		{
			name:          "created_long_ago_but_started_recently",
			created:       now.AddDate(0, 0, -10),
			started:       ptr.To(now.Add(-24 * time.Hour)),
			wantRemaining: 24 * time.Hour,
		},
		{
			name:       "started_longer_than_min_duration_ago",
			created:    now.AddDate(0, 0, -10),
			started:    ptr.To(now.Add(-49 * time.Hour)),
			wantPassed: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pod := testPod("pod", nil, tc.created)
			if tc.started != nil {
				pod.Status.StartTime = ptr.To(metav1.NewTime(*tc.started))
			}
			gotPassed, gotRemaining := minInitialRunPassed(wc, pod, now)
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
		lastTerminate *time.Time
		wantPassed    bool
		wantRemaining time.Duration
	}{
		{name: "no_pod_observed_terminating", grace: 30, wantPassed: true},
		{name: "terminating_now", grace: 30, lastTerminate: ptr.To(now), wantRemaining: 30 * time.Second},
		{
			name:          "terminating_partway_through_grace_period",
			grace:         30,
			lastTerminate: ptr.To(now.Add(-10 * time.Second)),
			wantRemaining: 20 * time.Second,
		},
		{name: "terminated_exactly_grace_period_ago", grace: 30, lastTerminate: ptr.To(now.Add(-30 * time.Second)), wantPassed: true},
		{name: "terminated_beyond_grace_period", grace: 86400, lastTerminate: ptr.To(now.AddDate(0, 0, -2)), wantPassed: true},
		{name: "grace_period_unset", grace: 0, lastTerminate: ptr.To(now), wantPassed: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := &workloadsv1alpha1.WorkloadClass{
				Spec: workloadsv1alpha1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{GraceTerminationDurationSeconds: tc.grace},
				},
			}
			if tc.lastTerminate != nil {
				wc.Status.LastPodTerminationTime = ptr.To(metav1.NewTime(*tc.lastTerminate))
			}

			gotPassed, gotRemaining := gracePeriodPassed(wc, now)
			if gotPassed != tc.wantPassed || gotRemaining != tc.wantRemaining {
				t.Errorf("gracePeriodPassed() = (%v, %v), want (%v, %v)", gotPassed, gotRemaining, tc.wantPassed, tc.wantRemaining)
			}
		})
	}
}

func TestRecordPodTerminations(t *testing.T) {
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	created := now.AddDate(0, 0, -5)

	withGrace := func(pod *corev1.Pod, seconds int64) *corev1.Pod {
		pod.DeletionGracePeriodSeconds = ptr.To(seconds)
		return pod
	}
	succeeded := terminatingTestPod("succeeded", nil, created, now)
	succeeded.Status.Phase = corev1.PodSucceeded

	testCases := []struct {
		name     string
		existing *time.Time
		pods     []corev1.Pod
		want     *time.Time
	}{
		{
			name: "no_terminating_pods_leaves_it_unset",
			pods: []corev1.Pod{*testPod("running", nil, created)},
		},
		{
			name:     "no_terminating_pods_keeps_the_existing_time",
			existing: ptr.To(now.Add(-time.Hour)),
			pods:     []corev1.Pod{*testPod("running", nil, created)},
			want:     ptr.To(now.Add(-time.Hour)),
		},
		{
			name: "records_the_most_recent_termination_start",
			pods: []corev1.Pod{
				*terminatingTestPod("older", nil, created, now.Add(-10*time.Minute)),
				*terminatingTestPod("newer", nil, created, now.Add(-time.Minute)),
			},
			want: ptr.To(now.Add(-time.Minute)),
		},
		{
			name: "subtracts_the_deletion_grace_period",
			pods: []corev1.Pod{*withGrace(terminatingTestPod("graceful", nil, created, now), 30)},
			want: ptr.To(now.Add(-30 * time.Second)),
		},
		{
			name:     "does_not_move_backwards",
			existing: ptr.To(now),
			pods:     []corev1.Pod{*terminatingTestPod("older", nil, created, now.Add(-time.Hour))},
			want:     ptr.To(now),
		},
		{
			name: "ignores_pods_in_a_terminal_phase",
			pods: []corev1.Pod{*succeeded},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := &workloadsv1alpha1.WorkloadClass{}
			if tc.existing != nil {
				wc.Status.LastPodTerminationTime = ptr.To(metav1.NewTime(*tc.existing))
			}
			recordPodTerminations(wc, tc.pods)

			got := wc.Status.LastPodTerminationTime
			switch {
			case got == nil && tc.want == nil:
			case got == nil || tc.want == nil || !got.Time.Equal(*tc.want):
				t.Errorf("recordPodTerminations() set LastPodTerminationTime = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDays(t *testing.T) {
	if got := days(2); got != 48*time.Hour {
		t.Errorf("days(2) = %v, want 48h", got)
	}
	if got := days(math.MaxInt32); got != math.MaxInt64 {
		t.Errorf("days(MaxInt32) = %v, want MaxInt64", got)
	}
	// A saturated duration must still count as "not elapsed", not wrap around to an elapsed negative duration.
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	wc := &workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(now.AddDate(-1, 0, 0))},
		Spec: workloadsv1alpha1.WorkloadClassSpec{
			DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{MaxNonDisruptionDurationDays: math.MaxInt32},
		},
	}
	if isOverdue, _ := overdue(wc, now); isOverdue {
		t.Error("overdue() = true for MaxNonDisruptionDurationDays = MaxInt32, want false")
	}
}

func TestCalculateReadiness(t *testing.T) {
	// 2026-04-20 is a Monday.
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	selected := map[string]string{"app": "nginx"}
	validated := metav1.Condition{Type: workloadsv1alpha1.ConditionTypeValidated, Status: metav1.ConditionTrue}
	notValidated := metav1.Condition{
		Type: workloadsv1alpha1.ConditionTypeValidated, Status: metav1.ConditionFalse, Reason: workloadsv1alpha1.ReasonValidationFailed,
	}
	overlapping := metav1.Condition{
		Type: workloadsv1alpha1.ConditionTypeValidated, Status: metav1.ConditionFalse, Reason: workloadsv1alpha1.ReasonSelectorOverlap,
	}
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
	withPhase := func(pod *corev1.Pod, phase corev1.PodPhase) *corev1.Pod {
		pod.Status.Phase = phase
		return pod
	}

	testCases := []struct {
		name                string
		wc                  *workloadsv1alpha1.WorkloadClass
		validated           metav1.Condition
		pods                []client.Object
		wantReadiness       workloadsv1alpha1.MaintenanceReadiness
		wantReason          string
		wantRequeue         time.Duration
		wantLastTermination *time.Time
	}{
		{
			name: "validation_failed_is_unknown",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.EmergencyOverride = true
			}),
			validated:     notValidated,
			wantReadiness: workloadsv1alpha1.ReadinessUnknown,
			wantReason:    workloadsv1alpha1.ReasonValidationFailed,
		},
		{
			name:          "selector_overlap_is_unknown_with_its_own_reason",
			wc:            newWC(func(*workloadsv1alpha1.DisruptionPolicy) {}),
			validated:     overlapping,
			wantReadiness: workloadsv1alpha1.ReadinessUnknown,
			wantReason:    workloadsv1alpha1.ReasonSelectorOverlap,
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
		},
		{
			name: "emergency_override_still_records_pod_terminations",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.GraceTerminationDurationSeconds = 600
				p.EmergencyOverride = true
			}),
			validated:           validated,
			pods:                []client.Object{terminatingTestPod("terminating", selected, now.AddDate(0, 0, -5), now.Add(-time.Minute))},
			wantReadiness:       workloadsv1alpha1.ReadinessReady,
			wantReason:          workloadsv1alpha1.ReasonEmergencyOverride,
			wantLastTermination: ptr.To(now.Add(-time.Minute)),
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
			wantRequeue:   6*24*time.Hour + transitionRequeueDelay,
		},
		{
			name:          "no_windows_and_no_max_non_disruption_duration_does_not_requeue",
			wc:            newWC(func(*workloadsv1alpha1.DisruptionPolicy) {}),
			validated:     validated,
			wantReadiness: workloadsv1alpha1.ReadinessReady,
			wantReason:    workloadsv1alpha1.ReasonWithinDisruptionWindow,
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
		},
		{
			name: "young_pods_in_a_terminal_phase_do_not_block",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.MinInitialRunDurationDays = 1
			}),
			validated: validated,
			pods: []client.Object{
				withPhase(testPod("succeeded", selected, now), corev1.PodSucceeded),
				withPhase(testPod("failed", selected, now), corev1.PodFailed),
			},
			wantReadiness: workloadsv1alpha1.ReadinessReady,
			wantReason:    workloadsv1alpha1.ReasonWithinDisruptionWindow,
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
			wantReadiness:       workloadsv1alpha1.ReadinessNotReady,
			wantReason:          workloadsv1alpha1.ReasonGraceTerminationInProgress,
			wantRequeue:         9*time.Minute + transitionRequeueDelay,
			wantLastTermination: ptr.To(now.Add(-time.Minute)),
		},
		{
			name: "pod_that_terminated_and_is_gone_still_blocks_within_grace_period",
			wc: func() *workloadsv1alpha1.WorkloadClass {
				wc := newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
					p.GraceTerminationDurationSeconds = 600
				})
				wc.Status.LastPodTerminationTime = ptr.To(metav1.NewTime(now.Add(-4 * time.Minute)))
				return wc
			}(),
			validated:           validated,
			wantReadiness:       workloadsv1alpha1.ReadinessNotReady,
			wantReason:          workloadsv1alpha1.ReasonGraceTerminationInProgress,
			wantRequeue:         6*time.Minute + transitionRequeueDelay,
			wantLastTermination: ptr.To(now.Add(-4 * time.Minute)),
		},
		{
			name: "terminating_pods_beyond_grace_period_do_not_block",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.GraceTerminationDurationSeconds = 600
			}),
			validated:           validated,
			pods:                []client.Object{terminatingTestPod("terminated", selected, now.AddDate(0, 0, -5), now.Add(-time.Hour))},
			wantReadiness:       workloadsv1alpha1.ReadinessReady,
			wantReason:          workloadsv1alpha1.ReasonWithinDisruptionWindow,
			wantLastTermination: ptr.To(now.Add(-time.Hour)),
		},
		{
			name: "terminating_young_pods_are_only_subject_to_the_grace_period",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.MinInitialRunDurationDays = 1
			}),
			validated:           validated,
			pods:                []client.Object{terminatingTestPod("young-terminating", selected, now, now)},
			wantReadiness:       workloadsv1alpha1.ReadinessReady,
			wantReason:          workloadsv1alpha1.ReasonWithinDisruptionWindow,
			wantLastTermination: ptr.To(now),
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
			wantReadiness:       workloadsv1alpha1.ReadinessNotReady,
			wantReason:          workloadsv1alpha1.ReasonMinInitialRunDurationNotMet,
			wantRequeue:         time.Hour + transitionRequeueDelay,
			wantLastTermination: ptr.To(now),
		},
		{
			name: "young_pods_governed_by_a_more_specific_class_do_not_block",
			wc: newWC(func(p *workloadsv1alpha1.DisruptionPolicy) {
				p.MinInitialRunDurationDays = 1
			}),
			validated: validated,
			pods: []client.Object{
				testPod("young-web", map[string]string{"app": "nginx", "tier": "web"}, now),
				testWorkloadClass("wc-web", "ns", map[string]string{"app": "nginx", "tier": "web"}),
			},
			wantReadiness: workloadsv1alpha1.ReadinessReady,
			wantReason:    workloadsv1alpha1.ReasonWithinDisruptionWindow,
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
			if got.readiness != tc.wantReadiness || got.reason != tc.wantReason || got.requeueAfter != tc.wantRequeue {
				t.Errorf("calculateReadiness() = (%s, %s, requeue %v), want (%s, %s, requeue %v)",
					got.readiness, got.reason, got.requeueAfter, tc.wantReadiness, tc.wantReason, tc.wantRequeue)
			}
			if got.message == "" {
				t.Error("calculateReadiness() returned an empty message")
			}
			last := tc.wc.Status.LastPodTerminationTime
			switch {
			case last == nil && tc.wantLastTermination == nil:
			case last == nil || tc.wantLastTermination == nil || !last.Time.Equal(*tc.wantLastTermination):
				t.Errorf("calculateReadiness() set LastPodTerminationTime = %v, want %v", last, tc.wantLastTermination)
			}
		})
	}
}

func TestReadinessMessages(t *testing.T) {
	now := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	selected := map[string]string{"app": "nginx"}
	validated := metav1.Condition{Type: workloadsv1alpha1.ConditionTypeValidated, Status: metav1.ConditionTrue}

	testCases := []struct {
		name   string
		policy workloadsv1alpha1.DisruptionPolicy
		status workloadsv1alpha1.WorkloadClassStatus
		pods   []client.Object
		want   string
	}{
		{
			name:   "outside_window_names_the_next_opening",
			policy: workloadsv1alpha1.DisruptionPolicy{AllowedDisruptionWindows: []workloadsv1alpha1.DisruptionWindow{{Name: "w", StartTime: "22:00", EndTime: "23:00"}}},
			want:   "The current time is outside of every allowed disruption window; the next window opens at 2026-04-20T22:00:00Z",
		},
		{
			name:   "within_window_names_the_closing",
			policy: workloadsv1alpha1.DisruptionPolicy{AllowedDisruptionWindows: []workloadsv1alpha1.DisruptionWindow{{Name: "w", StartTime: "10:00", EndTime: "15:00"}}},
			want:   "The current time is within an allowed disruption window; the window closes at 2026-04-20T15:00:00Z",
		},
		{
			name:   "young_pods_are_counted_and_named",
			policy: workloadsv1alpha1.DisruptionPolicy{MinInitialRunDurationDays: 1},
			pods: []client.Object{
				testPod("a", selected, now.Add(-20*time.Hour)),
				testPod("b", selected, now.Add(-22*time.Hour)),
			},
			want: "2 selected Pods (e.g. 'a') have not yet run for `minInitialRunDurationDays`; the last one will at 2026-04-20T16:00:00Z",
		},
		{
			name:   "single_young_pod",
			policy: workloadsv1alpha1.DisruptionPolicy{MinInitialRunDurationDays: 1},
			pods:   []client.Object{testPod("a", selected, now.Add(-20*time.Hour))},
			want:   "1 selected Pod (e.g. 'a') has not yet run for `minInitialRunDurationDays`; the last one will at 2026-04-20T16:00:00Z",
		},
		{
			name:   "grace_period_names_both_times",
			policy: workloadsv1alpha1.DisruptionPolicy{GraceTerminationDurationSeconds: 600},
			status: workloadsv1alpha1.WorkloadClassStatus{LastPodTerminationTime: ptr.To(metav1.NewTime(now.Add(-4 * time.Minute)))},
			want:   "A selected Pod started terminating at 2026-04-20T11:56:00Z; `graceTerminationDurationSeconds` passes at 2026-04-20T12:06:00Z",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			wc := testWorkloadClass("wc", "ns", selected)
			wc.CreationTimestamp = metav1.NewTime(now.AddDate(0, 0, -1))
			wc.Spec.DisruptionPolicy = tc.policy
			wc.Status = tc.status

			scheme := newTestScheme(t)
			r := &WorkloadClassReconciler{
				Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.pods...).Build(),
				Scheme: scheme,
				Clock:  clocktesting.NewFakePassiveClock(now),
			}
			got, err := r.calculateReadiness(t.Context(), wc, validated)
			if err != nil {
				t.Fatalf("calculateReadiness() returned unexpected error: %v", err)
			}
			if !strings.HasPrefix(got.message, tc.want) {
				t.Errorf("calculateReadiness() message = %q, want %q", got.message, tc.want)
			}
		})
	}
}

func TestReconcileReadinessCheckFailed(t *testing.T) {
	wc := testWorkloadClass("wc", "ns", map[string]string{"app": "nginx"})
	wc.Spec.DisruptionPolicy.MinInitialRunDurationDays = 1
	wc.Generation = 3

	scheme := newTestScheme(t)
	listErr := fmt.Errorf("pods are unavailable")
	r := &WorkloadClassReconciler{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(wc).
			WithStatusSubresource(wc).
			WithInterceptorFuncs(interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, ok := list.(*corev1.PodList); ok {
						return listErr
					}
					return c.List(ctx, list, opts...)
				},
			}).
			Build(),
		Scheme: scheme,
	}

	result, err := r.Reconcile(t.Context(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(wc)})
	if !stderrors.Is(err, listErr) {
		t.Fatalf("Reconcile() error = %v, want %v", err, listErr)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("Reconcile() RequeueAfter = %v, want 0 (the error triggers a retry with backoff)", result.RequeueAfter)
	}

	got := &workloadsv1alpha1.WorkloadClass{}
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(wc), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.MaintenanceReadiness != workloadsv1alpha1.ReadinessUnknown {
		t.Errorf("MaintenanceReadiness = %s, want %s", got.Status.MaintenanceReadiness, workloadsv1alpha1.ReadinessUnknown)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, workloadsv1alpha1.ConditionTypeMaintenanceAllowed)
	if cond == nil {
		t.Fatal("MaintenanceAllowed condition not set")
	}
	if cond.Status != metav1.ConditionUnknown || cond.Reason != workloadsv1alpha1.ReasonReadinessCheckFailed || cond.ObservedGeneration != 3 {
		t.Errorf("MaintenanceAllowed = %+v, want Unknown/ReadinessCheckFailed with ObservedGeneration 3", cond)
	}
	if !strings.Contains(cond.Message, listErr.Error()) {
		t.Errorf("MaintenanceAllowed message = %q, want it to contain %q", cond.Message, listErr.Error())
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

	t.Run("long_message_is_truncated", func(t *testing.T) {
		result := readinessResult{readiness: workloadsv1alpha1.ReadinessReady, reason: "R", message: strings.Repeat("x", maxConditionMessageLen+1)}
		if got := len(result.condition(1).Message); got != maxConditionMessageLen {
			t.Errorf("condition() message length = %d, want %d", got, maxConditionMessageLen)
		}
	})
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
			name:   "starts_running",
			newPod: withChange(func(p *corev1.Pod) { p.Status.StartTime = ptr.To(metav1.Now()) }),
			want:   true,
		},
		{
			name:   "becomes_running_without_a_start_time_change",
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

	const podUID = "uid-pod"
	withLabels := func(podLabels map[string]string) *corev1.Pod {
		return withChange(func(p *corev1.Pod) {
			p.UID = podUID
			p.Labels = podLabels
		})
	}
	unlabeled := withLabels(map[string]string{"app": "nginx"})
	labeled := withLabels(map[string]string{"app": "nginx", workloadsv1alpha1.PodUIDLabel: podUID})
	podUIDCases := []struct {
		name           string
		oldPod, newPod *corev1.Pod
		want           bool
	}{
		{name: "pod_uid_label_set_by_the_controller", oldPod: unlabeled, newPod: labeled, want: false},
		{name: "pod_uid_label_removed", oldPod: labeled, newPod: unlabeled, want: true},
		{
			name:   "pod_uid_label_changed",
			oldPod: labeled,
			newPod: withLabels(map[string]string{"app": "nginx", workloadsv1alpha1.PodUIDLabel: "other"}),
			want:   true,
		},
		{
			name:   "other_label_changed_with_pod_uid_label",
			oldPod: labeled,
			newPod: withLabels(map[string]string{"app": "redis", workloadsv1alpha1.PodUIDLabel: podUID}),
			want:   true,
		},
	}
	for _, tc := range podUIDCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Update(event.UpdateEvent{ObjectOld: tc.oldPod, ObjectNew: tc.newPod}); got != tc.want {
				t.Errorf("Update() = %v, want %v", got, tc.want)
			}
		})
	}
}

// testPDBPod returns a Pod for the Per-Pod PDB tests, with the UID "uid-<name>".
func testPDBPod(name string, podLabels map[string]string) *corev1.Pod {
	pod := testPod(name, podLabels, time.Now())
	pod.UID = types.UID("uid-" + name)
	return pod
}

// testOwnedPDB returns the Per-Pod PDB of the Pod, controlled by the WorkloadClass.
func testOwnedPDB(wc *workloadsv1alpha1.WorkloadClass, pod *corev1.Pod, maxUnavailable intstr.IntOrString) *policyv1.PodDisruptionBudget {
	pdb := utils.PDBBase(pod)
	pdb.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(wc, workloadsv1alpha1.GroupVersion.WithKind("WorkloadClass")),
	}
	pdb.Spec = policyv1.PodDisruptionBudgetSpec{
		MaxUnavailable: &maxUnavailable,
		Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{workloadsv1alpha1.PodUIDLabel: string(pod.UID)}},
	}
	return pdb
}

func TestReconcilePDBs(t *testing.T) {
	nginx := map[string]string{"app": "nginx"}
	closed, open := intstr.FromInt32(0), intstr.FromString("100%")
	validated := func(status metav1.ConditionStatus, reason string) metav1.Condition {
		return metav1.Condition{Type: workloadsv1alpha1.ConditionTypeValidated, Status: status, Reason: reason}
	}
	passed := validated(metav1.ConditionTrue, workloadsv1alpha1.ReasonValidationPassed)
	failed := validated(metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
	newWC := func(name string, readiness workloadsv1alpha1.MaintenanceReadiness) *workloadsv1alpha1.WorkloadClass {
		wc := testWorkloadClass(name, "ns", nginx)
		wc.UID = types.UID("uid-" + name)
		wc.Status.MaintenanceReadiness = readiness
		return wc
	}
	notReadyWC := newWC("wc", workloadsv1alpha1.ReadinessNotReady)
	readyWC := newWC("wc", workloadsv1alpha1.ReadinessReady)
	deletingWC := readyWC.DeepCopy()
	deletingWC.DeletionTimestamp = ptr.To(metav1.Now())
	succeeded := testPDBPod("succeeded", nginx)
	succeeded.Status.Phase = corev1.PodSucceeded
	userPDB := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "workload-web-1", Namespace: "ns"},
		Spec:       policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "unrelated"}}},
	}
	// wc-web is more specific than wc for the Pod web-1.
	webPod := testPDBPod("web-1", map[string]string{"app": "nginx", "tier": "web"})
	webWC := func(cond metav1.Condition) *workloadsv1alpha1.WorkloadClass {
		wc := newWC("wc-web", "")
		wc.Spec.PodSelector.MatchLabels = map[string]string{"app": "nginx", "tier": "web"}
		wc.Status.Conditions = []metav1.Condition{cond}
		return wc
	}
	deletingWebWC := webWC(passed)
	deletingWebWC.DeletionTimestamp = ptr.To(metav1.Now())
	deletingWebWC.Finalizers = []string{"workloads.x-k8s.io/test"}
	protectingWC := notReadyWC.DeepCopy()
	protectingWC.Status.Conditions = []metav1.Condition{passed}
	// Pods whose PDB name is truncated, carrying the Pod UID label that the controller adds. utils.PDBName trims the
	// hyphens before the hash suffix of trimmedPod's PDB name, so it is shorter than MaxPDBNameLength.
	labeledPDBPod := func(name string) *corev1.Pod {
		pod := testPDBPod(name, map[string]string{"app": "nginx"})
		pod.Labels[workloadsv1alpha1.PodUIDLabel] = string(pod.UID)
		return pod
	}
	longPod, goneLongPod := labeledPDBPod(strings.Repeat("a", 250)), testPDBPod(strings.Repeat("b", 250), nginx)
	trimmedPod := labeledPDBPod(strings.Repeat("c", 230) + strings.Repeat("-", 10) + strings.Repeat("c", 10))
	if n := len(utils.PDBName(trimmedPod.Name)); n >= utils.MaxPDBNameLength {
		t.Fatalf("len(PDBName(trimmedPod)) = %d, want < %d", n, utils.MaxPDBNameLength)
	}
	// recreated replaced a Pod with the same name (e.g. a StatefulSet Pod), so its PDB still selects the old UID.
	recreated := testPDBPod("web-0", nginx)
	recreated.UID = "uid-web-0-recreated"

	// wantPDB is the expected controller and maxUnavailable of a PDB. An empty owner means a PDB that is not managed
	// by a WorkloadClass and must be left untouched. leftAlone means a PDB that the reconciled WorkloadClass controls
	// but leaves for the WorkloadClass that now governs its Pod.
	type wantPDB struct {
		owner, maxUnavailable string
		leftAlone             bool
	}
	testCases := []struct {
		name      string
		wc        *workloadsv1alpha1.WorkloadClass
		validated metav1.Condition
		objects   []client.Object
		wantErr   string
		wantPDBs  map[string]*wantPDB // PDB name -> expected PDB, or nil if it must not exist
	}{
		{
			name:      "not_ready_class_creates_closed_pdbs_for_governed_pods",
			wc:        notReadyWC,
			validated: passed,
			objects:   []client.Object{testPDBPod("web-1", nginx), testPDBPod("web-2", nginx), testPDBPod("other", map[string]string{"app": "redis"})},
			wantPDBs: map[string]*wantPDB{
				"workload-web-1": {owner: "wc", maxUnavailable: "0"},
				"workload-web-2": {owner: "wc", maxUnavailable: "0"},
				"workload-other": nil,
			},
		},
		{
			name:      "ready_class_creates_open_pdbs",
			wc:        readyWC,
			validated: passed,
			objects:   []client.Object{testPDBPod("web-1", nginx)},
			wantPDBs:  map[string]*wantPDB{"workload-web-1": {owner: "wc", maxUnavailable: "100%"}},
		},
		{
			name:      "inactive_and_deleted_pods_get_no_pdb_and_lose_theirs",
			wc:        notReadyWC,
			validated: passed,
			objects: []client.Object{
				succeeded, terminatingTestPod("terminating", nginx, time.Now(), time.Now()), testPDBPod("web-1", nginx),
				testOwnedPDB(notReadyWC, succeeded, closed),
				testOwnedPDB(notReadyWC, testPDBPod("terminating", nginx), closed),
				testOwnedPDB(notReadyWC, testPDBPod("gone", nginx), closed),
				testOwnedPDB(notReadyWC, testPDBPod("web-1", nginx), closed),
			},
			wantPDBs: map[string]*wantPDB{
				"workload-succeeded":   nil,
				"workload-terminating": nil,
				"workload-gone":        nil,
				"workload-web-1":       {owner: "wc", maxUnavailable: "0"},
			},
		},
		{
			name:      "validation_failed_opens_owned_pdbs_and_creates_none",
			wc:        notReadyWC,
			validated: failed,
			objects: []client.Object{
				testPDBPod("web-1", nginx), testPDBPod("web-2", nginx), succeeded,
				testOwnedPDB(notReadyWC, testPDBPod("web-1", nginx), closed),
				testOwnedPDB(notReadyWC, testPDBPod("gone", nginx), closed),
				testOwnedPDB(notReadyWC, succeeded, closed),
			},
			wantPDBs: map[string]*wantPDB{
				"workload-web-1":     {owner: "wc", maxUnavailable: "100%"},
				"workload-web-2":     nil,
				"workload-gone":      nil,
				"workload-succeeded": nil,
			},
		},
		{
			name:      "pdb_of_pod_relabeled_out_is_deleted",
			wc:        notReadyWC,
			validated: passed,
			objects: []client.Object{
				testPDBPod("moved", map[string]string{"app": "redis"}),
				testOwnedPDB(notReadyWC, testPDBPod("moved", nil), closed),
			},
			wantPDBs: map[string]*wantPDB{"workload-moved": nil},
		},
		{
			name:      "pdb_is_left_for_a_more_specific_protecting_class",
			wc:        notReadyWC,
			validated: passed,
			objects:   []client.Object{webWC(passed), webPod, testOwnedPDB(notReadyWC, webPod, closed)},
			wantPDBs:  map[string]*wantPDB{"workload-web-1": {owner: "wc", maxUnavailable: "0", leftAlone: true}},
		},
		{
			name:      "more_specific_protecting_class_takes_the_pdb_over",
			wc:        webWC(passed),
			validated: passed,
			objects:   []client.Object{protectingWC, webPod, testOwnedPDB(notReadyWC, webPod, open)},
			wantPDBs:  map[string]*wantPDB{"workload-web-1": {owner: "wc-web", maxUnavailable: "0"}},
		},
		{
			name:      "pdb_is_deleted_when_the_more_specific_class_does_not_protect",
			wc:        notReadyWC,
			validated: passed,
			objects:   []client.Object{webWC(failed), webPod, testOwnedPDB(notReadyWC, webPod, closed)},
			wantPDBs:  map[string]*wantPDB{"workload-web-1": nil},
		},
		{
			name:      "more_specific_class_that_does_not_protect_opens_its_pdb",
			wc:        webWC(failed),
			validated: failed,
			objects:   []client.Object{protectingWC, webPod, testOwnedPDB(webWC(failed), webPod, closed)},
			wantPDBs:  map[string]*wantPDB{"workload-web-1": {owner: "wc-web", maxUnavailable: "100%"}},
		},
		{
			name:      "pdb_is_handed_back_when_the_more_specific_class_is_deleting",
			wc:        notReadyWC,
			validated: passed,
			objects:   []client.Object{deletingWebWC, webPod, testOwnedPDB(deletingWebWC, webPod, open)},
			wantPDBs:  map[string]*wantPDB{"workload-web-1": {owner: "wc", maxUnavailable: "0"}},
		},
		{
			name:      "pdbs_are_matched_to_their_pod_by_uid",
			wc:        notReadyWC,
			validated: failed,
			objects: []client.Object{
				longPod, testOwnedPDB(notReadyWC, longPod, closed),
				trimmedPod, testOwnedPDB(notReadyWC, trimmedPod, closed),
				testOwnedPDB(notReadyWC, goneLongPod, closed),
				recreated, testOwnedPDB(notReadyWC, testPDBPod("web-0", nginx), closed),
			},
			wantPDBs: map[string]*wantPDB{
				utils.PDBName(longPod.Name):     {owner: "wc", maxUnavailable: "100%"},
				utils.PDBName(trimmedPod.Name):  {owner: "wc", maxUnavailable: "100%"},
				utils.PDBName(goneLongPod.Name): nil,
				// The Pod that now has the PDB's name gets it.
				"workload-web-0": {owner: "wc", maxUnavailable: "100%"},
			},
		},
		{
			name:      "pdb_of_a_recreated_governed_pod_is_kept_and_updated",
			wc:        notReadyWC,
			validated: passed,
			objects:   []client.Object{recreated, testOwnedPDB(notReadyWC, testPDBPod("web-0", nginx), open)},
			wantPDBs:  map[string]*wantPDB{"workload-web-0": {owner: "wc", maxUnavailable: "0"}},
		},
		{
			name:      "no_guardrails_creates_none",
			wc:        readyWC,
			validated: validated(metav1.ConditionTrue, workloadsv1alpha1.ReasonNoGuardrails),
			objects:   []client.Object{testPDBPod("web-1", nginx)},
			wantPDBs:  map[string]*wantPDB{"workload-web-1": nil},
		},
		{
			name:      "user_pdb_with_the_same_name_is_left_untouched",
			wc:        readyWC,
			validated: passed,
			objects:   []client.Object{testPDBPod("web-1", nginx), testPDBPod("web-2", nginx), userPDB},
			wantErr:   "PodDisruptionBudget ns/workload-web-1 already exists and is not managed by a WorkloadClass",
			wantPDBs: map[string]*wantPDB{
				"workload-web-1": {},
				"workload-web-2": {owner: "wc", maxUnavailable: "100%"},
			},
		},
		{
			name:      "pdb_controlled_by_another_class_is_taken_over",
			wc:        notReadyWC,
			validated: passed,
			objects: []client.Object{
				testPDBPod("web-1", nginx),
				testOwnedPDB(newWC("wc-old", workloadsv1alpha1.ReadinessReady), testPDBPod("web-1", nginx), open),
			},
			wantPDBs: map[string]*wantPDB{"workload-web-1": {owner: "wc", maxUnavailable: "0"}},
		},
		{
			name:      "deleting_class_is_skipped",
			wc:        deletingWC,
			validated: passed,
			objects:   []client.Object{testPDBPod("web-1", nginx)},
			wantPDBs:  map[string]*wantPDB{"workload-web-1": nil},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objects...).Build()
			r := &WorkloadClassReconciler{Client: c, Scheme: scheme}

			err := r.reconcilePDBs(t.Context(), tc.wc, tc.validated)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("reconcilePDBs() returned unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("reconcilePDBs() error = %v, want it to contain %q", err, tc.wantErr)
			}

			for name, want := range tc.wantPDBs {
				pdb := &policyv1.PodDisruptionBudget{}
				err := c.Get(t.Context(), client.ObjectKey{Name: name, Namespace: "ns"}, pdb)
				if want == nil {
					if !errors.IsNotFound(err) {
						t.Errorf("PDB %s: want it to not exist, got err = %v", name, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("PDB %s: want it to exist, got err = %v", name, err)
				}
				owner := metav1.GetControllerOf(pdb)
				if want.owner == "" {
					if owner != nil || pdb.Spec.MaxUnavailable != nil {
						t.Errorf("PDB %s: want it untouched, got controller %v and maxUnavailable %v", name, owner, pdb.Spec.MaxUnavailable)
					}
					continue
				}
				if owner == nil || owner.Name != want.owner || len(pdb.OwnerReferences) != 1 {
					t.Fatalf("PDB %s: ownerReferences = %v, want only the controller %s", name, pdb.OwnerReferences, want.owner)
				}
				if !ptr.Deref(owner.BlockOwnerDeletion, false) {
					t.Errorf("PDB %s: controller blockOwnerDeletion = %v, want true", name, owner.BlockOwnerDeletion)
				}
				if got := pdb.Spec.MaxUnavailable.String(); got != want.maxUnavailable {
					t.Errorf("PDB %s: maxUnavailable = %s, want %s", name, got, want.maxUnavailable)
				}
				if want.leftAlone {
					continue
				}

				pod, err := r.getPodForPDB(t.Context(), pdb)
				if err != nil || pod == nil {
					t.Fatalf("getPodForPDB(%s) = %v, %v, want the Pod", name, pod, err)
				}
				wantLabels := map[string]string{workloadsv1alpha1.PodUIDLabel: string(pod.UID)}
				if !maps.Equal(pdb.Spec.Selector.MatchLabels, wantLabels) || len(pdb.Spec.Selector.MatchExpressions) > 0 ||
					!maps.Equal(pdb.Labels, wantLabels) {
					t.Errorf("PDB %s: selector = %v, labels = %v, want both %v", name, pdb.Spec.Selector, pdb.Labels, wantLabels)
				}
				if p := pdb.Spec.UnhealthyPodEvictionPolicy; p == nil || *p != policyv1.AlwaysAllow {
					t.Errorf("PDB %s: unhealthyPodEvictionPolicy = %v, want %s", name, p, policyv1.AlwaysAllow)
				}
				if protectsPods(tc.validated) && pod.Labels[workloadsv1alpha1.PodUIDLabel] != string(pod.UID) {
					t.Errorf("Pod %s: labels = %v, want %s=%s", pod.Name, pod.Labels, workloadsv1alpha1.PodUIDLabel, pod.UID)
				}
			}
		})
	}
}

func TestDeletePDB(t *testing.T) {
	wc, other := testWorkloadClass("wc", "ns", nil), testWorkloadClass("wc-other", "ns", nil)
	wc.UID, other.UID = "uid-wc", "uid-wc-other"
	pod := testPDBPod("web-1", nil)
	ownedBy := func(owner *workloadsv1alpha1.WorkloadClass) *policyv1.PodDisruptionBudget {
		return testOwnedPDB(owner, pod, intstr.FromInt32(0))
	}
	// takeOverOnDelete simulates another WorkloadClass taking the PDB over between the read and the delete.
	takeOverOnDelete := interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			current := &policyv1.PodDisruptionBudget{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
				return err
			}
			current.OwnerReferences = ownedBy(other).OwnerReferences
			if err := c.Update(ctx, current); err != nil {
				return err
			}
			return c.Delete(ctx, obj, opts...)
		},
	}

	testCases := []struct {
		name        string
		stored      *policyv1.PodDisruptionBudget // nil if the PDB does not exist
		interceptor interceptor.Funcs
		wantErr     bool
		wantOwner   string // the controller of the PDB afterwards, or "" if it must be deleted
	}{
		{name: "deletes_a_controlled_pdb", stored: ownedBy(wc)},
		{name: "ignores_a_missing_pdb"},
		{name: "keeps_a_pdb_taken_over_before_the_read", stored: ownedBy(other), wantOwner: "wc-other"},
		{
			name:        "keeps_a_pdb_taken_over_after_the_read",
			stored:      ownedBy(wc),
			interceptor: takeOverOnDelete,
			wantErr:     true, // The resourceVersion precondition fails, so the request is retried.
			wantOwner:   "wc-other",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			builder := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(tc.interceptor)
			if tc.stored != nil {
				builder = builder.WithObjects(tc.stored)
			}
			c := builder.Build()
			r := &WorkloadClassReconciler{Client: c, Scheme: scheme}

			// The PDB is passed as the WorkloadClass listed it, while it still controlled it.
			if err := r.deletePDB(t.Context(), wc, ownedBy(wc)); (err != nil) != tc.wantErr {
				t.Fatalf("deletePDB() error = %v, wantErr %v", err, tc.wantErr)
			}
			pdb := &policyv1.PodDisruptionBudget{}
			err := c.Get(t.Context(), client.ObjectKey{Name: "workload-web-1", Namespace: "ns"}, pdb)
			if tc.wantOwner == "" {
				if !errors.IsNotFound(err) {
					t.Errorf("PDB: want it deleted, got err = %v", err)
				}
				return
			}
			if owner := metav1.GetControllerOf(pdb); err != nil || owner == nil || owner.Name != tc.wantOwner {
				t.Errorf("PDB: controller = %v (err = %v), want %s", owner, err, tc.wantOwner)
			}
		})
	}
}

func TestWorkloadClassesForPod(t *testing.T) {
	selecting := testWorkloadClass("wc-nginx", "ns", map[string]string{"app": "nginx"})
	selecting.UID = "uid-wc-nginx"
	previous := testWorkloadClass("wc-previous", "ns", map[string]string{"app": "redis"})
	previous.UID = "uid-wc-previous"
	pod := testPDBPod("pod", map[string]string{"app": "nginx"})

	testCases := []struct {
		name string
		pdb  *policyv1.PodDisruptionBudget
		want []string
	}{
		{name: "no_pdb", want: []string{"ns/wc-nginx"}},
		{name: "pdb_of_a_selecting_class", pdb: testOwnedPDB(selecting, pod, intstr.FromInt32(0)), want: []string{"ns/wc-nginx"}},
		{name: "pdb_of_another_class", pdb: testOwnedPDB(previous, pod, intstr.FromInt32(0)), want: []string{"ns/wc-nginx", "ns/wc-previous"}},
		{name: "user_pdb_with_the_same_name", pdb: utils.PDBBase(pod), want: []string{"ns/wc-nginx"}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(selecting, previous)
			if tc.pdb != nil {
				builder = builder.WithObjects(tc.pdb)
			}
			r := &WorkloadClassReconciler{Client: builder.Build(), Scheme: scheme}

			reqs := r.workloadClassesForPod(t.Context(), pod)
			got := make([]string, 0, len(reqs))
			for _, req := range reqs {
				got = append(got, req.String())
			}
			slices.Sort(got)
			if !slices.Equal(got, tc.want) {
				t.Errorf("workloadClassesForPod() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestProtectionChangedPredicate(t *testing.T) {
	withValidated := func(status metav1.ConditionStatus, reason string) *workloadsv1alpha1.WorkloadClass {
		wc := testWorkloadClass("wc", "ns", nil)
		wc.Status.Conditions = []metav1.Condition{{Type: workloadsv1alpha1.ConditionTypeValidated, Status: status, Reason: reason}}
		return wc
	}
	passed := withValidated(metav1.ConditionTrue, workloadsv1alpha1.ReasonValidationPassed)
	failed := withValidated(metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
	ready := passed.DeepCopy()
	ready.Status.MaintenanceReadiness = workloadsv1alpha1.ReadinessReady

	testCases := []struct {
		name         string
		oldWC, newWC *workloadsv1alpha1.WorkloadClass
		want         bool
	}{
		{name: "starts_protecting", oldWC: testWorkloadClass("wc", "ns", nil), newWC: passed, want: true},
		{name: "stops_protecting", oldWC: passed, newWC: failed, want: true},
		{name: "other_status_change", oldWC: passed, newWC: ready, want: false},
		{name: "stays_unprotected", oldWC: failed, newWC: withValidated(metav1.ConditionTrue, workloadsv1alpha1.ReasonNoGuardrails), want: false},
	}
	p := protectionChangedPredicate()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Update(event.UpdateEvent{ObjectOld: tc.oldWC, ObjectNew: tc.newWC}); got != tc.want {
				t.Errorf("Update() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLastDisruptionTimeChangedPredicate(t *testing.T) {
	now := metav1.Now()
	base := testWorkloadClass("wc", "ns", nil)
	withChange := func(mutate func(*workloadsv1alpha1.WorkloadClass)) *workloadsv1alpha1.WorkloadClass {
		wc := base.DeepCopy()
		mutate(wc)
		return wc
	}

	testCases := []struct {
		name  string
		newWC *workloadsv1alpha1.WorkloadClass
		want  bool
	}{
		{
			name: "other_status_change",
			newWC: withChange(func(wc *workloadsv1alpha1.WorkloadClass) {
				wc.Status.MaintenanceReadiness = workloadsv1alpha1.ReadinessReady
			}),
			want: false,
		},
		{
			name:  "last_disruption_time_set",
			newWC: withChange(func(wc *workloadsv1alpha1.WorkloadClass) { wc.Status.LastDisruptionTime = &now }),
			want:  true,
		},
	}
	p := lastDisruptionTimeChangedPredicate()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: tc.newWC}); got != tc.want {
				t.Errorf("Update() = %v, want %v", got, tc.want)
			}
		})
	}
	t.Run("same_time_in_different_pointers", func(t *testing.T) {
		oldWC := withChange(func(wc *workloadsv1alpha1.WorkloadClass) { wc.Status.LastDisruptionTime = ptr.To(now) })
		newWC := withChange(func(wc *workloadsv1alpha1.WorkloadClass) { wc.Status.LastDisruptionTime = ptr.To(now) })
		if p.Update(event.UpdateEvent{ObjectOld: oldWC, ObjectNew: newWC}) {
			t.Error("Update() = true, want false")
		}
	})
	if !p.Create(event.CreateEvent{Object: base}) {
		t.Error("Create() = false, want true")
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

func TestWorkloadClassesSelectingPodInEnforcedNamespace(t *testing.T) {
	wcs := []client.Object{
		testWorkloadClass("wc-nginx", "ns", map[string]string{"app": "nginx"}),
		testWorkloadClass("wc-redis", "ns", map[string]string{"app": "redis"}),
	}
	testCases := []struct {
		name    string
		ns      *corev1.Namespace
		nsError error
		want    []string
	}{
		{
			name: "enforced_class_regardless_of_selector",
			ns:   testEnforcingNamespace("ns", "wc-redis"),
			want: []string{"ns/wc-redis"},
		},
		{
			name: "enforced_label_naming_a_missing_class",
			ns:   testEnforcingNamespace("ns", "wc-missing"),
			want: []string{},
		},
		{
			name:    "namespace_read_error_requeues_every_class_in_the_namespace",
			nsError: fmt.Errorf("namespaces are unavailable"),
			want:    []string{"ns/wc-nginx", "ns/wc-redis"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wcs...)
			if tc.ns != nil {
				builder = builder.WithObjects(tc.ns)
			}
			if tc.nsError != nil {
				builder = builder.WithInterceptorFuncs(namespaceGetError(tc.nsError))
			}
			r := &WorkloadClassReconciler{Client: builder.Build(), Scheme: scheme}

			reqs := r.workloadClassesSelectingPod(t.Context(), testPod("pod", map[string]string{"app": "nginx"}, time.Now()))
			got := make([]string, 0, len(reqs))
			for _, req := range reqs {
				got = append(got, req.String())
			}
			slices.Sort(got)
			if !slices.Equal(got, tc.want) {
				t.Errorf("workloadClassesSelectingPod() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEnforcedClassLabelChangedPredicate(t *testing.T) {
	ns := func(nsLabels map[string]string) *corev1.Namespace {
		return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns", Labels: nsLabels}}
	}
	unlabeled := ns(map[string]string{"team": "a"})
	enforcedA := ns(map[string]string{workloadsv1alpha1.EnforcedWorkloadClassLabel: "wc-a"})
	enforcedB := ns(map[string]string{workloadsv1alpha1.EnforcedWorkloadClassLabel: "wc-b"})
	enforcedEmpty := ns(map[string]string{workloadsv1alpha1.EnforcedWorkloadClassLabel: ""})
	enforcedARelabeled := ns(map[string]string{workloadsv1alpha1.EnforcedWorkloadClassLabel: "wc-a", "team": "b"})

	p := enforcedClassLabelChangedPredicate()
	createCases := []struct {
		name string
		ns   *corev1.Namespace
		want bool
	}{
		{name: "create_with_label", ns: enforcedA, want: true},
		{name: "create_without_label", ns: unlabeled, want: false},
		{name: "create_with_empty_label", ns: enforcedEmpty, want: false},
	}
	for _, tc := range createCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Create(event.CreateEvent{Object: tc.ns}); got != tc.want {
				t.Errorf("Create() = %v, want %v", got, tc.want)
			}
		})
	}

	updateCases := []struct {
		name         string
		oldNS, newNS *corev1.Namespace
		want         bool
	}{
		{name: "label_added", oldNS: unlabeled, newNS: enforcedA, want: true},
		{name: "label_changed", oldNS: enforcedA, newNS: enforcedB, want: true},
		{name: "label_removed", oldNS: enforcedA, newNS: unlabeled, want: true},
		{name: "label_unchanged", oldNS: enforcedA, newNS: enforcedARelabeled, want: false},
		{name: "empty_label_added", oldNS: unlabeled, newNS: enforcedEmpty, want: false},
	}
	for _, tc := range updateCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Update(event.UpdateEvent{ObjectOld: tc.oldNS, ObjectNew: tc.newNS}); got != tc.want {
				t.Errorf("Update() = %v, want %v", got, tc.want)
			}
		})
	}

	if p.Delete(event.DeleteEvent{Object: enforcedA}) {
		t.Error("Delete() = true, want false")
	}
	if p.Generic(event.GenericEvent{Object: enforcedA}) {
		t.Error("Generic() = true, want false")
	}
}

func TestGovernedPods(t *testing.T) {
	nginx := map[string]string{"app": "nginx"}
	nginxWeb := map[string]string{"app": "nginx", "tier": "web"}
	wcNginx := testWorkloadClass("wc-nginx", "ns", nginx)
	wcNginxWeb := testWorkloadClass("wc-nginx-web", "ns", nginxWeb)
	wcNil := testWorkloadClass("wc-nil", "ns", nil)
	wcNil.Spec.PodSelector = nil
	objects := []client.Object{
		wcNginx, wcNginxWeb, wcNil,
		testPod("pod-nginx", nginx, time.Now()),
		testPod("pod-nginx-web", nginxWeb, time.Now()),
		testPod("pod-redis", map[string]string{"app": "redis"}, time.Now()),
	}

	testCases := []struct {
		name string
		wc   *workloadsv1alpha1.WorkloadClass
		ns   *corev1.Namespace
		want []string
	}{
		{
			name: "best_match_excludes_pods_of_a_more_specific_class",
			wc:   wcNginx,
			want: []string{"pod-nginx"},
		},
		{
			name: "more_specific_class_governs_its_pods",
			wc:   wcNginxWeb,
			want: []string{"pod-nginx-web"},
		},
		{
			name: "nil_selector_governs_nothing",
			wc:   wcNil,
		},
		{
			name: "enforced_class_governs_every_pod_regardless_of_selector",
			wc:   wcNil,
			ns:   testEnforcingNamespace("ns", "wc-nil"),
			want: []string{"pod-nginx", "pod-nginx-web", "pod-redis"},
		},
		{
			name: "other_classes_in_an_enforced_namespace_govern_nothing",
			wc:   wcNginxWeb,
			ns:   testEnforcingNamespace("ns", "wc-nginx"),
		},
		{
			name: "enforced_label_naming_a_missing_class_governs_nothing",
			wc:   wcNginx,
			ns:   testEnforcingNamespace("ns", "wc-missing"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...)
			if tc.ns != nil {
				builder = builder.WithObjects(tc.ns)
			}
			r := &WorkloadClassReconciler{Client: builder.Build(), Scheme: scheme}

			pods, err := r.governedPods(t.Context(), tc.wc)
			if err != nil {
				t.Fatalf("governedPods() returned unexpected error: %v", err)
			}
			got := make([]string, 0, len(pods))
			for _, pod := range pods {
				got = append(got, pod.Name)
			}
			slices.Sort(got)
			if !slices.Equal(got, tc.want) {
				t.Errorf("governedPods() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNamespaceReadErrorsAreReturned(t *testing.T) {
	wc := testWorkloadClass("wc", "ns", map[string]string{"app": "nginx"})
	nsErr := fmt.Errorf("namespaces are unavailable")
	scheme := newTestScheme(t)
	r := &WorkloadClassReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(wc).WithInterceptorFuncs(namespaceGetError(nsErr)).Build(),
		Scheme: scheme,
	}

	if _, err := r.overlappingWorkloadClasses(t.Context(), wc); !stderrors.Is(err, nsErr) {
		t.Errorf("overlappingWorkloadClasses() error = %v, want %v", err, nsErr)
	}
	if _, err := r.governedPods(t.Context(), wc); !stderrors.Is(err, nsErr) {
		t.Errorf("governedPods() error = %v, want %v", err, nsErr)
	}
}

func TestStripPodForReadiness(t *testing.T) {
	now := metav1.Now()
	pod := testPod("pod", map[string]string{"app": "nginx"}, now.Time)
	pod.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "kubectl"}}
	pod.Spec = corev1.PodSpec{NodeName: "node-1", Containers: []corev1.Container{{Name: "app", Image: "busybox"}}}
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, StartTime: &now, PodIP: "10.0.0.1"}

	obj, err := StripPodForReadiness(pod)
	if err != nil {
		t.Fatalf("StripPodForReadiness() returned unexpected error: %v", err)
	}
	got, ok := obj.(*corev1.Pod)
	if !ok {
		t.Fatalf("StripPodForReadiness() returned %T, want *corev1.Pod", obj)
	}
	if got.ManagedFields != nil || got.Spec.NodeName != "" || got.Spec.Containers != nil || got.Status.PodIP != "" {
		t.Errorf("StripPodForReadiness() kept fields that the controller does not read: %+v", got)
	}
	if got.Labels["app"] != "nginx" || got.Status.Phase != corev1.PodRunning || got.Status.StartTime == nil {
		t.Errorf("StripPodForReadiness() dropped fields that the controller reads: %+v", got)
	}

	other := &corev1.ConfigMap{}
	if obj, err := StripPodForReadiness(other); err != nil || obj != other {
		t.Errorf("StripPodForReadiness(ConfigMap) = (%v, %v), want the object unchanged", obj, err)
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
