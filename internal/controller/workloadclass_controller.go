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
	"maps"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
	"sigs.k8s.io/workload-class/internal/utils"
)

const (
	validationPassedMsg    = "WorkloadClass adheres to all Guardrail constraints"
	noGuardrailsMsg        = "No Guardrails found to validate against"
	disjointDaysOfWeekMsg  = "cluster guardrails restrict `daysOfWeek` to disjoint sets, so no disruption window can comply; contact your cluster administrator"
	maxConditionMessageLen = 32768
	maxReportedOverlaps    = 5

	notValidatedMsg             = "Maintenance readiness cannot be determined because the WorkloadClass failed validation; see the Validated condition"
	selectorOverlapReadinessMsg = "Maintenance readiness cannot be determined because the `podSelector` overlaps with another WorkloadClass; see the Validated condition"
	emergencyOverrideMsg        = "`emergencyOverride` is set, so maintenance can disrupt Pods at any time"
	overdueMsg                  = "The workload has remained undisrupted for at least `maxNonDisruptionDurationDays`, so maintenance takes precedence"
	noWindowsMsg                = "No `allowedDisruptionWindows` are defined, so maintenance can disrupt Pods at any time"
	withinWindowMsg             = "The current time is within an allowed disruption window"
	outsideWindowMsg            = "The current time is outside of every allowed disruption window"

	// transitionRequeueDelay is added to requeues scheduled at a readiness transition, such as a disruption window
	// opening or closing, so that the reconcile runs just after the transition and observes the new readiness.
	transitionRequeueDelay = time.Second
)

// WorkloadClassReconciler reconciles a WorkloadClass object
type WorkloadClassReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Clock is used to evaluate disruption windows and Pod run and termination durations.
	// If nil, the real clock is used.
	Clock clock.PassiveClock
}

// +kubebuilder:rbac:groups=workloads.x-k8s.io,resources=workloadclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=workloads.x-k8s.io,resources=workloadclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=workloads.x-k8s.io,resources=workloadclassguardrails,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// It validates the WorkloadClass against all WorkloadClassGuardrails and against the
// other WorkloadClasses in its namespace, and surfaces the result as the Validated condition.
// It then calculates the MaintenanceReadiness of the WorkloadClass, surfaces it with the
// MaintenanceAllowed condition, and requeues the WorkloadClass at the next readiness transition.
func (r *WorkloadClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	wc := &workloadsv1alpha1.WorkloadClass{}
	if err := r.Get(ctx, req.NamespacedName, wc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	guardrails := &workloadsv1alpha1.WorkloadClassGuardrailList{}
	if err := r.List(ctx, guardrails); err != nil {
		log.Error(err, "Failed to list WorkloadClassGuardrails")
		return ctrl.Result{}, err
	}

	overlapping, err := r.overlappingWorkloadClasses(ctx, wc)
	if err != nil {
		log.Error(err, "Failed to validate PodSelector against other WorkloadClasses")
		return ctrl.Result{}, err
	}

	cond := validatedCondition(wc, guardrails.Items, overlapping)

	// base is the WorkloadClass as read, so that the status patch below contains only the changed fields.
	base := wc.DeepCopy()

	readiness, readinessErr := r.calculateReadiness(ctx, wc, cond)
	if readinessErr != nil {
		log.Error(readinessErr, "Failed to calculate maintenance readiness")
		readiness = readinessResult{
			readiness: workloadsv1alpha1.ReadinessUnknown,
			reason:    workloadsv1alpha1.ReasonReadinessCheckFailed,
			message:   fmt.Sprintf("Failed to calculate maintenance readiness: %v", readinessErr),
		}
	}

	meta.SetStatusCondition(&wc.Status.Conditions, cond)
	meta.SetStatusCondition(&wc.Status.Conditions, readiness.condition(wc.Generation))
	wc.Status.MaintenanceReadiness = readiness.readiness

	if !equality.Semantic.DeepEqual(&base.Status, &wc.Status) {
		log.Info("Updating WorkloadClass status",
			"validated", cond.Reason, "maintenanceReadiness", readiness.readiness, "maintenanceAllowed", readiness.reason)
		if err := r.Status().Patch(ctx, wc, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
	}

	if readinessErr != nil {
		return ctrl.Result{}, readinessErr
	}
	return ctrl.Result{RequeueAfter: readiness.requeueAfter}, nil
}

// validatedCondition builds the Validated condition for the WorkloadClass.
//
// Guardrail and spec violations take precedence and are reported with ReasonValidationFailed.
// If the only problem is that the PodSelector overlaps with other WorkloadClasses in the namespace,
// ReasonSelectorOverlap is used instead. Overlap is intentionally reported symmetrically on every
// WorkloadClass involved so that any affected workload owner sees the conflict, while keeping a
// distinct Reason from ReasonValidationFailed.
func validatedCondition(
	wc *workloadsv1alpha1.WorkloadClass,
	guardrails []workloadsv1alpha1.WorkloadClassGuardrail,
	overlapping []workloadsv1alpha1.WorkloadClass,
) metav1.Condition {
	cond := metav1.Condition{
		Type:               workloadsv1alpha1.ConditionTypeValidated,
		Status:             metav1.ConditionTrue,
		Reason:             workloadsv1alpha1.ReasonValidationPassed,
		Message:            validationPassedMsg,
		ObservedGeneration: wc.Generation,
	}

	violations := validateSpec(wc)
	violations = append(violations, validateAgainstGuardrails(wc, guardrails)...)
	overlapMsg := selectorOverlapMessage(overlapping)

	switch {
	case len(violations) > 0:
		if overlapMsg != "" {
			violations = append(violations, overlapMsg)
		}
		cond.Status = metav1.ConditionFalse
		cond.Reason = workloadsv1alpha1.ReasonValidationFailed
		cond.Message = truncateMessage(strings.Join(violations, "; "), maxConditionMessageLen)
	case overlapMsg != "":
		cond.Status = metav1.ConditionFalse
		cond.Reason = workloadsv1alpha1.ReasonSelectorOverlap
		cond.Message = truncateMessage(overlapMsg, maxConditionMessageLen)
	case len(guardrails) == 0:
		cond.Reason = workloadsv1alpha1.ReasonNoGuardrails
		cond.Message = noGuardrailsMsg
	}

	return cond
}

// truncateMessage caps msg at maxLen bytes on a UTF-8 rune boundary so that it fits within the CRD
// schema limit for metav1.Condition.Message.
func truncateMessage(msg string, maxLen int) string {
	const ellipsis = "..."
	if len(msg) <= maxLen {
		return msg
	}
	if maxLen <= len(ellipsis) {
		return ellipsis[:maxLen]
	}
	cut := maxLen - len(ellipsis)
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut] + ellipsis
}

// validateSpec validates the fields of the WorkloadClass spec that are not fully enforced by the
// CRD OpenAPI/CEL schema (semantic LabelSelector validation, valid weekday names, and IANA time zone
// database lookup), and acts as defense-in-depth for rules also checked at admission time (such as
// rejecting the 'Local' time zone).
func validateSpec(wc *workloadsv1alpha1.WorkloadClass) []string {
	var violations []string
	if wc.Spec.PodSelector != nil {
		if _, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector); err != nil {
			violations = append(violations, fmt.Sprintf("`podSelector` must be a valid label selector: %v", err))
		}
	}
	for _, dw := range wc.Spec.DisruptionPolicy.AllowedDisruptionWindows {
		if err := utils.WeekdaysValid(dw.DaysOfWeek); err != nil {
			violations = append(violations, fmt.Sprintf("disruption window '%s': %v", dw.Name, err))
		}
		if dw.TimeZone != "" && !utils.TimeZoneValid(dw.TimeZone) {
			violations = append(violations, fmt.Sprintf(
				"`timeZone` of disruption window '%s' must be a valid IANA time zone name other than 'Local', got '%s'",
				dw.Name, dw.TimeZone))
		}
	}
	return violations
}

// validateAgainstGuardrails returns the guardrail constraints that the WorkloadClass violates.
//
// Guardrail names are deliberately omitted: guardrails are cluster-scoped, and users who can read the
// WorkloadClass status may not have access to them. Constraint limits across all guardrails are combined
// into the effective (strictest) limit—minimum MaxAllowedWindows, minimum MaxNonDisruptionDurationDays,
// and the intersection of restricting DaysOfWeek—so workload owners see a single actionable limit per field.
func validateAgainstGuardrails(wc *workloadsv1alpha1.WorkloadClass, guardrails []workloadsv1alpha1.WorkloadClassGuardrail) []string {
	if len(guardrails) == 0 {
		return nil
	}

	var (
		effectiveDays  = utils.AllWeekdays()
		daysRestricted bool
		minWindows     int32
		minDuration    int32
	)
	for i := range guardrails {
		d := guardrails[i].Spec.Constraints.Disruption
		if len(d.DaysOfWeek) > 0 {
			daysRestricted = true
			allowed := sets.New(d.DaysOfWeek...)
			effectiveDays = slices.DeleteFunc(effectiveDays, func(day string) bool {
				return !allowed.Has(day)
			})
		}
		if d.MaxAllowedWindows > 0 && (minWindows == 0 || d.MaxAllowedWindows < minWindows) {
			minWindows = d.MaxAllowedWindows
		}
		if d.MaxNonDisruptionDurationDays > 0 && (minDuration == 0 || d.MaxNonDisruptionDurationDays < minDuration) {
			minDuration = d.MaxNonDisruptionDurationDays
		}
	}
	if !daysRestricted {
		effectiveDays = nil
	}

	var violations []string
	if daysRestricted && len(effectiveDays) == 0 {
		violations = append(violations, disjointDaysOfWeekMsg)
	}

	effective := workloadsv1alpha1.WorkloadClassGuardrail{
		Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{
			Constraints: workloadsv1alpha1.Constraints{
				Disruption: workloadsv1alpha1.Disruption{
					DaysOfWeek:                   effectiveDays,
					MaxAllowedWindows:            minWindows,
					MaxNonDisruptionDurationDays: minDuration,
				},
			},
		},
	}
	violations = append(violations, utils.GuardrailViolations(wc, &effective)...)
	return violations
}

// overlappingWorkloadClasses returns the other active WorkloadClasses in the same namespace whose
// PodSelector could select the same Pod as the WorkloadClass PodSelector, sorted by name.
//
// Overlap is symmetric: every WorkloadClass whose selector overlaps with another active WorkloadClass in
// the namespace is reported with ReasonSelectorOverlap. WorkloadClasses that are being deleted or have an
// invalid PodSelector are skipped; an invalid PodSelector is reported on the WorkloadClass that owns it.
//
// TODO: Skip selector overlap checks when the Namespace has the workloads.x-k8s.io/enforced-class
// (EnforcedWorkloadClassLabel) label set to an active WorkloadClass, once the controller watches Namespaces.
func (r *WorkloadClassReconciler) overlappingWorkloadClasses(
	ctx context.Context, wc *workloadsv1alpha1.WorkloadClass,
) ([]workloadsv1alpha1.WorkloadClass, error) {
	workloadClasses := &workloadsv1alpha1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses, client.InNamespace(wc.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list WorkloadClasses: %w", err)
	}

	var matches []workloadsv1alpha1.WorkloadClass
	for _, other := range workloadClasses.Items {
		if other.Name == wc.Name || !other.DeletionTimestamp.IsZero() {
			continue
		}
		overlap, err := utils.SelectorsOverlap(wc.Spec.PodSelector, other.Spec.PodSelector)
		if err != nil {
			continue
		}
		if overlap {
			matches = append(matches, other)
		}
	}
	slices.SortFunc(matches, func(a, b workloadsv1alpha1.WorkloadClass) int {
		return strings.Compare(a.Name, b.Name)
	})
	return matches, nil
}

// selectorOverlapMessage returns a message listing the WorkloadClasses whose PodSelector overlaps
// with the PodSelector of the reconciled WorkloadClass, or an empty string if there are none.
// Names are sorted so the message does not depend on informer cache iteration order, and at most
// maxReportedOverlaps names are listed explicitly, followed by "and K more" if additional classes overlap.
func selectorOverlapMessage(overlapping []workloadsv1alpha1.WorkloadClass) string {
	if len(overlapping) == 0 {
		return ""
	}

	names := make([]string, 0, len(overlapping))
	for _, o := range overlapping {
		names = append(names, o.Name)
	}
	slices.Sort(names)

	limit := min(len(names), maxReportedOverlaps)
	listed := utils.QuotedList(names[:limit])
	if extra := len(names) - limit; extra > 0 {
		listed = fmt.Sprintf("%s, and %d more", listed, extra)
	}
	return fmt.Sprintf("`podSelector` must not select the same Pods as another WorkloadClass in the namespace, "+
		"but it overlaps with the `podSelector` of %s", listed)
}

// readinessResult is the MaintenanceReadiness of a WorkloadClass, the Reason and Message of its MaintenanceAllowed
// condition, and when the readiness should next be re-evaluated (0 if it only changes when the cluster changes).
type readinessResult struct {
	readiness    workloadsv1alpha1.MaintenanceReadiness
	reason       string
	message      string
	requeueAfter time.Duration
}

// condition returns the MaintenanceAllowed condition for the readiness result.
func (r readinessResult) condition(generation int64) metav1.Condition {
	status := metav1.ConditionFalse
	switch r.readiness {
	case workloadsv1alpha1.ReadinessReady, workloadsv1alpha1.ReadinessOverdue:
		status = metav1.ConditionTrue
	case workloadsv1alpha1.ReadinessUnknown:
		status = metav1.ConditionUnknown
	}
	return metav1.Condition{
		Type:               workloadsv1alpha1.ConditionTypeMaintenanceAllowed,
		Status:             status,
		Reason:             r.reason,
		Message:            truncateMessage(r.message, maxConditionMessageLen),
		ObservedGeneration: generation,
	}
}

// calculateReadiness calculates the MaintenanceReadiness of the WorkloadClass at the current time, and when it should
// next be re-evaluated. In order of precedence, the WorkloadClass is:
//
//  1. Unknown if it is not validated, since its disruption policy is not enforced.
//  2. Ready if EmergencyOverride is set.
//  3. Overdue if it has remained undisrupted for at least MaxNonDisruptionDurationDays.
//  4. NotReady if the current time is outside of every AllowedDisruptionWindow.
//  5. NotReady if a selected Pod has not yet run for MinInitialRunDurationDays, or a selected Pod started terminating
//     less than GraceTerminationDurationSeconds ago.
//  6. Ready otherwise.
//
// Unless the readiness only changes when the cluster changes (Unknown, EmergencyOverride, or Overdue), the
// WorkloadClass is requeued at the earliest of: the next disruption window start or end, the time it becomes overdue,
// and the time the selected Pods stop blocking maintenance.
//
// As a side effect, Status.LastPodTerminationTime is advanced to the most recent time a selected Pod was observed to
// start terminating, so that GraceTerminationDurationSeconds is honored after the Pod is gone.
func (r *WorkloadClassReconciler) calculateReadiness(
	ctx context.Context, wc *workloadsv1alpha1.WorkloadClass, validated metav1.Condition,
) (readinessResult, error) {
	if validated.Status != metav1.ConditionTrue {
		message := notValidatedMsg
		if validated.Reason == workloadsv1alpha1.ReasonSelectorOverlap {
			message = selectorOverlapReadinessMsg
		}
		return readinessResult{
			readiness: workloadsv1alpha1.ReadinessUnknown,
			reason:    validated.Reason,
			message:   message,
		}, nil
	}

	policy := wc.Spec.DisruptionPolicy
	now := r.now()

	// Observe the selected Pods on every reconcile so that terminating Pods are recorded before they are gone.
	var pods []corev1.Pod
	if policy.MinInitialRunDurationDays > 0 || policy.GraceTerminationDurationSeconds > 0 {
		var err error
		if pods, err = r.selectedPods(ctx, wc); err != nil {
			return readinessResult{}, err
		}
		recordPodTerminations(wc, pods)
	}

	if policy.EmergencyOverride {
		return readinessResult{
			readiness: workloadsv1alpha1.ReadinessReady,
			reason:    workloadsv1alpha1.ReasonEmergencyOverride,
			message:   emergencyOverrideMsg,
		}, nil
	}

	isOverdue, untilOverdue := overdue(wc, now)
	if isOverdue {
		return readinessResult{
			readiness: workloadsv1alpha1.ReadinessOverdue,
			reason:    workloadsv1alpha1.ReasonMaintenanceOverdue,
			message:   overdueMsg,
		}, nil
	}

	inWindow, untilWindowChange := utils.IsTimeInWindows(ctx, now, policy.AllowedDisruptionWindows)
	nextTransition := minPositiveDuration(untilOverdue, untilWindowChange)
	if !inWindow {
		return readinessResult{
			readiness:    workloadsv1alpha1.ReadinessNotReady,
			reason:       workloadsv1alpha1.ReasonOutsideDisruptionWindow,
			message:      withTime(outsideWindowMsg, "the next window opens at", now, untilWindowChange),
			requeueAfter: requeueAfterTransition(nextTransition),
		}, nil
	}

	if blocked, untilUnblocked := podsBlockingMaintenance(wc, pods, now); blocked != nil {
		blocked.requeueAfter = requeueAfterTransition(minPositiveDuration(nextTransition, untilUnblocked))
		return *blocked, nil
	}

	message := withTime(withinWindowMsg, "the window closes at", now, untilWindowChange)
	if len(policy.AllowedDisruptionWindows) == 0 {
		message = noWindowsMsg
	}
	return readinessResult{
		readiness:    workloadsv1alpha1.ReadinessReady,
		reason:       workloadsv1alpha1.ReasonWithinDisruptionWindow,
		message:      message,
		requeueAfter: requeueAfterTransition(nextTransition),
	}, nil
}

// now returns the current time in UTC from the reconciler's Clock, or from the real clock if it is unset.
func (r *WorkloadClassReconciler) now() time.Time {
	if r.Clock == nil {
		return time.Now().UTC()
	}
	return r.Clock.Now().UTC()
}

// selectedPods returns the Pods in the namespace of the WorkloadClass that are selected by its PodSelector.
// A nil PodSelector selects no Pods, and an empty PodSelector selects every Pod in the namespace.
//
// TODO: Select every Pod in the namespace when the Namespace has the workloads.x-k8s.io/enforced-class
// (EnforcedWorkloadClassLabel) label set to the WorkloadClass, once the controller watches Namespaces.
func (r *WorkloadClassReconciler) selectedPods(ctx context.Context, wc *workloadsv1alpha1.WorkloadClass) ([]corev1.Pod, error) {
	if wc.Spec.PodSelector == nil {
		return nil, nil
	}
	selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
	if err != nil {
		return nil, fmt.Errorf("invalid podSelector: %w", err)
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(wc.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, fmt.Errorf("failed to list Pods: %w", err)
	}
	return pods.Items, nil
}

// recordPodTerminations advances Status.LastPodTerminationTime of the WorkloadClass to the most recent time one of
// the Pods started terminating. Pods in a terminal phase are ignored.
func recordPodTerminations(wc *workloadsv1alpha1.WorkloadClass, pods []corev1.Pod) {
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp == nil || isPodTerminal(pod) {
			continue
		}
		started := podTerminationStart(pod)
		if wc.Status.LastPodTerminationTime == nil || started.After(wc.Status.LastPodTerminationTime.Time) {
			wc.Status.LastPodTerminationTime = ptr.To(metav1.NewTime(started))
		}
	}
}

// podTerminationStart returns the time the deletion of a terminating Pod was requested. The API server sets
// deletionTimestamp to the requested time plus deletionGracePeriodSeconds.
func podTerminationStart(pod *corev1.Pod) time.Time {
	return pod.DeletionTimestamp.Add(-time.Duration(ptr.Deref(pod.DeletionGracePeriodSeconds, 0)) * time.Second)
}

// podsBlockingMaintenance returns a NotReady readiness result if the Pods block maintenance, along with the time
// until the reported reason stops applying. It returns nil if no Pod blocks maintenance.
//
// A Pod that is running (not terminating and not in a terminal phase) blocks maintenance until it has run for
// MinInitialRunDurationDays. Separately, maintenance is blocked until GraceTerminationDurationSeconds have passed
// since Status.LastPodTerminationTime, the most recent time a selected Pod started terminating. If both apply,
// MinInitialRunDurationNotMet is reported.
func podsBlockingMaintenance(
	wc *workloadsv1alpha1.WorkloadClass, pods []corev1.Pod, now time.Time,
) (*readinessResult, time.Duration) {
	if wc.Spec.DisruptionPolicy.MinInitialRunDurationDays > 0 {
		var young []*corev1.Pod
		var untilRun time.Duration
		for i := range pods {
			pod := &pods[i]
			if pod.DeletionTimestamp != nil || isPodTerminal(pod) {
				continue
			}
			if passed, remaining := minInitialRunPassed(wc, pod, now); !passed {
				young = append(young, pod)
				untilRun = max(untilRun, remaining)
			}
		}
		if len(young) > 0 {
			return &readinessResult{
				readiness: workloadsv1alpha1.ReadinessNotReady,
				reason:    workloadsv1alpha1.ReasonMinInitialRunDurationNotMet,
				message: fmt.Sprintf("%d selected %s (e.g. '%s') %s not yet run for `minInitialRunDurationDays`; the last one will at %s",
					len(young), pluralize(len(young), "Pod", "Pods"), young[0].Name, pluralize(len(young), "has", "have"),
					now.Add(untilRun).Format(time.RFC3339)),
			}, untilRun
		}
	}

	if passed, remaining := gracePeriodPassed(wc, now); !passed {
		return &readinessResult{
			readiness: workloadsv1alpha1.ReadinessNotReady,
			reason:    workloadsv1alpha1.ReasonGraceTerminationInProgress,
			message: fmt.Sprintf("A selected Pod started terminating at %s; `graceTerminationDurationSeconds` passes at %s",
				wc.Status.LastPodTerminationTime.Format(time.RFC3339), now.Add(remaining).Format(time.RFC3339)),
		}, remaining
	}
	return nil, 0
}

// overdue returns true if the WorkloadClass has remained undisrupted for at least MaxNonDisruptionDurationDays,
// measured from Status.LastDisruptionTime, or from the creation of the WorkloadClass if no disruption has been
// observed. Otherwise, it also returns the time until the WorkloadClass becomes overdue, which is 0 if
// MaxNonDisruptionDurationDays is unset (no limit).
func overdue(wc *workloadsv1alpha1.WorkloadClass, now time.Time) (bool, time.Duration) {
	maxDays := wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays
	if maxDays <= 0 {
		return false, 0
	}

	undisruptedSince := wc.CreationTimestamp.Time
	if wc.Status.LastDisruptionTime != nil {
		undisruptedSince = wc.Status.LastDisruptionTime.Time
	}
	return elapsed(days(maxDays), undisruptedSince, now)
}

// minInitialRunPassed returns true if the Pod has run for the MinInitialRunDurationDays of the WorkloadClass,
// measured from its Status.StartTime, or from its creationTimestamp if it has not been acknowledged by a kubelet yet.
// Otherwise, it also returns the time remaining until it has.
func minInitialRunPassed(wc *workloadsv1alpha1.WorkloadClass, pod *corev1.Pod, now time.Time) (bool, time.Duration) {
	return elapsed(days(wc.Spec.DisruptionPolicy.MinInitialRunDurationDays), podStartTime(pod), now)
}

// podStartTime returns the time the Pod started running: its Status.StartTime if set, or its creationTimestamp.
func podStartTime(pod *corev1.Pod) time.Time {
	if pod.Status.StartTime != nil {
		return pod.Status.StartTime.Time
	}
	return pod.CreationTimestamp.Time
}

// gracePeriodPassed returns true if no selected Pod has been observed terminating, or if the
// GraceTerminationDurationSeconds of the WorkloadClass have passed since Status.LastPodTerminationTime.
// Otherwise, it also returns the time remaining until they have.
func gracePeriodPassed(wc *workloadsv1alpha1.WorkloadClass, now time.Time) (bool, time.Duration) {
	last := wc.Status.LastPodTerminationTime
	if wc.Spec.DisruptionPolicy.GraceTerminationDurationSeconds <= 0 || last == nil {
		return true, 0
	}
	gracePeriod := time.Duration(wc.Spec.DisruptionPolicy.GraceTerminationDurationSeconds) * time.Second
	return elapsed(gracePeriod, last.Time, now)
}

// elapsed returns true if at least d has passed between since and now. Otherwise, it also returns the time remaining.
func elapsed(d time.Duration, since, now time.Time) (bool, time.Duration) {
	remaining := d - now.Sub(since)
	if remaining <= 0 {
		return true, 0
	}
	return false, remaining
}

// isPodTerminal returns true if the Pod has succeeded or failed.
func isPodTerminal(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

// maxDays is the largest number of days that fits in a time.Duration.
const maxDays = math.MaxInt64 / (24 * time.Hour)

// days returns the duration of n days, saturating at the largest duration that time.Duration can represent.
func days(n int32) time.Duration {
	if time.Duration(n) > maxDays {
		return math.MaxInt64
	}
	return time.Duration(n) * 24 * time.Hour
}

// minPositiveDuration returns the smallest positive duration, or 0 if none is positive.
func minPositiveDuration(durations ...time.Duration) time.Duration {
	var minimum time.Duration
	for _, d := range durations {
		if d > 0 && (minimum == 0 || d < minimum) {
			minimum = d
		}
	}
	return minimum
}

// requeueAfterTransition returns the requeue delay for a readiness transition after d, or 0 if there is none.
func requeueAfterTransition(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d + transitionRequeueDelay
}

// withTime appends "; <label> <time>" to msg, where the time is now plus in, unless in is 0 (no transition).
func withTime(msg, label string, now time.Time, in time.Duration) string {
	if in <= 0 {
		return msg
	}
	return fmt.Sprintf("%s; %s %s", msg, label, now.Add(in).Format(time.RFC3339))
}

// pluralize returns singular if n is 1, and plural otherwise.
func pluralize(n int, singular, plural string) string {
	if n == 1 {
		return singular
	}
	return plural
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(
			&workloadsv1alpha1.WorkloadClass{},
			// Re-calculate maintenance readiness when a disruption is recorded, which can end an Overdue state.
			builder.WithPredicates(predicate.Or(predicate.GenerationChangedPredicate{}, lastDisruptionTimeChangedPredicate())),
		).
		// Re-validate all WorkloadClasses when a guardrail spec changes, is created, or is deleted.
		Watches(
			&workloadsv1alpha1.WorkloadClassGuardrail{},
			handler.EnqueueRequestsFromMapFunc(r.allWorkloadClasses),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		// Re-validate the other WorkloadClasses in a namespace when a WorkloadClass is created, deleted, or its spec
		// changes, so that their SelectorOverlap status stays current.
		Watches(
			&workloadsv1alpha1.WorkloadClass{},
			handler.EnqueueRequestsFromMapFunc(r.workloadClassesInNamespace),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		// Re-calculate the maintenance readiness of the WorkloadClasses that select a Pod when the Pod is created or
		// deleted, starts running or terminating, reaches a terminal phase, or changes labels.
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.workloadClassesSelectingPod),
			builder.WithPredicates(podReadinessPredicate()),
		).
		Named("workloadclass").
		Complete(r)
}

// StripPodForReadiness is a cache transform that drops the parts of a Pod that the WorkloadClass controller does not
// read, to bound the memory used by the cluster-wide Pod informer. It keeps the metadata (minus managed fields),
// Status.Phase, and Status.StartTime.
func StripPodForReadiness(obj any) (any, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return obj, nil
	}
	pod.ManagedFields = nil
	pod.Spec = corev1.PodSpec{}
	pod.Status = corev1.PodStatus{Phase: pod.Status.Phase, StartTime: pod.Status.StartTime}
	return pod, nil
}

// lastDisruptionTimeChangedPredicate passes WorkloadClass updates that change Status.LastDisruptionTime.
// Creation, deletion, and generic events pass through.
func lastDisruptionTimeChangedPredicate() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldWC, okOld := e.ObjectOld.(*workloadsv1alpha1.WorkloadClass)
			newWC, okNew := e.ObjectNew.(*workloadsv1alpha1.WorkloadClass)
			if !okOld || !okNew {
				return false
			}
			return !ptr.Equal(oldWC.Status.LastDisruptionTime, newWC.Status.LastDisruptionTime)
		},
	}
}

// podReadinessPredicate passes Pod creation and deletion events, and Pod updates that can change the maintenance
// readiness of the WorkloadClasses that select the Pod: label changes, the Pod starting to run (Status.StartTime
// set), the Pod starting to terminate, and the Pod reaching a terminal phase.
func podReadinessPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return true },
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, okOld := e.ObjectOld.(*corev1.Pod)
			newPod, okNew := e.ObjectNew.(*corev1.Pod)
			if !okOld || !okNew {
				return false
			}
			return !maps.Equal(oldPod.Labels, newPod.Labels) ||
				!podStartTime(oldPod).Equal(podStartTime(newPod)) ||
				oldPod.DeletionTimestamp.IsZero() != newPod.DeletionTimestamp.IsZero() ||
				isPodTerminal(oldPod) != isPodTerminal(newPod)
		},
	}
}

// workloadClassesSelectingPod maps a Pod to reconcile requests for the WorkloadClasses in its namespace whose
// PodSelector selects it. On updates, it is called for both the old and the new Pod, so WorkloadClasses that stop
// selecting a Pod after a label change are also re-queued.
func (r *WorkloadClassReconciler) workloadClassesSelectingPod(ctx context.Context, obj client.Object) []reconcile.Request {
	workloadClasses := &workloadsv1alpha1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses, client.InNamespace(obj.GetNamespace())); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list WorkloadClasses", "namespace", obj.GetNamespace())
		return nil
	}

	podLabels := labels.Set(obj.GetLabels())
	var requests []reconcile.Request
	for _, wc := range workloadClasses.Items {
		if wc.Spec.PodSelector == nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(wc.Spec.PodSelector)
		if err != nil || !selector.Matches(podLabels) {
			continue
		}
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&wc)})
	}
	return requests
}

// allWorkloadClasses maps any object to reconcile requests for every WorkloadClass in the cluster.
func (r *WorkloadClassReconciler) allWorkloadClasses(ctx context.Context, _ client.Object) []reconcile.Request {
	return r.workloadClassRequests(ctx)
}

// workloadClassesInNamespace maps a WorkloadClass to reconcile requests for the other WorkloadClasses in its namespace.
func (r *WorkloadClassReconciler) workloadClassesInNamespace(ctx context.Context, obj client.Object) []reconcile.Request {
	requests := r.workloadClassRequests(ctx, client.InNamespace(obj.GetNamespace()))
	others := make([]reconcile.Request, 0, len(requests))
	for _, req := range requests {
		if req.Name != obj.GetName() {
			others = append(others, req)
		}
	}
	return others
}

func (r *WorkloadClassReconciler) workloadClassRequests(ctx context.Context, opts ...client.ListOption) []reconcile.Request {
	workloadClasses := &workloadsv1alpha1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses, opts...); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list WorkloadClasses")
		return nil
	}

	requests := make([]reconcile.Request, 0, len(workloadClasses.Items))
	for _, item := range workloadClasses.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKey{Name: item.GetName(), Namespace: item.GetNamespace()},
		})
	}
	return requests
}
