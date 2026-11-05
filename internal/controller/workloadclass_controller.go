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
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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

	notValidatedMsg      = "Maintenance readiness cannot be determined because the WorkloadClass failed validation; see the Validated condition"
	emergencyOverrideMsg = "`emergencyOverride` is set, so maintenance can disrupt Pods at any time"
	overdueMsg           = "The workload has remained undisrupted for at least `maxNonDisruptionDurationDays`, so maintenance takes precedence"
	noWindowsMsg         = "No `allowedDisruptionWindows` are defined, so maintenance can disrupt Pods at any time"
	withinWindowMsg      = "The current time is within an allowed disruption window"
	outsideWindowMsg     = "The current time is outside of every allowed disruption window"

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
// +kubebuilder:rbac:groups="",resources=pods,verbs=list;watch;patch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// It validates the WorkloadClass against all WorkloadClassGuardrails and against the
// other WorkloadClasses in its namespace, and surfaces the result as the Validated condition.
// It then calculates the MaintenanceReadiness of the WorkloadClass, surfaces it with the
// MaintenanceAllowed condition, reconciles the Per-Pod PodDisruptionBudgets of the Pods it
// governs (see reconcilePDBs), and requeues the WorkloadClass at the next readiness transition.
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

	pdbErr := r.reconcilePDBs(ctx, wc, cond, len(guardrails.Items) > 0)
	if pdbErr != nil {
		log.Error(pdbErr, "Failed to reconcile PodDisruptionBudgets")
	}

	if err := errors.Join(readinessErr, pdbErr); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: readiness.requeueAfter}, nil
}

// validatedCondition builds the Validated condition for the WorkloadClass.
//
// Guardrail and spec violations take precedence and are reported with ReasonValidationFailed.
// If the only problem is that the PodSelector overlaps with other WorkloadClasses in the namespace,
// ReasonSelectorOverlap is used instead. Overlap is intentionally reported symmetrically on every
// WorkloadClass involved so that any affected workload owner sees the conflict, while keeping a
// distinct Reason from ReasonValidationFailed. Overlapping WorkloadClasses still enforce their disruption
// policy over the Pods they are the best match for (see utils.IsValidated), so ties are broken per Pod.
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
// No overlap is reported when the Namespace has the workloads.x-k8s.io/enforced-class (EnforcedWorkloadClassLabel)
// label set, even if it names a WorkloadClass that does not exist: the label overrides PodSelector matching for every
// Pod in the Namespace, so the PodSelectors are not used there.
func (r *WorkloadClassReconciler) overlappingWorkloadClasses(
	ctx context.Context, wc *workloadsv1alpha1.WorkloadClass,
) ([]workloadsv1alpha1.WorkloadClass, error) {
	if _, enforced, err := utils.NamespaceEnforcedWorkloadClass(ctx, r.Client, wc.Namespace); err != nil || enforced {
		return nil, err
	}

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
//  1. Unknown if it failed validation, since its disruption policy is not enforced (see utils.IsValidated). A
//     WorkloadClass whose PodSelector overlaps with another one (ReasonSelectorOverlap) is still evaluated, over the
//     Pods it governs (see governedPods), so overlapping WorkloadClasses split their Pods by best match.
//  2. Ready if EmergencyOverride is set.
//  3. Overdue if it has remained undisrupted for at least MaxNonDisruptionDurationDays.
//  4. NotReady if the current time is outside of every AllowedDisruptionWindow.
//  5. NotReady if a governed Pod has not yet run for MinInitialRunDurationDays, or a governed Pod started terminating
//     less than GraceTerminationDurationSeconds ago (see governedPods).
//  6. Ready otherwise.
//
// Unless the readiness only changes when the cluster changes (Unknown, EmergencyOverride, or Overdue), the
// WorkloadClass is requeued at the earliest of: the next disruption window start or end, the time it becomes overdue,
// and the time the governed Pods stop blocking maintenance.
//
// As a side effect, Status.LastPodTerminationTime is advanced to the most recent time a governed Pod was observed to
// start terminating, so that GraceTerminationDurationSeconds is honored after the Pod is gone.
func (r *WorkloadClassReconciler) calculateReadiness(
	ctx context.Context, wc *workloadsv1alpha1.WorkloadClass, validated metav1.Condition,
) (readinessResult, error) {
	if !utils.IsValidated(&validated) {
		return readinessResult{
			readiness: workloadsv1alpha1.ReadinessUnknown,
			reason:    validated.Reason,
			message:   notValidatedMsg,
		}, nil
	}

	policy := wc.Spec.DisruptionPolicy
	now := r.now()

	// Observe the governed Pods on every reconcile so that terminating Pods are recorded before they are gone.
	var pods []corev1.Pod
	if policy.MinInitialRunDurationDays > 0 || policy.GraceTerminationDurationSeconds > 0 {
		var err error
		if pods, err = r.governedPods(ctx, wc); err != nil {
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

// governedPods returns the Pods in the namespace of the WorkloadClass that it governs, including terminating Pods and
// Pods in a terminal phase:
//
//   - If the Namespace has the workloads.x-k8s.io/enforced-class (EnforcedWorkloadClassLabel) label set, the enforced
//     WorkloadClass governs every Pod in the namespace, and the other WorkloadClasses govern none. If the label names
//     a WorkloadClass that does not exist or is being deleted, no WorkloadClass governs the namespace's Pods.
//   - Otherwise, the WorkloadClass governs the Pods that its PodSelector selects and for which it is the best match
//     among the WorkloadClasses in the namespace (see utils.SelectorBestMatch). A nil PodSelector governs no Pods.
func (r *WorkloadClassReconciler) governedPods(ctx context.Context, wc *workloadsv1alpha1.WorkloadClass) ([]corev1.Pod, error) {
	enforcedWC, enforced, err := utils.NamespaceEnforcedWorkloadClass(ctx, r.Client, wc.Namespace)
	if err != nil {
		return nil, err
	}

	selector := labels.Everything()
	if enforced {
		if enforcedWC == nil || enforcedWC.Name != wc.Name {
			return nil, nil
		}
	} else {
		if wc.Spec.PodSelector == nil {
			return nil, nil
		}
		if selector, err = metav1.LabelSelectorAsSelector(wc.Spec.PodSelector); err != nil {
			return nil, fmt.Errorf("invalid podSelector: %w", err)
		}
	}

	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(wc.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, fmt.Errorf("failed to list Pods: %w", err)
	}
	if enforced || len(pods.Items) == 0 {
		return pods.Items, nil
	}

	candidates, err := r.bestMatchCandidates(ctx, wc)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(pods.Items, func(pod corev1.Pod) bool {
		bestMatch, _, _ := utils.SelectorBestMatch(&pod, candidates)
		return bestMatch == nil || bestMatch.Name != wc.Name
	}), nil
}

// bestMatchCandidates returns the WorkloadClasses in the namespace of the WorkloadClass, with the listed copy of the
// WorkloadClass replaced by wc, so that it is ranked as it is being reconciled even if the list holds an older copy or
// does not include it.
func (r *WorkloadClassReconciler) bestMatchCandidates(
	ctx context.Context, wc *workloadsv1alpha1.WorkloadClass,
) ([]workloadsv1alpha1.WorkloadClass, error) {
	workloadClasses := &workloadsv1alpha1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses, client.InNamespace(wc.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list WorkloadClasses: %w", err)
	}
	candidates := slices.DeleteFunc(workloadClasses.Items, func(other workloadsv1alpha1.WorkloadClass) bool {
		return other.Name == wc.Name
	})
	return append(candidates, *wc), nil
}

// protectsPods reports whether a WorkloadClass with the given Validated condition protects the Pods it governs with
// Per-Pod PodDisruptionBudgets that follow its MaintenanceReadiness.
//
// A WorkloadClass does if it enforces its disruption policy (see utils.IsValidated: it passed validation, or its
// PodSelector only overlaps with another WorkloadClass) and at least one guardrail exists (hasGuardrails), so that
// PDBs block disruptions only where a guardrail bounds the disruption policy, e.g. through
// maxNonDisruptionDurationDays. Overlapping WorkloadClasses keep protecting the Pods they are the best match for, so
// each Pod's PDB is owned by its best match (see governedPods).
func protectsPods(validated metav1.Condition, hasGuardrails bool) bool {
	return hasGuardrails && utils.IsValidated(&validated) && validated.Reason != workloadsv1alpha1.ReasonNoGuardrails
}

// statusProtectsPods reports whether the Validated condition in the status of the WorkloadClass shows that it protects
// the Pods it governs (see protectsPods). Guardrails are cluster-scoped, so hasGuardrails is the same for every
// WorkloadClass.
func statusProtectsPods(wc *workloadsv1alpha1.WorkloadClass, hasGuardrails bool) bool {
	validated := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1alpha1.ConditionTypeValidated)
	return validated != nil && protectsPods(*validated, hasGuardrails)
}

// reconcilePDBs reconciles the Per-Pod PodDisruptionBudgets (see utils.PDBName) of the Pods governed by the
// WorkloadClass (see governedPods):
//
//   - If the WorkloadClass protects Pods (see protectsPods), each active governed Pod is labeled with its UID
//     (PodUIDLabel) and gets a PDB that selects only that Pod, is controlled by the WorkloadClass, and follows its
//     MaintenanceReadiness (see utils.SyncPDBWithWorkloadClass). Terminating and terminal Pods get no PDB. The other
//     PDBs that the WorkloadClass controls are handed over or deleted (see reconcileOwnedPDBs).
//   - Otherwise, the PDBs that the WorkloadClass controls are opened (MaxUnavailable = 100%) for the Pods that are
//     still active, rather than deleted, and no PDB is created (see reconcileOwnedPDBs).
//
// User-created PDBs take precedence (see utils.IsPodCoveredByUserPDB): a Pod that a user-created PDB covers gets no
// PDB, and the PDB the WorkloadClass controls for it is deleted, since the eviction API rejects evicting a Pod that
// more than one PDB covers. The eviction webhook still enforces the disruption policy for such Pods. A Pod whose PDB
// name is taken by a user-created PDB that does not cover it is skipped as well, since claimPDB refuses to modify that
// PDB; it is retried when that PDB changes.
//
// In a Namespace with the workloads.x-k8s.io/enforced-class label, the enforced WorkloadClass governs every Pod in the
// namespace, whether or not its PodSelector selects them, so it takes over the PDBs of the other WorkloadClasses (see
// claimPDB), which govern no Pods and leave their PDBs to it. If the label names a WorkloadClass that does not exist or
// is being deleted, no WorkloadClass governs the Pods, so their PDBs are deleted (or opened by a WorkloadClass that
// does not protect Pods). Governance does not depend on protection: an enforced WorkloadClass that does not protect
// Pods still governs them, so it opens its own PDBs and the other WorkloadClasses delete theirs.
//
// A WorkloadClass that is being deleted is skipped, since its PDBs are garbage collected through their owner
// references. An error for one Pod does not stop the others; all errors are returned.
//
// hasGuardrails reports whether at least one WorkloadClassGuardrail exists (see protectsPods).
func (r *WorkloadClassReconciler) reconcilePDBs(
	ctx context.Context, wc *workloadsv1alpha1.WorkloadClass, validated metav1.Condition, hasGuardrails bool,
) error {
	if !wc.DeletionTimestamp.IsZero() {
		return nil
	}
	pdbs := &policyv1.PodDisruptionBudgetList{}
	if err := r.List(ctx, pdbs, client.InNamespace(wc.Namespace)); err != nil {
		return fmt.Errorf("failed to list PodDisruptionBudgets: %w", err)
	}
	var owned, userPDBs []policyv1.PodDisruptionBudget
	for i := range pdbs.Items {
		switch pdb := &pdbs.Items[i]; {
		case metav1.IsControlledBy(pdb, wc):
			owned = append(owned, *pdb)
		case !utils.IsWorkloadClassManagedPDB(pdb):
			userPDBs = append(userPDBs, *pdb)
		}
	}
	if !protectsPods(validated, hasGuardrails) {
		return r.reconcileOwnedPDBs(ctx, wc, owned, userPDBs, nil, true, hasGuardrails)
	}

	pods, err := r.governedPods(ctx, wc)
	if err != nil {
		return err
	}
	var errs []error
	protected := sets.New[string]()
	for i := range pods {
		pod := &pods[i]
		if !utils.IsPodActive(pod) || utils.IsPodCoveredByUserPDB(pod, userPDBs) {
			continue
		}
		protected.Insert(utils.PDBName(pod.Name))
		if err := r.ensurePodUIDLabel(ctx, pod); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := r.syncPodPDB(ctx, wc, pod, true); errors.Is(err, errUnmanagedPDB) {
			logf.FromContext(ctx).Info("Skipping Pod whose PodDisruptionBudget name is taken by a PodDisruptionBudget "+
				"that is not managed by a WorkloadClass", "pod", pod.Name, "podDisruptionBudget", utils.PDBName(pod.Name))
		} else {
			errs = append(errs, err)
		}
	}
	errs = append(errs, r.reconcileOwnedPDBs(ctx, wc, owned, userPDBs, protected, false, hasGuardrails))
	return errors.Join(errs...)
}

// reconcileOwnedPDBs reconciles the PodDisruptionBudgets controlled by the WorkloadClass, except those named in
// protected (the PDBs of the active Pods it protects):
//
//   - A PDB whose Pod is gone, terminating, in a terminal phase, or covered by one of the user-created PDBs (see
//     utils.IsPodCoveredByUserPDB) is deleted.
//   - A PDB whose Pod is now governed by another WorkloadClass that protects Pods is left as it is, for that
//     WorkloadClass to take over (see claimPDB).
//   - Otherwise, the PDB is opened (MaxUnavailable = 100%) if open is true, or deleted, since no WorkloadClass that
//     protects Pods governs its Pod anymore (e.g. the Pod was relabeled).
//
// hasGuardrails reports whether at least one WorkloadClassGuardrail exists (see statusProtectsPods).
func (r *WorkloadClassReconciler) reconcileOwnedPDBs(
	ctx context.Context, wc *workloadsv1alpha1.WorkloadClass, owned, userPDBs []policyv1.PodDisruptionBudget,
	protected sets.Set[string], open, hasGuardrails bool,
) error {
	if len(owned) == 0 {
		return nil
	}
	enforcedWC, enforced, err := utils.NamespaceEnforcedWorkloadClass(ctx, r.Client, wc.Namespace)
	if err != nil {
		return err
	}
	candidates, err := r.bestMatchCandidates(ctx, wc)
	if err != nil {
		return err
	}

	var errs []error
	for i := range owned {
		pdb := &owned[i]
		if protected.Has(pdb.Name) {
			continue
		}
		pod, err := r.getPodForPDB(ctx, pdb)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if utils.IsPodActive(pod) && !utils.IsPodCoveredByUserPDB(pod, userPDBs) {
			bestMatch := utils.BestMatchWorkloadClassForPod(pod, enforcedWC, enforced, candidates)
			if bestMatch != nil && bestMatch.Name != wc.Name && statusProtectsPods(bestMatch, hasGuardrails) {
				continue
			}
			if open {
				errs = append(errs, r.syncPodPDB(ctx, wc, pod, false))
				continue
			}
		}
		errs = append(errs, r.deletePDB(ctx, wc, pdb))
	}
	return errors.Join(errs...)
}

// deletePDB deletes the PodDisruptionBudget if the WorkloadClass still controls it. The PDB is read again and deleted
// with UID and resourceVersion preconditions, so that a PDB that another WorkloadClass has taken over in the meantime
// is never deleted.
func (r *WorkloadClassReconciler) deletePDB(
	ctx context.Context, wc *workloadsv1alpha1.WorkloadClass, pdb *policyv1.PodDisruptionBudget,
) error {
	current := &policyv1.PodDisruptionBudget{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pdb), current); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(current, wc) {
		return nil
	}
	preconditions := client.Preconditions{UID: &current.UID, ResourceVersion: &current.ResourceVersion}
	if err := r.Delete(ctx, current, preconditions); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to delete PodDisruptionBudget %s: %w", pdb.Name, err)
	}
	logf.FromContext(ctx).Info("Deleted PodDisruptionBudget", "podDisruptionBudget", pdb.Name)
	return nil
}

// getPodForPDB returns the Pod of a Per-Pod PodDisruptionBudget, or nil if it does not exist.
//
// The Pod is looked up by the name the PDB name was derived from (see utils.PodNameFromPDBName). If no such Pod
// exists, or it is not the Pod instance the PDB was created for (its UID differs from the PDB's PodUIDLabel label,
// e.g. the name was truncated by utils.PDBName, or the Pod was recreated with the same name), the Pod is looked up by
// the PodUIDLabel label instead. If that finds no Pod either, the Pod with the derived name is returned, if any.
func (r *WorkloadClassReconciler) getPodForPDB(ctx context.Context, pdb *policyv1.PodDisruptionBudget) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, client.ObjectKey{Name: utils.PodNameFromPDBName(pdb.Name), Namespace: pdb.Namespace}, pod); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get Pod for PodDisruptionBudget %s: %w", pdb.Name, err)
		}
		pod = nil
	}
	uid := pdb.Labels[workloadsv1alpha1.PodUIDLabel]
	if uid == "" || (pod != nil && string(pod.UID) == uid) {
		return pod, nil
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(pdb.Namespace), client.MatchingLabels{workloadsv1alpha1.PodUIDLabel: uid}); err != nil {
		return nil, fmt.Errorf("failed to list Pods: %w", err)
	}
	for i := range pods.Items {
		if string(pods.Items[i].UID) == uid {
			return &pods.Items[i], nil
		}
	}
	return pod, nil
}

// ensurePodUIDLabel labels the Pod with its UID (PodUIDLabel) so that its Per-Pod PodDisruptionBudget can select it.
// The label is added with a merge patch rather than an update, since the cached Pod may be stripped (see
// StripPodForReadiness).
func (r *WorkloadClassReconciler) ensurePodUIDLabel(ctx context.Context, pod *corev1.Pod) error {
	if pod.Labels[workloadsv1alpha1.PodUIDLabel] == string(pod.UID) {
		return nil
	}
	patch := client.MergeFrom(pod.DeepCopy())
	metav1.SetMetaDataLabel(&pod.ObjectMeta, workloadsv1alpha1.PodUIDLabel, string(pod.UID))
	if err := r.Patch(ctx, pod, patch); err != nil {
		return fmt.Errorf("failed to label Pod %s: %w", pod.Name, err)
	}
	return nil
}

// syncPodPDB creates or updates the Per-Pod PodDisruptionBudget of the Pod to match the WorkloadClass (see
// utils.SyncPDBWithWorkloadClass), after claiming it for the WorkloadClass (see claimPDB).
func (r *WorkloadClassReconciler) syncPodPDB(
	ctx context.Context, wc *workloadsv1alpha1.WorkloadClass, pod *corev1.Pod, protects bool,
) error {
	pdb := utils.PDBBase(pod)
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		if err := r.claimPDB(wc, pdb); err != nil {
			return err
		}
		return utils.SyncPDBWithWorkloadClass(wc, pod, pdb, protects)
	})
	if err != nil {
		return fmt.Errorf("failed to reconcile PodDisruptionBudget %s: %w", pdb.Name, err)
	}
	if op != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled PodDisruptionBudget",
			"podDisruptionBudget", pdb.Name, "pod", pod.Name, "operation", op, "maxUnavailable", pdb.Spec.MaxUnavailable)
	}
	return nil
}

// errUnmanagedPDB is returned by claimPDB for an existing PodDisruptionBudget that is not managed by a WorkloadClass.
var errUnmanagedPDB = errors.New("already exists and is not managed by a WorkloadClass")

// claimPDB sets the WorkloadClass as the controller of the PodDisruptionBudget, taking it over from another
// WorkloadClass if needed (e.g. when the best-match WorkloadClass of the Pod changes). It refuses to modify an
// existing PDB that is not managed by a WorkloadClass (see utils.IsWorkloadClassManagedPDB) with errUnmanagedPDB.
func (r *WorkloadClassReconciler) claimPDB(wc *workloadsv1alpha1.WorkloadClass, pdb *policyv1.PodDisruptionBudget) error {
	if pdb.ResourceVersion != "" && !utils.IsWorkloadClassManagedPDB(pdb) {
		return fmt.Errorf("PodDisruptionBudget %s/%s %w", pdb.Namespace, pdb.Name, errUnmanagedPDB)
	}
	if owner := metav1.GetControllerOf(pdb); owner != nil && owner.UID != wc.UID {
		pdb.OwnerReferences = slices.DeleteFunc(pdb.OwnerReferences, func(ref metav1.OwnerReference) bool {
			return ptr.Deref(ref.Controller, false)
		})
	}
	return controllerutil.SetControllerReference(wc, pdb, r.Scheme)
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
		// changes, so that their SelectorOverlap status stays current, and when it starts or stops protecting Pods, so
		// that they hand over or delete the PDBs of the Pods it governs.
		Watches(
			&workloadsv1alpha1.WorkloadClass{},
			handler.EnqueueRequestsFromMapFunc(r.workloadClassesInNamespace),
			builder.WithPredicates(predicate.Or(predicate.GenerationChangedPredicate{}, protectionChangedPredicate())),
		).
		// Restore a managed PodDisruptionBudget when it is deleted or its spec is changed by someone else.
		Owns(&policyv1.PodDisruptionBudget{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// User-created PodDisruptionBudgets take precedence: delete the managed PDB of a Pod when a user-created PDB
		// starts covering it, and create it again when the user-created PDB is deleted or stops covering the Pod.
		Watches(
			&policyv1.PodDisruptionBudget{},
			handler.EnqueueRequestsFromMapFunc(r.objectNamespaceWorkloadClasses),
			builder.WithPredicates(userPDBPredicate()),
		).
		// Re-calculate the maintenance readiness of the WorkloadClasses that select a Pod when the Pod is created or
		// deleted, starts running or terminating, reaches a terminal phase, or changes labels, and let the WorkloadClass
		// that controls its PDB delete or hand it over.
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.workloadClassesForPod),
			builder.WithPredicates(podReadinessPredicate()),
		).
		// Re-validate and re-calculate the maintenance readiness of every WorkloadClass in a Namespace when its enforced
		// WorkloadClass changes, since that changes which Pods each WorkloadClass governs and whether overlap applies.
		Watches(
			&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.namespaceWorkloadClasses),
			builder.WithPredicates(enforcedClassLabelChangedPredicate()),
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

// protectionChangedPredicate passes WorkloadClass updates after which its status shows that it started or stopped
// protecting the Pods it governs (see statusProtectsPods). Creation, deletion, and generic events pass through.
//
// It assumes that a guardrail exists: without one, no WorkloadClass protects Pods, so the extra events it passes then
// are harmless, and creating or deleting a guardrail re-reconciles every WorkloadClass anyway.
func protectionChangedPredicate() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldWC, okOld := e.ObjectOld.(*workloadsv1alpha1.WorkloadClass)
			newWC, okNew := e.ObjectNew.(*workloadsv1alpha1.WorkloadClass)
			return okOld && okNew && statusProtectsPods(oldWC, true) != statusProtectsPods(newWC, true)
		},
	}
}

// podReadinessPredicate passes Pod creation and deletion events, and Pod updates that can change the maintenance
// readiness or the Per-Pod PodDisruptionBudget of the WorkloadClasses that select the Pod: label changes (see
// podLabelsChanged), the Pod starting to run (Status.StartTime set), the Pod starting to terminate, and the Pod
// reaching a terminal phase.
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
			return podLabelsChanged(oldPod, newPod) ||
				!podStartTime(oldPod).Equal(podStartTime(newPod)) ||
				oldPod.DeletionTimestamp.IsZero() != newPod.DeletionTimestamp.IsZero() ||
				isPodTerminal(oldPod) != isPodTerminal(newPod)
		},
	}
}

// podLabelsChanged reports whether the labels of the Pod changed, ignoring the controller's own update that sets the
// workloads.x-k8s.io/pod-uid (PodUIDLabel) label to the Pod's UID. Removing or changing that label still counts, so
// that the controller restores it.
func podLabelsChanged(oldPod, newPod *corev1.Pod) bool {
	if maps.Equal(oldPod.Labels, newPod.Labels) {
		return false
	}
	return newPod.Labels[workloadsv1alpha1.PodUIDLabel] != string(newPod.UID) ||
		!maps.Equal(labelsWithoutPodUID(oldPod.Labels), labelsWithoutPodUID(newPod.Labels))
}

// labelsWithoutPodUID returns the Pod labels without the workloads.x-k8s.io/pod-uid (PodUIDLabel) label.
func labelsWithoutPodUID(podLabels map[string]string) map[string]string {
	if _, ok := podLabels[workloadsv1alpha1.PodUIDLabel]; !ok {
		return podLabels
	}
	filtered := maps.Clone(podLabels)
	delete(filtered, workloadsv1alpha1.PodUIDLabel)
	return filtered
}

// userPDBPredicate passes events for PodDisruptionBudgets that are not managed by a WorkloadClass (see
// utils.IsWorkloadClassManagedPDB). Updates pass only if they change the spec (e.g. the selector) or if the PDB was
// managed by a WorkloadClass before, since the status of a PDB changes whenever the health of its Pods does.
func userPDBPredicate() predicate.Funcs {
	isUserPDB := func(obj client.Object) bool {
		pdb, ok := obj.(*policyv1.PodDisruptionBudget)
		return ok && !utils.IsWorkloadClassManagedPDB(pdb)
	}
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return isUserPDB(e.Object) },
		DeleteFunc: func(e event.DeleteEvent) bool { return isUserPDB(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool {
			return isUserPDB(e.ObjectNew) &&
				(!isUserPDB(e.ObjectOld) || e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration())
		},
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// enforcedClassLabelChangedPredicate passes Namespace creations with a non-empty workloads.x-k8s.io/enforced-class
// (EnforcedWorkloadClassLabel) label, and Namespace updates that set, change, or remove it. Deletions are ignored,
// since deleting a Namespace deletes its WorkloadClasses.
func enforcedClassLabelChangedPredicate() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return e.Object.GetLabels()[workloadsv1alpha1.EnforcedWorkloadClassLabel] != ""
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetLabels()[workloadsv1alpha1.EnforcedWorkloadClassLabel] !=
				e.ObjectNew.GetLabels()[workloadsv1alpha1.EnforcedWorkloadClassLabel]
		},
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// workloadClassesForPod maps a Pod to reconcile requests for the WorkloadClasses that can govern it (see
// workloadClassesSelectingPod), and for the WorkloadClass that controls its Per-Pod PodDisruptionBudget, so that the
// PDB is deleted or handed over when that WorkloadClass no longer governs the Pod (e.g. the Pod was deleted).
func (r *WorkloadClassReconciler) workloadClassesForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	requests := r.workloadClassesSelectingPod(ctx, obj)
	pdb := &policyv1.PodDisruptionBudget{}
	if err := r.Get(ctx, client.ObjectKey{Name: utils.PDBName(obj.GetName()), Namespace: obj.GetNamespace()}, pdb); err != nil {
		if !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).Error(err, "Failed to get the PodDisruptionBudget of the Pod",
				"namespace", obj.GetNamespace(), "pod", obj.GetName())
		}
		return requests
	}
	owner := metav1.GetControllerOf(pdb)
	if !utils.IsWorkloadClassManagedPDB(pdb) || slices.ContainsFunc(requests, func(req reconcile.Request) bool {
		return req.Name == owner.Name
	}) {
		return requests
	}
	return append(requests, reconcile.Request{NamespacedName: client.ObjectKey{Name: owner.Name, Namespace: pdb.Namespace}})
}

// workloadClassesSelectingPod maps a Pod to reconcile requests for the WorkloadClasses that can govern it: the
// Namespace's enforced WorkloadClass if the Namespace has the workloads.x-k8s.io/enforced-class label set (none if it
// names a WorkloadClass that does not exist), and otherwise the WorkloadClasses in its namespace whose PodSelector
// selects it. On updates, it is called for both the old and the new Pod, so WorkloadClasses that stop selecting a Pod
// after a label change are also re-queued. If the Namespace cannot be read, every WorkloadClass in it is re-queued.
func (r *WorkloadClassReconciler) workloadClassesSelectingPod(ctx context.Context, obj client.Object) []reconcile.Request {
	enforcedWC, enforced, err := utils.NamespaceEnforcedWorkloadClass(ctx, r.Client, obj.GetNamespace())
	switch {
	case err != nil:
		logf.FromContext(ctx).Error(err, "Failed to get the enforced WorkloadClass", "namespace", obj.GetNamespace())
		return r.workloadClassRequests(ctx, client.InNamespace(obj.GetNamespace()))
	case enforced && enforcedWC == nil:
		return nil
	case enforced:
		return []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(enforcedWC)}}
	}

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

// namespaceWorkloadClasses maps a Namespace to reconcile requests for every WorkloadClass in it.
func (r *WorkloadClassReconciler) namespaceWorkloadClasses(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.workloadClassRequests(ctx, client.InNamespace(obj.GetName()))
}

// objectNamespaceWorkloadClasses maps an object to reconcile requests for every WorkloadClass in its namespace.
func (r *WorkloadClassReconciler) objectNamespaceWorkloadClasses(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.workloadClassRequests(ctx, client.InNamespace(obj.GetNamespace()))
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
