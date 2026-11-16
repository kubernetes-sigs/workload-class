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
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	noSelectorOverlapMsg   = "`podSelector` does not overlap with the `podSelector` of any other WorkloadClass in the namespace"
	disjointDaysOfWeekMsg  = "cluster guardrails restrict `daysOfWeek` to disjoint sets, so no disruption window can comply; contact your cluster administrator"
	maxConditionMessageLen = 32768
	maxReportedOverlaps    = 5
)

// WorkloadClassReconciler reconciles a WorkloadClass object
type WorkloadClassReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=workloads.x-k8s.io,resources=workloadclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=workloads.x-k8s.io,resources=workloadclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=workloads.x-k8s.io,resources=workloadclassguardrails,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// It validates the WorkloadClass against all WorkloadClassGuardrails and surfaces the result as the
// Validated condition. It also checks the PodSelector against the other WorkloadClasses in its namespace
// and surfaces the result as the informational PodSelectorOverlap condition.
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

	validated := validatedCondition(wc, guardrails.Items)
	overlap := podSelectorOverlapCondition(wc, overlapping)
	validatedChanged := meta.SetStatusCondition(&wc.Status.Conditions, validated)
	overlapChanged := meta.SetStatusCondition(&wc.Status.Conditions, overlap)
	if !validatedChanged && !overlapChanged {
		return ctrl.Result{}, nil
	}

	log.Info("Updating WorkloadClass conditions",
		"validatedStatus", validated.Status, "validatedReason", validated.Reason,
		"podSelectorOverlapStatus", overlap.Status, "podSelectorOverlapReason", overlap.Reason)
	if err := r.Status().Update(ctx, wc); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// validatedCondition builds the Validated condition for the WorkloadClass.
//
// Guardrail and spec violations are reported with ReasonValidationFailed. PodSelector overlap with other
// WorkloadClasses is deliberately not part of this condition (see podSelectorOverlapCondition): a Validated=False
// WorkloadClass does not protect its Pods, so overlap must not be able to turn off protection.
func validatedCondition(
	wc *workloadsv1alpha1.WorkloadClass,
	guardrails []workloadsv1alpha1.WorkloadClassGuardrail,
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

	switch {
	case len(violations) > 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = workloadsv1alpha1.ReasonValidationFailed
		cond.Message = truncateMessage(strings.Join(violations, "; "), maxConditionMessageLen)
	case len(guardrails) == 0:
		cond.Reason = workloadsv1alpha1.ReasonNoGuardrails
		cond.Message = noGuardrailsMsg
	}

	return cond
}

// podSelectorOverlapCondition builds the informational PodSelectorOverlap condition for the WorkloadClass.
//
// Overlap is reported symmetrically on every WorkloadClass involved so that any affected workload owner sees
// it. It does not affect the Validated condition or protection: a Pod selected by more than one WorkloadClass
// is governed by the oldest of them (by creationTimestamp, then name), and the other WorkloadClasses keep
// governing the Pods only they select.
func podSelectorOverlapCondition(
	wc *workloadsv1alpha1.WorkloadClass,
	overlapping []workloadsv1alpha1.WorkloadClass,
) metav1.Condition {
	cond := metav1.Condition{
		Type:               workloadsv1alpha1.ConditionTypePodSelectorOverlap,
		Status:             metav1.ConditionFalse,
		Reason:             workloadsv1alpha1.ReasonNoSelectorOverlap,
		Message:            noSelectorOverlapMsg,
		ObservedGeneration: wc.Generation,
	}
	if msg := selectorOverlapMessage(overlapping); msg != "" {
		cond.Status = metav1.ConditionTrue
		cond.Reason = workloadsv1alpha1.ReasonSelectorOverlap
		cond.Message = truncateMessage(msg, maxConditionMessageLen)
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
// the namespace reports PodSelectorOverlap=True. WorkloadClasses that are being deleted or have an
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
	return fmt.Sprintf("`podSelector` may select the same Pods as the `podSelector` of %s; "+
		"a Pod selected by more than one WorkloadClass is governed by the oldest of them", listed)
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(
			&workloadsv1alpha1.WorkloadClass{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		// Re-validate all WorkloadClasses when a guardrail spec changes, is created, or is deleted.
		Watches(
			&workloadsv1alpha1.WorkloadClassGuardrail{},
			handler.EnqueueRequestsFromMapFunc(r.allWorkloadClasses),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		// Re-validate the other WorkloadClasses in a namespace when a WorkloadClass is created, deleted, or its spec
		// changes, so that their PodSelectorOverlap condition stays current.
		Watches(
			&workloadsv1alpha1.WorkloadClass{},
			handler.EnqueueRequestsFromMapFunc(r.workloadClassesInNamespace),
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Named("workloadclass").
		Complete(r)
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
