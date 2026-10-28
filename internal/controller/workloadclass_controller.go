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
// It validates the WorkloadClass against all WorkloadClassGuardrails and against the
// other WorkloadClasses in its namespace, and surfaces the result as the Validated condition.
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
	if !meta.SetStatusCondition(&wc.Status.Conditions, cond) {
		return ctrl.Result{}, nil
	}

	log.Info("Updating WorkloadClass Validated condition", "status", cond.Status, "reason", cond.Reason)
	if err := r.Status().Update(ctx, wc); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
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
		effectiveDays  []string
		daysRestricted bool
		minWindows     int32
		minDuration    int32
	)
	for i := range guardrails {
		d := guardrails[i].Spec.Constraints.Disruption
		if len(d.DaysOfWeek) > 0 {
			if !daysRestricted {
				daysRestricted = true
				seen := sets.New[string]()
				for _, day := range d.DaysOfWeek {
					if !seen.Has(day) {
						seen.Insert(day)
						effectiveDays = append(effectiveDays, day)
					}
				}
			} else {
				allowed := sets.New(d.DaysOfWeek...)
				effectiveDays = slices.DeleteFunc(effectiveDays, func(day string) bool {
					return !allowed.Has(day)
				})
			}
		}
		if d.MaxAllowedWindows > 0 && (minWindows == 0 || d.MaxAllowedWindows < minWindows) {
			minWindows = d.MaxAllowedWindows
		}
		if d.MaxNonDisruptionDurationDays > 0 && (minDuration == 0 || d.MaxNonDisruptionDurationDays < minDuration) {
			minDuration = d.MaxNonDisruptionDurationDays
		}
	}

	var violations []string
	if daysRestricted && len(effectiveDays) == 0 {
		// Guardrails have disjoint DaysOfWeek sets with no common allowed day; report each distinct
		// DaysOfWeek constraint so the conflict is visible.
		seen := sets.New[string]()
		for i := range guardrails {
			days := guardrails[i].Spec.Constraints.Disruption.DaysOfWeek
			if len(days) == 0 {
				continue
			}
			onlyDays := workloadsv1alpha1.WorkloadClassGuardrail{
				Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1alpha1.Constraints{
						Disruption: workloadsv1alpha1.Disruption{DaysOfWeek: days},
					},
				},
			}
			for _, v := range utils.GuardrailViolations(wc, &onlyDays) {
				if !seen.Has(v) {
					seen.Insert(v)
					violations = append(violations, v)
				}
			}
		}
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
// PodSelector could select the same Pod as the WorkloadClass PodSelector.
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
	return matches, nil
}

// selectorOverlapMessage returns a message listing the WorkloadClasses whose PodSelector overlaps
// with the PodSelector of the reconciled WorkloadClass, or an empty string if there are none.
// At most maxReportedOverlaps names are listed explicitly, followed by "and K more" if additional
// classes overlap.
func selectorOverlapMessage(overlapping []workloadsv1alpha1.WorkloadClass) string {
	if len(overlapping) == 0 {
		return ""
	}

	limit := min(len(overlapping), maxReportedOverlaps)
	names := make([]string, 0, limit)
	for _, o := range overlapping[:limit] {
		names = append(names, o.Name)
	}
	listed := utils.QuotedList(names)
	if extra := len(overlapping) - limit; extra > 0 {
		listed = fmt.Sprintf("%s, and %d more", listed, extra)
	}
	return fmt.Sprintf("`podSelector` must not select the same Pods as another WorkloadClass in the namespace, "+
		"but it overlaps with the `podSelector` of %s", listed)
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workloadsv1alpha1.WorkloadClass{}).
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
