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
	"strings"

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
	validationPassedMsg = "WorkloadClass adheres to all Guardrail constraints"
	noGuardrailsMsg     = "No Guardrails found to validate against"
)

// WorkloadClassReconciler reconciles a WorkloadClass object
type WorkloadClassReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=workloads.x-k8s.io,resources=workloadclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=workloads.x-k8s.io,resources=workloadclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=workloads.x-k8s.io,resources=workloadclasses/finalizers,verbs=update
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
// ReasonSelectorOverlap is used instead.
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
		cond.Message = strings.Join(violations, "; ")
	case overlapMsg != "":
		cond.Status = metav1.ConditionFalse
		cond.Reason = workloadsv1alpha1.ReasonSelectorOverlap
		cond.Message = overlapMsg
	case len(guardrails) == 0:
		cond.Reason = workloadsv1alpha1.ReasonNoGuardrails
		cond.Message = noGuardrailsMsg
	}

	return cond
}

// validateSpec validates the fields of the WorkloadClass spec that cannot be fully expressed
// through CRD schema validation.
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
// WorkloadClass status may not have access to them. Only the limits needed to fix the WorkloadClass are
// reported, and identical violations from several guardrails are reported once.
func validateAgainstGuardrails(wc *workloadsv1alpha1.WorkloadClass, guardrails []workloadsv1alpha1.WorkloadClassGuardrail) []string {
	violations := make([]string, 0, len(guardrails))
	seen := sets.New[string]()
	for i := range guardrails {
		for _, v := range utils.GuardrailViolations(wc, &guardrails[i]) {
			if !seen.Has(v) {
				seen.Insert(v)
				violations = append(violations, v)
			}
		}
	}
	return violations
}

// overlappingWorkloadClasses returns the other WorkloadClasses in the same namespace whose PodSelector
// could select the same Pod as the WorkloadClass PodSelector.
//
// WorkloadClasses with an invalid PodSelector are skipped; the invalid PodSelector is reported on the
// WorkloadClass that owns it.
func (r *WorkloadClassReconciler) overlappingWorkloadClasses(
	ctx context.Context, wc *workloadsv1alpha1.WorkloadClass,
) ([]workloadsv1alpha1.WorkloadClass, error) {
	workloadClasses := &workloadsv1alpha1.WorkloadClassList{}
	if err := r.List(ctx, workloadClasses, client.InNamespace(wc.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list WorkloadClasses: %w", err)
	}

	var matches []workloadsv1alpha1.WorkloadClass
	for _, other := range workloadClasses.Items {
		if other.Name == wc.Name {
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
func selectorOverlapMessage(overlapping []workloadsv1alpha1.WorkloadClass) string {
	if len(overlapping) == 0 {
		return ""
	}

	names := make([]string, 0, len(overlapping))
	for _, o := range overlapping {
		names = append(names, o.Name)
	}
	return fmt.Sprintf("`podSelector` must not select the same Pods as another WorkloadClass in the namespace, "+
		"but it overlaps with the `podSelector` of %s", utils.QuotedList(names))
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&workloadsv1alpha1.WorkloadClass{}).
		// Re-validate all WorkloadClasses when a guardrail changes.
		Watches(
			&workloadsv1alpha1.WorkloadClassGuardrail{},
			handler.EnqueueRequestsFromMapFunc(r.allWorkloadClasses),
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
