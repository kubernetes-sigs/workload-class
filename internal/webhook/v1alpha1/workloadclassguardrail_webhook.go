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

package v1alpha1

import (
	"context"
	"fmt"
	"strings"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
	"sigs.k8s.io/workload-class/internal/utils"
)

// nolint:unused
var workloadclassguardraillog = logf.Log.WithName("workloadclassguardrail-resource")

// SetupWorkloadClassGuardrailWebhookWithManager registers the webhook for WorkloadClassGuardrail in the manager.
func SetupWorkloadClassGuardrailWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &workloadsv1alpha1.WorkloadClassGuardrail{}).
		WithValidator(&WorkloadClassGuardrailCustomValidator{Client: mgr.GetClient()}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-workloads-x-k8s-io-v1alpha1-workloadclassguardrail,mutating=false,failurePolicy=fail,sideEffects=None,groups=workloads.x-k8s.io,resources=workloadclassguardrails,verbs=create;update,versions=v1alpha1,name=vworkloadclassguardrail-v1alpha1.kb.io,admissionReviewVersions=v1

// WorkloadClassGuardrailCustomValidator validates WorkloadClassGuardrail resources when created or updated.
// +kubebuilder:object:generate=false
type WorkloadClassGuardrailCustomValidator struct {
	Client client.Client
}

// ValidateCreate implements webhook.CustomValidator for WorkloadClassGuardrail.
func (v *WorkloadClassGuardrailCustomValidator) ValidateCreate(ctx context.Context, obj *workloadsv1alpha1.WorkloadClassGuardrail) (admission.Warnings, error) {
	workloadclassguardraillog.Info("Validation for WorkloadClassGuardrail upon creation", "name", obj.GetName())

	if err := utils.WeekdaysValid(obj.Spec.Constraints.Disruption.DaysOfWeek); err != nil {
		return []string{err.Error()}, err
	}

	return v.validateAgainstWorkloadClasses(ctx, obj)
}

// ValidateUpdate implements webhook.CustomValidator for WorkloadClassGuardrail.
func (v *WorkloadClassGuardrailCustomValidator) ValidateUpdate(ctx context.Context, _, newObj *workloadsv1alpha1.WorkloadClassGuardrail) (admission.Warnings, error) {
	workloadclassguardraillog.Info("Validation for WorkloadClassGuardrail upon update", "name", newObj.GetName())

	if err := utils.WeekdaysValid(newObj.Spec.Constraints.Disruption.DaysOfWeek); err != nil {
		return []string{err.Error()}, err
	}

	return v.validateAgainstWorkloadClasses(ctx, newObj)
}

// ValidateDelete implements webhook.CustomValidator for WorkloadClassGuardrail.
func (v *WorkloadClassGuardrailCustomValidator) ValidateDelete(_ context.Context, obj *workloadsv1alpha1.WorkloadClassGuardrail) (admission.Warnings, error) {
	workloadclassguardraillog.Info("Validation for WorkloadClassGuardrail upon deletion", "name", obj.GetName())
	return nil, nil
}

func (v *WorkloadClassGuardrailCustomValidator) validateAgainstWorkloadClasses(ctx context.Context, g *workloadsv1alpha1.WorkloadClassGuardrail) (admission.Warnings, error) {
	wcs := &workloadsv1alpha1.WorkloadClassList{}
	if err := v.Client.List(ctx, wcs); err != nil {
		return []string{fmt.Sprintf("failed to list WorkloadClasses: %v", err)}, err
	}

	var warnings admission.Warnings
	for _, wc := range wcs.Items {
		if violations := utils.GuardrailViolations(&wc, g); len(violations) > 0 {
			workloadclassguardraillog.Info("Existing WorkloadClass violates guardrail", "violations", violations, "workloadclass", wc.Name, "namespace", wc.Namespace)
			warnings = append(warnings, fmt.Sprintf("existing WorkloadClass '%s/%s' does not comply with this guardrail: %s",
				wc.Namespace, wc.Name, strings.Join(violations, "; ")))
		}
	}

	if len(warnings) > 0 {
		return warnings, nil
	}

	workloadclassguardraillog.Info("WorkloadClassGuardrail is valid for all WorkloadClasses", "name", g.GetName())
	return nil, nil
}
