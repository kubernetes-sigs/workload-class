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

package webhook

import (
	"context"
	"fmt"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

const conditionTypeMaintenanceAllowed = "MaintenanceAllowed"

// DisruptionWebhook handles Pod eviction requests.
type DisruptionWebhook struct {
	Client  client.Client
	decoder *admission.Decoder
}

// Handle handles admission requests for Pod evictions.
func (v *DisruptionWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := logf.FromContext(ctx).WithValues("name", req.Name, "namespace", req.Namespace, "user", req.UserInfo.Username)

	// 1. Verify this is an eviction, not a standard pod create/delete
	if req.SubResource != "eviction" {
		return admission.Allowed("Not an eviction")
	}

	// 2. Identify the Pod
	pod := &corev1.Pod{}
	if err := v.Client.Get(ctx, client.ObjectKey{Name: req.Name, Namespace: req.Namespace}, pod); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// 3. Find matching WorkloadClass
	bestWC, err := v.bestMatchWorkloadClass(ctx, pod)
	if err != nil {
		log.Error(err, "failed to get WorkloadClass for Pod", "pod", pod)
		return admission.Allowed("Failed to get WorkloadClass matches this pod or namespace")
	}

	if bestWC == nil {
		// No WorkloadClass matches this pod or namespace, allow eviction by default.
		return admission.Allowed("No WorkloadClass matches this pod or namespace")
	}

	log = log.WithValues("workloadClass", bestWC.Name)

	// 4. Check Guardrail/spec validation (if it fails, we don't enforce constraints)
	for _, cond := range bestWC.Status.Conditions {
		if cond.Type == workloadsv1alpha1.ConditionTypeValidated && cond.Status == metav1.ConditionFalse {
			log.Info("WorkloadClass failed Guardrail validation, ignoring constraints")
			return admission.Allowed("WorkloadClass failed Guardrail validation")
		}
	}

	// 5. Request-specific check: Identity-Based Filtering
	for _, allowedSubject := range bestWC.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow {
		allowed, err := matchesIdentity(req.UserInfo, allowedSubject)
		if err != nil {
			log.Error(err, "Failed to check if UserInfo matches allowed Subject", "subject", allowedSubject)
		}
		if allowed {
			return admission.Allowed(fmt.Sprintf("Disruption allowed for authorized user: %s", allowedSubject))
		}
	}

	// 6. Decide from the controller's MaintenanceReadiness status
	switch bestWC.Status.MaintenanceReadiness {
	case workloadsv1alpha1.ReadinessReady:
		return admission.Allowed("Eviction allowed by WorkloadClass policy")
	case workloadsv1alpha1.ReadinessOverdue:
		return admission.Allowed("Workload class is overdue for maintenance, bypassing constraints")
	default:
		for _, cond := range bestWC.Status.Conditions {
			if cond.Type == conditionTypeMaintenanceAllowed && cond.Message != "" {
				return admission.Denied(fmt.Sprintf("Eviction blocked for WorkloadClass %s: %s", bestWC.Name, cond.Message))
			}
		}
		return admission.Denied(fmt.Sprintf("Eviction blocked: WorkloadClass %s is not ready for maintenance", bestWC.Name))
	}
}

// InjectDecoder injects the decoder.
func (v *DisruptionWebhook) InjectDecoder(d *admission.Decoder) error {
	v.decoder = d
	return nil
}
