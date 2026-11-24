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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

const conditionTypeMaintenanceAllowed = "MaintenanceAllowed"

// DisruptionWebhookPath is the path the DisruptionWebhook is served on. It must match the path in the
// +kubebuilder:webhook marker below.
const DisruptionWebhookPath = "/validate-disruption"

// The webhook only receives pods/eviction requests, so a Pod delete that does not go through the Eviction API is
// never intercepted.
//
// failurePolicy is Ignore: if the webhook cannot be reached (e.g. the controller is down or being rolled out), the
// API server admits the eviction instead of rejecting every eviction in the cluster, which would block node drains,
// node upgrades, and cluster autoscaler scale-down. The Per-Pod PodDisruptionBudgets managed by the WorkloadClass
// controller keep enforcing the disruption policy while the webhook is unavailable. Pods without a controller-managed
// Per-Pod PodDisruptionBudget lose disruption-window enforcement while the webhook is unavailable: Pods covered by a
// user-created PodDisruptionBudget (which do not get a Per-Pod PodDisruptionBudget) and newly created Pods that the
// controller has not yet reconciled.
//
// The namespaceSelector that excludes kube-system and the controller's own namespace (so that draining the node of
// the controller Pod cannot deadlock on the webhook) is added in config/default, since the marker cannot express it.
//
// +kubebuilder:webhook:path=/validate-disruption,mutating=false,failurePolicy=ignore,sideEffects=None,timeoutSeconds=5,groups="",resources=pods/eviction,verbs=create,versions=v1,name=vpoddisruption.workloads.x-k8s.io,admissionReviewVersions=v1

// The webhook reads the evicted Pod, its Namespace, and the WorkloadClasses in the Namespace from the manager's cache.
//
// +kubebuilder:rbac:groups="",resources=pods;namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=workloads.x-k8s.io,resources=workloadclasses,verbs=get;list;watch

// DisruptionWebhook handles Pod eviction requests.
type DisruptionWebhook struct {
	Client client.Client
}

// SetupDisruptionWebhookWithManager registers the DisruptionWebhook with the manager's webhook server.
func SetupDisruptionWebhookWithManager(mgr ctrl.Manager) error {
	// Pre-register informers so the cache syncs at manager startup rather than lazily inside the 5s timeout of the
	// first pods/eviction request (no controller watches Namespaces).
	for _, obj := range []client.Object{
		&corev1.Pod{},
		&corev1.Namespace{},
		&workloadsv1alpha1.WorkloadClass{},
	} {
		if _, err := mgr.GetCache().GetInformer(context.Background(), obj); err != nil {
			return err
		}
	}

	mgr.GetWebhookServer().Register(DisruptionWebhookPath, &admission.Webhook{
		Handler: &DisruptionWebhook{Client: mgr.GetClient()},
	})
	return nil
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
