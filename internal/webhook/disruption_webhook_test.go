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
	"fmt"
	"net/http"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

func TestHandle(t *testing.T) {
	const (
		eviction      = "eviction"
		adminUsername = "admin@mycompany.com"
		podName       = "podrick"
	)
	labels := map[string]string{testLabelKeyA: testLabelValA}

	wc := &workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: testNamespace},
		Spec: workloadsv1alpha1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: labels},
			DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{
				AllowedDisruptionsOutsideOfWindow: []workloadsv1alpha1.Subject{{Kind: rbacv1.UserKind, Name: adminUsername}},
			},
		},
	}
	wcValFailed := &workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: testNamespace},
		Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: &metav1.LabelSelector{MatchLabels: labels}},
		Status: workloadsv1alpha1.WorkloadClassStatus{
			Conditions: []metav1.Condition{{Type: workloadsv1alpha1.ConditionTypeValidated, Status: metav1.ConditionFalse}},
		},
	}
	wcNotReadyWithCondition := &workloadsv1alpha1.WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: testNamespace},
		Spec:       workloadsv1alpha1.WorkloadClassSpec{PodSelector: &metav1.LabelSelector{MatchLabels: labels}},
		Status: workloadsv1alpha1.WorkloadClassStatus{
			Conditions: []metav1.Condition{
				{
					Type:    conditionTypeMaintenanceAllowed,
					Status:  metav1.ConditionFalse,
					Message: "The current time is outside of every allowed disruption window",
				},
			},
		},
	}

	nsNoAnnotation := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: testNamespace, Annotations: map[string]string{"something-else": "true"}},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: testNamespace, Labels: labels}}

	evictionRequest := admissionv1.AdmissionRequest{
		Name: podName, Namespace: testNamespace, SubResource: eviction,
		UserInfo: authv1.UserInfo{Username: adminUsername},
	}
	evictionRequestNonMatchingUser := admissionv1.AdmissionRequest{
		Name: podName, Namespace: testNamespace, SubResource: eviction,
		UserInfo: authv1.UserInfo{Username: "something-else"},
	}

	testCases := []struct {
		name      string
		req       admissionv1.AdmissionRequest
		getPodErr error
		listWCErr error
		getWCErr  error
		getWCResp *workloadsv1alpha1.WorkloadClass
		readiness workloadsv1alpha1.MaintenanceReadiness
		want      admission.Response
	}{
		{
			name: "errorGettingPod", req: evictionRequest, getPodErr: fmt.Errorf("error getting Pod"),
			readiness: workloadsv1alpha1.ReadinessReady,
			want:      admission.Errored(http.StatusInternalServerError, fmt.Errorf("error getting Pod")),
		},
		{
			name:      "notAnEviction",
			req:       admissionv1.AdmissionRequest{Name: adminUsername, Namespace: testNamespace, SubResource: "not-an-eviction"},
			readiness: workloadsv1alpha1.ReadinessNotReady,
			want:      admission.Allowed("Not an eviction"),
		},
		{
			name: "errorGettingBestMatchWC", req: evictionRequest,
			readiness: workloadsv1alpha1.ReadinessNotReady, listWCErr: fmt.Errorf("error listing WorkloadClasses"),
			want: admission.Allowed("Failed to get WorkloadClass matches this pod or namespace"),
		},
		{
			name: "bestMatchWCIsNil", req: evictionRequest,
			readiness: workloadsv1alpha1.ReadinessNotReady,
			want:      admission.Allowed("No WorkloadClass matches this pod or namespace"),
		},
		{
			name: "guardrailValidationFailed", req: evictionRequestNonMatchingUser,
			getWCResp: wcValFailed, readiness: workloadsv1alpha1.ReadinessNotReady,
			want: admission.Allowed("WorkloadClass failed Guardrail validation"),
		},
		{
			name: "allowedUser", req: evictionRequest, getWCResp: wc,
			readiness: workloadsv1alpha1.ReadinessNotReady,
			want:      admission.Allowed(fmt.Sprintf("Disruption allowed for authorized user: %s", wc.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow[0])),
		},
		{
			name: "overdue", req: evictionRequestNonMatchingUser,
			readiness: workloadsv1alpha1.ReadinessOverdue, getWCResp: wc,
			want: admission.Allowed("Workload class is overdue for maintenance, bypassing constraints"),
		},
		{
			name: "notReadyWithoutConditionMessage", req: evictionRequestNonMatchingUser,
			getWCResp: wc, readiness: workloadsv1alpha1.ReadinessNotReady,
			want: admission.Denied(fmt.Sprintf("Eviction blocked: WorkloadClass %s is not ready for maintenance", wc.Name)),
		},
		{
			name: "notReadyWithMaintenanceAllowedConditionMessage", req: evictionRequestNonMatchingUser,
			getWCResp: wcNotReadyWithCondition, readiness: workloadsv1alpha1.ReadinessNotReady,
			want: admission.Denied(fmt.Sprintf("Eviction blocked for WorkloadClass %s: The current time is outside of every allowed disruption window", wc.Name)),
		},
		{
			name: "ready", req: evictionRequestNonMatchingUser, getWCResp: wc,
			readiness: workloadsv1alpha1.ReadinessReady,
			want:      admission.Allowed("Eviction allowed by WorkloadClass policy"),
		},
	}

	ctx := t.Context()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var wcCopy *workloadsv1alpha1.WorkloadClass
			listResp := &workloadsv1alpha1.WorkloadClassList{}
			if tc.getWCResp != nil {
				wcCopy = tc.getWCResp.DeepCopy()
				wcCopy.Status.MaintenanceReadiness = tc.readiness
				listResp.Items = []workloadsv1alpha1.WorkloadClass{*wcCopy}
			}

			v := &DisruptionWebhook{Client: createClient(nil, tc.getWCErr, tc.listWCErr, tc.getPodErr, nsNoAnnotation, wcCopy, listResp, pod)}
			admissionResponse := v.Handle(ctx, admission.Request{AdmissionRequest: tc.req})

			if admissionResponse.Allowed != tc.want.Allowed {
				t.Errorf("Handle() returned an unexpected response: got %v, want %v", admissionResponse, tc.want)
			}
			if admissionResponse.Result != nil && tc.want.Result != nil && admissionResponse.Result.Message != tc.want.Result.Message {
				t.Errorf("Handle() returned an unexpected message: got %q, want %q", admissionResponse.Result.Message, tc.want.Result.Message)
			}
		})
	}
}
