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
	"testing"

	authv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

const (
	testUserJane   = "jane.doe@example.com"
	testUserBob    = "bob@example.com"
	testGroupDev   = "devops@example.com"
	testSAName     = "my-app-sa"
	testDefaultNS  = "default"
	testSAFullName = "system:serviceaccount:default:my-app-sa"
	testSAOtherNS  = "system:serviceaccount:kube-system:my-app-sa"
)

func TestMatchesUserKind(t *testing.T) {
	testCases := []struct {
		name     string
		userInfo authv1.UserInfo
		subject  workloadsv1alpha1.Subject
		want     bool
		wantErr  bool
	}{
		{
			name:     "exact_username_match_without_namespace",
			userInfo: authv1.UserInfo{Username: testUserJane},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.UserKind, Name: testUserJane},
			want:     true,
		},
		{
			name:     "username_mismatch_without_namespace",
			userInfo: authv1.UserInfo{Username: testUserBob},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.UserKind, Name: testUserJane},
		},
		{
			name:     "returns_error_if_namespace_is_provided_even_if_username_matches",
			userInfo: authv1.UserInfo{Username: testUserJane},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.UserKind, Name: testUserJane, Namespace: testDefaultNS},
			wantErr:  true,
		},
		{
			name:     "returns_error_if_namespace_is_provided_mismatch",
			userInfo: authv1.UserInfo{Username: testUserBob},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.UserKind, Name: testUserJane, Namespace: "kube-system"},
			wantErr:  true,
		},
		{
			name:     "empty_username_does_not_match",
			userInfo: authv1.UserInfo{Username: ""},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.UserKind, Name: testUserJane},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := matchesUserKind(tc.userInfo, tc.subject)
			if (err != nil) != tc.wantErr {
				t.Fatalf("matchesUserKind() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("matchesUserKind() got = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatchesGroupKind(t *testing.T) {
	testCases := []struct {
		name     string
		userInfo authv1.UserInfo
		subject  workloadsv1alpha1.Subject
		want     bool
		wantErr  bool
	}{
		{
			name:     "exact_group_match_single_group",
			userInfo: authv1.UserInfo{Groups: []string{"system:masters"}},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.GroupKind, Name: "system:masters"},
			want:     true,
		},
		{
			name:     "exact_group_match_multiple_groups",
			userInfo: authv1.UserInfo{Groups: []string{"system:authenticated", testGroupDev, "developers"}},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.GroupKind, Name: "system:authenticated"},
			want:     true,
		},
		{
			name:     "group_mismatch",
			userInfo: authv1.UserInfo{Groups: []string{"system:authenticated", "developers"}},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.GroupKind, Name: "system:masters"},
		},
		{
			name:     "returns_error_if_namespace_is_provided_even_if_group_matches",
			userInfo: authv1.UserInfo{Groups: []string{testGroupDev}},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.GroupKind, Name: testGroupDev, Namespace: testDefaultNS},
			wantErr:  true,
		},
		{
			name:     "user_with_no_groups_does_not_match",
			userInfo: authv1.UserInfo{Groups: []string{}},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.GroupKind, Name: "developers"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := matchesGroupKind(tc.userInfo, tc.subject)
			if (err != nil) != tc.wantErr {
				t.Fatalf("matchesGroupKind() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("matchesGroupKind() got = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatchesServiceAccountKind(t *testing.T) {
	testCases := []struct {
		name     string
		userInfo authv1.UserInfo
		subject  workloadsv1alpha1.Subject
		want     bool
		wantErr  bool
	}{
		{
			name:     "exact_service_account_match",
			userInfo: authv1.UserInfo{Username: testSAFullName},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.ServiceAccountKind, Name: testSAName, Namespace: testDefaultNS},
			want:     true,
		},
		{
			name:     "mismatch_on_namespace",
			userInfo: authv1.UserInfo{Username: testSAOtherNS},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.ServiceAccountKind, Name: testSAName, Namespace: testDefaultNS},
		},
		{
			name:     "mismatch_on_name",
			userInfo: authv1.UserInfo{Username: "system:serviceaccount:default:other-sa"},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.ServiceAccountKind, Name: testSAName, Namespace: testDefaultNS},
		},
		{
			name:     "returns_error_if_namespace_is_empty",
			userInfo: authv1.UserInfo{Username: testSAFullName},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.ServiceAccountKind, Name: testSAName},
			wantErr:  true,
		},
		{
			name:     "regular_user_trying_to_spoof_service_account_structure_fails",
			userInfo: authv1.UserInfo{Username: testUserJane},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.ServiceAccountKind, Name: testUserJane, Namespace: testDefaultNS},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := matchesServiceAccountKind(tc.userInfo, tc.subject)
			if (err != nil) != tc.wantErr {
				t.Fatalf("matchesServiceAccountKind() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("matchesServiceAccountKind() got = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatchesIdentity(t *testing.T) {
	testCases := []struct {
		name     string
		userInfo authv1.UserInfo
		subject  workloadsv1alpha1.Subject
		want     bool
		wantErr  bool
	}{
		{
			name:     "returns_error_if_subject_name_is_empty",
			userInfo: authv1.UserInfo{Username: testUserJane},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.UserKind, Name: ""},
			wantErr:  true,
		},
		{
			name:     "returns_error_for_unsupported_or_invalid_Kind",
			userInfo: authv1.UserInfo{Username: testUserJane},
			subject:  workloadsv1alpha1.Subject{Kind: "RoleBinding", Name: "my-role"},
			wantErr:  true,
		},
		{
			name:     "propagates_errors_from_sub-functions_User_with_Namespace",
			userInfo: authv1.UserInfo{Username: testUserJane},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.UserKind, Name: testUserJane, Namespace: testDefaultNS},
			wantErr:  true,
		},
		{
			name:     "propagates_errors_from_sub-functions_ServiceAccount_without_Namespace",
			userInfo: authv1.UserInfo{Username: testSAFullName},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.ServiceAccountKind, Name: testSAName},
			wantErr:  true,
		},
		{
			name:     "successfully_routes_and_matches_User",
			userInfo: authv1.UserInfo{Username: testUserJane},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.UserKind, Name: testUserJane},
			want:     true,
		},
		{
			name:     "successfully_routes_and_matches_Group",
			userInfo: authv1.UserInfo{Groups: []string{testGroupDev}},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.GroupKind, Name: testGroupDev},
			want:     true,
		},
		{
			name:     "successfully_routes_and_matches_ServiceAccount",
			userInfo: authv1.UserInfo{Username: testSAFullName},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.ServiceAccountKind, Name: testSAName, Namespace: testDefaultNS},
			want:     true,
		},
		{
			name:     "correctly_routes_but_fails_match_for_ServiceAccount",
			userInfo: authv1.UserInfo{Username: testSAOtherNS},
			subject:  workloadsv1alpha1.Subject{Kind: rbacv1.ServiceAccountKind, Name: testSAName, Namespace: testDefaultNS},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := matchesIdentity(tc.userInfo, tc.subject)
			if (err != nil) != tc.wantErr {
				t.Fatalf("matchesIdentity() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("matchesIdentity() got = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatchDisruptionCause(t *testing.T) {
	const (
		vpa        = workloadsv1alpha1.DisruptionCauseVerticalPodAutoscaler
		autoscaler = workloadsv1alpha1.DisruptionCauseClusterAutoscaler
	)
	vpaSA := workloadsv1alpha1.Subject{Kind: rbacv1.ServiceAccountKind, Name: testSAName, Namespace: testDefaultNS}
	invalidSA := workloadsv1alpha1.Subject{Kind: rbacv1.ServiceAccountKind, Name: testSAName}
	jane := workloadsv1alpha1.Subject{Kind: rbacv1.UserKind, Name: testUserJane}
	guardrail := func(name string, mappings ...workloadsv1alpha1.DisruptionCauseMapping) workloadsv1alpha1.WorkloadClassGuardrail {
		g := workloadsv1alpha1.WorkloadClassGuardrail{Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{DisruptionCauses: mappings}}
		g.Name = name
		return g
	}
	mapping := func(cause workloadsv1alpha1.DisruptionCause, subjects ...workloadsv1alpha1.Subject) workloadsv1alpha1.DisruptionCauseMapping {
		return workloadsv1alpha1.DisruptionCauseMapping{Name: cause, Subjects: subjects}
	}

	testCases := []struct {
		name        string
		userInfo    authv1.UserInfo
		causes      []workloadsv1alpha1.DisruptionCause
		guardrails  []workloadsv1alpha1.WorkloadClassGuardrail
		wantCause   workloadsv1alpha1.DisruptionCause
		wantSubject workloadsv1alpha1.Subject
		wantMatch   bool
		wantErr     bool
	}{
		{
			name:        "matches_ServiceAccount_mapped_to_allowed_cause",
			userInfo:    authv1.UserInfo{Username: testSAFullName},
			causes:      []workloadsv1alpha1.DisruptionCause{vpa},
			guardrails:  []workloadsv1alpha1.WorkloadClassGuardrail{guardrail("g", mapping(vpa, vpaSA))},
			wantCause:   vpa,
			wantSubject: vpaSA,
			wantMatch:   true,
		},
		{
			name:     "does_not_match_identity_mapped_to_a_cause_the_WorkloadClass_does_not_allow",
			userInfo: authv1.UserInfo{Username: testSAFullName},
			causes:   []workloadsv1alpha1.DisruptionCause{autoscaler},
			guardrails: []workloadsv1alpha1.WorkloadClassGuardrail{
				guardrail("g", mapping(vpa, vpaSA), mapping(autoscaler, jane)),
			},
		},
		{
			name:       "does_not_match_cause_no_guardrail_maps",
			userInfo:   authv1.UserInfo{Username: testUserJane},
			causes:     []workloadsv1alpha1.DisruptionCause{autoscaler},
			guardrails: []workloadsv1alpha1.WorkloadClassGuardrail{guardrail("g", mapping(vpa, jane))},
		},
		{
			name:     "does_not_match_without_guardrails",
			userInfo: authv1.UserInfo{Username: testUserJane},
			causes:   []workloadsv1alpha1.DisruptionCause{vpa},
		},
		{
			name:     "matches_subject_of_any_guardrail_that_maps_the_cause",
			userInfo: authv1.UserInfo{Username: testUserJane},
			causes:   []workloadsv1alpha1.DisruptionCause{vpa},
			guardrails: []workloadsv1alpha1.WorkloadClassGuardrail{
				guardrail("a", mapping(vpa, vpaSA)),
				guardrail("b", mapping(vpa, jane)),
			},
			wantCause:   vpa,
			wantSubject: jane,
			wantMatch:   true,
		},
		{
			name:     "matches_second_allowed_cause",
			userInfo: authv1.UserInfo{Username: testUserJane},
			causes:   []workloadsv1alpha1.DisruptionCause{vpa, autoscaler},
			guardrails: []workloadsv1alpha1.WorkloadClassGuardrail{
				guardrail("g", mapping(vpa, vpaSA), mapping(autoscaler, jane)),
			},
			wantCause:   autoscaler,
			wantSubject: jane,
			wantMatch:   true,
		},
		{
			name:        "keeps_looking_past_invalid_subject_and_reports_it",
			userInfo:    authv1.UserInfo{Username: testSAFullName},
			causes:      []workloadsv1alpha1.DisruptionCause{vpa},
			guardrails:  []workloadsv1alpha1.WorkloadClassGuardrail{guardrail("g", mapping(vpa, invalidSA, vpaSA))},
			wantCause:   vpa,
			wantSubject: vpaSA,
			wantMatch:   true,
			wantErr:     true,
		},
		{
			name:       "reports_invalid_subject_without_match",
			userInfo:   authv1.UserInfo{Username: testSAFullName},
			causes:     []workloadsv1alpha1.DisruptionCause{vpa},
			guardrails: []workloadsv1alpha1.WorkloadClassGuardrail{guardrail("g", mapping(vpa, invalidSA))},
			wantErr:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cause, subject, matched, err := matchDisruptionCause(tc.userInfo, tc.causes, tc.guardrails)
			if (err != nil) != tc.wantErr {
				t.Errorf("matchDisruptionCause() error = %v, wantErr %v", err, tc.wantErr)
			}
			if matched != tc.wantMatch || cause != tc.wantCause || subject != tc.wantSubject {
				t.Errorf("matchDisruptionCause() = (%q, %+v, %v), want (%q, %+v, %v)",
					cause, subject, matched, tc.wantCause, tc.wantSubject, tc.wantMatch)
			}
		})
	}
}

func TestFormatSubject(t *testing.T) {
	testCases := []struct {
		subject workloadsv1alpha1.Subject
		want    string
	}{
		{subject: workloadsv1alpha1.Subject{Kind: rbacv1.UserKind, Name: testUserJane}, want: "User " + testUserJane},
		{
			subject: workloadsv1alpha1.Subject{Kind: rbacv1.ServiceAccountKind, Name: testSAName, Namespace: testDefaultNS},
			want:    "ServiceAccount default/my-app-sa",
		},
	}
	for _, tc := range testCases {
		if got := formatSubject(tc.subject); got != tc.want {
			t.Errorf("formatSubject(%+v) = %q, want %q", tc.subject, got, tc.want)
		}
	}
}

// guardrailClient serves WorkloadClassGuardrail lists and delegates everything else to fakeClient.
type guardrailClient struct {
	fakeClient
	guardrails *workloadsv1alpha1.WorkloadClassGuardrailList
	listErr    error
}

func (c guardrailClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	guardrails, ok := list.(*workloadsv1alpha1.WorkloadClassGuardrailList)
	if !ok {
		return c.fakeClient.List(ctx, list, opts...)
	}
	if c.listErr != nil {
		return c.listErr
	}
	c.guardrails.DeepCopyInto(guardrails)
	return nil
}
