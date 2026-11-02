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
	"slices"

	authv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

func matchesIdentity(userInfo authv1.UserInfo, subject workloadsv1alpha1.Subject) (bool, error) {
	if subject.Name == "" {
		return false, fmt.Errorf("subject name cannot be empty")
	}

	switch subject.Kind {
	case rbacv1.UserKind:
		return matchesUserKind(userInfo, subject)
	case rbacv1.GroupKind:
		return matchesGroupKind(userInfo, subject)
	case rbacv1.ServiceAccountKind:
		return matchesServiceAccountKind(userInfo, subject)
	default:
		return false, fmt.Errorf("subject has invalid Kind: %s", subject.Kind)
	}
}

func matchesUserKind(userInfo authv1.UserInfo, subject workloadsv1alpha1.Subject) (bool, error) {
	if subject.Namespace != "" {
		return false, fmt.Errorf("subject is kind %s, but has Namespace: %s", subject.Kind, subject.Namespace)
	}

	return userInfo.Username == subject.Name, nil
}

func matchesGroupKind(userInfo authv1.UserInfo, subject workloadsv1alpha1.Subject) (bool, error) {
	if subject.Namespace != "" {
		return false, fmt.Errorf("subject is kind %s, but has Namespace: %s", subject.Kind, subject.Namespace)
	}

	return slices.Contains(userInfo.Groups, subject.Name), nil
}

func matchesServiceAccountKind(userInfo authv1.UserInfo, subject workloadsv1alpha1.Subject) (bool, error) {
	if subject.Namespace == "" {
		return false, fmt.Errorf("subject is kind %s, but Namespace cannot be empty", subject.Kind)
	}

	expectedUsername := fmt.Sprintf("system:serviceaccount:%s:%s", subject.Namespace, subject.Name)
	return userInfo.Username == expectedUsername, nil
}
