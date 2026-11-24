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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

var _ = Describe("WorkloadClass allowedDisruptionsOutsideOfWindow", func() {
	const (
		namespace    = "default"
		fieldManager = "subjects-test"
		saName       = "controller-manager"
	)

	newWorkloadClass := func(name string, subjects ...workloadsv1alpha1.Subject) *workloadsv1alpha1.WorkloadClass {
		return &workloadsv1alpha1.WorkloadClass{
			TypeMeta: metav1.TypeMeta{
				APIVersion: workloadsv1alpha1.GroupVersion.String(),
				Kind:       "WorkloadClass",
			},
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: workloadsv1alpha1.WorkloadClassSpec{
				DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{
					AllowedDisruptionsOutsideOfWindow: subjects,
				},
			},
		}
	}

	serviceAccount := func(ns string) workloadsv1alpha1.Subject {
		return workloadsv1alpha1.Subject{Kind: rbacv1.ServiceAccountKind, Name: saName, Namespace: ns}
	}

	getSubjects := func(name string) []workloadsv1alpha1.Subject {
		wc := &workloadsv1alpha1.WorkloadClass{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, wc)).To(Succeed())
		return wc.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow
	}

	It("accepts ServiceAccounts with the same name in different namespaces", func() {
		wc := newWorkloadClass("same-name-service-accounts", serviceAccount("vpa-system"), serviceAccount("karpenter"))
		Expect(k8sClient.Create(ctx, wc)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, wc)

		Expect(getSubjects(wc.Name)).To(Equal([]workloadsv1alpha1.Subject{
			serviceAccount("vpa-system"), serviceAccount("karpenter"),
		}))
	})

	It("replaces the whole list on Server-Side Apply instead of merging entries", func() {
		wc := newWorkloadClass("server-side-apply-subjects", serviceAccount("vpa-system"))
		Expect(k8sClient.Patch(ctx, wc, client.Apply, client.FieldOwner(fieldManager))).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, wc)

		// With the old kind/name list map keys, this apply overwrote the first entry's namespace
		// instead of keeping two distinct subjects.
		wc = newWorkloadClass(wc.Name, serviceAccount("vpa-system"), serviceAccount("karpenter"))
		Expect(k8sClient.Patch(ctx, wc, client.Apply, client.FieldOwner(fieldManager))).To(Succeed())
		Expect(getSubjects(wc.Name)).To(Equal([]workloadsv1alpha1.Subject{
			serviceAccount("vpa-system"), serviceAccount("karpenter"),
		}))

		wc = newWorkloadClass(wc.Name, serviceAccount("karpenter"))
		Expect(k8sClient.Patch(ctx, wc, client.Apply, client.FieldOwner(fieldManager))).To(Succeed())
		Expect(getSubjects(wc.Name)).To(Equal([]workloadsv1alpha1.Subject{serviceAccount("karpenter")}))
	})
})
