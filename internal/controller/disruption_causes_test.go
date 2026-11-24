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

var _ = Describe("Disruption causes", func() {
	const (
		namespace    = "default"
		fieldManager = "disruption-causes-test"
		saName       = "controller-manager"
	)

	Context("WorkloadClass allowedDisruptionsOutsideOfWindow", func() {
		newWorkloadClass := func(name string, causes ...workloadsv1alpha1.DisruptionCause) *workloadsv1alpha1.WorkloadClass {
			return &workloadsv1alpha1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec: workloadsv1alpha1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{AllowedDisruptionsOutsideOfWindow: causes},
				},
			}
		}

		It("accepts well-known and custom disruption causes", func() {
			wc := newWorkloadClass("disruption-causes",
				workloadsv1alpha1.DisruptionCauseVerticalPodAutoscaler,
				workloadsv1alpha1.DisruptionCauseClusterAutoscaler,
				workloadsv1alpha1.DisruptionCauseDriverUpdate,
				workloadsv1alpha1.DisruptionCauseSecurityPatch,
				"KernelUpgrade",
			)
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())
			DeferCleanup(k8sClient.Delete, ctx, wc)
		})

		It("rejects a cause listed twice", func() {
			wc := newWorkloadClass("duplicate-disruption-causes",
				workloadsv1alpha1.DisruptionCauseClusterAutoscaler, workloadsv1alpha1.DisruptionCauseClusterAutoscaler)
			Expect(k8sClient.Create(ctx, wc)).To(MatchError(ContainSubstring("Duplicate value")))
		})

		It("rejects a cause that isn't an UpperCamelCase name", func() {
			wc := newWorkloadClass("invalid-disruption-cause", "system:serviceaccount:kube-system:vpa-updater")
			Expect(k8sClient.Create(ctx, wc)).To(MatchError(ContainSubstring("allowedDisruptionsOutsideOfWindow[0]")))
		})
	})

	Context("WorkloadClassGuardrail disruptionCauses", func() {
		serviceAccount := func(ns string) workloadsv1alpha1.Subject {
			return workloadsv1alpha1.Subject{Kind: rbacv1.ServiceAccountKind, Name: saName, Namespace: ns}
		}

		newGuardrail := func(name string, mappings ...workloadsv1alpha1.DisruptionCauseMapping) *workloadsv1alpha1.WorkloadClassGuardrail {
			return &workloadsv1alpha1.WorkloadClassGuardrail{
				TypeMeta: metav1.TypeMeta{
					APIVersion: workloadsv1alpha1.GroupVersion.String(),
					Kind:       "WorkloadClassGuardrail",
				},
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1alpha1.Constraints{
						Disruption: workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 30},
					},
					DisruptionCauses: mappings,
				},
			}
		}

		autoscaler := func(subjects ...workloadsv1alpha1.Subject) workloadsv1alpha1.DisruptionCauseMapping {
			return workloadsv1alpha1.DisruptionCauseMapping{
				Name: workloadsv1alpha1.DisruptionCauseClusterAutoscaler, Subjects: subjects,
			}
		}

		getSubjects := func(name string) []workloadsv1alpha1.Subject {
			g := &workloadsv1alpha1.WorkloadClassGuardrail{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name}, g)).To(Succeed())
			Expect(g.Spec.DisruptionCauses).To(HaveLen(1))
			return g.Spec.DisruptionCauses[0].Subjects
		}

		It("accepts ServiceAccounts with the same name in different namespaces", func() {
			g := newGuardrail("same-name-service-accounts", autoscaler(serviceAccount("vpa-system"), serviceAccount("karpenter")))
			Expect(k8sClient.Create(ctx, g)).To(Succeed())
			DeferCleanup(k8sClient.Delete, ctx, g)

			Expect(getSubjects(g.Name)).To(Equal([]workloadsv1alpha1.Subject{
				serviceAccount("vpa-system"), serviceAccount("karpenter"),
			}))
		})

		It("replaces the whole subject list on Server-Side Apply instead of merging entries", func() {
			g := newGuardrail("server-side-apply-subjects", autoscaler(serviceAccount("vpa-system")))
			Expect(k8sClient.Patch(ctx, g, client.Apply, client.FieldOwner(fieldManager))).To(Succeed())
			DeferCleanup(k8sClient.Delete, ctx, g)

			g = newGuardrail(g.Name, autoscaler(serviceAccount("vpa-system"), serviceAccount("karpenter")))
			Expect(k8sClient.Patch(ctx, g, client.Apply, client.FieldOwner(fieldManager))).To(Succeed())
			Expect(getSubjects(g.Name)).To(Equal([]workloadsv1alpha1.Subject{
				serviceAccount("vpa-system"), serviceAccount("karpenter"),
			}))

			g = newGuardrail(g.Name, autoscaler(serviceAccount("karpenter")))
			Expect(k8sClient.Patch(ctx, g, client.Apply, client.FieldOwner(fieldManager))).To(Succeed())
			Expect(getSubjects(g.Name)).To(Equal([]workloadsv1alpha1.Subject{serviceAccount("karpenter")}))
		})

		It("rejects a cause mapped twice", func() {
			g := newGuardrail("duplicate-disruption-cause",
				autoscaler(serviceAccount("vpa-system")), autoscaler(serviceAccount("karpenter")))
			Expect(k8sClient.Create(ctx, g)).To(MatchError(ContainSubstring("Duplicate value")))
		})

		It("rejects a cause mapped to no subjects", func() {
			g := newGuardrail("no-subjects", autoscaler())
			Expect(k8sClient.Create(ctx, g)).To(MatchError(ContainSubstring("subjects")))
		})
	})
})
