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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

var _ = Describe("WorkloadClassGuardrail Webhook", func() {
	var (
		obj       *workloadsv1alpha1.WorkloadClassGuardrail
		validator WorkloadClassGuardrailCustomValidator
	)

	BeforeEach(func() {
		obj = &workloadsv1alpha1.WorkloadClassGuardrail{}
		validator = WorkloadClassGuardrailCustomValidator{
			Client: k8sClient,
		}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
	})

	Context("When creating or updating WorkloadClassGuardrail under Validating Webhook", func() {
		It("Should admit valid guardrail when no WorkloadClasses exist", func() {
			obj = &workloadsv1alpha1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "test-guardrail"},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("Should admit valid guardrail when compliant WorkloadClass exists", func() {
			wc := &workloadsv1alpha1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "wc-1",
					Namespace: "default",
				},
				Spec: workloadsv1alpha1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 5,
						AllowedDisruptionWindows: []workloadsv1alpha1.DisruptionWindow{
							{
								Name:       "saturday-window",
								DaysOfWeek: []string{"Saturday"},
								StartTime:  "01:00",
								EndTime:    "05:00",
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, wc) }()

			obj = &workloadsv1alpha1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "test-guardrail"},
				Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1alpha1.Constraints{
						Disruption: workloadsv1alpha1.Disruption{
							MaxNonDisruptionDurationDays: 10,
							DaysOfWeek:                   []string{"Saturday", "Sunday"},
						},
					},
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeEmpty())
		})

		It("Should admit guardrail with warnings if an existing WorkloadClass exceeds maxNonDisruptionDurationDays", func() {
			wc := &workloadsv1alpha1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lax-workloadclass",
					Namespace: "default",
				},
				Spec: workloadsv1alpha1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 30, // Exceeds the guardrail limit of 10
					},
				},
			}
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, wc) }()

			obj = &workloadsv1alpha1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "strict-guardrail"},
				Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1alpha1.Constraints{
						Disruption: workloadsv1alpha1.Disruption{
							MaxNonDisruptionDurationDays: 10,
						},
					},
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).NotTo(BeEmpty())
			Expect(warnings[0]).To(ContainSubstring("lax-workloadclass"))
		})

		It("Should admit guardrail with warnings if an existing WorkloadClass omits maxNonDisruptionDurationDays or allowedDisruptionWindows", func() {
			wc := &workloadsv1alpha1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "unset-fields-workloadclass",
					Namespace: "default",
				},
			}
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, wc) }()

			obj = &workloadsv1alpha1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "strict-guardrail-unset"},
				Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1alpha1.Constraints{
						Disruption: workloadsv1alpha1.Disruption{
							MaxNonDisruptionDurationDays: 10,
							DaysOfWeek:                   []string{"Saturday", "Sunday"},
						},
					},
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).NotTo(BeEmpty())
			Expect(warnings[0]).To(ContainSubstring("unset-fields-workloadclass"))
			Expect(warnings[0]).To(ContainSubstring("`allowedDisruptionWindows` must be specified"))
			Expect(warnings[0]).To(ContainSubstring("0 means no limit"))
		})

		It("Should admit guardrail with warnings if an existing WorkloadClass window omits daysOfWeek when guardrail restricts days", func() {
			wc := &workloadsv1alpha1.WorkloadClass{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "empty-window-days-workloadclass",
					Namespace: "default",
				},
				Spec: workloadsv1alpha1.WorkloadClassSpec{
					DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{
						MaxNonDisruptionDurationDays: 5,
						AllowedDisruptionWindows: []workloadsv1alpha1.DisruptionWindow{
							{
								Name:      "all-days-window",
								StartTime: "01:00",
								EndTime:   "05:00",
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, wc)).To(Succeed())
			defer func() { _ = k8sClient.Delete(ctx, wc) }()

			obj = &workloadsv1alpha1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "weekend-only-guardrail"},
				Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1alpha1.Constraints{
						Disruption: workloadsv1alpha1.Disruption{
							MaxNonDisruptionDurationDays: 10,
							DaysOfWeek:                   []string{"Saturday", "Sunday"},
						},
					},
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).NotTo(BeEmpty())
			Expect(warnings[0]).To(ContainSubstring("empty-window-days-workloadclass"))
		})

		It("Should deny creation if the guardrail itself specifies invalid daysOfWeek", func() {
			obj = &workloadsv1alpha1.WorkloadClassGuardrail{
				ObjectMeta: metav1.ObjectMeta{Name: "invalid-weekday-guardrail"},
				Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{
					Constraints: workloadsv1alpha1.Constraints{
						Disruption: workloadsv1alpha1.Disruption{
							MaxNonDisruptionDurationDays: 10,
							DaysOfWeek:                   []string{"Funday"},
						},
					},
				},
			}
			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(warnings).NotTo(BeEmpty())
		})
	})
})
