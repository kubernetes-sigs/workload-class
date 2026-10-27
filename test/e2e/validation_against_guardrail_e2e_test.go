//go:build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
	"sigs.k8s.io/workload-class/test/utils"
)

const (
	// testNamespace holds the WorkloadClasses created by this file.
	testNamespace = "wc-validation-e2e"

	// testLabel is set on every cluster-scoped WorkloadClassGuardrail created by this file, so that cleanup
	// only deletes the guardrails this file owns.
	testLabel = "e2e.workloads.x-k8s.io/suite=wc-validation"

	pollTimeout  = 2 * time.Minute
	pollInterval = 2 * time.Second
)

// These specs deploy the controller manager to a Kind cluster and exercise WorkloadClass validation end to
// end: the Validated status condition written by the WorkloadClass controller, re-validation triggered by
// watches on WorkloadClassGuardrails and sibling WorkloadClasses, CRD schema validation, and the
// WorkloadClassGuardrail validating admission webhook.
var _ = Describe("WorkloadClass validation", Ordered, func() {
	BeforeAll(func() {
		By("creating the manager namespace")
		_, err := utils.Run(exec.Command("kubectl", "create", "ns", namespace))
		Expect(err).NotTo(HaveOccurred(), "Failed to create the manager namespace")

		By("labeling the manager namespace to enforce the restricted security policy")
		_, err = utils.Run(exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted"))
		Expect(err).NotTo(HaveOccurred(), "Failed to label the manager namespace")

		By("installing CRDs")
		_, err = utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller manager")
		_, err = utils.Run(exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage)))
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller manager")

		By("waiting for the controller manager pod to be running")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", namespace,
				"-l", "control-plane=controller-manager",
				"-o", "jsonpath={.items[*].status.phase}"))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.Fields(out)).To(ConsistOf("Running"))
		}, 5*time.Minute, pollInterval).Should(Succeed())

		By("creating the test namespace")
		_, err = utils.Run(exec.Command("kubectl", "create", "ns", testNamespace))
		Expect(err).NotTo(HaveOccurred(), "Failed to create the test namespace")

		By("waiting for the WorkloadClassGuardrail webhook to serve requests")
		Eventually(func() error {
			_, err := apply(newGuardrail("wc-e2e-webhook-probe",
				workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 365}))
			return err
		}, pollTimeout, 5*time.Second).Should(Succeed())
		deleteTestGuardrails()
	})

	AfterAll(func() {
		By("deleting the test resources")
		deleteTestGuardrails()
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", testNamespace, "--ignore-not-found"))

		By("undeploying the controller manager")
		_, _ = utils.Run(exec.Command("make", "undeploy"))

		By("uninstalling CRDs")
		_, _ = utils.Run(exec.Command("make", "uninstall"))

		By("removing the manager namespace")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found"))
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			dumpDiagnostics()
		}

		By("deleting the WorkloadClasses and WorkloadClassGuardrails created by the spec")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclasses", "--all", "-n", testNamespace, "--wait"))
		deleteTestGuardrails()
	})

	Context("Validated status condition", func() {
		It("reports NoGuardrails when no WorkloadClassGuardrail exists", func() {
			mustApply(newWorkloadClass("no-guardrails", map[string]string{"app": "no-guardrails"},
				withMaxNonDisruptionDays(7)))

			cond := eventuallyValidated("no-guardrails", metav1.ConditionTrue, workloadsv1alpha1.ReasonNoGuardrails)
			Expect(cond.Message).To(Equal("No Guardrails found to validate against"))
		})

		It("reports ValidationPassed for a WorkloadClass that complies with every guardrail", func() {
			mustApply(newGuardrail("wc-e2e-weekend", workloadsv1alpha1.Disruption{
				DaysOfWeek:                   []string{"Saturday", "Sunday"},
				MaxAllowedWindows:            2,
				MaxNonDisruptionDurationDays: 14,
			}))
			mustApply(newGuardrail("wc-e2e-monthly", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 30}))
			mustApply(newWorkloadClass("compliant", map[string]string{"app": "compliant"},
				withWindows(window("weekend", "Saturday", "Sunday")),
				withMaxNonDisruptionDays(14)))

			cond := eventuallyValidated("compliant", metav1.ConditionTrue, workloadsv1alpha1.ReasonValidationPassed)
			Expect(cond.Message).To(Equal("WorkloadClass adheres to all Guardrail constraints"))
		})

		It("reports every guardrail violation with the limits, but without guardrail names", func() {
			const guardrailName = "wc-e2e-strict"
			mustApply(newGuardrail(guardrailName, workloadsv1alpha1.Disruption{
				DaysOfWeek:                   []string{"Saturday", "Sunday"},
				MaxAllowedWindows:            2,
				MaxNonDisruptionDurationDays: 14,
			}))
			// maxNonDisruptionDurationDays is omitted, which means no limit.
			mustApply(newWorkloadClass("non-compliant", map[string]string{"app": "non-compliant"},
				withWindows(window("mon", "Monday"), window("sat", "Saturday"), window("sun", "Sunday"))))

			cond := eventuallyValidated("non-compliant", metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
			Expect(cond.Message).To(SatisfyAll(
				ContainSubstring("`daysOfWeek` of disruption window 'mon' must contain only 'Sunday', 'Saturday' "+
					"(guardrail limit), got 'Monday'"),
				ContainSubstring("`allowedDisruptionWindows` must have at most 2 items (guardrail limit), got 3"),
				ContainSubstring("`maxNonDisruptionDurationDays` must be between 1 and 14, inclusive (guardrail limit), "+
					"got 0 (0 means no limit)"),
				Not(ContainSubstring(guardrailName)),
			))
		})

		It("reports a violation shared by several guardrails only once", func() {
			mustApply(newGuardrail("wc-e2e-two-weeks-a", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 14}))
			mustApply(newGuardrail("wc-e2e-two-weeks-b", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 14}))
			mustApply(newWorkloadClass("shared-violation", map[string]string{"app": "shared-violation"},
				withMaxNonDisruptionDays(30)))

			cond := eventuallyValidated("shared-violation", metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
			Expect(cond.Message).To(Equal(
				"`maxNonDisruptionDurationDays` must be between 1 and 14, inclusive (guardrail limit), got 30 (0 means no limit)"))
		})

		It("combines several guardrails into the strictest limit for each constraint", func() {
			mustApply(newGuardrail("wc-e2e-combined-a", workloadsv1alpha1.Disruption{
				DaysOfWeek:                   []string{"Friday", "Saturday", "Sunday"},
				MaxAllowedWindows:            3,
				MaxNonDisruptionDurationDays: 30,
			}))
			mustApply(newGuardrail("wc-e2e-combined-b", workloadsv1alpha1.Disruption{
				DaysOfWeek:                   []string{"Saturday", "Sunday", "Monday"},
				MaxAllowedWindows:            1,
				MaxNonDisruptionDurationDays: 14,
			}))
			// Every value satisfies at least one guardrail, but not the strictest combined limit.
			mustApply(newWorkloadClass("combined", map[string]string{"app": "combined"},
				withWindows(window("fri", "Friday"), window("sat", "Saturday")),
				withMaxNonDisruptionDays(21)))

			cond := eventuallyValidated("combined", metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
			Expect(cond.Message).To(SatisfyAll(
				ContainSubstring("`daysOfWeek` of disruption window 'fri' must contain only 'Sunday', 'Saturday' "+
					"(guardrail limit), got 'Friday'"),
				ContainSubstring("`allowedDisruptionWindows` must have at most 1 items (guardrail limit), got 2"),
				ContainSubstring("`maxNonDisruptionDurationDays` must be between 1 and 14, inclusive (guardrail limit), "+
					"got 21"),
				Not(ContainSubstring("window 'sat'")),
				Not(ContainSubstring("between 1 and 30")),
				Not(ContainSubstring("at most 3 items")),
			))
		})

		It("reports a single actionable message when guardrails allow no common day", func() {
			mustApply(newGuardrail("wc-e2e-disjoint-weekend", workloadsv1alpha1.Disruption{
				DaysOfWeek:                   []string{"Saturday", "Sunday"},
				MaxNonDisruptionDurationDays: 30,
			}))
			mustApply(newGuardrail("wc-e2e-disjoint-monday", workloadsv1alpha1.Disruption{
				DaysOfWeek:                   []string{"Monday"},
				MaxNonDisruptionDurationDays: 30,
			}))
			mustApply(newWorkloadClass("disjoint", map[string]string{"app": "disjoint"},
				withWindows(window("sat", "Saturday")),
				withMaxNonDisruptionDays(7)))

			cond := eventuallyValidated("disjoint", metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
			Expect(cond.Message).To(Equal("cluster guardrails restrict `daysOfWeek` to disjoint sets, so no " +
				"disruption window can comply; contact your cluster administrator"))
		})

		It("reports invalid weekday names and time zones even without guardrails", func() {
			badDay := window("funday", "Funday")
			badZone := window("mars", "Saturday")
			badZone.TimeZone = "Mars/Phobos"
			mustApply(newWorkloadClass("invalid-spec", map[string]string{"app": "invalid-spec"},
				withWindows(badDay, badZone),
				withMaxNonDisruptionDays(7)))

			cond := eventuallyValidated("invalid-spec", metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
			Expect(cond.Message).To(SatisfyAll(
				ContainSubstring("disruption window 'funday': `daysOfWeek` must only contain"),
				ContainSubstring("got 'Funday'"),
				ContainSubstring("`timeZone` of disruption window 'mars' must be a valid IANA time zone name "+
					"other than 'Local', got 'Mars/Phobos'"),
			))
		})

		It("updates observedGeneration and the result when the WorkloadClass spec changes", func() {
			mustApply(newGuardrail("wc-e2e-limit", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 14}))
			mustApply(newWorkloadClass("spec-change", map[string]string{"app": "spec-change"},
				withMaxNonDisruptionDays(7)))
			eventuallyValidated("spec-change", metav1.ConditionTrue, workloadsv1alpha1.ReasonValidationPassed)

			By("raising maxNonDisruptionDurationDays above the guardrail limit")
			mustPatch("workloadclass", "spec-change", testNamespace,
				`{"spec":{"disruptionPolicy":{"maxNonDisruptionDurationDays":21}}}`)
			cond := eventuallyValidated("spec-change", metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
			Expect(cond.Message).To(ContainSubstring("got 21"))

			By("lowering it back within the limit")
			mustPatch("workloadclass", "spec-change", testNamespace,
				`{"spec":{"disruptionPolicy":{"maxNonDisruptionDurationDays":14}}}`)
			eventuallyValidated("spec-change", metav1.ConditionTrue, workloadsv1alpha1.ReasonValidationPassed)
		})
	})

	Context("re-validation when guardrails change", func() {
		It("re-validates existing WorkloadClasses when a guardrail is created, updated, or deleted", func() {
			mustApply(newWorkloadClass("watched", map[string]string{"app": "watched"},
				withWindows(window("weekend", "Saturday", "Sunday")),
				withMaxNonDisruptionDays(14)))
			eventuallyValidated("watched", metav1.ConditionTrue, workloadsv1alpha1.ReasonNoGuardrails)

			By("creating a guardrail the WorkloadClass complies with")
			mustApply(newGuardrail("wc-e2e-changing", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 30}))
			eventuallyValidated("watched", metav1.ConditionTrue, workloadsv1alpha1.ReasonValidationPassed)

			By("tightening the guardrail below the WorkloadClass value")
			mustPatch("workloadclassguardrail", "wc-e2e-changing", "",
				`{"spec":{"constraints":{"disruption":{"maxNonDisruptionDurationDays":7}}}}`)
			cond := eventuallyValidated("watched", metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
			Expect(cond.Message).To(ContainSubstring("must be between 1 and 7, inclusive (guardrail limit), got 14"))

			By("restricting the allowed days so the window no longer complies")
			mustPatch("workloadclassguardrail", "wc-e2e-changing", "",
				`{"spec":{"constraints":{"disruption":{"maxNonDisruptionDurationDays":30,"daysOfWeek":["Sunday"]}}}}`)
			cond = eventuallyValidated("watched", metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
			Expect(cond.Message).To(Equal(
				"`daysOfWeek` of disruption window 'weekend' must contain only 'Sunday' (guardrail limit), " +
					"got 'Saturday', 'Sunday'"))

			By("deleting the guardrail")
			deleteTestGuardrails()
			eventuallyValidated("watched", metav1.ConditionTrue, workloadsv1alpha1.ReasonNoGuardrails)
		})

		It("treats omitted allowedDisruptionWindows as every day once a guardrail restricts days", func() {
			mustApply(newWorkloadClass("no-windows", map[string]string{"app": "no-windows"},
				withMaxNonDisruptionDays(7)))
			eventuallyValidated("no-windows", metav1.ConditionTrue, workloadsv1alpha1.ReasonNoGuardrails)

			mustApply(newGuardrail("wc-e2e-weekdays", workloadsv1alpha1.Disruption{
				DaysOfWeek:                   []string{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday"},
				MaxNonDisruptionDurationDays: 30,
			}))
			cond := eventuallyValidated("no-windows", metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
			Expect(cond.Message).To(ContainSubstring(
				"`allowedDisruptionWindows` must be specified, with `daysOfWeek` containing only " +
					"'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday' (guardrail limit)"))
		})
	})

	Context("podSelector overlap", func() {
		It("marks both WorkloadClasses and clears the overlap when one selector changes", func() {
			mustApply(newWorkloadClass("web", map[string]string{"app": "web"}, withMaxNonDisruptionDays(7)))
			mustApply(newWorkloadClass("web-frontend", map[string]string{"app": "web", "tier": "frontend"},
				withMaxNonDisruptionDays(7)))

			cond := eventuallyValidated("web", metav1.ConditionFalse, workloadsv1alpha1.ReasonSelectorOverlap)
			Expect(cond.Message).To(Equal("`podSelector` must not select the same Pods as another WorkloadClass in the " +
				"namespace, but it overlaps with the `podSelector` of 'web-frontend'"))
			cond = eventuallyValidated("web-frontend", metav1.ConditionFalse, workloadsv1alpha1.ReasonSelectorOverlap)
			Expect(cond.Message).To(ContainSubstring("overlaps with the `podSelector` of 'web'"))

			By("changing web-frontend so it no longer selects app=web Pods")
			mustPatch("workloadclass", "web-frontend", testNamespace,
				`{"spec":{"podSelector":{"matchLabels":{"app":"db"}}}}`)

			// Only web-frontend changed, so web is re-validated through the watch on sibling WorkloadClasses.
			eventuallyValidated("web", metav1.ConditionTrue, workloadsv1alpha1.ReasonNoGuardrails)
			eventuallyValidated("web-frontend", metav1.ConditionTrue, workloadsv1alpha1.ReasonNoGuardrails)
		})

		It("clears the overlap when the overlapping WorkloadClass is deleted", func() {
			mustApply(newWorkloadClass("batch", map[string]string{"app": "batch"}, withMaxNonDisruptionDays(7)))
			mustApply(newWorkloadClass("batch-copy", map[string]string{"app": "batch"}, withMaxNonDisruptionDays(7)))
			eventuallyValidated("batch", metav1.ConditionFalse, workloadsv1alpha1.ReasonSelectorOverlap)

			_, err := utils.Run(exec.Command("kubectl", "delete", "workloadclass", "batch-copy", "-n", testNamespace))
			Expect(err).NotTo(HaveOccurred())

			eventuallyValidated("batch", metav1.ConditionTrue, workloadsv1alpha1.ReasonNoGuardrails)
		})

		It("evaluates matchExpressions when deciding whether selectors overlap", func() {
			mustApply(newWorkloadClass("api", map[string]string{"app": "api"}, withMaxNonDisruptionDays(7)))
			mustApply(newWorkloadClass("not-api", nil, withMaxNonDisruptionDays(7),
				withMatchExpressions(metav1.LabelSelectorRequirement{
					Key: "app", Operator: metav1.LabelSelectorOpNotIn, Values: []string{"api"},
				})))
			mustApply(newWorkloadClass("canary", nil, withMaxNonDisruptionDays(7),
				withMatchExpressions(metav1.LabelSelectorRequirement{
					Key: "track", Operator: metav1.LabelSelectorOpExists,
				})))

			// app=api and app NotIn (api) are disjoint, but a Pod with app=api and a track label matches both api
			// and canary, and a Pod with app=web and a track label matches both not-api and canary.
			cond := eventuallyValidated("canary", metav1.ConditionFalse, workloadsv1alpha1.ReasonSelectorOverlap)
			Expect(cond.Message).To(Equal("`podSelector` must not select the same Pods as another WorkloadClass in the " +
				"namespace, but it overlaps with the `podSelector` of 'api', 'not-api'"))

			cond = eventuallyValidated("api", metav1.ConditionFalse, workloadsv1alpha1.ReasonSelectorOverlap)
			Expect(cond.Message).To(SatisfyAll(ContainSubstring("'canary'"), Not(ContainSubstring("'not-api'"))))

			cond = eventuallyValidated("not-api", metav1.ConditionFalse, workloadsv1alpha1.ReasonSelectorOverlap)
			Expect(cond.Message).To(SatisfyAll(ContainSubstring("'canary'"), Not(ContainSubstring("'api'"))))
		})

		It("reports guardrail violations and the overlap together as ValidationFailed", func() {
			mustApply(newGuardrail("wc-e2e-overlap-limit", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 7}))
			mustApply(newWorkloadClass("jobs", map[string]string{"app": "jobs"}, withMaxNonDisruptionDays(30)))
			mustApply(newWorkloadClass("jobs-copy", map[string]string{"app": "jobs"}, withMaxNonDisruptionDays(7)))

			cond := eventuallyValidated("jobs", metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
			Expect(cond.Message).To(SatisfyAll(
				ContainSubstring("must be between 1 and 7, inclusive (guardrail limit), got 30"),
				ContainSubstring("overlaps with the `podSelector` of 'jobs-copy'"),
			))
			eventuallyValidated("jobs-copy", metav1.ConditionFalse, workloadsv1alpha1.ReasonSelectorOverlap)
		})
	})

	Context("admission", func() {
		It("rejects a WorkloadClass whose timeZone is Local", func() {
			local := window("local", "Saturday")
			local.TimeZone = "Local"
			out, err := apply(newWorkloadClass("local-zone", map[string]string{"app": "local-zone"},
				withWindows(local), withMaxNonDisruptionDays(7)))
			Expect(err).To(HaveOccurred())
			Expect(out).To(ContainSubstring("must not be 'Local' or 'local'"))
		})

		It("rejects a WorkloadClassGuardrail with invalid daysOfWeek", func() {
			out, err := apply(newGuardrail("wc-e2e-invalid-days", workloadsv1alpha1.Disruption{
				DaysOfWeek:                   []string{"Saturday", "Caturday"},
				MaxNonDisruptionDurationDays: 7,
			}))
			Expect(err).To(HaveOccurred())
			Expect(out).To(SatisfyAll(
				ContainSubstring("`daysOfWeek` must only contain"),
				ContainSubstring("got 'Caturday'"),
			))
		})

		It("admits a WorkloadClassGuardrail but warns about existing WorkloadClasses that do not comply", func() {
			mustApply(newWorkloadClass("lax", map[string]string{"app": "lax"}, withMaxNonDisruptionDays(30)))
			mustApply(newWorkloadClass("tight", map[string]string{"app": "tight"}, withMaxNonDisruptionDays(5)))

			out, err := apply(newGuardrail("wc-e2e-warns", workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 10}))
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(SatisfyAll(
				ContainSubstring(fmt.Sprintf(
					"Warning: existing WorkloadClass '%s/lax' does not comply with this guardrail", testNamespace)),
				ContainSubstring("must be between 1 and 10, inclusive (guardrail limit), got 30"),
				Not(ContainSubstring(fmt.Sprintf("'%s/tight'", testNamespace))),
			))

			By("marking the non-compliant WorkloadClass as ValidationFailed")
			eventuallyValidated("lax", metav1.ConditionFalse, workloadsv1alpha1.ReasonValidationFailed)
			eventuallyValidated("tight", metav1.ConditionTrue, workloadsv1alpha1.ReasonValidationPassed)
		})
	})

	Context("controller manager", func() {
		It("runs without restarts or RBAC errors", func() {
			podName := controllerPodName()
			Expect(podName).NotTo(BeEmpty())

			restarts, err := utils.Run(exec.Command("kubectl", "get", "pod", podName, "-n", namespace,
				"-o", "jsonpath={.status.containerStatuses[*].restartCount}"))
			Expect(err).NotTo(HaveOccurred())
			Expect(strings.Fields(restarts)).To(HaveEach("0"))

			logs, err := utils.Run(exec.Command("kubectl", "logs", podName, "-n", namespace))
			Expect(err).NotTo(HaveOccurred())
			Expect(logs).NotTo(ContainSubstring("is forbidden"))
		})
	})
})

// wcOption customizes a WorkloadClass built by newWorkloadClass.
type wcOption func(*workloadsv1alpha1.WorkloadClass)

// newWorkloadClass returns a WorkloadClass in the test namespace that selects Pods with matchLabels.
func newWorkloadClass(name string, matchLabels map[string]string, opts ...wcOption) *workloadsv1alpha1.WorkloadClass {
	wc := &workloadsv1alpha1.WorkloadClass{
		TypeMeta: metav1.TypeMeta{
			APIVersion: workloadsv1alpha1.GroupVersion.String(),
			Kind:       "WorkloadClass",
		},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: workloadsv1alpha1.WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{MatchLabels: matchLabels},
		},
	}
	for _, opt := range opts {
		opt(wc)
	}
	return wc
}

func withWindows(windows ...workloadsv1alpha1.DisruptionWindow) wcOption {
	return func(wc *workloadsv1alpha1.WorkloadClass) {
		wc.Spec.DisruptionPolicy.AllowedDisruptionWindows = windows
	}
}

func withMaxNonDisruptionDays(days int32) wcOption {
	return func(wc *workloadsv1alpha1.WorkloadClass) {
		wc.Spec.DisruptionPolicy.MaxNonDisruptionDurationDays = days
	}
}

func withMatchExpressions(reqs ...metav1.LabelSelectorRequirement) wcOption {
	return func(wc *workloadsv1alpha1.WorkloadClass) {
		wc.Spec.PodSelector.MatchExpressions = reqs
	}
}

// window returns a 22:00-04:00 UTC DisruptionWindow active on the given days.
func window(name string, days ...string) workloadsv1alpha1.DisruptionWindow {
	return workloadsv1alpha1.DisruptionWindow{
		Name:       name,
		DaysOfWeek: days,
		TimeZone:   "UTC",
		StartTime:  "22:00",
		EndTime:    "04:00",
	}
}

// newGuardrail returns a WorkloadClassGuardrail labeled with testLabel so that deleteTestGuardrails removes it.
func newGuardrail(name string, disruption workloadsv1alpha1.Disruption) *workloadsv1alpha1.WorkloadClassGuardrail {
	key, value, _ := strings.Cut(testLabel, "=")
	return &workloadsv1alpha1.WorkloadClassGuardrail{
		TypeMeta: metav1.TypeMeta{
			APIVersion: workloadsv1alpha1.GroupVersion.String(),
			Kind:       "WorkloadClassGuardrail",
		},
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{key: value}},
		Spec: workloadsv1alpha1.WorkloadClassGuardrailSpec{
			Constraints: workloadsv1alpha1.Constraints{Disruption: disruption},
		},
	}
}

// apply creates or updates obj with `kubectl apply` and returns the combined output, including any
// admission warnings.
func apply(obj any) (string, error) {
	manifest, err := yaml.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("failed to marshal manifest: %w", err)
	}
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(string(manifest))
	return utils.Run(cmd)
}

// mustApply applies obj and fails the spec if kubectl returns an error.
func mustApply(obj any) {
	GinkgoHelper()
	out, err := apply(obj)
	Expect(err).NotTo(HaveOccurred(), out)
}

// mustPatch applies a JSON merge patch to a resource. An empty namespace targets a cluster-scoped resource.
func mustPatch(resource, name, ns, patch string) {
	GinkgoHelper()
	args := []string{"patch", resource, name, "--type", "merge", "-p", patch}
	if ns != "" {
		args = append(args, "-n", ns)
	}
	out, err := utils.Run(exec.Command("kubectl", args...))
	Expect(err).NotTo(HaveOccurred(), out)
}

// getWorkloadClass reads a WorkloadClass from the test namespace.
func getWorkloadClass(name string) (*workloadsv1alpha1.WorkloadClass, error) {
	out, err := utils.Run(exec.Command("kubectl", "get", "workloadclass", name, "-n", testNamespace, "-o", "json"))
	if err != nil {
		return nil, err
	}
	wc := &workloadsv1alpha1.WorkloadClass{}
	if err := json.Unmarshal([]byte(out), wc); err != nil {
		return nil, fmt.Errorf("failed to decode WorkloadClass %q: %w", name, err)
	}
	return wc, nil
}

// eventuallyValidated waits until the Validated condition of the WorkloadClass reflects its current
// generation and has the expected status and reason, and returns the condition.
func eventuallyValidated(name string, status metav1.ConditionStatus, reason string) *metav1.Condition {
	GinkgoHelper()
	var cond *metav1.Condition
	Eventually(func(g Gomega) {
		wc, err := getWorkloadClass(name)
		g.Expect(err).NotTo(HaveOccurred())

		c := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1alpha1.ConditionTypeValidated)
		g.Expect(c).NotTo(BeNil(), "WorkloadClass %q has no Validated condition", name)
		g.Expect(c.ObservedGeneration).To(Equal(wc.Generation), "Validated condition is stale")
		g.Expect(c.Status).To(Equal(status), "unexpected status; message: %s", c.Message)
		g.Expect(c.Reason).To(Equal(reason), "unexpected reason; message: %s", c.Message)
		cond = c
	}, pollTimeout, pollInterval).Should(Succeed())
	return cond
}

// deleteTestGuardrails deletes every WorkloadClassGuardrail created by this file.
func deleteTestGuardrails() {
	_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclassguardrails", "-l", testLabel, "--wait"))
}

// controllerPodName returns the name of the controller manager pod, or an empty string if it is not found.
func controllerPodName() string {
	out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", namespace,
		"-l", "control-plane=controller-manager", "-o", "jsonpath={.items[0].metadata.name}"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// dumpDiagnostics writes controller logs, events, and the test resources to the Ginkgo output.
func dumpDiagnostics() {
	commands := [][]string{
		{"kubectl", "logs", "-n", namespace, "-l", "control-plane=controller-manager", "--tail=-1"},
		{"kubectl", "describe", "pods", "-n", namespace, "-l", "control-plane=controller-manager"},
		{"kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp"},
		{"kubectl", "get", "events", "-n", testNamespace, "--sort-by=.lastTimestamp"},
		{"kubectl", "get", "workloadclasses", "-n", testNamespace, "-o", "yaml"},
		{"kubectl", "get", "workloadclassguardrails", "-o", "yaml"},
	}
	for _, args := range commands {
		out, err := utils.Run(exec.Command(args[0], args[1:]...))
		if err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "Failed to run %q: %v\n", strings.Join(args, " "), err)
			continue
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "--- %s\n%s\n", strings.Join(args, " "), out)
	}
}
