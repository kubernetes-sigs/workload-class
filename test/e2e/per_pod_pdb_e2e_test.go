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
	"maps"
	"os/exec"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
	"sigs.k8s.io/workload-class/test/utils"
)

const (
	// ppNamespace holds the WorkloadClasses, workloads, and PDBs created by this file.
	ppNamespace = "per-pod-pdb-e2e"

	// ppLabel is set on the cluster-scoped WorkloadClassGuardrail created by this file, so that cleanup only deletes
	// the guardrails this file owns.
	ppLabel     = "e2e.workloads.x-k8s.io/suite=per-pod-pdb"
	ppGuardrail = "wc-e2e-per-pod-pdb"

	// ppWebPods are the Pods of the Deployment that ppWC governs. ppOtherPods are the Pods of a Deployment that no
	// WorkloadClass selects.
	ppWebPods   = "app=web"
	ppOtherPods = "app=other"
	ppReplicas  = 3

	ppWC         = "web"
	ppCanaryWC   = "web-canary"
	ppEnforcedWC = "ns-default"

	ppPDBPrefix = "workload-"
	ppClosed    = "0"
	ppOpen      = "100%"

	// ppPauseImage is preloaded on Kind nodes.
	ppPauseImage = "registry.k8s.io/pause:3.10"
)

// These specs deploy the controller manager to a Kind cluster and exercise Per-Pod PodDisruptionBudgets end to end,
// with the real garbage collector and disruption controller: the WorkloadClass controller creates one PDB per
// governed Pod and keeps it in sync with the disruption window, defers to user-created PDBs, opens the PDBs of an
// invalid WorkloadClass, and hands PDBs over between WorkloadClasses.
//
// The specs share one WorkloadClass and two Deployments and run in order. Each spec restores the state it changes.
var _ = Describe("Per-Pod PodDisruptionBudgets", Ordered, ContinueOnFailure, func() {
	BeforeAll(func() {
		By("creating the manager namespace")
		// The previous suite may still be deleting it.
		Eventually(func() error {
			_, err := utils.Run(exec.Command("kubectl", "create", "ns", namespace))
			return err
		}, pollTimeout, 5*time.Second).Should(Succeed())

		By("labeling the manager namespace to enforce the restricted security policy")
		_, err := utils.Run(exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted"))
		Expect(err).NotTo(HaveOccurred(), "Failed to label the manager namespace")

		By("installing CRDs")
		_, err = utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller manager")
		_, err = utils.Run(exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage)))
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller manager")

		By("creating the test namespace")
		Eventually(func() error {
			_, err := utils.Run(exec.Command("kubectl", "create", "ns", ppNamespace))
			return err
		}, pollTimeout, 5*time.Second).Should(Succeed())

		By("creating a guardrail, retrying until the WorkloadClassGuardrail webhook serves requests")
		guardrail := newGuardrail(ppGuardrail, workloadsv1alpha1.Disruption{MaxNonDisruptionDurationDays: 30})
		key, value, _ := strings.Cut(ppLabel, "=")
		guardrail.Labels = map[string]string{key: value}
		Eventually(func() error {
			_, err := apply(guardrail)
			return err
		}, 5*time.Minute, 5*time.Second).Should(Succeed())

		By("creating a WorkloadClass outside of its disruption window, and the Deployments")
		mustApply(newPPWorkloadClass(ppWC, map[string]string{"app": "web"}, false))
		eventuallyPPValidated(ppWC, workloadsv1alpha1.ReasonValidationPassed)
		applyPPDeployment("web", ppReplicas)
		applyPPDeployment("other", 1)
	})

	AfterAll(func() {
		By("deleting the test resources")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", ppNamespace, "--ignore-not-found"))
		_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclassguardrails", "-l", ppLabel, "--wait"))

		By("undeploying the controller manager")
		_, _ = utils.Run(exec.Command("make", "undeploy"))

		By("uninstalling CRDs")
		_, _ = utils.Run(exec.Command("make", "uninstall"))

		By("removing the manager namespace")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found"))
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			dumpPPDiagnostics()
		}
	})

	It("creates one closed PDB per governed Pod that selects only that Pod", func() {
		expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas)

		By("verifying Pods that no WorkloadClass selects have no PDB")
		expectNoPerPodPDBs(ppOtherPods)
	})

	It("opens the PDBs inside the disruption window and closes them outside of it", func() {
		By("moving the disruption window to cover now")
		mustApply(newPPWorkloadClass(ppWC, map[string]string{"app": "web"}, true))
		expectPerPodPDBs(ppWebPods, ppWC, ppOpen, ppReplicas)

		By("moving the disruption window away from now")
		mustApply(newPPWorkloadClass(ppWC, map[string]string{"app": "web"}, false))
		expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas)
	})

	It("replaces the PDB of a deleted Pod and follows the Deployment as it scales", func() {
		deleted := podNames(expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas))[0]

		By("deleting a Pod")
		_, err := utils.Run(exec.Command("kubectl", "delete", "pod", deleted, "-n", ppNamespace, "--wait=false"))
		Expect(err).NotTo(HaveOccurred())

		By("verifying its PDB is deleted and its replacement gets one")
		Expect(podNames(expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas))).NotTo(ContainElement(deleted))
		expectNoStalePDBs()

		By("scaling the Deployment up")
		applyPPDeployment("web", ppReplicas+2)
		expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas+2)

		By("scaling the Deployment back down")
		applyPPDeployment("web", ppReplicas)
		expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas)
		expectNoStalePDBs()
	})

	It("deletes the PDBs of Pods a user-created PDB covers, and recreates them when it is removed", func() {
		expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas)

		By("creating a user PDB that selects the Deployment's Pods")
		mustApply(&policyv1.PodDisruptionBudget{
			TypeMeta:   metav1.TypeMeta{APIVersion: "policy/v1", Kind: "PodDisruptionBudget"},
			ObjectMeta: metav1.ObjectMeta{Name: "user-web", Namespace: ppNamespace},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: ptr.To(intstr.FromInt32(1)),
				Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			},
		})
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command("kubectl", "delete", "pdb", "user-web", "-n", ppNamespace, "--ignore-not-found"))
		})

		By("verifying the WorkloadClass deletes its PDBs and leaves the user PDB untouched")
		expectNoPerPodPDBs(ppWebPods)
		Consistently(func(g Gomega) {
			pdbs, err := ppPDBs()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pdbs).To(HaveKey("user-web"))
			userPDB := pdbs["user-web"]
			g.Expect(metav1.GetControllerOf(&userPDB)).To(BeNil())
			g.Expect(userPDB.Labels).NotTo(HaveKey(workloadsv1alpha1.PodUIDLabel))
			g.Expect(userPDB.Spec.MinAvailable.String()).To(Equal("1"))
			g.Expect(userPDB.Spec.MaxUnavailable).To(BeNil())
		}, 10*time.Second, pollInterval).Should(Succeed())

		By("deleting the user PDB")
		_, err := utils.Run(exec.Command("kubectl", "delete", "pdb", "user-web", "-n", ppNamespace))
		Expect(err).NotTo(HaveOccurred())
		expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas)
	})

	It("opens the PDBs instead of deleting them while the WorkloadClass fails validation", func() {
		pdbUIDs := expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas)
		valid := newPPWorkloadClass(ppWC, map[string]string{"app": "web"}, false)
		DeferCleanup(func() { mustApply(valid) })

		By("raising maxNonDisruptionDurationDays above the guardrail limit")
		mustPatch("workloadclass", ppWC, ppNamespace,
			`{"spec":{"disruptionPolicy":{"maxNonDisruptionDurationDays":40}}}`)
		eventuallyPPValidated(ppWC, workloadsv1alpha1.ReasonValidationFailed)

		By("verifying the same PDBs are opened")
		Expect(expectPerPodPDBs(ppWebPods, ppWC, ppOpen, ppReplicas)).To(Equal(pdbUIDs))

		By("restoring a valid WorkloadClass")
		mustApply(valid)
		eventuallyPPValidated(ppWC, workloadsv1alpha1.ReasonValidationPassed)
		Expect(expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas)).To(Equal(pdbUIDs))
	})

	It("hands the PDBs over to the namespace's enforced WorkloadClass and back", func() {
		expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas)

		By("creating a WorkloadClass without a podSelector and making it the namespace's enforced WorkloadClass")
		mustApply(newPPWorkloadClass(ppEnforcedWC, nil, false))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command("kubectl", "label", "ns", ppNamespace,
				workloadsv1alpha1.EnforcedWorkloadClassLabel+"-"))
			_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclass", ppEnforcedWC, "-n", ppNamespace,
				"--ignore-not-found"))
		})
		eventuallyPPValidated(ppEnforcedWC, workloadsv1alpha1.ReasonValidationPassed)
		_, err := utils.Run(exec.Command("kubectl", "label", "--overwrite", "ns", ppNamespace,
			workloadsv1alpha1.EnforcedWorkloadClassLabel+"="+ppEnforcedWC))
		Expect(err).NotTo(HaveOccurred())

		By("verifying the enforced WorkloadClass governs every Pod in the namespace")
		expectPerPodPDBs(ppWebPods, ppEnforcedWC, ppClosed, ppReplicas)
		expectPerPodPDBs(ppOtherPods, ppEnforcedWC, ppClosed, 1)

		By("removing the label")
		_, err = utils.Run(exec.Command("kubectl", "label", "ns", ppNamespace,
			workloadsv1alpha1.EnforcedWorkloadClassLabel+"-"))
		Expect(err).NotTo(HaveOccurred())

		By("verifying the selecting WorkloadClass takes its Pods' PDBs back and the other Pod's PDB is deleted")
		expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas)
		expectNoPerPodPDBs(ppOtherPods)
		expectNoStalePDBs()
	})

	It("gives each Pod a PDB owned by its best-matching WorkloadClass when selectors overlap", func() {
		canary := podNames(expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas))[0]

		By("labeling one Pod and creating a more specific WorkloadClass that selects it")
		_, err := utils.Run(exec.Command("kubectl", "label", "pod", canary, "-n", ppNamespace, "tier=canary"))
		Expect(err).NotTo(HaveOccurred())
		mustApply(newPPWorkloadClass(ppCanaryWC, map[string]string{"app": "web", "tier": "canary"}, false))
		DeferCleanup(func() {
			_, _ = utils.Run(exec.Command("kubectl", "delete", "workloadclass", ppCanaryWC, "-n", ppNamespace,
				"--ignore-not-found"))
			_, _ = utils.Run(exec.Command("kubectl", "label", "pod", canary, "-n", ppNamespace, "tier-"))
		})
		eventuallyPPValidated(ppCanaryWC, workloadsv1alpha1.ReasonSelectorOverlap)
		eventuallyPPValidated(ppWC, workloadsv1alpha1.ReasonSelectorOverlap)

		By("verifying both WorkloadClasses keep protecting the Pods they are the best match for")
		expectPerPodPDBs(ppWebPods+",tier=canary", ppCanaryWC, ppClosed, 1)
		expectPerPodPDBs(ppWebPods+",!tier", ppWC, ppClosed, ppReplicas-1)

		By("deleting the more specific WorkloadClass")
		_, err = utils.Run(exec.Command("kubectl", "delete", "workloadclass", ppCanaryWC, "-n", ppNamespace))
		Expect(err).NotTo(HaveOccurred())

		// The garbage collector may delete the PDB before the broader WorkloadClass adopts it, so its UID may change.
		By("verifying the broader WorkloadClass protects the labeled Pod")
		expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas)
		eventuallyPPValidated(ppWC, workloadsv1alpha1.ReasonValidationPassed)
	})

	It("rejects evicting a Pod while its PDB is closed, and allows it once the window opens", func() {
		evicted := podNames(expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas))[0]
		DeferCleanup(func() { mustApply(newPPWorkloadClass(ppWC, map[string]string{"app": "web"}, false)) })

		By("evicting a Pod outside of the window")
		Expect(evictPPPod(evicted)).To(MatchError(SatisfyAll(
			ContainSubstring("TooManyRequests"),
			ContainSubstring("disruption budget"),
		)))

		By("moving the disruption window to cover now and evicting the Pod again")
		mustApply(newPPWorkloadClass(ppWC, map[string]string{"app": "web"}, true))
		expectPerPodPDBs(ppWebPods, ppWC, ppOpen, ppReplicas)
		Eventually(func() error { return evictPPPod(evicted) }, pollTimeout, pollInterval).Should(Succeed())

		By("verifying the replacement Pod gets a PDB")
		Expect(podNames(expectPerPodPDBs(ppWebPods, ppWC, ppOpen, ppReplicas))).NotTo(ContainElement(evicted))
		expectNoStalePDBs()
	})

	It("garbage-collects the PDBs when the WorkloadClass is deleted", func() {
		pods := podNames(expectPerPodPDBs(ppWebPods, ppWC, ppClosed, ppReplicas))

		By("deleting the WorkloadClass")
		_, err := utils.Run(exec.Command("kubectl", "delete", "workloadclass", ppWC, "-n", ppNamespace))
		Expect(err).NotTo(HaveOccurred())

		By("verifying its PDBs are deleted and the Pods keep running")
		Eventually(func(g Gomega) {
			pdbs, err := ppPDBs()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pdbs).To(BeEmpty())
			current, err := ppActivePods(ppWebPods)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(current).To(HaveLen(len(pods)))
			for _, pod := range current {
				g.Expect(pods).To(ContainElement(pod.Name))
			}
		}, pollTimeout, pollInterval).Should(Succeed())
	})

	It("runs the controller manager without restarts or RBAC errors", func() {
		podName := controllerPodName()
		Expect(podName).NotTo(BeEmpty())

		restarts, err := utils.Run(exec.Command("kubectl", "get", "pod", podName, "-n", namespace,
			"-o", "jsonpath={.status.containerStatuses[*].restartCount}"))
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.Fields(restarts)).To(HaveEach("0"))

		logs, err := utils.Run(exec.Command("kubectl", "logs", podName, "-n", namespace))
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.ToLower(logs)).NotTo(SatisfyAny(
			ContainSubstring("forbidden"),
			ContainSubstring("cannot list resource"),
		))
	})
})

// newPPWorkloadClass returns a WorkloadClass in ppNamespace whose whole-day disruption window covers every day
// (open), or only the day after tomorrow in UTC, so that now stays outside of it even if the suite runs past
// midnight. A nil matchLabels leaves the podSelector unset.
func newPPWorkloadClass(name string, matchLabels map[string]string, open bool) *workloadsv1alpha1.WorkloadClass {
	days := []string{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}
	if !open {
		days = []string{time.Now().UTC().AddDate(0, 0, 2).Weekday().String()}
	}
	wc := &workloadsv1alpha1.WorkloadClass{
		TypeMeta: metav1.TypeMeta{
			APIVersion: workloadsv1alpha1.GroupVersion.String(),
			Kind:       "WorkloadClass",
		},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ppNamespace},
		Spec: workloadsv1alpha1.WorkloadClassSpec{
			DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{
				// A window whose start and end times are equal lasts the whole day.
				AllowedDisruptionWindows: []workloadsv1alpha1.DisruptionWindow{{
					Name: "e2e", DaysOfWeek: days, TimeZone: "UTC", StartTime: "00:00", EndTime: "00:00",
				}},
				MaxNonDisruptionDurationDays: 10,
			},
		},
	}
	if matchLabels != nil {
		wc.Spec.PodSelector = &metav1.LabelSelector{MatchLabels: matchLabels}
	}
	return wc
}

// applyPPDeployment creates or updates a pause Deployment in ppNamespace whose Pods are labeled app=<app>.
func applyPPDeployment(app string, replicas int32) {
	GinkgoHelper()
	labels := map[string]string{"app": app}
	mustApply(&appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Name: app, Namespace: ppNamespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr.To[int64](0),
					Containers:                    []corev1.Container{{Name: "pause", Image: ppPauseImage}},
				},
			},
		},
	})
}

// kubectlGet runs `kubectl get` with JSON output in ppNamespace and decodes the result into obj.
func kubectlGet(obj any, args ...string) error {
	args = append(append([]string{"get"}, args...), "-n", ppNamespace, "-o", "json")
	out, err := utils.Run(exec.Command("kubectl", args...))
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(out), obj)
}

// ppActivePods returns the Pods in ppNamespace that match the label selector and are neither terminating nor
// terminated.
func ppActivePods(selector string) ([]corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := kubectlGet(list, "pods", "-l", selector); err != nil {
		return nil, err
	}
	return slices.DeleteFunc(list.Items, func(p corev1.Pod) bool {
		return p.DeletionTimestamp != nil || p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
	}), nil
}

// ppPDBs returns the PDBs in ppNamespace by name.
func ppPDBs() (map[string]policyv1.PodDisruptionBudget, error) {
	list := &policyv1.PodDisruptionBudgetList{}
	if err := kubectlGet(list, "pdb"); err != nil {
		return nil, err
	}
	pdbs := make(map[string]policyv1.PodDisruptionBudget, len(list.Items))
	for _, pdb := range list.Items {
		pdbs[pdb.Name] = pdb
	}
	return pdbs, nil
}

// podNames returns the sorted Pod names of the PDB UIDs returned by expectPerPodPDBs.
func podNames(pdbUIDs map[string]types.UID) []string {
	return slices.Sorted(maps.Keys(pdbUIDs))
}

// evictPPPod evicts a Pod in ppNamespace through the Eviction API.
func evictPPPod(pod string) error {
	eviction, err := json.Marshal(&policyv1.Eviction{
		TypeMeta:   metav1.TypeMeta{APIVersion: "policy/v1", Kind: "Eviction"},
		ObjectMeta: metav1.ObjectMeta{Name: pod, Namespace: ppNamespace},
	})
	if err != nil {
		return err
	}
	cmd := exec.Command("kubectl", "create", "--raw",
		fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/eviction", ppNamespace, pod), "-f", "-")
	cmd.Stdin = strings.NewReader(string(eviction))
	_, err = utils.Run(cmd)
	return err
}

// expectPerPodPDBs waits until n active Pods match the label selector, each labeled with its UID and covered by its
// own PDB that selects only that Pod, is controlled by the owner WorkloadClass, and has the given maxUnavailable.
// The disruption controller must count exactly one healthy Pod for each PDB, and allow one disruption only if the
// PDB is open. It returns the UID of each Pod's PDB by Pod name.
func expectPerPodPDBs(selector, owner, maxUnavailable string, n int) map[string]types.UID {
	GinkgoHelper()
	allowed := int32(0)
	if maxUnavailable == ppOpen {
		allowed = 1
	}
	var pdbUIDs map[string]types.UID
	Eventually(func(g Gomega) {
		pods, err := ppActivePods(selector)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(pods).To(HaveLen(n))
		pdbs, err := ppPDBs()
		g.Expect(err).NotTo(HaveOccurred())

		pdbUIDs = make(map[string]types.UID, n)
		for _, pod := range pods {
			uid := string(pod.UID)
			g.Expect(pod.Labels).To(HaveKeyWithValue(workloadsv1alpha1.PodUIDLabel, uid), "Pod %s label", pod.Name)

			name := ppPDBPrefix + pod.Name
			g.Expect(pdbs).To(HaveKey(name), "Pod %s should have a PDB", pod.Name)
			pdb := pdbs[name]
			ref := metav1.GetControllerOf(&pdb)
			g.Expect(ref).NotTo(BeNil(), "PDB %s has no controller", name)
			g.Expect(ref.Kind).To(Equal("WorkloadClass"))
			g.Expect(ref.Name).To(Equal(owner), "PDB %s controller", name)
			g.Expect(ref.BlockOwnerDeletion).To(HaveValue(BeTrue()))
			g.Expect(pdb.Labels).To(HaveKeyWithValue(workloadsv1alpha1.PodUIDLabel, uid), "PDB %s label", name)
			g.Expect(pdb.Spec.Selector).To(Equal(&metav1.LabelSelector{
				MatchLabels: map[string]string{workloadsv1alpha1.PodUIDLabel: uid},
			}), "PDB %s selector", name)
			g.Expect(pdb.Spec.MaxUnavailable).NotTo(BeNil())
			g.Expect(pdb.Spec.MaxUnavailable.String()).To(Equal(maxUnavailable), "PDB %s maxUnavailable", name)
			g.Expect(pdb.Spec.UnhealthyPodEvictionPolicy).To(HaveValue(Equal(policyv1.AlwaysAllow)))
			g.Expect(pdb.Status.ObservedGeneration).To(Equal(pdb.Generation), "PDB %s status is stale", name)
			g.Expect(pdb.Status.CurrentHealthy).To(Equal(int32(1)), "PDB %s should cover one healthy Pod", name)
			g.Expect(pdb.Status.DisruptionsAllowed).To(Equal(allowed), "PDB %s disruptionsAllowed", name)
			pdbUIDs[pod.Name] = pdb.UID
		}
	}, pollTimeout, pollInterval).Should(Succeed())
	return pdbUIDs
}

// expectNoPerPodPDBs waits until none of the active Pods that match the label selector has a PDB named after it.
func expectNoPerPodPDBs(selector string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		pods, err := ppActivePods(selector)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(pods).NotTo(BeEmpty())
		pdbs, err := ppPDBs()
		g.Expect(err).NotTo(HaveOccurred())
		for _, pod := range pods {
			g.Expect(pdbs).NotTo(HaveKey(ppPDBPrefix+pod.Name), "Pod %s should not have a PDB", pod.Name)
		}
	}, pollTimeout, pollInterval).Should(Succeed())
}

// expectNoStalePDBs waits until every WorkloadClass-managed PDB in ppNamespace belongs to an active Pod with the
// same UID.
func expectNoStalePDBs() {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		pods, err := ppActivePods("app")
		g.Expect(err).NotTo(HaveOccurred())
		uids := make(map[string]string, len(pods))
		for _, pod := range pods {
			uids[ppPDBPrefix+pod.Name] = string(pod.UID)
		}
		pdbs, err := ppPDBs()
		g.Expect(err).NotTo(HaveOccurred())
		for name, pdb := range pdbs {
			if ref := metav1.GetControllerOf(&pdb); ref == nil || ref.Kind != "WorkloadClass" {
				continue
			}
			g.Expect(uids).To(HaveKey(name), "PDB %s should belong to an active Pod", name)
			g.Expect(pdb.Labels).To(HaveKeyWithValue(workloadsv1alpha1.PodUIDLabel, uids[name]),
				"PDB %s should belong to the current Pod", name)
		}
	}, pollTimeout, pollInterval).Should(Succeed())
}

// eventuallyPPValidated waits until the Validated condition of a WorkloadClass in ppNamespace reflects its current
// generation and has the expected reason.
func eventuallyPPValidated(name, reason string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		wc := &workloadsv1alpha1.WorkloadClass{}
		g.Expect(kubectlGet(wc, "workloadclass", name)).To(Succeed())
		c := meta.FindStatusCondition(wc.Status.Conditions, workloadsv1alpha1.ConditionTypeValidated)
		g.Expect(c).NotTo(BeNil(), "WorkloadClass %q has no Validated condition", name)
		g.Expect(c.ObservedGeneration).To(Equal(wc.Generation), "Validated condition is stale")
		g.Expect(c.Reason).To(Equal(reason), "unexpected reason; message: %s", c.Message)
	}, pollTimeout, pollInterval).Should(Succeed())
}

// dumpPPDiagnostics writes controller logs, events, and the test resources to the Ginkgo output.
func dumpPPDiagnostics() {
	commands := [][]string{
		{"kubectl", "logs", "-n", namespace, "-l", "control-plane=controller-manager", "--tail=-1"},
		{"kubectl", "describe", "pods", "-n", namespace, "-l", "control-plane=controller-manager"},
		{"kubectl", "get", "events", "-n", ppNamespace, "--sort-by=.lastTimestamp"},
		{"kubectl", "get", "namespace", ppNamespace, "--show-labels"},
		{"kubectl", "get", "pods", "-n", ppNamespace, "-o", "wide", "--show-labels"},
		{"kubectl", "get", "workloadclasses,pdb", "-n", ppNamespace, "-o", "yaml"},
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
