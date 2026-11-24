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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

const (
	webhookName       = "vpoddisruption.workloads.x-k8s.io"
	evictionApp       = "web"
	autoscalerUser    = "cluster-autoscaler"
	autoscalerSAName  = "autoscaler"
	maintenanceDenied = "the current time is outside of every allowed disruption window"
)

// Evictions in these specs are dry runs: the API server still calls the webhook (sideEffects=None), but never
// deletes the Pod, so a Pod can be evicted repeatedly as the WorkloadClass status changes.
var _ = Describe("DisruptionWebhook", func() {
	var namespace string

	waitForCached := func(obj client.Object) {
		GinkgoHelper()
		Eventually(func(g Gomega) {
			got := obj.DeepCopyObject().(client.Object)
			g.Expect(mgrCache.Get(ctx, client.ObjectKeyFromObject(obj), got)).To(Succeed())
			g.Expect(got.GetResourceVersion()).To(Equal(obj.GetResourceVersion()))
		}).Should(Succeed())
	}

	newNamespace := func(labels map[string]string) string {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "disruption-", Labels: labels}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, ns))).To(Succeed()) })
		waitForCached(ns)
		return ns.Name
	}

	newPod := func(name string, labels map[string]string) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.10"}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		waitForCached(pod)
		return pod
	}

	// newWorkloadClass creates a WorkloadClass selecting app=web and sets its status as the controller would.
	newWorkloadClass := func(
		name string, validated metav1.ConditionStatus, readiness workloadsv1alpha1.MaintenanceReadiness,
		maintenanceMessage string, subjects ...workloadsv1alpha1.Subject,
	) *workloadsv1alpha1.WorkloadClass {
		wc := &workloadsv1alpha1.WorkloadClass{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: workloadsv1alpha1.WorkloadClassSpec{
				PodSelector:      &metav1.LabelSelector{MatchLabels: map[string]string{"app": evictionApp}},
				DisruptionPolicy: workloadsv1alpha1.DisruptionPolicy{AllowedDisruptionsOutsideOfWindow: subjects},
			},
		}
		Expect(k8sClient.Create(ctx, wc)).To(Succeed())

		reason := workloadsv1alpha1.ReasonValidationPassed
		if validated == metav1.ConditionFalse {
			reason = workloadsv1alpha1.ReasonValidationFailed
		}
		meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
			Type: workloadsv1alpha1.ConditionTypeValidated, Status: validated, Reason: reason, Message: "set by test",
		})
		if maintenanceMessage != "" {
			meta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
				Type: conditionTypeMaintenanceAllowed, Status: metav1.ConditionFalse,
				Reason: "OutsideDisruptionWindow", Message: maintenanceMessage,
			})
		}
		wc.Status.MaintenanceReadiness = readiness
		Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())
		waitForCached(wc)
		return wc
	}

	evict := func(c client.Client, pod *corev1.Pod) error {
		eviction := &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace}}
		return c.SubResource("eviction").Create(ctx, pod, eviction, client.DryRunAll)
	}

	// impersonating returns a client that sends requests as the given user. The user is also put in system:masters,
	// so that RBAC allows the eviction and only the webhook decides.
	impersonating := func(username string) client.Client {
		impersonated := rest.CopyConfig(cfg)
		impersonated.Impersonate = rest.ImpersonationConfig{UserName: username, Groups: []string{"system:masters"}}
		c, err := client.New(impersonated, client.Options{Scheme: scheme.Scheme})
		Expect(err).NotTo(HaveOccurred())
		return c
	}

	// The webhook reads WorkloadClasses and Namespaces from the manager's cache, so decisions are checked with
	// Eventually until the cache has observed the latest objects.
	expectAllowed := func(c client.Client, pod *corev1.Pod) {
		GinkgoHelper()
		Eventually(func() error { return evict(c, pod) }).Should(Succeed())
	}
	expectDenied := func(c client.Client, pod *corev1.Pod, messages ...string) {
		GinkgoHelper()
		Eventually(func(g Gomega) {
			err := evict(c, pod)
			g.Expect(apierrors.IsForbidden(err)).To(BeTrue(), "eviction error: %v", err)
			g.Expect(err.Error()).To(ContainSubstring(webhookName))
			for _, msg := range messages {
				g.Expect(err.Error()).To(ContainSubstring(msg))
			}
		}).Should(Succeed())
	}

	BeforeEach(func() {
		namespace = newNamespace(nil)
	})

	It("is installed for pods/eviction with failurePolicy Ignore and a 5s timeout", func() {
		configs := &admissionregistrationv1.ValidatingWebhookConfigurationList{}
		Expect(k8sClient.List(ctx, configs)).To(Succeed())
		var found *admissionregistrationv1.ValidatingWebhook
		for i := range configs.Items {
			for j := range configs.Items[i].Webhooks {
				if configs.Items[i].Webhooks[j].Name == webhookName {
					found = &configs.Items[i].Webhooks[j]
				}
			}
		}
		Expect(found).NotTo(BeNil())
		Expect(found.FailurePolicy).To(HaveValue(Equal(admissionregistrationv1.Ignore)))
		Expect(found.TimeoutSeconds).To(HaveValue(BeEquivalentTo(5)))
		Expect(found.SideEffects).To(HaveValue(Equal(admissionregistrationv1.SideEffectClassNone)))
		Expect(found.Rules).To(ConsistOf(SatisfyAll(
			HaveField("Operations", ConsistOf(admissionregistrationv1.Create)),
			HaveField("Rule.APIGroups", ConsistOf("")),
			HaveField("Rule.APIVersions", ConsistOf("v1")),
			HaveField("Rule.Resources", ConsistOf("pods/eviction")),
		)))
	})

	It("allows evicting a Pod that no WorkloadClass selects", func() {
		newWorkloadClass("web", metav1.ConditionTrue, workloadsv1alpha1.ReadinessNotReady, maintenanceDenied)
		pod := newPod("other", map[string]string{"app": "other"})
		expectAllowed(k8sClient, pod)
	})

	It("decides from the WorkloadClass maintenance readiness", func() {
		wc := newWorkloadClass("web", metav1.ConditionTrue, workloadsv1alpha1.ReadinessNotReady, maintenanceDenied)
		pod := newPod("web-1", map[string]string{"app": evictionApp})

		By("denying with the MaintenanceAllowed message while NotReady")
		expectDenied(k8sClient, pod, "Eviction blocked for WorkloadClass web", maintenanceDenied)

		By("denying without a MaintenanceAllowed message")
		meta.RemoveStatusCondition(&wc.Status.Conditions, conditionTypeMaintenanceAllowed)
		Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())
		waitForCached(wc)
		expectDenied(k8sClient, pod, "WorkloadClass web is not ready for maintenance")

		By("allowing while Ready")
		wc.Status.MaintenanceReadiness = workloadsv1alpha1.ReadinessReady
		Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())
		waitForCached(wc)
		expectAllowed(k8sClient, pod)

		By("allowing while Overdue")
		wc.Status.MaintenanceReadiness = workloadsv1alpha1.ReadinessOverdue
		Expect(k8sClient.Status().Update(ctx, wc)).To(Succeed())
		waitForCached(wc)
		expectAllowed(k8sClient, pod)
	})

	It("does not enforce a WorkloadClass that failed validation", func() {
		newWorkloadClass("web", metav1.ConditionFalse, workloadsv1alpha1.ReadinessNotReady, maintenanceDenied)
		pod := newPod("web-1", map[string]string{"app": evictionApp})
		expectAllowed(k8sClient, pod)
	})

	It("allows subjects in allowedDisruptionsOutsideOfWindow", func() {
		newWorkloadClass("web", metav1.ConditionTrue, workloadsv1alpha1.ReadinessNotReady, maintenanceDenied,
			workloadsv1alpha1.Subject{Kind: "User", Name: autoscalerUser},
			workloadsv1alpha1.Subject{Kind: "ServiceAccount", Name: autoscalerSAName, Namespace: namespace},
		)
		pod := newPod("web-1", map[string]string{"app": evictionApp})

		expectAllowed(impersonating(autoscalerUser), pod)
		expectAllowed(impersonating("system:serviceaccount:"+namespace+":"+autoscalerSAName), pod)
		expectDenied(impersonating("someone-else"), pod, maintenanceDenied)
		expectDenied(impersonating("system:serviceaccount:other:"+autoscalerSAName), pod, maintenanceDenied)
	})

	It("applies the Namespace's enforced WorkloadClass to every Pod", func() {
		namespace = newNamespace(map[string]string{workloadsv1alpha1.EnforcedWorkloadClassLabel: "enforced"})
		newWorkloadClass("enforced", metav1.ConditionTrue, workloadsv1alpha1.ReadinessNotReady, maintenanceDenied)
		pod := newPod("other", map[string]string{"app": "other"})
		expectDenied(k8sClient, pod, "Eviction blocked for WorkloadClass enforced")
	})

	It("allows evictions when the enforced WorkloadClass does not exist", func() {
		namespace = newNamespace(map[string]string{workloadsv1alpha1.EnforcedWorkloadClassLabel: "missing"})
		newWorkloadClass("web", metav1.ConditionTrue, workloadsv1alpha1.ReadinessNotReady, maintenanceDenied)
		pod := newPod("web-1", map[string]string{"app": evictionApp})
		expectAllowed(k8sClient, pod)
	})

	It("does not intercept Pod deletes that bypass the Eviction API", func() {
		newWorkloadClass("web", metav1.ConditionTrue, workloadsv1alpha1.ReadinessNotReady, maintenanceDenied)
		pod := newPod("web-1", map[string]string{"app": evictionApp})
		expectDenied(k8sClient, pod, maintenanceDenied)
		Expect(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed())
	})
})
