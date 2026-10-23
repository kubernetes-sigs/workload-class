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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestAddToScheme(t *testing.T) {
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme failed: %v", err)
	}

	if GroupVersion.Group != "workloads.x-k8s.io" || GroupVersion.Version != "v1alpha1" {
		t.Errorf("unexpected GroupVersion: %v", GroupVersion)
	}

	if EnforcedWorkloadClassLabel != "workloads.x-k8s.io/enforced-class" {
		t.Errorf("unexpected EnforcedWorkloadClassLabel: %s", EnforcedWorkloadClassLabel)
	}

	if ReasonSelectorOverlap != "SelectorOverlap" {
		t.Errorf("unexpected ReasonSelectorOverlap: %s", ReasonSelectorOverlap)
	}

	for _, obj := range []runtime.Object{
		&WorkloadClass{},
		&WorkloadClassList{},
		&WorkloadClassGuardrail{},
		&WorkloadClassGuardrailList{},
	} {
		gvks, _, err := s.ObjectKinds(obj)
		if err != nil {
			t.Fatalf("ObjectKinds(%T) failed: %v", obj, err)
		}
		if len(gvks) == 0 {
			t.Errorf("expected at least one GVK registered for %T", obj)
		}
	}
}

func TestWorkloadClassDeepCopy(t *testing.T) {
	now := metav1.NewTime(time.Now())
	orig := &WorkloadClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "critical-batch",
			Namespace: "sample",
		},
		Spec: WorkloadClassSpec{
			PodSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"role": "batch-processor"},
			},
			DisruptionPolicy: DisruptionPolicy{
				AllowedDisruptionWindows: []DisruptionWindow{
					{
						Name:       "weekend-maintenance",
						DaysOfWeek: []string{"Saturday", "Sunday"},
						TimeZone:   "America/Toronto",
						StartTime:  "00:00",
						EndTime:    "04:00",
					},
				},
				AllowedDisruptionsOutsideOfWindow: []DisruptionCause{
					"VPA",
					"ClusterAutoscaler",
				},
				MaxNonDisruptionDurationDays:    1,
				MinInitialRunDurationDays:       2,
				GraceTerminationDurationSeconds: 30,
				EmergencyOverride:               false,
			},
		},
		Status: WorkloadClassStatus{
			MaintenanceReadiness: ReadinessReady,
			LastDisruptionTime:   &now,
			Conditions: []metav1.Condition{
				{
					Type:   ConditionTypeValidated,
					Status: metav1.ConditionTrue,
					Reason: ReasonValidationPassed,
				},
			},
		},
	}

	copied := orig.DeepCopy()
	if copied == nil {
		t.Fatal("DeepCopy returned nil")
	}

	copied.Spec.DisruptionPolicy.AllowedDisruptionWindows[0].DaysOfWeek[0] = "Monday"
	if orig.Spec.DisruptionPolicy.AllowedDisruptionWindows[0].DaysOfWeek[0] != "Saturday" {
		t.Error("modifying copied DaysOfWeek mutated original")
	}

	copied.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow[0] = "changed"
	if orig.Spec.DisruptionPolicy.AllowedDisruptionsOutsideOfWindow[0] != "VPA" {
		t.Error("modifying copied AllowedDisruptionsOutsideOfWindow mutated original")
	}

	copied.Status.Conditions[0].Reason = ReasonValidationFailed
	if orig.Status.Conditions[0].Reason != ReasonValidationPassed {
		t.Error("modifying copied Conditions mutated original")
	}

	if obj := orig.DeepCopyObject(); obj == nil {
		t.Error("DeepCopyObject returned nil")
	}

	list := &WorkloadClassList{Items: []WorkloadClass{*orig}}
	listCopy := list.DeepCopy()
	listCopy.Items[0].Name = "mutated"
	if list.Items[0].Name != "critical-batch" {
		t.Error("modifying copied WorkloadClassList mutated original")
	}
	if obj := list.DeepCopyObject(); obj == nil {
		t.Error("WorkloadClassList.DeepCopyObject returned nil")
	}
}

func TestWorkloadClassGuardrailDeepCopy(t *testing.T) {
	orig := &WorkloadClassGuardrail{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default",
		},
		Spec: WorkloadClassGuardrailSpec{
			Constraints: Constraints{
				Disruption: Disruption{
					DaysOfWeek:                       []string{"Saturday", "Sunday"},
					MaxAllowedWindows:                2,
					MaxNonDisruptionDurationDays:     30,
					EnforcedDisruptionTimeoutSeconds: 600,
					EmergencyOverride:                false,
				},
			},
		},
		Status: WorkloadClassGuardrailStatus{
			Conditions: []metav1.Condition{
				{
					Type:   "Available",
					Status: metav1.ConditionTrue,
					Reason: "Ready",
				},
			},
		},
	}

	copied := orig.DeepCopy()
	if copied == nil {
		t.Fatal("DeepCopy returned nil")
	}

	copied.Spec.Constraints.Disruption.DaysOfWeek[0] = "Friday"
	if orig.Spec.Constraints.Disruption.DaysOfWeek[0] != "Saturday" {
		t.Error("modifying copied DaysOfWeek mutated original")
	}

	copied.Status.Conditions[0].Status = metav1.ConditionFalse
	if orig.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Error("modifying copied Conditions mutated original")
	}

	if obj := orig.DeepCopyObject(); obj == nil {
		t.Error("DeepCopyObject returned nil")
	}

	list := &WorkloadClassGuardrailList{Items: []WorkloadClassGuardrail{*orig}}
	listCopy := list.DeepCopy()
	listCopy.Items[0].Name = "mutated"
	if list.Items[0].Name != "default" {
		t.Error("modifying copied WorkloadClassGuardrailList mutated original")
	}
	if obj := list.DeepCopyObject(); obj == nil {
		t.Error("WorkloadClassGuardrailList.DeepCopyObject returned nil")
	}
}
