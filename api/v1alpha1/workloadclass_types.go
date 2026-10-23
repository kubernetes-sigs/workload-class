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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DisruptionPolicy specifies the policy governing pod disruptions.
type DisruptionPolicy struct {
	// AllowedDisruptionWindows defines when disruptions are allowed.
	// When unset or empty, disruptions are allowed at all times on all days of the week.
	// +listType=map
	// +listMapKey=name
	// +optional
	AllowedDisruptionWindows []DisruptionWindow `json:"allowedDisruptionWindows,omitempty"`

	// AllowedDisruptionsOutsideOfWindow specifies subjects that can disrupt even outside of windows. (e.g. VPA, ClusterAutoscaler)
	// +listType=map
	// +listMapKey=kind
	// +listMapKey=name
	// +optional
	AllowedDisruptionsOutsideOfWindow []Subject `json:"allowedDisruptionsOutsideOfWindow,omitempty"`

	// MaxNonDisruptionDurationDays is the maximum duration a workload can remain undisrupted.
	// If exceeded, maintenance takes precedence over per-pod run-duration hints.
	// When unset (0), no upper bound is placed on how long the workload can remain undisrupted.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxNonDisruptionDurationDays int32 `json:"maxNonDisruptionDurationDays,omitempty"`

	// MinInitialRunDurationDays is the minimum duration a pod must run before it can be disrupted.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MinInitialRunDurationDays int32 `json:"minInitialRunDurationDays,omitempty"`

	// GraceTerminationDurationSeconds is the duration in seconds after a pod's deletionTimestamp is set during which
	// the controller keeps the WorkloadClass MaintenanceReadiness as NotReady to allow in-flight pod terminations to drain
	// before further disruptions occur. It does not override Pod.spec.terminationGracePeriodSeconds or set eviction DeleteOptions.gracePeriodSeconds.
	// +optional
	// +kubebuilder:validation:Minimum=0
	GraceTerminationDurationSeconds int32 `json:"graceTerminationDurationSeconds,omitempty"`

	// EmergencyOverride allows bypassing all constraints immediately.
	// +optional
	EmergencyOverride bool `json:"emergencyOverride,omitempty"`
}

// DisruptionWindow defines a temporal window when disruptions are allowed.
type DisruptionWindow struct {
	// Name is the name of the DisruptionWindow.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// DaysOfWeek specifies the days of the week (Monday-Sunday) on which the window starts.
	// When unset or empty, the window applies to every day of the week.
	// +optional
	DaysOfWeek []string `json:"daysOfWeek,omitempty"`

	// TimeZone is the IANA Time Zone Database name (e.g., "America/Los_Angeles", "America/Argentina/Buenos_Aires", "Etc/UTC", "UTC"). See https://www.iana.org/time-zones.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^([A-Za-z0-9_+-]+(/[A-Za-z0-9_+-]+){0,2}|UTC|GMT)$`
	// +kubebuilder:validation:XValidation:rule="self != 'Local' && self != 'local'",message="must not be 'Local' or 'local'"
	TimeZone string `json:"timeZone,omitempty"`

	// StartTime is the start time of the window in HH:MM format (e.g. 22:00) in the specified timeZone (or UTC if timeZone is omitted).
	// +optional
	// +kubebuilder:validation:Pattern=`^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$`
	StartTime string `json:"startTime,omitempty"`

	// EndTime is the end time of the window in HH:MM format (e.g. 04:00) in the specified timeZone (or UTC if timeZone is omitted).
	// +optional
	// +kubebuilder:validation:Pattern=`^([0-1]?[0-9]|2[0-3]):[0-5][0-9]$`
	EndTime string `json:"endTime,omitempty"`
}

// Subject defines the identity of a user, group, or service account.
// It is used to specify who or what is granted permissions or is the target
// of a policy. The structure aligns with standard Kubernetes RBAC subjects.
type Subject struct {
	// Kind can be User, Group, or ServiceAccount
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=User;Group;ServiceAccount
	Kind string `json:"kind"`

	// Name of the identity
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Namespace of the ServiceAccount (only for ServiceAccount kind)
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern="^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"
	Namespace string `json:"namespace,omitempty"`
}

// WorkloadClassSpec defines the desired state of WorkloadClass
type WorkloadClassSpec struct {
	// PodSelector matches the pods in the same namespace that this class applies to.
	// If a Namespace has the "workloads.x-k8s.io/enforced-class" label (EnforcedWorkloadClassLabel) set,
	// the referenced WorkloadClass is enforced for all pods in that namespace and overrides PodSelector
	// matching. Otherwise, PodSelector selects the target pods in the namespace; overlapping PodSelectors
	// across WorkloadClasses in the same namespace are surfaced as a status Condition.
	// +optional
	PodSelector *metav1.LabelSelector `json:"podSelector,omitempty"`

	// DisruptionPolicy specifies the policy governing pod disruptions.
	// +optional
	DisruptionPolicy DisruptionPolicy `json:"disruptionPolicy,omitempty"`
}

type MaintenanceReadiness string

const (
	ReadinessReady    MaintenanceReadiness = "Ready"
	ReadinessNotReady MaintenanceReadiness = "NotReady"
	ReadinessOverdue  MaintenanceReadiness = "Overdue"
)

const (
	// EnforcedWorkloadClassLabel is the label key applied to a Namespace to enforce a single
	// WorkloadClass across all pods in that namespace. When set on a Namespace, the referenced
	// WorkloadClass applies to all pods in the namespace and overrides PodSelector matching.
	EnforcedWorkloadClassLabel = "workloads.x-k8s.io/enforced-class"

	// ConditionTypeValidated indicates if the WorkloadClass has been validated against Guardrails.
	ConditionTypeValidated = "Validated"

	// ReasonValidationPassed indicates that the WorkloadClass passed all Guardrail checks.
	ReasonValidationPassed = "ValidationPassed"
	// ReasonValidationFailed indicates that the WorkloadClass failed one or more Guardrail checks.
	ReasonValidationFailed = "ValidationFailed"
	// ReasonNoGuardrails indicates that no Guardrails were found to validate against.
	ReasonNoGuardrails = "NoGuardrails"
	// ReasonSelectorOverlap indicates that the WorkloadClass PodSelector overlaps with another WorkloadClass in the same namespace.
	ReasonSelectorOverlap = "SelectorOverlap"
)

// WorkloadClassStatus defines the observed state of WorkloadClass.
type WorkloadClassStatus struct {
	// MaintenanceReadiness indicates if the workload is currently ready for maintenance.
	// +optional
	MaintenanceReadiness MaintenanceReadiness `json:"maintenanceReadiness,omitempty"`

	// LastDisruptionTime is the last time a disruption was observed for pods in this class.
	// +optional
	LastDisruptionTime *metav1.Time `json:"lastDisruptionTime,omitempty"`

	// Conditions represent the current state of the WorkloadClass resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced

// WorkloadClass is the Schema for the workloadclasses API
type WorkloadClass struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of WorkloadClass
	// +required
	Spec WorkloadClassSpec `json:"spec"`

	// status defines the observed state of WorkloadClass
	// +optional
	Status WorkloadClassStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WorkloadClassList contains a list of WorkloadClass
type WorkloadClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []WorkloadClass `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WorkloadClass{}, &WorkloadClassList{})
}
