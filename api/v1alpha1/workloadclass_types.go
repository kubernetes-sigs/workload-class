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

	// MaxNonDisruptionDurationDays is the maximum duration a workload can remain undisrupted, counted from
	// status.lastDisruptionTime, or from the creation of the WorkloadClass if no disruption has been observed yet.
	// Once reached, the WorkloadClass is Overdue for maintenance: maintenance takes precedence over
	// allowedDisruptionWindows and over per-pod run-duration hints.
	// When unset (0), no upper bound is placed on how long the workload can remain undisrupted.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=36500
	MaxNonDisruptionDurationDays int32 `json:"maxNonDisruptionDurationDays,omitempty"`

	// MinInitialRunDurationDays is the minimum duration a pod must run before it can be disrupted. It is counted from
	// the pod's status.startTime, or from its creationTimestamp until the pod has been acknowledged by a kubelet.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=36500
	MinInitialRunDurationDays int32 `json:"minInitialRunDurationDays,omitempty"`

	// GraceTerminationDurationSeconds is the duration in seconds after a pod starts terminating during which the
	// controller keeps the WorkloadClass MaintenanceReadiness as NotReady to allow in-flight pod terminations to drain
	// before further disruptions occur. A pod starts terminating when its deletion is requested, which is its
	// deletionTimestamp minus its deletionGracePeriodSeconds; the controller records the most recent such time in
	// status.lastPodTerminationTime so that the duration is honored after the pod is gone. Pods that are deleted
	// before the controller observes them terminating, such as pods deleted with a grace period of 0, are not counted.
	// It does not override Pod.spec.terminationGracePeriodSeconds or set eviction DeleteOptions.gracePeriodSeconds.
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

// MaintenanceReadiness indicates whether the pods selected by a WorkloadClass can currently be disrupted for maintenance.
// +kubebuilder:validation:Enum=Ready;NotReady;Overdue;Unknown
type MaintenanceReadiness string

const (
	// ReadinessReady indicates that maintenance can proceed: the current time is within an allowed disruption window
	// and no selected pod is protected by MinInitialRunDurationDays or GraceTerminationDurationSeconds, or
	// EmergencyOverride is set.
	ReadinessReady MaintenanceReadiness = "Ready"
	// ReadinessNotReady indicates that maintenance must wait: the current time is outside of every allowed disruption
	// window, or a selected pod is protected by MinInitialRunDurationDays or GraceTerminationDurationSeconds.
	ReadinessNotReady MaintenanceReadiness = "NotReady"
	// ReadinessOverdue indicates that the workload has remained undisrupted for at least MaxNonDisruptionDurationDays,
	// so maintenance takes precedence over disruption windows and per-pod run-duration hints.
	ReadinessOverdue MaintenanceReadiness = "Overdue"
	// ReadinessUnknown indicates that readiness cannot be determined, because the WorkloadClass is not validated or
	// because of an error.
	ReadinessUnknown MaintenanceReadiness = "Unknown"
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

	// ConditionTypeMaintenanceAllowed indicates whether maintenance can currently disrupt the pods selected by the
	// WorkloadClass. It is True when MaintenanceReadiness is Ready or Overdue, False when it is NotReady, and Unknown
	// when it is Unknown. If the WorkloadClass is not validated, its Reason is the Reason of the Validated condition
	// (ReasonValidationFailed or ReasonSelectorOverlap).
	ConditionTypeMaintenanceAllowed = "MaintenanceAllowed"

	// ReasonEmergencyOverride indicates that maintenance is allowed because EmergencyOverride is set.
	ReasonEmergencyOverride = "EmergencyOverride"
	// ReasonMaintenanceOverdue indicates that maintenance is allowed because the workload has remained undisrupted for
	// at least MaxNonDisruptionDurationDays.
	ReasonMaintenanceOverdue = "MaintenanceOverdue"
	// ReasonWithinDisruptionWindow indicates that maintenance is allowed because the current time is within an allowed
	// disruption window (or no windows are defined) and no selected pod is protected.
	ReasonWithinDisruptionWindow = "WithinDisruptionWindow"
	// ReasonOutsideDisruptionWindow indicates that maintenance is not allowed because the current time is outside of
	// every allowed disruption window.
	ReasonOutsideDisruptionWindow = "OutsideDisruptionWindow"
	// ReasonMinInitialRunDurationNotMet indicates that maintenance is not allowed because a selected pod has not yet run
	// for MinInitialRunDurationDays, counted from its status.startTime, or from its creationTimestamp until it has been
	// acknowledged by a kubelet.
	ReasonMinInitialRunDurationNotMet = "MinInitialRunDurationNotMet"
	// ReasonGraceTerminationInProgress indicates that maintenance is not allowed because a selected pod started
	// terminating less than GraceTerminationDurationSeconds ago (see status.lastPodTerminationTime).
	ReasonGraceTerminationInProgress = "GraceTerminationInProgress"
	// ReasonReadinessCheckFailed indicates that maintenance readiness could not be determined because of an error.
	ReasonReadinessCheckFailed = "ReadinessCheckFailed"
)

// WorkloadClassStatus defines the observed state of WorkloadClass.
type WorkloadClassStatus struct {
	// MaintenanceReadiness indicates if the workload is currently ready for maintenance.
	// +optional
	MaintenanceReadiness MaintenanceReadiness `json:"maintenanceReadiness,omitempty"`

	// LastDisruptionTime is the last time a disruption was observed for pods in this class.
	// +optional
	LastDisruptionTime *metav1.Time `json:"lastDisruptionTime,omitempty"`

	// LastPodTerminationTime is the most recent time a pod selected by this class was observed to start terminating,
	// that is, the time its deletion was requested. The controller keeps MaintenanceReadiness as NotReady until
	// GraceTerminationDurationSeconds have passed since this time, even if the pod no longer exists.
	// +optional
	LastPodTerminationTime *metav1.Time `json:"lastPodTerminationTime,omitempty"`

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
