# workload-class

A high-level Kubernetes control layer for managing workload outcomes and disruption guardrails during maintenance.

## Overview

`workload-class` provides a two-tier API under `workloads.x-k8s.io/v1alpha1` that allows platform administrators and workload owners to collaborate on managing workload disruptions during cluster maintenance:

1. **`WorkloadClassGuardrail` (Cluster-scoped)**: Defined by platform engineers to enforce cluster-wide boundaries on disruption policies (such as allowed days of the week, maximum number of maintenance windows, and maximum non-disruption duration to prevent maintenance starvation).
2. **`WorkloadClass` (Namespace-scoped)**: Defined by workload owners to declare pod disruption policies (temporal maintenance windows, minimum initial run duration, graceful termination cool-down, and outside-of-window subject exceptions) within platform guardrails.

The controller also expresses each workload's disruption policy as a PodDisruptionBudget for every Pod a `WorkloadClass` governs (see [Per-Pod PodDisruptionBudgets](#per-pod-poddisruptionbudgets)).

### Pod Selection & Namespace Enforced Class Precedence

A `WorkloadClass` is resolved for a Pod using the following order of precedence:

1. **Namespace-enforced class (`workloads.x-k8s.io/enforced-class`)**: If a `Namespace` carries the `workloads.x-k8s.io/enforced-class: <workload-class-name>` label (`EnforcedWorkloadClassLabel`), that `WorkloadClass` is enforced across all Pods in the namespace, overriding its own `spec.podSelector` as well as any other `WorkloadClass` in that namespace.
2. **Pod label selector (`spec.podSelector`)**: If no namespace-enforced class is set, Pods are matched by `spec.podSelector` in the same namespace. `WorkloadClass` selectors within a namespace should be non-overlapping; overlapping `podSelector`s are surfaced via a status `Condition` on the `WorkloadClass`.

### Relationship to Upstream `scheduling.k8s.io` `Workload`

`workloads.x-k8s.io` `WorkloadClass` and upstream `scheduling.k8s.io` `Workload` (KEP-4671) address orthogonal layers of workload management:

- **`scheduling.k8s.io` `Workload`** models a **runtime scheduling unit and pod-group topology** (such as gang scheduling constraints for batch/AI jobs and gang disruption policies that define *what unit* must be disrupted together—e.g., whether evicting a single Pod requires disrupting and recreating the entire Pod group).
- **`workloads.x-k8s.io` `WorkloadClass`** defines **operational maintenance and disruption policies** (*when* and *under what organizational guardrails* disruptions are allowed—such as recurring time-zone-aware maintenance windows, minimum initial run durations, and starvation bounds via `maxNonDisruptionDurationDays`).

The two APIs are complementary: `scheduling.k8s.io` `Workload` governs structural pod-group scheduling and gang disruption scope, while `WorkloadClass` governs temporal readiness and guardrail enforcement for maintenance disruptions.

## Per-Pod PodDisruptionBudgets

The controller expresses each workload's disruption policy as standard PodDisruptionBudgets (PDBs), which Kubernetes components such as the cluster autoscaler, `kubectl drain`, and node upgrades already respect when planning and performing disruptions. It creates one PDB for each Pod a `WorkloadClass` governs. The eviction webhook still applies: it checks every eviction against the Pod's `WorkloadClass`, including for Pods that have no managed PDB.

> **NOTE:** Subjects listed in `allowedDisruptionsOutsideOfWindow` are only exempt from the webhook. While a Pod's PDB is closed (`maxUnavailable: 0`), it still blocks their evictions.

### Which Pods Get a PDB

A Pod is governed by its best-match `WorkloadClass`, which is the same one the eviction webhook uses (see [Pod Selection & Namespace Enforced Class Precedence](#pod-selection--namespace-enforced-class-precedence)):

1. If the namespace has the `workloads.x-k8s.io/enforced-class` label, the named `WorkloadClass` governs every Pod in the namespace, whatever its `podSelector`, and the other `WorkloadClasses` in the namespace govern none. If the label names a `WorkloadClass` that doesn't exist or is being deleted, no `WorkloadClass` governs the namespace's Pods. The label never falls back to `podSelector` matching.
2. Otherwise, the Pod is governed by the most specific `WorkloadClass` whose `podSelector` selects it (the one with the most `matchLabels` plus `matchExpressions`). Ties go to the oldest `WorkloadClass`, and then to the name that sorts first. `WorkloadClasses` that are being deleted are skipped.

The controller creates a PDB for every governed Pod, as long as:

- **At least one `WorkloadClassGuardrail` exists.** Without a guardrail, nothing bounds `maxNonDisruptionDurationDays`, so PDBs could block node drains and upgrades indefinitely. The `Validated` condition of a `WorkloadClass` has reason `NoGuardrails` in that case.
- **The Pod is active**: it isn't terminating and hasn't reached the `Succeeded` or `Failed` phase.
- **No user-created PDB covers the Pod.** If any PDB that isn't managed by a `WorkloadClass` selects the Pod, the user's PDB takes precedence, since the Eviction API rejects evicting a Pod that more than one PDB covers. The webhook still enforces the `WorkloadClass` for that Pod, and the Pod still counts toward its `maintenanceReadiness`.

> **NOTE:** A `WorkloadClass` without a `podSelector` selects no Pods (like a PDB without a selector), so it only governs Pods when the namespace enforces it. An empty `podSelector` (`{}`) selects every Pod in the namespace.

### PDB Spec

Each PDB is named `workload-<pod name>`. If that name would exceed the 253-character limit, it is truncated and suffixed with a hyphen and the first 8 hex characters of the SHA-256 hash of the Pod name, so that it stays unique and deterministic.

The controller labels each Pod it protects with `workloads.x-k8s.io/pod-uid: <pod UID>` (it doesn't remove the label later). The PDB carries the same label and selects only that label, so it covers exactly one Pod and never a later Pod that reuses the same name, such as a recreated StatefulSet Pod.

The PDB uses `unhealthyPodEvictionPolicy: AlwaysAllow`, so a Pod that isn't Ready (for example, one in `CrashLoopBackOff`) can still be evicted and doesn't hold up node drains or upgrades while `maxUnavailable` is `0`. Its `maxUnavailable` follows the `WorkloadClass`:

| `WorkloadClass` state | `maxUnavailable` |
| --- | --- |
| `Validated` reason `ValidationPassed`, with `maintenanceReadiness: Ready` or `Overdue` | `100%`: evictions are allowed. |
| `Validated` reason `ValidationPassed`, with `maintenanceReadiness: NotReady` (outside every window, a Pod hasn't run for `minInitialRunDurationDays`, or a Pod started terminating less than `graceTerminationDurationSeconds` ago) or `Unknown` (readiness couldn't be calculated) | `0`: evictions are blocked. |
| `Validated` reason `SelectorOverlap` | Same as `ValidationPassed`: each Pod is governed by its best match, so overlapping selectors don't open PDBs. |
| `Validated` reason `ValidationFailed` (fails guardrail or spec validation) | `100%`: the controller opens existing PDBs instead of deleting them, and doesn't create new ones. |
| No `WorkloadClassGuardrail` exists (reason `NoGuardrails`) | No PDBs are created. Existing PDBs, for example after the last guardrail is deleted, are opened (`100%`). |

```sh
kubectl get pdb -n sample
```
```
NAME                                        MIN AVAILABLE   MAX UNAVAILABLE   ALLOWED DISRUPTIONS   AGE
workload-test-deployment-5f8c7d9b6c-x2k4p   N/A             0                 0                     2m
```

### Ownership and Cleanup

Each PDB has a controller owner reference (`controller: true`, `blockOwnerDeletion: true`) to the `WorkloadClass` that governs its Pod, so deleting the `WorkloadClass` garbage-collects its PDBs. The controller also deletes a Pod's PDB when:

- the Pod is deleted, starts terminating, or reaches the `Succeeded` or `Failed` phase;
- a user-created PDB starts covering the Pod (the controller recreates its PDB when the user-created PDB is deleted or stops covering the Pod);
- no `WorkloadClass` that protects Pods governs the Pod anymore, for example because the Pod was relabeled out of the `podSelector`.

A `WorkloadClass` that stops protecting Pods (for example, because it fails validation) opens the PDBs it owns for active Pods that no user-created PDB covers, instead of deleting them, as described in the table above.

When a Pod's best-match `WorkloadClass` changes, for example because the namespace starts enforcing a class or a more specific `WorkloadClass` is created, the new `WorkloadClass` takes ownership of the existing PDB rather than deleting and recreating it, so the Pod stays protected throughout the handover. The PDB is handed back the same way, for example when the `workloads.x-k8s.io/enforced-class` label is removed. If the new best match doesn't protect Pods, the previous `WorkloadClass` deletes the PDB instead.

The controller never modifies a user-created PDB, even one that happens to be named `workload-<pod name>`. If such a PDB doesn't cover the Pod, the Pod gets no managed PDB until the user-created PDB is changed or deleted.

### RBAC

To manage Per-Pod PDBs, the controller's ClusterRole (generated into `config/rbac/role.yaml`) grants:

- `patch` on `pods` (along with `list` and `watch`), to set the `workloads.x-k8s.io/pod-uid` label;
- `get`, `list`, `watch`, `create`, `update`, `patch`, and `delete` on `poddisruptionbudgets` in the `policy` API group.

If you deploy the controller with your own RBAC, grant the same permissions.

### Example

**1. Create a guardrail, a workload, and a `WorkloadClass`**

Apply the sample guardrail and create a Deployment whose Pods are labeled `app: web`:

```sh
kubectl apply -f config/samples/workloads_v1alpha1_workloadclassguardrail.yaml
kubectl create namespace demo
kubectl create deployment web -n demo --image=registry.k8s.io/pause:3.9 --replicas=2
```

Then apply (`kubectl apply -f`) a `WorkloadClass` that allows disruptions only early on weekends:

```yaml
apiVersion: workloads.x-k8s.io/v1alpha1
kind: WorkloadClass
metadata:
  name: web
  namespace: demo
spec:
  podSelector:
    matchLabels:
      app: web
  disruptionPolicy:
    allowedDisruptionWindows:
      - name: weekend-maintenance
        daysOfWeek:
          - Saturday
          - Sunday
        startTime: "00:00"
        endTime: "04:00"
        timeZone: UTC
    maxNonDisruptionDurationDays: 30
```

**2. List the PDBs**

Outside the window, each Pod has a closed PDB, and each Pod carries the `workloads.x-k8s.io/pod-uid` label that its PDB selects:

```sh
kubectl get pdb -n demo
kubectl get pods -n demo -L workloads.x-k8s.io/pod-uid
```
```
NAME                            MIN AVAILABLE   MAX UNAVAILABLE   ALLOWED DISRUPTIONS   AGE
workload-web-6c9d8f7b5d-4jzqx   N/A             0                 0                     1m
workload-web-6c9d8f7b5d-x8k2p   N/A             0                 0                     1m

NAME                   READY   STATUS    RESTARTS   AGE   POD-UID
web-6c9d8f7b5d-4jzqx   1/1     Running   0          1m    0b6f3c1e-7d2a-4f5b-9c8e-1a2b3c4d5e6f
web-6c9d8f7b5d-x8k2p   1/1     Running   0          1m    5e8a9d0c-2b1f-4c3d-8e7a-6f5b4a3c2d1e
```

Inside the window, `MAX UNAVAILABLE` becomes `100%` and each PDB allows one disruption.

**3. Fail guardrail validation**

Setting `maxNonDisruptionDurationDays` above the guardrail limit of 30 makes the `WorkloadClass` fail validation, so the controller opens its PDBs (`100%`). Setting it back restores them:

```sh
kubectl patch workloadclass web -n demo --type=merge -p '{"spec":{"disruptionPolicy":{"maxNonDisruptionDurationDays":31}}}'
kubectl patch workloadclass web -n demo --type=merge -p '{"spec":{"disruptionPolicy":{"maxNonDisruptionDurationDays":30}}}'
```

**4. Add a user-created PDB**

A user-created PDB that selects the Pods takes precedence, so the controller deletes its PDBs, and recreates them when the user-created PDB is deleted:

```sh
kubectl create pdb web -n demo --selector=app=web --max-unavailable=1
kubectl get pdb -n demo   # only "web" is listed
kubectl delete pdb web -n demo
```

**5. Enforce a class on the namespace**

Create a `WorkloadClass` without a `podSelector`. It governs no Pods until the namespace enforces it:

```yaml
apiVersion: workloads.x-k8s.io/v1alpha1
kind: WorkloadClass
metadata:
  name: demo-default
  namespace: demo
spec:
  disruptionPolicy:
    allowedDisruptionWindows:
      - name: sunday-maintenance
        daysOfWeek:
          - Sunday
        startTime: "02:00"
        endTime: "06:00"
        timeZone: UTC
    maxNonDisruptionDurationDays: 14
```

Label the namespace to enforce it. `demo-default` takes over the existing PDBs, which keep their names and UIDs (run the `kubectl get pdb` command before labeling to compare):

```sh
kubectl label namespace demo workloads.x-k8s.io/enforced-class=demo-default
kubectl get pdb -n demo -o custom-columns='NAME:.metadata.name,OWNER:.metadata.ownerReferences[0].name,UID:.metadata.uid'
```
```
NAME                            OWNER          UID
workload-web-6c9d8f7b5d-4jzqx   demo-default   9d4c2b7a-1e3f-4a6b-8c5d-2f1e0a9b8c7d
workload-web-6c9d8f7b5d-x8k2p   demo-default   3a7e5f1b-6c2d-4e8a-9b0c-7d6e5f4a3b2c
```

Removing the label hands the PDBs back to `web`:

```sh
kubectl label namespace demo workloads.x-k8s.io/enforced-class-
```

**6. Clean up**

Deleting the `WorkloadClasses` garbage-collects their PDBs:

```sh
kubectl delete workloadclass web demo-default -n demo
kubectl delete namespace demo
```

## Community, discussion, contribution, and support

Learn how to engage with the Kubernetes community on the [community page](http://kubernetes.io/community/).

You can reach the maintainers of this project at:

- [Slack channel](https://kubernetes.slack.com/messages/sig-apps)
- [Mailing List](https://groups.google.com/a/kubernetes.io/g/sig-apps)

### Code of conduct

Participation in the Kubernetes community is governed by the [Kubernetes Code of Conduct](code-of-conduct.md).
