# kubernetes-switchcloud

Cozystack application package for managed Kubernetes clusters on
[Switch Cloud](https://cloud.switch.ch) (OpenStack + Talos nodes via
Cluster API).

## Architecture

```text
Cozystack management cluster
├── Kamaji TenantControlPlane   — hosted control plane (etcd, kube-apiserver, …)
├── CAPI Cluster                — cluster lifecycle
├── CAPO OpenStackCluster       — Switch Cloud infra (network, security groups)
├── MachineDeployment / MachineSet
│   └── OpenStackMachine        — Talos worker VMs in Switch Cloud
├── FIP Reconciler (optional)   — associates pre-allocated FIPs with worker VMs
└── Cluster Autoscaler          — scales MachineDeployments based on pod demand
```

Worker nodes run **Talos Linux**. The control plane is hosted inside the
management cluster by [Kamaji](https://kamaji.clastix.io/).

## Repository layout

```text
charts/
  kubernetes-switchcloud/       Main application Helm chart
  capi-bootstrap-talos/         CAPI bootstrap provider for Talos
  capi-infraprovider-openstack/ CAPI infrastructure provider for OpenStack
  provider-id-setter/           DaemonSet that sets spec.providerID on nodes
packages/core/platform/         Cozystack platform chart (ApplicationDefinition + HelmReleases)
init.yaml                       Bootstrap file: apply this once to register the package
fip-reconciler/                 Go source for the FIP reconciler sidecar
provider-id-setter/             Go source for the provider-id-setter DaemonSet
packages/apps/example-values.yaml   Example cluster values
```

## Prerequisites

- Cozystack management cluster with the following packages enabled:

  | Package | Purpose |
  | --- | --- |
  | `cozystack.networking` | Cilium CNI on the management cluster |
  | `cozystack.capi-operator` | Cluster API operator |
  | `cozystack.capi-provider-core` | CAPI core provider |
  | `cozystack.capi-provider-cp-kamaji` | Kamaji control-plane provider |

- Switch Cloud project with Application Credentials (see below).
- A Glance image named `talos-openstack-amd64` (or your own name set in
  `nodeGroups.<name>.imageName`). Build it with
  [image-factory](https://factory.talos.dev/) targeting `openstack`.

## Installation on Cozystack

### 1. Apply init.yaml

Apply the bootstrap file once. It creates a `GitRepository` in `cozy-public` and
a `HelmRelease` in `cozy-system` that installs the platform chart:

```bash
kubectl --context <management-cluster> apply \
  --filename https://raw.githubusercontent.com/aenix-org/kubernetes-switchcloud/main/init.yaml
```

For GitOps (recommended), commit `init.yaml` into your cluster's Flux repository
(e.g. alongside other cluster-level manifests) and let Flux apply it automatically.

This installs:

- `ApplicationDefinition` for `KubernetesSwitchcloud` — makes the resource type
  available in the Cozystack dashboard under **IaaS**
- `HelmRelease` objects for `capi-bootstrap-talos` and `capi-infraprovider-openstack`
  CAPI providers in the `cozy-cluster-api` namespace

After a successful sync, the `KubernetesSwitchcloud` resource type appears in the
Cozystack dashboard under **IaaS**.

### 2. Verify the HelmReleases are ready

```bash
kubectl --context <management-cluster> get helmreleases \
  --namespace cozy-cluster-api | grep -E "capi-bootstrap-talos|capi-infraprovider-openstack"
kubectl --context <management-cluster> get applicationdefinitions kubernetes-switchcloud
```

Expected output:

```text
capi-bootstrap-talos              True    ...
capi-infraprovider-openstack      True    ...

NAME                       ...
kubernetes-switchcloud     ...
```

## Creating a cluster

### Option A: Cozystack API (recommended)

Once the `ApplicationDefinition` is installed, the Cozystack API serves the
`KubernetesSwitchcloud` resource. Create a cluster in a tenant namespace:

```yaml
apiVersion: apps.cozystack.io/v1alpha1
kind: KubernetesSwitchcloud
metadata:
  name: my-cluster
  namespace: tenant-myproject
spec:
  openstack:
    authURL: "https://identity.api.zhw.cloud.switch.ch/v3"
    regionName: "zhw"
    applicationCredentialID: "<id>"
    applicationCredentialSecret: "<secret>"
    network:
      id: "<openstack-network-uuid>"
    floatingIPNetwork: "public"
  version: "v1.32.6"
  talosVersion: "v1.10.0"
  controlPlane:
    replicas: 2
  nodeGroups:
    md0:
      minReplicas: 0
      maxReplicas: 5
      flavorName: "c004r008"
      imageName: "talos-openstack-amd64"
      resources:
        cpu: 4
        memory: 8Gi
```

### Option B: Helm directly (for testing)

```bash
helm install my-cluster \
  oci://registry-1.docker.io/999669/kubernetes-switchcloud \
  --namespace tenant-myproject \
  --values packages/apps/example-values.yaml \
  --set openstack.applicationCredentialID=<id> \
  --set openstack.applicationCredentialSecret=<secret>
```

## Credentials

### Inline credentials (simple)

Pass credentials directly in the spec or `--set` flags. They are stored in the
HelmRelease values (Kubernetes Secret inside Cozystack).

```yaml
spec:
  openstack:
    applicationCredentialID: "abc123"
    applicationCredentialSecret: "s3cr3t"
```

### Existing Secret (recommended for production)

Create a Secret in the **same namespace** as the cluster with a `clouds.yaml` key:

```bash
kubectl --context <management-cluster> create secret generic my-cluster-openstack \
  --namespace tenant-myproject \
  --from-literal=clouds.yaml="$(cat <<'EOF'
clouds:
  openstack:
    auth:
      auth_url: https://identity.api.zhw.cloud.switch.ch/v3
      application_credential_id: "<id>"
      application_credential_secret: "<secret>"
    region_name: "zhw"
EOF
)"
```

Then reference it in the spec:

```yaml
spec:
  openstack:
    existingSecret: "my-cluster-openstack"
    cloudName: "openstack"
    # authURL and applicationCredential* are ignored when existingSecret is set
```

### Switch Cloud: creating Application Credentials

1. Log in to [cloud.switch.ch](https://cloud.switch.ch)
2. Go to **Identity** → **Application Credentials**
3. Click **Create Application Credential**
4. Set a name, leave roles empty (inherits your user roles), click **Create**
5. Copy the **ID** and **Secret** — the secret is shown only once

## Configuration reference

### Core parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `openstack.authURL` | `""` | Keystone identity endpoint |
| `openstack.regionName` | `""` | OpenStack region (e.g. `zhw`) |
| `openstack.applicationCredentialID` | `""` | App credential ID |
| `openstack.applicationCredentialSecret` | `""` | App credential secret |
| `openstack.existingSecret` | `""` | Existing Secret name with `clouds.yaml` |
| `openstack.network.id` | `""` | OpenStack network UUID for worker VMs |
| `openstack.floatingIPNetwork` | `""` | External network for FIPs (empty = SNAT only) |
| `version` | `v1.32.6` | Kubernetes version |
| `talosVersion` | `v1.10.0` | Talos OS version |
| `controlPlane.replicas` | `2` | Kamaji control-plane replicas |
| `host` | `""` | External API hostname (auto-derived when empty) |

### Node groups

Each entry under `nodeGroups` creates a CAPI `MachineDeployment`.

```yaml
nodeGroups:
  md0:                       # group name (used as MachineDeployment suffix)
    minReplicas: 0           # autoscaler minimum
    maxReplicas: 10          # autoscaler maximum
    flavorName: "c004r008"   # Nova flavor
    imageName: "talos-openstack-amd64"
    roles: []                # propagated as node labels
    resources:
      cpu: 4
      memory: 8Gi
    securityGroups:
      - default
```

Multiple groups are supported:

```yaml
nodeGroups:
  workers:
    flavorName: "c004r008"
    minReplicas: 1
    maxReplicas: 10
    resources: {cpu: 4, memory: 8Gi}
  gpu:
    flavorName: "g004r032"
    minReplicas: 0
    maxReplicas: 4
    resources: {cpu: 4, memory: 32Gi}
```

## Addons

| Addon | Default | Notes |
| --- | --- | --- |
| `addons.cilium` | enabled | CNI — required, always on |
| `addons.certManager` | enabled | cert-manager in `cozy-cert-manager` namespace |
| `addons.metricsServer` | enabled | metrics-server for HPA/VPA |
| `addons.ingressNginx` | disabled | ingress-nginx controller |
| `addons.fluxcd` | disabled | FluxCD inside the tenant cluster |
| `addons.clusterAutoscaler` | disabled | see below |
| `addons.providerIdSetter` | disabled | see below |
| `addons.openstackCCM` | disabled | OpenStack Cloud Controller Manager |

Each addon accepts a `valuesOverride` map for fine-tuning the underlying chart.

## Cluster Autoscaler

The autoscaler uses `--cloud-provider=clusterapi` and scales CAPI
`MachineDeployments` based on pod scheduling demand.

### Enable

```yaml
addons:
  clusterAutoscaler:
    enabled: true
    # Image version must match the cluster's Kubernetes minor version
    image: "registry.k8s.io/autoscaling/cluster-autoscaler:v1.32.0"
    scaleDownDelayAfterAdd: "10m"
    scaleDownUnneededTime: "10m"
    maxNodeProvisionTime: "20m"
```

Scale-to-zero works when `minReplicas: 0` and the node has no non-DaemonSet
pods that cannot be rescheduled.

### Kilo mesh: protect the leader node

When using a Kilo WireGuard mesh (required for Switch Cloud multi-site
topologies), one node must act as the protected leader. Annotate it to prevent
accidental scale-down:

```bash
kubectl --context <tenant-cluster> annotate node <leader-node-name> \
  cluster-autoscaler.kubernetes.io/scale-down-disabled="true"
```

This node will never be removed by the autoscaler even if idle.

## Provider ID Setter

Switch Cloud does not inject `spec.providerID` into nodes via cloud-init.
The `provider-id-setter` DaemonSet fetches the instance UUID from the OpenStack
metadata service (`169.254.169.254`) and patches the node.

Enable it after the cluster is running:

```yaml
addons:
  providerIdSetter:
    enabled: true
    chartVersion: "0.1.1"
```

`spec.providerID` must be set before enabling the Cluster Autoscaler — otherwise
the autoscaler cannot map nodes to cloud instances.

## FIP Reconciler

CAPO v0.12.x cannot associate pre-allocated Floating IPs on Switch Cloud because
the VM port lives on the internal network, not the external one. The FIP
reconciler watches `OpenStackServer` objects for `FloatingIPError` and calls
Neutron directly.

The reconciler runs as a sidecar in the management cluster alongside the main
chart. No extra configuration is needed — it is always deployed when
`openstack.floatingIPNetwork` is set.

To adjust the polling interval:

```yaml
fipReconciler:
  interval: "30s"
```

## Troubleshooting

### Nodes stuck in `NotReady`

The Talos nodes bootstrap via Ignition. If they cannot reach the Kubernetes API,
check:

- Security groups allow TCP 6443 from the worker network to the management
  cluster (or the external hostname resolves and is routable).
- The `talosVersion` matches the image uploaded to Glance.

### Cluster Autoscaler not scaling down to zero

Scale-down to zero requires no non-DaemonSet pods on the last node. System pods
(`cilium-operator`, `coredns`, `cert-manager`) block scale-down of the last node.
This is expected — they have nowhere to reschedule. Delete the cluster instead.

### FIP not being associated

Check the FIP reconciler logs:

```bash
kubectl --context <management-cluster> logs \
  --namespace <tenant-namespace> \
  --selector "app=<release-name>-fip-reconciler"
```

Common causes: wrong `floatingIPNetwork` name, FIP already associated, or
application credentials lacking Neutron permissions.

### `spec.providerID` not set

Ensure `addons.providerIdSetter.enabled: true`. The setter requires access to
`169.254.169.254` from within the VM — check the Switch Cloud security group
allows this metadata endpoint.
