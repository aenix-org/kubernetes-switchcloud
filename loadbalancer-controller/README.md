# loadbalancer-controller

Centralised OpenStack LoadBalancer provisioner for Cozystack
`KubernetesSwitchcloud` tenants.

## Why

Tenants running on Switch Cloud (Switch.ch zhw OpenStack) want
`type: LoadBalancer` Services with real publicly-routable IPs, the same
way they'd get one from a cloud-provider CCM. The standard answer is
to deploy OpenStack CCM inside each tenant cluster — but that requires
shipping OpenStack application credentials into every tenant, which
breaks the Cozystack tenancy boundary (tenant cluster admins can read
the cloud-config Secret and exfiltrate the credentials).

This controller lives in the **management cluster** (where Kamaji
already terminates each tenant's apiserver) and provisions Octavia LBs
on behalf of every tenant, using the OpenStack credentials already
declared on the tenant's `KubernetesSwitchcloud` CR. Tenant cluster
users only see the resulting `status.loadBalancer.ingress[].ip` —
never the token that minted it.

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│ Management cluster (Kamaji host)                               │
│                                                                │
│  loadbalancer-controller (this repo)                           │
│   ├─ multicluster.Registry                                     │
│   │    discovers tenants via                                   │
│   │    Secrets `kubernetes-switchcloud-<t>-admin-kubeconfig`   │
│   │    in `tenant-root`                                        │
│   │                                                            │
│   ├─ controller.ServiceReconciler                              │
│   │    one controller per tenant, each with its own            │
│   │    cluster.Cache → tenant Service watch                    │
│   │                                                            │
│   └─ (v1) openstack.Octavia                                    │
│        gophercloud per tenant; reads creds from KSC CR or      │
│        from existingSecret; CRUD on LB/listener/pool/members   │
│                                                                │
└──────────────────────────┬─────────────────────────────────────┘
                           │ OpenStack API
                           ▼
                ┌──────────────────────┐
                │   Switch Cloud (zhw) │
                │   Octavia (OVN)      │
                └──────────────────────┘
```

## What the controller does

For every Service of `type: LoadBalancer` in a participating tenant
cluster the controller ensures, end-to-end:

1. **Octavia LB** (provider `ovn`) on the tenant network with a
   listener per Service port and a pool of `(workerInternalIP, NodePort)`
   members synced from Ready Nodes.
2. **Floating IP** allocated from the configured external network and
   bound to the LB's VIP port. Required in Switch Cloud zhw because
   the project's allocations on the `public` IPv4 subnet are not
   BGP-announced; the FIP is what reaches the public internet.
3. **Per-cluster security group** `cozystack-lb-<cluster>` with
   intra-SG baseline + per-Service NodePort ingress rules. The SG
   is attached to every worker port via Neutron port update, and
   the project `default` SG is removed — giving full L4 isolation
   between clusters in the same OpenStack project (distinct SGs
   mean allow-from-same-SG never crosses cluster boundaries).
4. **Cleanup** via two finalizers (one per-Service, one on the
   `KubernetesSwitchcloud` CR itself) plus a periodic orphan sweeper
   that catches resources whose owning CR / Service vanished while
   the controller was offline.

The FIP address is patched back into
`Service.status.loadBalancer.ingress` so tenant users see it via
`kubectl get svc`.

## OVN driver constraints (Switch Cloud zhw)

Probed empirically:

* Algorithms: `SOURCE_IP_PORT` only. ROUND_ROBIN / LEAST_CONNECTIONS
  return 400.
* Listener protocols: TCP, UDP, SCTP — no HTTP/HTTPS L7. K8s Services
  are L4 so this is not a limitation in practice.
* Health monitors: PING, TCP, UDP-CONNECT — no HTTP monitors. NodePort
  TCP probe is sufficient for kube-proxy backends.
* VIP placement: must be a tenant-private network, NOT directly on the
  `public` external network. External reachability is wired via the
  floating IP. See `floatingNetworkID` below.

## Configuration

Helm values (see `packages/system/loadbalancer-controller/values.yaml`):

| key | default | notes |
|---|---|---|
| `image.tag` | `""` (uses chart `appVersion`) | |
| `replicaCount` | `1` | leader-election enabled by default; safe to bump for HA |
| `leaderElection.enabled` | `true` | adds RBAC for configmaps + coordination.k8s.io/leases in release namespace |
| `log.level` | `info` | one of `debug`, `info`, `warn`, `error` |
| `log.encoding` | `json` | one of `json`, `console` |

Per-cluster configuration lives on each `KubernetesSwitchcloud` CR:

```yaml
spec:
  openstack:
    # ... credentials, network ...
    loadBalancer:
      enabled: false                  # opt-in per cluster
      providerDriver: "ovn"           # only "ovn" works in Switch Cloud zhw
      vipNetworkID: ""                # auto-discovered from OpenStackCluster.status.network.id;
                                      # set explicitly only in legacy/override scenarios
      floatingNetworkID: ""           # external network for FIP allocation
                                      # (empty = internal-only LB, no public IPv4)
      floatingSubnetID: ""            # optional pin within floatingNetworkID
      workerSecurityGroupID: ""       # operator override; when empty the controller
                                      # auto-creates and manages cozystack-lb-<cluster>.
                                      # Pin to an operator-owned SG to keep the project
                                      # default SG attached to workers (e.g. for SSH).
      allowedCIDRs: ["0.0.0.0/0"]     # source CIDRs allowed to reach the NodePort
```

In the typical Switch Cloud zhw setup with `spec.openstack.network.id`
left empty (auto-managed mode), the operator needs to set only
`enabled: true` + `floatingNetworkID: <public-net-uuid>`; everything
else either auto-discovers or has a sensible default.

## Development

```
make tidy       # go mod tidy
make build      # local binary at bin/loadbalancer-controller
make test       # go test ./...
make vet        # go vet
make image      # docker build → ghcr.io/aenix-org/loadbalancer-controller:dev
```
