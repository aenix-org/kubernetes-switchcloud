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

## Versioning roadmap

* **v0 (this scaffold)** — boots, discovers tenants, watches Services,
  logs `LoadBalancer` events. No OpenStack API calls. Goal: get the
  helm chart, image build, and platform wiring on production so the
  Octavia integration can ship as an isolated next PR.

* **v1** — Octavia CRUD: ensure LB, listener per Service port, pool,
  members synced from tenant Node IPs (or Endpoints once we wire
  externalTrafficPolicy=Local). Patch `Service.status.loadBalancer`.

* **v2** — finalizer on `KubernetesSwitchcloud` CR to cascade-delete
  every Octavia LB attached to that tenant before Kamaji tears the
  control plane down. Without this, deleting a tenant orphans paid
  LB resources in OpenStack.

* **v3** — Service annotations matching upstream OpenStack-CCM
  (`loadbalancer.openstack.org/*`), UDP/SCTP listeners, multi-port
  Services, `externalTrafficPolicy=Local` source-IP preservation.

## OVN driver constraints (Switch Cloud zhw)

Probed empirically (zhw lacks Amphora flavors; only OVN provider
actually accepts LB creation):

* Algorithms: `SOURCE_IP_PORT` only. ROUND_ROBIN / LEAST_CONNECTIONS
  return 400.
* Listener protocols: TCP, UDP, SCTP — no HTTP/HTTPS L7. K8s Services
  are L4 so this is not a limitation in practice.
* Health monitors: PING, TCP, UDP-CONNECT — no HTTP monitors. nodePort
  TCP probe is sufficient for kube-proxy backends.
* VIP allocation: directly on the `public` external network. No
  separate FIP step.

## Configuration

Helm values (see `charts/loadbalancer-controller/values.yaml`):

| key | default | notes |
|---|---|---|
| `image.tag` | `""` (uses chart `appVersion`) | |
| `replicaCount` | `1` | leader-election enabled by default; safe to bump for HA |
| `leaderElection.enabled` | `true` | adds RBAC for configmaps + coordination.k8s.io/leases in release namespace |
| `log.level` | `info` | one of `debug`, `info`, `warn`, `error` |
| `log.encoding` | `json` | one of `json`, `console` |

Per-tenant configuration lives on each `KubernetesSwitchcloud` CR
(added to the CRD schema in v1):

```yaml
spec:
  openstack:
    # existing fields ...
    loadBalancer:
      enabled: false        # opt-in; default off
      vipSubnetID: ""       # override; default: auto-pick first IPv4 subnet
                            # of first router:external=true network
      providerDriver: ""    # override; default "ovn"
                            # (only OVN is currently functional in zhw)
```

## Development

```
make tidy       # go mod tidy
make build      # local binary at bin/loadbalancer-controller
make test       # go test ./...
make vet        # go vet
make image      # docker build → ghcr.io/aenix-org/loadbalancer-controller:dev
```
