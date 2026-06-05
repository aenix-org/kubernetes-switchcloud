/*
Copyright 2026 The Aenix Authors.
*/

package openstack

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/gophercloud/gophercloud/v2/openstack/compute/v2/servers"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/loadbalancers"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/layer3/floatingips"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/security/groups"
	"github.com/gophercloud/gophercloud/v2/pagination"
)

// ServiceExistsFunc reports whether the tenant Service identified by
// (cluster, namespace, name) still exists in the tenant's apiserver.
// Returning an error tells the sweeper to leave the resource alone
// this round and retry on the next tick — bias-towards-safe because
// the alternative is deleting an LB that a healthy tenant still
// claims.
//
// A nil function (typical for the cluster-only finalizer path)
// disables the per-Service drift check; only the cluster-orphan
// path runs.
type ServiceExistsFunc func(ctx context.Context, cluster, namespace, name string) (bool, error)

// kscServerNamePrefix is what the KSC chart's release prefix
// resolves to in Nova: every worker that came up via the chart's
// OpenStackMachineTemplate is named
// `kubernetes-switchcloud-<cluster>-<nodegroup>-<hash>-<hash>`. CAPO
// configures these as boot-from-volume with delete_on_termination, so
// a plain `server delete` reclaims both the instance and its root
// volume — no separate cinder cleanup needed.
const kscServerNamePrefix = "kubernetes-switchcloud-"

// SweepClusterResources removes every OpenStack resource the controller
// could have provisioned on behalf of `cluster`. Used by the
// CR-finalizer path when a KubernetesSwitchcloud CR is deleted — at
// that point we can no longer reach the tenant cluster to drive
// per-Service finalizers, so we tear resources down by deterministic
// name prefix instead.
//
// Resources covered:
//
//   - Octavia LBs named `cozystack:<cluster>/<ns>/<svc>` (any namespace,
//     any service) — cascade-deleted, plus their attached FIP
//   - Cluster security group `cozystack-lb-<cluster>` (best-effort —
//     fails harmlessly if the SG is still referenced by surviving
//     worker ports).
//
// Idempotent. Safe to call when nothing exists. Operator-managed
// resources (no `cozystack:` / `cozystack-lb-` prefix) are never
// touched.
func SweepClusterResources(ctx context.Context, c *Clients, cluster string) error {
	lbPrefix := managedNamePrefix + cluster + "/"

	pager := loadbalancers.List(c.LoadBalancer, loadbalancers.ListOpts{})

	var toDelete []loadbalancers.LoadBalancer

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := loadbalancers.ExtractLoadBalancers(page)
		if err != nil {
			return false, err
		}

		for _, lb := range list {
			if strings.HasPrefix(lb.Name, lbPrefix) {
				toDelete = append(toDelete, lb)
			}
		}

		return true, nil
	})
	if err != nil {
		return errors.Wrap(err, "listing LBs for cluster sweep") //nolint:wrapcheck
	}

	for _, lb := range toDelete {
		// FIP must come down before LB cascade — Neutron FIPs live
		// outside Octavia's purview.
		if err := DeleteFloatingIP(ctx, c, lb.VipPortID); err != nil {
			return errors.Wrapf(err, "deleting FIP for orphan LB %s", lb.ID)
		}

		err := loadbalancers.Delete(ctx, c.LoadBalancer, lb.ID, loadbalancers.DeleteOpts{Cascade: true}).ExtractErr()
		if err != nil {
			return errors.Wrapf(err, "cascade-deleting orphan LB %s", lb.ID)
		}
	}

	sg, err := findSGByName(ctx, c, ClusterSGName(cluster))
	if err != nil {
		return errors.Wrap(err, "looking up cluster SG for sweep")
	}

	if sg != nil {
		// Best-effort: if the SG is still pinned by a surviving port
		// (worker that hasn't yet been torn down by CAPI, or one
		// whose port-update to remove the SG is still in flight)
		// Neutron returns 409 Conflict. We deliberately do NOT
		// propagate that error — the next sweep cycle picks the SG
		// up once references clear. Any other error is unexpected
		// and worth surfacing.
		err := groups.Delete(ctx, c.Network, sg.ID).ExtractErr()
		if err != nil && !isConflictError(err) {
			return errors.Wrapf(err, "deleting cluster SG %s", sg.ID)
		}
	}

	return nil
}

// isConflictError reports whether err is a 409 Conflict from
// OpenStack — gophercloud surfaces these via the v2 ErrUnexpectedResponseCode
// helper rather than a typed error, so we string-match the status.
// gophercloud v2 renders the message as "... but got 409 instead";
// the v1 form was "Bad response code: 409". Match both so the
// detector survives a future v1→v2 message tweak too.
func isConflictError(err error) bool {
	if err == nil {
		return false
	}

	msg := err.Error()

	return strings.Contains(msg, "got 409 instead") ||
		strings.Contains(msg, "Bad response code: 409") ||
		strings.Contains(msg, "SecurityGroupInUse")
}

// SweepOrphans walks every OpenStack resource that carries the
// controller's deterministic prefix and deletes any whose owning
// cluster (KubernetesSwitchcloud CR) is no longer in `knownClusters`,
// or — when serviceExists is provided — whose owning Service is no
// longer present in a known tenant cluster. The latter catches a
// real production trap: when an operator deletes and recreates a
// tenant with the same name (mesh1 → delete → mesh1 again), every
// OpenStack resource the previous incarnation left behind still
// matches the cluster-name prefix, so the cluster-orphan loop alone
// would keep it forever.
//
// knownClusters is the set of cluster names the controller currently
// has live Sessions for. Resources whose cluster is not in this set
// are unconditional orphans. Resources whose cluster IS in the set
// are subject to the per-Service drift check via serviceExists.
//
// serviceExists may be nil, in which case the per-Service drift
// check is skipped entirely (preserves the prior behaviour for
// callers that have no easy way to dial tenant apiservers — tests,
// the cluster-finalizer fast path).
func SweepOrphans(ctx context.Context, c *Clients, knownClusters map[string]struct{}, serviceExists ServiceExistsFunc) error {
	pager := loadbalancers.List(c.LoadBalancer, loadbalancers.ListOpts{})

	var (
		orphanClusters    []string
		orphanServiceLBs  []loadbalancers.LoadBalancer
		serviceExistsSeen = map[string]bool{} // result cache, keyed by cluster/ns/name
	)

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := loadbalancers.ExtractLoadBalancers(page)
		if err != nil {
			return false, err
		}

		for _, lb := range list {
			if !strings.HasPrefix(lb.Name, managedNamePrefix) {
				continue
			}

			cluster, ok := parseClusterFromLBName(lb.Name)
			if !ok {
				continue
			}

			if _, alive := knownClusters[cluster]; !alive {
				orphanClusters = append(orphanClusters, cluster)

				continue
			}

			// Cluster is alive — check the per-Service drift.
			if serviceExists == nil {
				continue
			}

			lbCluster, ns, name, parsed := parseServiceFromTag(lb.Name)
			if !parsed {
				continue
			}

			cacheKey := lbCluster + "/" + ns + "/" + name
			if cached, ok := serviceExistsSeen[cacheKey]; ok {
				if !cached {
					orphanServiceLBs = append(orphanServiceLBs, lb)
				}

				continue
			}

			exists, err := serviceExists(ctx, lbCluster, ns, name)
			if err != nil {
				// Bias-towards-safe: do not delete on uncertainty.
				// Cache the inconclusive result as "exists" so we
				// do not flood the tenant apiserver on subsequent
				// LBs for the same Service in this pass.
				serviceExistsSeen[cacheKey] = true

				continue
			}

			serviceExistsSeen[cacheKey] = exists

			if !exists {
				orphanServiceLBs = append(orphanServiceLBs, lb)
			}
		}

		return true, nil
	})
	if err != nil {
		return errors.Wrap(err, "listing LBs for orphan sweep") //nolint:wrapcheck
	}

	// Delete per-Service-orphan LBs by ID. We bypass the higher-level
	// DeleteLB(svc) call because the tenant Service object is precisely
	// the thing that does NOT exist — synthesising a fake corev1.Service
	// just to thread the same call would be the wrong shape.
	for _, lb := range orphanServiceLBs {
		if err := loadbalancers.Delete(ctx, c.LoadBalancer, lb.ID, loadbalancers.DeleteOpts{Cascade: true}).ExtractErr(); err != nil {
			if !isConflictError(err) {
				return errors.Wrapf(err, "deleting per-Service orphan LB %s (%s)", lb.Name, lb.ID)
			}
		}
	}

	// Same per-Service drift check for FIPs: the FIP description
	// carries the same cozystack:<cluster>/<ns>/<svc> tag. A FIP whose
	// Service no longer exists is a stranded public IP — costs money,
	// confuses DNS, and is hard to spot without this sweep.
	if serviceExists != nil {
		if err := sweepOrphanFloatingIPs(ctx, c, knownClusters, serviceExists, serviceExistsSeen); err != nil {
			return errors.Wrap(err, "sweeping per-Service orphan FIPs")
		}
	}

	// Dedup orphans (one cluster can own many LBs) before sweeping.
	seen := make(map[string]struct{}, len(orphanClusters))
	for _, cl := range orphanClusters {
		if _, ok := seen[cl]; ok {
			continue
		}

		seen[cl] = struct{}{}

		if err := SweepClusterResources(ctx, c, cl); err != nil {
			return errors.Wrapf(err, "sweeping resources for orphan cluster %q", cl)
		}
	}

	// Also sweep any cozystack-lb-<cluster> SGs whose cluster is no
	// longer known — covers the case where the cluster CR was deleted
	// before any LB was provisioned, leaving only the SG behind.
	sgPager := groups.List(c.Network, groups.ListOpts{})

	err = sgPager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := groups.ExtractGroups(page)
		if err != nil {
			return false, err
		}

		for _, sg := range list {
			if !strings.HasPrefix(sg.Name, clusterSGNamePrefix) {
				continue
			}

			cluster := strings.TrimPrefix(sg.Name, clusterSGNamePrefix)
			if _, alive := knownClusters[cluster]; alive {
				continue
			}

			if _, alreadySwept := seen[cluster]; alreadySwept {
				continue
			}

			if err := groups.Delete(ctx, c.Network, sg.ID).ExtractErr(); err != nil {
				return false, errors.Wrapf(err, "deleting orphan SG %s", sg.ID)
			}
		}

		return true, nil
	})
	if err != nil {
		return errors.Wrap(err, "listing SGs for orphan sweep") //nolint:wrapcheck
	}

	return nil
}

// SweepOrphanNovaServers terminates every Nova server whose name
// starts with the KSC chart's release prefix
// (`kubernetes-switchcloud-<cluster>-...`) when <cluster> is not in
// knownClusters. Used as a safety net when CAPO's normal teardown
// (cluster delete -> OpenStackCluster delete -> Machine delete ->
// Nova server delete) failed to complete — e.g. when a previous
// release of this controller deleted the KubernetesSwitchcloud CR
// before its finalizer could fire and left workers behind.
//
// Matching is by exact cluster-name prefix
// (`kubernetes-switchcloud-<known>-`), so multi-token cluster names
// (`prod-east`, etc.) are handled correctly: any server whose name
// starts with that prefix is claimed by a known cluster and skipped.
func SweepOrphanNovaServers(ctx context.Context, c *Clients, knownClusters map[string]struct{}) error {
	if c.Compute == nil {
		// Nova endpoint absent (project catalog hides compute, or
		// build-time Nova auth failed). The sweep is a safety net,
		// so we treat the missing client as a soft no-op — callers
		// log the skip and the LB / SG reconcile paths remain
		// healthy.
		return nil
	}

	pager := servers.List(c.Compute, servers.ListOpts{})

	var orphans []servers.Server

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := servers.ExtractServers(page)
		if err != nil {
			return false, err
		}

		for _, s := range list {
			if !strings.HasPrefix(s.Name, kscServerNamePrefix) {
				continue
			}

			if isClaimedByKnown(s.Name, knownClusters) {
				continue
			}

			orphans = append(orphans, s)
		}

		return true, nil
	})
	if err != nil {
		return errors.Wrap(err, "listing Nova servers for orphan sweep") //nolint:wrapcheck
	}

	for _, s := range orphans {
		if err := servers.Delete(ctx, c.Compute, s.ID).ExtractErr(); err != nil {
			return errors.Wrapf(err, "deleting orphan Nova server %s (%s)", s.Name, s.ID)
		}
	}

	return nil
}

// isClaimedByKnown reports whether serverName looks like
// `kubernetes-switchcloud-<known>-...` for any `<known>` in
// knownClusters. The full prefix anchor avoids the mesh1-vs-mesh10
// false-positive a plain HasPrefix(serverName, prefix+known) would
// produce.
func isClaimedByKnown(serverName string, knownClusters map[string]struct{}) bool {
	for cluster := range knownClusters {
		// Defensive: an empty cluster name would collapse the anchor
		// to `kubernetes-switchcloud--`, which technically matches
		// nothing but is fragile against future name patterns.
		if cluster == "" {
			continue
		}

		if strings.HasPrefix(serverName, kscServerNamePrefix+cluster+"-") {
			return true
		}
	}

	return false
}

// sweepOrphanFloatingIPs walks every FIP whose description matches
// the controller-managed `cozystack:<cluster>/<ns>/<svc>` shape and
// deletes the ones whose Service is gone but whose cluster is still
// known. FIPs whose cluster is unknown are NOT touched here: those
// are picked up by SweepClusterResources via the cluster-orphan
// branch above (it walks LBs and deletes attached FIPs in one go),
// so handling them again here would double-delete and produce 404
// noise.
func sweepOrphanFloatingIPs(ctx context.Context, c *Clients, knownClusters map[string]struct{}, serviceExists ServiceExistsFunc, cache map[string]bool) error {
	pager := floatingips.List(c.Network, floatingips.ListOpts{})

	var orphans []floatingips.FloatingIP

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := floatingips.ExtractFloatingIPs(page)
		if err != nil {
			return false, err
		}

		for _, fip := range list {
			if !strings.HasPrefix(fip.Description, managedNamePrefix) {
				continue
			}

			cluster, ns, name, parsed := parseServiceFromTag(fip.Description)
			if !parsed {
				continue
			}

			if _, alive := knownClusters[cluster]; !alive {
				// Cluster-orphan path will get this one through
				// SweepClusterResources on the next outer-loop tick.
				continue
			}

			cacheKey := cluster + "/" + ns + "/" + name
			if cached, ok := cache[cacheKey]; ok {
				if !cached {
					orphans = append(orphans, fip)
				}

				continue
			}

			exists, err := serviceExists(ctx, cluster, ns, name)
			if err != nil {
				cache[cacheKey] = true

				continue
			}

			cache[cacheKey] = exists

			if !exists {
				orphans = append(orphans, fip)
			}
		}

		return true, nil
	})
	if err != nil {
		return errors.Wrap(err, "listing FIPs for per-Service orphan sweep") //nolint:wrapcheck
	}

	for _, fip := range orphans {
		if err := floatingips.Delete(ctx, c.Network, fip.ID).ExtractErr(); err != nil {
			if !isConflictError(err) {
				return errors.Wrapf(err, "deleting per-Service orphan FIP %s (%s)", fip.FloatingIP, fip.ID)
			}
		}
	}

	return nil
}

// parseClusterFromLBName extracts the <cluster> token from a name of
// shape `cozystack:<cluster>/<ns>/<svc>`. Returns ok=false on any
// other shape.
func parseClusterFromLBName(name string) (string, bool) {
	if !strings.HasPrefix(name, managedNamePrefix) {
		return "", false
	}

	rest := strings.TrimPrefix(name, managedNamePrefix)
	slash := strings.IndexByte(rest, '/')

	if slash <= 0 {
		return "", false
	}

	return rest[:slash], true
}

// parseServiceFromTag splits a tag of shape
// `cozystack:<cluster>/<namespace>/<service>` into its three
// components. Used by the orphan sweeper to attribute LBs and FIPs
// to a specific tenant Service so we can ask the tenant apiserver
// whether the Service still exists. Returns ok=false on any shape
// that does not match (including the longer NodePort-rule shape
// `cozystack:<cluster>/<ns>/<svc>:<port>/<proto>:<cidr>`, which is
// a SG-rule tag and is handled separately).
func parseServiceFromTag(tag string) (cluster, namespace, name string, ok bool) {
	if !strings.HasPrefix(tag, managedNamePrefix) {
		return "", "", "", false
	}

	rest := strings.TrimPrefix(tag, managedNamePrefix)

	parts := strings.SplitN(rest, "/", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}

	// Service name must not contain the ':<port>...' rule suffix.
	// Sweeper passes per-Service drift checking against LB names
	// (clean shape) and FIP descriptions (clean shape); the rule
	// tag is parsed by a separate helper.
	if strings.ContainsRune(parts[2], ':') {
		return "", "", "", false
	}

	return parts[0], parts[1], parts[2], true
}
