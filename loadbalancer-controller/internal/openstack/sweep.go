/*
Copyright 2026 The Aenix Authors.
*/

package openstack

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/loadbalancers"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/security/groups"
	"github.com/gophercloud/gophercloud/v2/pagination"
)

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
		// (e.g. worker that hasn't been torn down by CAPI yet)
		// Neutron returns 409 Conflict — fine, the next sweep cycle
		// catches it once the port is gone.
		if err := groups.Delete(ctx, c.Network, sg.ID).ExtractErr(); err != nil {
			return errors.Wrapf(err, "deleting cluster SG %s", sg.ID)
		}
	}

	return nil
}

// SweepOrphans walks every OpenStack resource that carries the
// controller's deterministic prefix and deletes any whose owning
// cluster (KubernetesSwitchcloud CR) is no longer in `knownClusters`.
// Catches the "controller was off when Service/CR was deleted" and
// "operator removed something out-of-band" cases — strictly safety
// net, the per-reconcile cleanup path is the primary cleanup.
//
// knownClusters is the set of cluster names the controller has seen
// in the management cluster on its most recent registry build.
// Resources whose cluster is not in this set are considered orphans.
func SweepOrphans(ctx context.Context, c *Clients, knownClusters map[string]struct{}) error {
	pager := loadbalancers.List(c.LoadBalancer, loadbalancers.ListOpts{})

	var orphanClusters []string

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

			if _, alive := knownClusters[cluster]; alive {
				continue
			}

			orphanClusters = append(orphanClusters, cluster)
		}

		return true, nil
	})
	if err != nil {
		return errors.Wrap(err, "listing LBs for orphan sweep") //nolint:wrapcheck
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
