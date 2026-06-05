/*
Copyright 2026 The Aenix Authors.
*/

package openstack

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/ports"
	"github.com/gophercloud/gophercloud/v2/pagination"
)

// EnsureSGAttachedToWorkers ADDs sgID to every Neutron port that
// carries one of memberIPs as a fixed IP on workerNetworkID. Existing
// SGs on the port are preserved — this is additive, never destructive.
// Idempotent: ports already carrying sgID are skipped.
//
// CAPI-OpenStack owns these ports too (it created them when standing
// up the worker VMs). In practice it does not reconcile SG drift
// against the spec on every loop, so the extra SG we add survives;
// if a future CAPI release starts enforcing, the operator falls back
// to wiring our SG name into nodeGroups.securityGroups so CAPI itself
// keeps it on the port.
func EnsureSGAttachedToWorkers(
	ctx context.Context,
	c *Clients,
	sgID string,
	memberIPs []string,
	workerNetworkID string,
) error {
	if sgID == "" || workerNetworkID == "" || len(memberIPs) == 0 {
		return nil
	}

	for _, ip := range memberIPs {
		port, err := findPortByIP(ctx, c, workerNetworkID, ip)
		if err != nil {
			return errors.Wrapf(err, "looking up port for member %s", ip)
		}

		if port == nil {
			// Worker is present in Kubernetes (Ready Node) but its
			// Neutron port isn't visible — could be a CAPI race
			// window during create, or a non-OpenStack node. Skip;
			// the next reconcile will retry.
			continue
		}

		if hasSG(port.SecurityGroups, sgID) {
			continue
		}

		newSGs := append([]string{}, port.SecurityGroups...)
		newSGs = append(newSGs, sgID)

		_, err = ports.Update(ctx, c.Network, port.ID, ports.UpdateOpts{
			SecurityGroups: &newSGs,
		}).Extract()
		if err != nil {
			return errors.Wrapf(err, "attaching SG %s to worker port %s", sgID, port.ID)
		}
	}

	return nil
}

func findPortByIP(ctx context.Context, c *Clients, networkID, ip string) (*ports.Port, error) {
	pager := ports.List(c.Network, ports.ListOpts{
		NetworkID: networkID,
		FixedIPs:  []ports.FixedIPOpts{{IPAddress: ip}},
	})

	var found *ports.Port

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := ports.ExtractPorts(page)
		if err != nil {
			return false, err
		}

		for i := range list {
			p := list[i]
			found = &p

			return false, nil
		}

		return true, nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	return found, nil
}

func hasSG(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}

	return false
}
