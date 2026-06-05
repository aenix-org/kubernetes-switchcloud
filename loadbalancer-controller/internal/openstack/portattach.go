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

// ListWorkerIPsOnNetwork returns the fixed-IP addresses of every
// Nova-owned port (`device_owner=compute:nova`) on the given network
// — i.e. the OpenStack-side IPs of the workers attached to the
// cluster's managed subnet.
//
// Used instead of Node.status.addresses[InternalIP] for building
// Octavia pool members: kubelet may report a CNI/overlay address
// (e.g. Kilo's WireGuard mesh IP) as InternalIP, which is not
// reachable through OVN. The Neutron port is the authoritative
// source for the IP the LB actually has a path to.
func ListWorkerIPsOnNetwork(ctx context.Context, c *Clients, workerNetworkID string) ([]string, error) {
	if workerNetworkID == "" {
		return nil, errors.New("workerNetworkID is empty")
	}

	pager := ports.List(c.Network, ports.ListOpts{
		NetworkID:   workerNetworkID,
		DeviceOwner: "compute:nova",
	})

	var ips []string

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := ports.ExtractPorts(page)
		if err != nil {
			return false, err
		}

		for i := range list {
			for _, fip := range list[i].FixedIPs {
				if fip.IPAddress == "" {
					continue
				}

				ips = append(ips, fip.IPAddress)
			}
		}

		return true, nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	return ips, nil
}

func hasSG(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}

	return false
}

// DetachDefaultSGFromWorkers strips the project's "default" SG from
// each worker port so the implicit allow-from-same-SG rule on the
// shared default SG no longer carries traffic between workers of
// different clusters (they all sit in the same project, and would
// otherwise see each other through that one rule).
//
// Safety rails: the caller must have already attached the
// controller-managed cluster SG via EnsureSGAttachedToWorkers before
// calling this — we refuse to leave a port without any security group.
// The "default" name is the OpenStack convention for the project-scoped
// default group; the lookup is project-scoped so we never touch a
// neighbour project's SG.
//
// Side effect for operators: any rule they added to the default SG
// (SSH from a jump host, monitoring scrape ranges, etc.) no longer
// applies to worker ports. To preserve those, mirror them into the
// cluster-managed SG via spec.openstack.loadBalancer.allowedCIDRs +
// matching Services, or pin workerSecurityGroupID to an operator-owned
// SG instead (the controller will not touch port SG attachments in
// override mode).
func DetachDefaultSGFromWorkers(
	ctx context.Context,
	c *Clients,
	memberIPs []string,
	workerNetworkID string,
) error {
	if workerNetworkID == "" || len(memberIPs) == 0 {
		return nil
	}

	defaultSG, err := findSGByName(ctx, c, "default")
	if err != nil {
		return errors.Wrap(err, "looking up project default SG")
	}

	if defaultSG == nil {
		return nil
	}

	for _, ip := range memberIPs {
		port, err := findPortByIP(ctx, c, workerNetworkID, ip)
		if err != nil {
			return errors.Wrapf(err, "looking up port for member %s", ip)
		}

		if port == nil {
			continue
		}

		if !hasSG(port.SecurityGroups, defaultSG.ID) {
			continue
		}

		newSGs := make([]string, 0, len(port.SecurityGroups))

		for _, sg := range port.SecurityGroups {
			if sg != defaultSG.ID {
				newSGs = append(newSGs, sg)
			}
		}

		if len(newSGs) == 0 {
			return errors.Newf("refusing to detach default SG from port %s: it would leave the port with no SG at all (cluster SG attach must run first)", port.ID)
		}

		_, err = ports.Update(ctx, c.Network, port.ID, ports.UpdateOpts{
			SecurityGroups: &newSGs,
		}).Extract()
		if err != nil {
			return errors.Wrapf(err, "detaching default SG from worker port %s", port.ID)
		}
	}

	return nil
}
