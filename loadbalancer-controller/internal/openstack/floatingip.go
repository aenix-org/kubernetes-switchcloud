/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

package openstack

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/layer3/floatingips"
	"github.com/gophercloud/gophercloud/v2/pagination"
)

// EnsureFloatingIP makes sure a Floating IP from floatingNetworkID is
// allocated and bound to vipPortID. Returns the FIP address. Idempotent:
// if the port already has a FIP attached we return its existing address
// instead of allocating a second one.
//
// The FIP is described with the LB's name (cozystack:<tenant>/<ns>/<svc>)
// so it can be discovered later for delete even if the LB is gone.
func EnsureFloatingIP(
	ctx context.Context,
	c *Clients,
	vipPortID string,
	floatingNetworkID string,
	floatingSubnetID string,
	description string,
) (address string, err error) {
	if vipPortID == "" {
		return "", errors.New("vipPortID is empty")
	}

	if floatingNetworkID == "" {
		return "", errors.New("floatingNetworkID is empty")
	}

	existing, err := findFIPByPortID(ctx, c, vipPortID)
	if err != nil {
		return "", errors.Wrap(err, "looking up existing FIP for VIP port")
	}

	if existing != nil {
		return existing.FloatingIP, nil
	}

	created, err := floatingips.Create(ctx, c.Network, floatingips.CreateOpts{
		Description:       description,
		FloatingNetworkID: floatingNetworkID,
		SubnetID:          floatingSubnetID,
		PortID:            vipPortID,
	}).Extract()
	if err != nil {
		return "", errors.Wrap(err, "creating floating IP")
	}

	return created.FloatingIP, nil
}

// DeleteFloatingIP removes the FIP bound to vipPortID, if any. Safe to
// call when no FIP exists (returns nil).
func DeleteFloatingIP(ctx context.Context, c *Clients, vipPortID string) error {
	if vipPortID == "" {
		return nil
	}

	existing, err := findFIPByPortID(ctx, c, vipPortID)
	if err != nil {
		return errors.Wrap(err, "looking up FIP for delete")
	}

	if existing == nil {
		return nil
	}

	if err := floatingips.Delete(ctx, c.Network, existing.ID).ExtractErr(); err != nil {
		return errors.Wrapf(err, "deleting floating IP %s", existing.ID)
	}

	return nil
}

// findFIPByPortID returns the FIP currently bound to portID, if any.
// Uses server-side filter on port_id so the result set is at most one.
func findFIPByPortID(ctx context.Context, c *Clients, portID string) (*floatingips.FloatingIP, error) {
	pager := floatingips.List(c.Network, floatingips.ListOpts{PortID: portID})

	var found *floatingips.FloatingIP

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := floatingips.ExtractFloatingIPs(page)
		if err != nil {
			return false, err
		}

		if len(list) > 0 {
			fip := list[0]
			found = &fip

			return false, nil
		}

		return true, nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	return found, nil
}
