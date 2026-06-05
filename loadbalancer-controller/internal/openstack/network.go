/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

package openstack

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/networks"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/subnets"
	"github.com/gophercloud/gophercloud/v2/pagination"
)

// FindFirstExternalNetwork returns the Neutron network ID of the
// first router:external=true network visible to the tenant project.
// Used as the auto-discovery fallback for spec.openstack.loadBalancer.floatingNetworkID:
// when the operator leaves the field empty, the controller picks
// the project's single external network (matches the Switch Cloud
// zhw `public` topology — exactly one external network per project).
// Returns "" without error if no external network is visible, so the
// caller can treat that as an internal-only LB configuration.
func FindFirstExternalNetwork(ctx context.Context, c *Clients) (string, error) {
	pager := networks.List(c.Network, networks.ListOpts{})

	var found string

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		var list []struct {
			ID             string `json:"id"`
			RouterExternal bool   `json:"router:external"`
		}

		if err := networks.ExtractNetworksInto(page, &list); err != nil {
			return false, errors.Wrap(err, "extracting networks page")
		}

		for _, n := range list {
			if n.RouterExternal {
				found = n.ID

				return false, nil
			}
		}

		return true, nil
	})
	if err != nil {
		return "", err //nolint:wrapcheck
	}

	return found, nil
}

// ResolveMemberSubnet returns the IPv4 subnet ID of the tenant network
// so Octavia knows which subnet to dial pool members on. The tenant
// owns this network, so subnets.list with NetworkID filter works
// reliably (admin-owned external networks would not — their subnets
// are hidden from the tenant project).
func ResolveMemberSubnet(ctx context.Context, c *Clients, tenantNetworkID string) (string, error) {
	if tenantNetworkID == "" {
		return "", errors.New("tenant network ID is empty")
	}

	pager := subnets.List(c.Network, subnets.ListOpts{NetworkID: tenantNetworkID})

	var found string

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := subnets.ExtractSubnets(page)
		if err != nil {
			return false, errors.Wrap(err, "extracting subnets page")
		}

		for _, s := range list {
			if s.IPVersion == 4 || !strings.Contains(s.CIDR, ":") {
				found = s.ID

				return false, nil
			}
		}

		return true, nil
	})
	if err != nil {
		return "", err //nolint:wrapcheck
	}

	if found == "" {
		return "", errors.Newf("no IPv4 subnet on network %q", tenantNetworkID)
	}

	return found, nil
}
