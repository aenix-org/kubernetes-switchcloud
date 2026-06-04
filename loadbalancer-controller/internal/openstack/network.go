/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

package openstack

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/subnets"
	"github.com/gophercloud/gophercloud/v2/pagination"
)

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
