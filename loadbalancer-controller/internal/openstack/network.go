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

// ResolveVIPSubnet returns the Neutron subnet ID where the controller
// should ask Octavia to allocate the LB VIP. The override path (caller
// passes a non-empty explicit subnet ID) short-circuits; otherwise the
// function auto-picks the first IPv4 subnet of the first
// router:external=true network in the tenant project.
//
// The auto-pick logic matches the Switch Cloud zhw layout (one external
// network "public" with dual-stack subnets), but is generic: any
// OpenStack project with at least one external network and at least
// one IPv4 subnet on it will resolve deterministically. Operators with
// multiple external networks or non-trivial layouts must set the
// override on KubernetesSwitchcloud.spec.openstack.loadBalancer.vipSubnetID.
func ResolveVIPSubnet(ctx context.Context, c *Clients, overrideID string) (string, error) {
	if overrideID != "" {
		return overrideID, nil
	}

	external, err := firstExternalNetwork(ctx, c)
	if err != nil {
		return "", errors.Wrap(err, "discovering external network")
	}

	ipv4SubnetID, err := firstIPv4Subnet(ctx, c, external.ID)
	if err != nil {
		return "", errors.Wrapf(err, "discovering IPv4 subnet of network %q", external.Name)
	}

	return ipv4SubnetID, nil
}

// externalNetwork is the trimmed subset of fields we need from a
// networks.Network — the full struct has dozens of fields, most of
// which gophercloud's default extract handles fine but the
// router:external attribute requires the external extension.
type externalNetwork struct {
	ID   string
	Name string
}

func firstExternalNetwork(ctx context.Context, c *Clients) (externalNetwork, error) {
	pager := networks.List(c.Network, networks.ListOpts{})

	var found externalNetwork

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		var list []struct {
			ID              string `json:"id"`
			Name            string `json:"name"`
			RouterExternal  bool   `json:"router:external"`
		}

		err := networks.ExtractNetworksInto(page, &list)
		if err != nil {
			return false, errors.Wrap(err, "extracting networks page")
		}

		for _, n := range list {
			if n.RouterExternal {
				found = externalNetwork{ID: n.ID, Name: n.Name}

				return false, nil // stop pagination, we have one
			}
		}

		return true, nil
	})
	if err != nil {
		return externalNetwork{}, err
	}

	if found.ID == "" {
		return externalNetwork{}, errors.New("no router:external=true network found in tenant project")
	}

	return found, nil
}

func firstIPv4Subnet(ctx context.Context, c *Clients, networkID string) (string, error) {
	pager := subnets.List(c.Network, subnets.ListOpts{NetworkID: networkID})

	var found string

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := subnets.ExtractSubnets(page)
		if err != nil {
			return false, errors.Wrap(err, "extracting subnets page")
		}

		for _, s := range list {
			// Subnets list returns IPVersion as int; 4 == IPv4, 6 == IPv6.
			// CIDR is also distinguishable by ":" presence, kept as
			// belt-and-braces for OpenStack clouds that may not set
			// IPVersion correctly.
			if s.IPVersion == 4 || !strings.Contains(s.CIDR, ":") {
				found = s.ID

				return false, nil
			}
		}

		return true, nil
	})
	if err != nil {
		return "", err
	}

	if found == "" {
		return "", errors.Newf("no IPv4 subnet on external network %q", networkID)
	}

	return found, nil
}
