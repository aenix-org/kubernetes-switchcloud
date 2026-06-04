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

// VIPTarget tells Octavia where to allocate the VIP. Either SubnetID
// or NetworkID is set, never both. When the operator provides an
// explicit subnet override on the KubernetesSwitchcloud CR we use
// SubnetID; otherwise we fall back to NetworkID and let Octavia pick
// the subnet itself.
//
// The fallback exists because in Switch Cloud zhw (and many other
// public OpenStack deployments) the external network "public" is
// admin-owned: tenants can see the network but the
// `subnets.list?network_id=...` call returns empty for them. Passing
// NetworkID side-steps that visibility hole — Octavia's API runs
// elevated and can resolve the subnet on the tenant's behalf.
type VIPTarget struct {
	SubnetID  string
	NetworkID string
}

// ResolveVIPTarget chooses the VIP allocation target. If the caller
// passes an explicit subnet ID (from KubernetesSwitchcloud.spec.openstack.loadBalancer.vipSubnetID)
// we honour it; otherwise we auto-pick the first router:external=true
// network in the tenant project and return its ID for Octavia to
// resolve. Operators with multiple external networks must set the
// override.
func ResolveVIPTarget(ctx context.Context, c *Clients, overrideSubnetID string) (VIPTarget, error) {
	if overrideSubnetID != "" {
		return VIPTarget{SubnetID: overrideSubnetID}, nil
	}

	external, err := firstExternalNetwork(ctx, c)
	if err != nil {
		return VIPTarget{}, errors.Wrap(err, "discovering external network")
	}

	return VIPTarget{NetworkID: external.ID}, nil
}

// externalNetwork is the trimmed subset of fields we need from a
// networks.Network — the router:external attribute lives in the
// external extension, so we extract it via ExtractNetworksInto.
type externalNetwork struct {
	ID   string
	Name string
}

// ResolveMemberSubnet returns the IPv4 subnet ID of the worker-node
// network so Octavia knows which subnet to dial pool members on. The
// tenant owns this network, so subnets.list with NetworkID filter
// works (unlike the admin-owned external network used for the VIP).
func ResolveMemberSubnet(ctx context.Context, c *Clients, workerNetworkID string) (string, error) {
	if workerNetworkID == "" {
		return "", errors.New("worker network ID is empty (spec.openstack.network.id must be set on KubernetesSwitchcloud CR)")
	}

	pager := subnets.List(c.Network, subnets.ListOpts{NetworkID: workerNetworkID})

	var found string

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := subnets.ExtractSubnets(page)
		if err != nil {
			return false, errors.Wrap(err, "extracting worker subnets page")
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
		return "", err
	}

	if found == "" {
		return "", errors.Newf("no IPv4 subnet on worker network %q", workerNetworkID)
	}

	return found, nil
}

func firstExternalNetwork(ctx context.Context, c *Clients) (externalNetwork, error) {
	pager := networks.List(c.Network, networks.ListOpts{})

	var found externalNetwork

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		var list []struct {
			ID             string `json:"id"`
			Name           string `json:"name"`
			RouterExternal bool   `json:"router:external"`
		}

		err := networks.ExtractNetworksInto(page, &list)
		if err != nil {
			return false, errors.Wrap(err, "extracting networks page")
		}

		for _, n := range list {
			if n.RouterExternal {
				found = externalNetwork{ID: n.ID, Name: n.Name}

				return false, nil
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
