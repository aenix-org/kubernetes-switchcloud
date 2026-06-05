/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

// Package openstack wraps gophercloud v2 with the subset of OpenStack
// behaviour the LoadBalancer controller needs: per-tenant authenticated
// clients, external-network/subnet discovery, and Octavia LB CRUD.
//
// Authentication is always v3 application-credential, the only auth
// mode Switch Cloud exposes to tenant projects. The Credentials struct
// is the minimal flat shape — either inlined in KubernetesSwitchcloud
// CR or extracted from an existingSecret's clouds.yaml — both
// resolution paths converge here.
package openstack

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/gophercloud/gophercloud/v2"
	osclient "github.com/gophercloud/gophercloud/v2/openstack"
)

// Credentials is the resolved auth bundle for a single tenant project.
type Credentials struct {
	AuthURL                     string
	RegionName                  string
	ApplicationCredentialID     string
	ApplicationCredentialSecret string
}

// Clients groups the per-tenant service clients the LB reconciler uses.
type Clients struct {
	Provider     *gophercloud.ProviderClient
	LoadBalancer *gophercloud.ServiceClient
	Network      *gophercloud.ServiceClient
	Compute      *gophercloud.ServiceClient
	Region       string
}

// NewClients authenticates against the tenant's project using
// v3applicationcredential and returns ready-to-use Octavia + Neutron
// clients. Caller is expected to cache the returned *Clients per
// tenant; gophercloud refreshes tokens automatically through the
// ProviderClient, so the cached value stays valid until creds rotate.
func NewClients(ctx context.Context, creds Credentials) (*Clients, error) {
	if creds.AuthURL == "" {
		return nil, errors.New("openstack: AuthURL is required")
	}

	if creds.ApplicationCredentialID == "" || creds.ApplicationCredentialSecret == "" {
		return nil, errors.New("openstack: application credential id+secret are required")
	}

	if creds.RegionName == "" {
		return nil, errors.New("openstack: RegionName is required (matches KubernetesSwitchcloud.spec.openstack.regionName)")
	}

	provider, err := osclient.AuthenticatedClient(ctx, gophercloud.AuthOptions{
		IdentityEndpoint:            creds.AuthURL,
		ApplicationCredentialID:     creds.ApplicationCredentialID,
		ApplicationCredentialSecret: creds.ApplicationCredentialSecret,
		// AllowReauth so tokens refresh automatically on 401 without
		// the caller having to re-authenticate manually.
		AllowReauth: true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "authenticating against OpenStack")
	}

	eo := gophercloud.EndpointOpts{Region: creds.RegionName}

	lbv2, err := osclient.NewLoadBalancerV2(provider, eo)
	if err != nil {
		return nil, errors.Wrap(err, "building Octavia LBv2 client")
	}

	netv2, err := osclient.NewNetworkV2(provider, eo)
	if err != nil {
		return nil, errors.Wrap(err, "building Neutron v2 client")
	}

	// Nova client failure is non-fatal: the controller's primary
	// duties (LB / SG reconcile) only need Octavia + Neutron. Compute
	// is consumed solely by the orphan-Nova-server sweep, which is
	// already a best-effort safety net. If a project's service
	// catalog doesn't expose Nova (locked-down tenancy, transient
	// outage in keystone), we leave Compute=nil and have callers
	// no-op rather than break the LB path for every Service.
	computev2, computeErr := osclient.NewComputeV2(provider, eo)

	out := &Clients{
		Provider:     provider,
		LoadBalancer: lbv2,
		Network:      netv2,
		Region:       creds.RegionName,
	}

	if computeErr == nil {
		out.Compute = computev2
	}

	return out, nil
}
