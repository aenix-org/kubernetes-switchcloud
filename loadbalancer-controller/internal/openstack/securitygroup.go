/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

package openstack

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/security/groups"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/security/rules"
	"github.com/gophercloud/gophercloud/v2/pagination"
	corev1 "k8s.io/api/core/v1"
)

// clusterSGNamePrefix is the deterministic prefix used for the
// controller-managed security group when an operator hasn't provided
// an override via spec.openstack.loadBalancer.workerSecurityGroupID.
// Combined with the cluster (KSC CR) name it gives one SG per cluster
// — distinct SGs across clusters mean no implicit inter-cluster
// allow-from-same-SG, which is the basis of L4 isolation between
// otherwise co-tenant clusters.
const clusterSGNamePrefix = "cozystack-lb-"

// ClusterSGName is the deterministic security group name for the
// given cluster. Used both when ensuring the SG exists and when
// looking it up on subsequent reconciles.
func ClusterSGName(cluster string) string {
	return clusterSGNamePrefix + cluster
}

// EnsureClusterSecurityGroup makes sure a per-cluster security group
// exists and carries the baseline rules:
//
//   - ingress IPv4/IPv6 from the same SG (intra-cluster pod-to-pod
//     and workload-to-workload traffic, mirroring the implicit rule
//     OpenStack adds to the default SG)
//   - the default egress-all rules that OpenStack adds on create.
//
// Per-Service NodePort ingress rules are added separately by
// EnsureNodePortRules. Returns the SG ID for the caller to feed into
// the rest of the pipeline.
func EnsureClusterSecurityGroup(ctx context.Context, c *Clients, cluster string) (string, error) {
	name := ClusterSGName(cluster)

	existing, err := findSGByName(ctx, c, name)
	if err != nil {
		return "", errors.Wrap(err, "looking up cluster SG")
	}

	if existing != nil {
		if err := ensureBaselineRules(ctx, c, existing.ID); err != nil {
			return "", errors.Wrap(err, "ensuring baseline rules on existing SG")
		}

		return existing.ID, nil
	}

	created, err := groups.Create(ctx, c.Network, groups.CreateOpts{
		Name:        name,
		Description: fmt.Sprintf("Managed by Cozystack loadbalancer-controller; cluster=%s", cluster),
	}).Extract()
	if err != nil {
		return "", errors.Wrap(err, "creating cluster SG")
	}

	if err := ensureBaselineRules(ctx, c, created.ID); err != nil {
		return "", errors.Wrap(err, "ensuring baseline rules on new SG")
	}

	return created.ID, nil
}

// LookupClusterSecurityGroup returns the controller-managed SG ID for
// the cluster if it exists, or "" if not. Used by cleanup paths that
// must not auto-create the SG.
func LookupClusterSecurityGroup(ctx context.Context, c *Clients, cluster string) (string, error) {
	sg, err := findSGByName(ctx, c, ClusterSGName(cluster))
	if err != nil {
		return "", errors.Wrap(err, "looking up cluster SG for cleanup")
	}

	if sg == nil {
		return "", nil
	}

	return sg.ID, nil
}

func findSGByName(ctx context.Context, c *Clients, name string) (*groups.SecGroup, error) {
	pager := groups.List(c.Network, groups.ListOpts{Name: name})

	var found *groups.SecGroup

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := groups.ExtractGroups(page)
		if err != nil {
			return false, err
		}

		for i := range list {
			if list[i].Name == name {
				sg := list[i]
				found = &sg

				return false, nil
			}
		}

		return true, nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	return found, nil
}

// ensureBaselineRules adds the intra-SG ingress rules (IPv4 + IPv6)
// idempotently. OpenStack's default-on-create rules cover egress but
// not ingress; without these the controller-managed SG would block
// every same-cluster packet that would otherwise have flown via the
// default SG's allow-from-same-SG behaviour.
func ensureBaselineRules(ctx context.Context, c *Clients, sgID string) error {
	desired := []rules.CreateOpts{
		{
			SecGroupID:    sgID,
			Direction:     rules.DirIngress,
			EtherType:     rules.EtherType4,
			RemoteGroupID: sgID,
			Description:   "cozystack-lb baseline: intra-SG IPv4",
		},
		{
			SecGroupID:    sgID,
			Direction:     rules.DirIngress,
			EtherType:     rules.EtherType6,
			RemoteGroupID: sgID,
			Description:   "cozystack-lb baseline: intra-SG IPv6",
		},
	}

	pager := rules.List(c.Network, rules.ListOpts{SecGroupID: sgID})

	have := make(map[string]bool)

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := rules.ExtractRules(page)
		if err != nil {
			return false, err
		}

		for _, r := range list {
			if r.Direction == "ingress" && r.RemoteGroupID == sgID && r.Protocol == "" {
				have[string(r.EtherType)] = true
			}
		}

		return true, nil
	})
	if err != nil {
		return errors.Wrap(err, "listing baseline rules") //nolint:wrapcheck
	}

	for _, opts := range desired {
		if have[string(opts.EtherType)] {
			continue
		}

		if _, err := rules.Create(ctx, c.Network, opts).Extract(); err != nil {
			return errors.Wrapf(err, "creating baseline %s rule", opts.EtherType)
		}
	}

	return nil
}

// EnsureNodePortRules makes sure the security group identified by
// workerSecurityGroupID has ingress rules permitting every Service
// port (NodePort) from every CIDR in allowedCIDRs. Existing rules
// owned by this Service (matched via description) that no longer
// belong are removed; missing rules are created. Rules from other
// Services in the same SG are left alone.
//
// description format encodes ownership and is stable across
// reconciles: "cozystack:<tenant>/<ns>/<svc>:<port>/<proto>:<cidr>"
func EnsureNodePortRules(
	ctx context.Context,
	c *Clients,
	workerSecurityGroupID string,
	tenant string,
	svc *corev1.Service,
	allowedCIDRs []string,
) error {
	if workerSecurityGroupID == "" {
		return errors.New("workerSecurityGroupID is empty")
	}

	if len(allowedCIDRs) == 0 {
		return errors.New("allowedCIDRs is empty")
	}

	existing, err := listOwnedRules(ctx, c, workerSecurityGroupID, tenant, svc)
	if err != nil {
		return errors.Wrap(err, "listing existing SG rules for Service")
	}

	desired := buildDesiredRules(tenant, svc, allowedCIDRs)

	// Drop rules we own but no longer want.
	for key, r := range existing {
		if _, ok := desired[key]; ok {
			continue
		}

		if err := rules.Delete(ctx, c.Network, r.ID).ExtractErr(); err != nil {
			return errors.Wrapf(err, "deleting stale SG rule %s", r.ID)
		}
	}

	// Create rules we want but don't yet have.
	for key, opts := range desired {
		if _, ok := existing[key]; ok {
			continue
		}

		opts.SecGroupID = workerSecurityGroupID

		if _, err := rules.Create(ctx, c.Network, opts).Extract(); err != nil {
			return errors.Wrapf(err, "creating SG rule %s", key)
		}
	}

	return nil
}

// DeleteNodePortRules removes every ingress rule owned by this
// Service from the given SG. Safe to call when no rules exist
// (returns nil).
func DeleteNodePortRules(
	ctx context.Context,
	c *Clients,
	workerSecurityGroupID string,
	tenant string,
	svc *corev1.Service,
) error {
	if workerSecurityGroupID == "" {
		return nil
	}

	existing, err := listOwnedRules(ctx, c, workerSecurityGroupID, tenant, svc)
	if err != nil {
		return errors.Wrap(err, "listing SG rules for delete")
	}

	for _, r := range existing {
		if err := rules.Delete(ctx, c.Network, r.ID).ExtractErr(); err != nil {
			return errors.Wrapf(err, "deleting SG rule %s", r.ID)
		}
	}

	return nil
}

// ruleKey is the (port, proto, cidr) tuple we use to diff existing
// rules against desired ones. Description carries the same info
// embedded for ownership lookup on the server side.
type ruleKey struct {
	port  int
	proto string
	cidr  string
}

func (k ruleKey) String() string {
	return fmt.Sprintf("%d/%s:%s", k.port, k.proto, k.cidr)
}

func ruleDescription(tenant string, svc *corev1.Service, k ruleKey) string {
	return fmt.Sprintf("cozystack:%s/%s/%s:%s", tenant, svc.Namespace, svc.Name, k)
}

func ruleDescriptionPrefix(tenant string, svc *corev1.Service) string {
	return fmt.Sprintf("cozystack:%s/%s/%s:", tenant, svc.Namespace, svc.Name)
}

// buildDesiredRules expands the Service ports × allowedCIDRs grid
// into the rule-key set we want present.
func buildDesiredRules(tenant string, svc *corev1.Service, allowedCIDRs []string) map[ruleKey]rules.CreateOpts {
	out := make(map[ruleKey]rules.CreateOpts, len(svc.Spec.Ports)*len(allowedCIDRs))

	for _, p := range svc.Spec.Ports {
		if p.NodePort == 0 {
			continue
		}

		proto := strings.ToLower(string(p.Protocol))

		for _, cidr := range allowedCIDRs {
			k := ruleKey{
				port:  int(p.NodePort),
				proto: proto,
				cidr:  cidr,
			}

			out[k] = rules.CreateOpts{
				Direction:      rules.DirIngress,
				EtherType:      etherTypeFor(cidr),
				PortRangeMin:   int(p.NodePort),
				PortRangeMax:   int(p.NodePort),
				Protocol:       rules.RuleProtocol(proto),
				RemoteIPPrefix: cidr,
				Description:    ruleDescription(tenant, svc, k),
			}
		}
	}

	return out
}

// listOwnedRules pulls every rule from the SG and filters server-side
// (by SG ID) plus client-side (by description prefix) to a map keyed
// by the (port, proto, cidr) tuple this controller manages.
func listOwnedRules(
	ctx context.Context,
	c *Clients,
	sgID string,
	tenant string,
	svc *corev1.Service,
) (map[ruleKey]rules.SecGroupRule, error) {
	pager := rules.List(c.Network, rules.ListOpts{SecGroupID: sgID})

	prefix := ruleDescriptionPrefix(tenant, svc)
	out := make(map[ruleKey]rules.SecGroupRule)

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := rules.ExtractRules(page)
		if err != nil {
			return false, err
		}

		for _, r := range list {
			if !strings.HasPrefix(r.Description, prefix) {
				continue
			}

			if r.Direction != "ingress" {
				continue
			}

			if r.PortRangeMin == 0 || r.PortRangeMin != r.PortRangeMax {
				continue
			}

			k := ruleKey{
				port:  r.PortRangeMin,
				proto: r.Protocol,
				cidr:  r.RemoteIPPrefix,
			}
			out[k] = r
		}

		return true, nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	return out, nil
}

// etherTypeFor returns the EtherType matching the CIDR family —
// gophercloud requires it explicit, and IPv4/IPv6 must match the
// remote prefix or Neutron returns 400.
func etherTypeFor(cidr string) rules.RuleEtherType {
	if strings.Contains(cidr, ":") {
		return rules.EtherType6
	}

	return rules.EtherType4
}
