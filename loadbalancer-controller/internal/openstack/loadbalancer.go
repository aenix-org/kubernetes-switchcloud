/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

package openstack

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/loadbalancers"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/gophercloud/gophercloud/v2/pagination"
	corev1 "k8s.io/api/core/v1"
)

// nameLabel is the Octavia LB `name` we set so that we can find the LB
// later without relying on a separate tag store. Format:
//
//	cozystack:<tenant>/<namespace>/<service>
//
// Plenty of room — Octavia name allows 255 chars and we encode three
// short Kubernetes identifiers. Resolution is unique because tenants
// can't share project namespaces (each KubernetesSwitchcloud lives in
// its own OpenStack project).
func ServiceLBName(tenant, namespace, name string) string {
	return fmt.Sprintf("cozystack:%s/%s/%s", tenant, namespace, name)
}

// IsManaged reports whether an Octavia LB was created by this
// controller, by looking at the `name` prefix. Used during delete /
// sweep to avoid stomping on LBs that an operator created by hand or
// another tool provisioned.
func IsManaged(lb *loadbalancers.LoadBalancer) bool {
	return strings.HasPrefix(lb.Name, "cozystack:")
}

// EnsureLB returns the LoadBalancer for the given Service (creating it
// if missing), driving it to provisioning_status=ACTIVE before
// returning. provider defaults to "ovn" when empty — the only working
// provider in Switch Cloud zhw.
func EnsureLB(
	ctx context.Context,
	c *Clients,
	tenant string,
	svc *corev1.Service,
	vipSubnetID string,
	provider string,
) (*loadbalancers.LoadBalancer, error) {
	name := ServiceLBName(tenant, svc.Namespace, svc.Name)

	existing, err := findLBByName(ctx, c, name)
	if err != nil {
		return nil, errors.Wrap(err, "looking up existing LB")
	}

	if existing != nil {
		if existing.ProvisioningStatus == "ACTIVE" {
			return existing, nil
		}

		settled, err := waitForLBActive(ctx, c, existing.ID)
		if err != nil {
			return nil, errors.Wrapf(err, "waiting for existing LB %s to settle", existing.ID)
		}

		return settled, nil
	}

	if provider == "" {
		provider = "ovn"
	}

	created, err := loadbalancers.Create(ctx, c.LoadBalancer, loadbalancers.CreateOpts{
		Name:        name,
		Description: fmt.Sprintf("Managed by Cozystack loadbalancer-controller; tenant=%s ns=%s svc=%s", tenant, svc.Namespace, svc.Name),
		Provider:    provider,
		VipSubnetID: vipSubnetID,
	}).Extract()
	if err != nil {
		return nil, errors.Wrap(err, "creating Octavia LB")
	}

	settled, err := waitForLBActive(ctx, c, created.ID)
	if err != nil {
		return nil, errors.Wrapf(err, "waiting for new LB %s to become ACTIVE", created.ID)
	}

	return settled, nil
}

// DeleteLB cascade-deletes the LB (drops listeners, pools, members in
// one call) if it exists and is managed by us. Returns nil when the LB
// is already gone — idempotent for repeated reconciles.
func DeleteLB(ctx context.Context, c *Clients, tenant string, svc *corev1.Service) error {
	name := ServiceLBName(tenant, svc.Namespace, svc.Name)

	existing, err := findLBByName(ctx, c, name)
	if err != nil {
		return errors.Wrap(err, "looking up LB for delete")
	}

	if existing == nil {
		return nil
	}

	if !IsManaged(existing) {
		return errors.Newf("refusing to delete LB %s: name does not carry the cozystack: prefix", existing.ID)
	}

	err = loadbalancers.Delete(ctx, c.LoadBalancer, existing.ID, loadbalancers.DeleteOpts{Cascade: true}).ExtractErr()
	if err != nil {
		return errors.Wrapf(err, "deleting LB %s", existing.ID)
	}

	return nil
}

// SyncListenersAndMembers reconciles the listener/pool/member set on
// the LB to match the Service's ports and the tenant's worker node
// IPs. v1 implements the minimal happy-path:
//
//   - one listener per Service.spec.ports[] (protocol must be TCP/UDP/SCTP;
//     OVN rejects HTTP/HTTPS so we filter)
//   - one pool per listener, algorithm SOURCE_IP_PORT (the only one OVN
//     supports), no health monitor (KUBE-proxy NodePort handles
//     liveness from inside the cluster; HTTP monitors are unsupported
//     by OVN anyway)
//   - members = (nodeIP, port.NodePort) for every healthy worker
//
// memberIPs is the caller-resolved list of tenant Node InternalIPs.
func SyncListenersAndMembers(
	ctx context.Context,
	c *Clients,
	lb *loadbalancers.LoadBalancer,
	svc *corev1.Service,
	memberIPs []string,
) error {
	desiredPorts := make([]corev1.ServicePort, 0, len(svc.Spec.Ports))

	for _, p := range svc.Spec.Ports {
		// OVN driver only supports TCP/UDP/SCTP listeners. Filter the
		// rest at this layer so we surface a clean error instead of a
		// cryptic OpenStack 400.
		switch p.Protocol {
		case corev1.ProtocolTCP, corev1.ProtocolUDP, corev1.ProtocolSCTP:
			desiredPorts = append(desiredPorts, p)
		default:
			return errors.Newf("port %d has unsupported protocol %q (OVN listener requires TCP/UDP/SCTP)", p.Port, p.Protocol)
		}

		if p.NodePort == 0 {
			return errors.Newf("port %d has no nodePort allocated; ensure Service.spec.type=LoadBalancer triggered kube-apiserver to assign one", p.Port)
		}
	}

	existing, err := listListenersForLB(ctx, c, lb.ID)
	if err != nil {
		return errors.Wrap(err, "listing existing listeners")
	}

	byName := make(map[string]*listeners.Listener, len(existing))
	for i := range existing {
		byName[existing[i].Name] = &existing[i]
	}

	keep := make(map[string]struct{}, len(desiredPorts))

	for _, p := range desiredPorts {
		listenerName := fmt.Sprintf("port-%d-%s", p.Port, strings.ToLower(string(p.Protocol)))
		keep[listenerName] = struct{}{}

		listener, exists := byName[listenerName]
		if !exists {
			listener, err = createListener(ctx, c, lb.ID, listenerName, p)
			if err != nil {
				return errors.Wrapf(err, "creating listener %s", listenerName)
			}
		}

		if _, err := waitForLBActive(ctx, c, lb.ID); err != nil {
			return errors.Wrapf(err, "waiting for LB %s after listener %s", lb.ID, listenerName)
		}

		pool, err := ensurePool(ctx, c, lb.ID, listener.ID, p)
		if err != nil {
			return errors.Wrapf(err, "ensuring pool for listener %s", listenerName)
		}

		if _, err := waitForLBActive(ctx, c, lb.ID); err != nil {
			return errors.Wrapf(err, "waiting for LB %s after pool for %s", lb.ID, listenerName)
		}

		if err := syncMembers(ctx, c, pool.ID, p.NodePort, memberIPs, lb.VipSubnetID); err != nil {
			return errors.Wrapf(err, "syncing members for pool %s", pool.ID)
		}

		if _, err := waitForLBActive(ctx, c, lb.ID); err != nil {
			return errors.Wrapf(err, "waiting for LB %s after member sync for %s", lb.ID, listenerName)
		}
	}

	for name, listener := range byName {
		if _, ok := keep[name]; ok {
			continue
		}

		// Listener no longer in the Service spec — drop it. Octavia
		// cascade also drops its pool + members, so a single delete is
		// enough.
		if err := listeners.Delete(ctx, c.LoadBalancer, listener.ID).ExtractErr(); err != nil {
			return errors.Wrapf(err, "deleting stale listener %s", listener.ID)
		}

		if _, err := waitForLBActive(ctx, c, lb.ID); err != nil {
			return errors.Wrapf(err, "waiting for LB %s after deleting %s", lb.ID, listener.ID)
		}
	}

	return nil
}

// ---- helpers below ----

const (
	lbActiveTimeout = 2 * time.Minute
	lbPollInterval  = 3 * time.Second
)

func findLBByName(ctx context.Context, c *Clients, name string) (*loadbalancers.LoadBalancer, error) {
	pager := loadbalancers.List(c.LoadBalancer, loadbalancers.ListOpts{Name: name})

	var found *loadbalancers.LoadBalancer

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := loadbalancers.ExtractLoadBalancers(page)
		if err != nil {
			return false, err
		}

		for i := range list {
			if list[i].Name == name {
				lb := list[i]
				found = &lb

				return false, nil
			}
		}

		return true, nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // surfaces gophercloud error to caller; wrapped one level up
	}

	return found, nil
}

func waitForLBActive(ctx context.Context, c *Clients, lbID string) (*loadbalancers.LoadBalancer, error) {
	deadline := time.Now().Add(lbActiveTimeout)

	for {
		lb, err := loadbalancers.Get(ctx, c.LoadBalancer, lbID).Extract()
		if err != nil {
			return nil, errors.Wrapf(err, "polling LB %s", lbID)
		}

		switch lb.ProvisioningStatus {
		case "ACTIVE":
			return lb, nil
		case "ERROR":
			return nil, errors.Newf("LB %s entered provisioning_status=ERROR (operating_status=%s)", lbID, lb.OperatingStatus)
		}

		if time.Now().After(deadline) {
			return nil, errors.Newf("LB %s did not reach ACTIVE within %s (last status=%s)", lbID, lbActiveTimeout, lb.ProvisioningStatus)
		}

		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "context cancelled while waiting for LB")
		case <-time.After(lbPollInterval):
		}
	}
}

func listListenersForLB(ctx context.Context, c *Clients, lbID string) ([]listeners.Listener, error) {
	pager := listeners.List(c.LoadBalancer, listeners.ListOpts{LoadbalancerID: lbID})

	var all []listeners.Listener

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := listeners.ExtractListeners(page)
		if err != nil {
			return false, err
		}

		all = append(all, list...)

		return true, nil
	})

	return all, err //nolint:wrapcheck
}

func createListener(ctx context.Context, c *Clients, lbID, name string, port corev1.ServicePort) (*listeners.Listener, error) {
	return listeners.Create(ctx, c.LoadBalancer, listeners.CreateOpts{ //nolint:wrapcheck
		Name:           name,
		LoadbalancerID: lbID,
		Protocol:       listeners.Protocol(string(port.Protocol)), // TCP/UDP/SCTP
		ProtocolPort:   int(port.Port),
	}).Extract()
}

func ensurePool(ctx context.Context, c *Clients, lbID, listenerID string, port corev1.ServicePort) (*pools.Pool, error) {
	existing, err := findPoolForListener(ctx, c, lbID, listenerID)
	if err != nil {
		return nil, err
	}

	if existing != nil {
		return existing, nil
	}

	created, err := pools.Create(ctx, c.LoadBalancer, pools.CreateOpts{
		Name:        fmt.Sprintf("pool-%s", listenerID),
		LBMethod:    pools.LBMethodSourceIpPort, // OVN-only choice
		Protocol:    pools.Protocol(string(port.Protocol)),
		ListenerID:  listenerID,
		Description: "Managed by Cozystack loadbalancer-controller",
	}).Extract()
	if err != nil {
		return nil, errors.Wrap(err, "creating pool")
	}

	return created, nil
}

// findPoolForListener filters server-side by LoadBalancer (pools.ListOpts
// has no ListenerID field) and then client-side by listener reference.
func findPoolForListener(ctx context.Context, c *Clients, lbID, listenerID string) (*pools.Pool, error) {
	pager := pools.List(c.LoadBalancer, pools.ListOpts{LoadbalancerID: lbID})

	var found *pools.Pool

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := pools.ExtractPools(page)
		if err != nil {
			return false, err
		}

		for i := range list {
			for _, l := range list[i].Listeners {
				if l.ID == listenerID {
					p := list[i]
					found = &p

					return false, nil
				}
			}
		}

		return true, nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	return found, nil
}

func syncMembers(ctx context.Context, c *Clients, poolID string, nodePort int32, memberIPs []string, subnetID string) error {
	existing, err := listMembers(ctx, c, poolID)
	if err != nil {
		return errors.Wrap(err, "listing pool members")
	}

	want := make(map[string]struct{}, len(memberIPs))
	for _, ip := range memberIPs {
		want[ip] = struct{}{}
	}

	have := make(map[string]string, len(existing)) // ip -> member id
	for _, m := range existing {
		have[m.Address] = m.ID
	}

	for ip := range want {
		if _, exists := have[ip]; exists {
			continue
		}

		_, err := pools.CreateMember(ctx, c.LoadBalancer, poolID, pools.CreateMemberOpts{
			Name:         fmt.Sprintf("worker-%s", strings.ReplaceAll(ip, ".", "-")),
			Address:      ip,
			ProtocolPort: int(nodePort),
			SubnetID:     subnetID,
		}).Extract()
		if err != nil {
			return errors.Wrapf(err, "adding pool member %s:%d", ip, nodePort)
		}
	}

	for ip, id := range have {
		if _, ok := want[ip]; ok {
			continue
		}

		if err := pools.DeleteMember(ctx, c.LoadBalancer, poolID, id).ExtractErr(); err != nil {
			return errors.Wrapf(err, "removing pool member %s (%s)", ip, id)
		}
	}

	return nil
}

func listMembers(ctx context.Context, c *Clients, poolID string) ([]pools.Member, error) {
	pager := pools.ListMembers(c.LoadBalancer, poolID, pools.ListMembersOpts{})

	var all []pools.Member

	err := pager.EachPage(ctx, func(_ context.Context, page pagination.Page) (bool, error) {
		list, err := pools.ExtractMembers(page)
		if err != nil {
			return false, err
		}

		all = append(all, list...)

		return true, nil
	})

	return all, err //nolint:wrapcheck
}
