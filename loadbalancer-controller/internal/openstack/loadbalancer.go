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
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/listeners"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/loadbalancers"
	"github.com/gophercloud/gophercloud/v2/openstack/loadbalancer/v2/pools"
	"github.com/gophercloud/gophercloud/v2/pagination"
	corev1 "k8s.io/api/core/v1"
)

// Octavia provisioning_status values we care about. PENDING_CREATE and
// PENDING_UPDATE are intentionally absent: every site that inspects
// status treats them through a `default:` arm (yield + requeue),
// so naming them would just be dead aliases.
const (
	statusActive        = "ACTIVE"
	statusError         = "ERROR"
	statusPendingDelete = "PENDING_DELETE"
	managedNamePrefix   = "cozystack:"
)

// ServiceLBName is the Octavia LB `name` we set so that we can find the
// LB later without relying on a separate tag store. Format:
//
//	cozystack:<tenant>/<namespace>/<service>
//
// Resolution is unique because tenants can't share project namespaces
// (each KubernetesSwitchcloud lives in its own OpenStack project).
func ServiceLBName(tenant, namespace, name string) string {
	return fmt.Sprintf("%s%s/%s/%s", managedNamePrefix, tenant, namespace, name)
}

// IsManaged reports whether an Octavia LB was created by this
// controller, by looking at the `name` prefix.
func IsManaged(lb *loadbalancers.LoadBalancer) bool {
	return strings.HasPrefix(lb.Name, managedNamePrefix)
}

// EnsureLB returns the LoadBalancer for the given Service, creating it
// if missing. It never blocks waiting for status to settle: callers get
// pending=true whenever the LB is mid-operation and should requeue
// instead of holding the worker. provider defaults to "ovn" when empty
// (the only working provider in Switch Cloud zhw).
func EnsureLB(
	ctx context.Context,
	c *Clients,
	tenant string,
	svc *corev1.Service,
	vipSubnetID string,
	provider string,
) (lb *loadbalancers.LoadBalancer, pending bool, err error) {
	name := ServiceLBName(tenant, svc.Namespace, svc.Name)

	existing, err := findLBByName(ctx, c, name)
	if err != nil {
		return nil, false, errors.Wrap(err, "looking up existing LB")
	}

	if existing != nil {
		switch existing.ProvisioningStatus {
		case statusActive:
			return existing, false, nil
		case statusError:
			return nil, false, errors.Newf("LB %s is in provisioning_status=ERROR (operating_status=%s)", existing.ID, existing.OperatingStatus)
		default:
			// PENDING_* — yield the worker; next reconcile will pick
			// it up when the LB settles.
			return existing, true, nil
		}
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
		return nil, false, errors.Wrap(err, "creating Octavia LB")
	}

	// Newly created LBs are PENDING_CREATE; force a requeue.
	return created, true, nil
}

// DeleteLB cascade-deletes the LB (drops listeners, pools, members in
// one call) if it exists and is managed by us. Returns pending=true
// when the LB exists but is mid-delete (PENDING_DELETE) or when we
// just initiated the delete. Returns pending=false and no error when
// the LB is already gone — idempotent for repeated reconciles.
func DeleteLB(ctx context.Context, c *Clients, tenant string, svc *corev1.Service) (pending bool, err error) {
	name := ServiceLBName(tenant, svc.Namespace, svc.Name)

	existing, err := findLBByName(ctx, c, name)
	if err != nil {
		return false, errors.Wrap(err, "looking up LB for delete")
	}

	if existing == nil {
		return false, nil
	}

	if !IsManaged(existing) {
		return false, errors.Newf("refusing to delete LB %s: name does not carry the %s prefix", existing.ID, managedNamePrefix)
	}

	if existing.ProvisioningStatus == statusPendingDelete {
		return true, nil
	}

	err = loadbalancers.Delete(ctx, c.LoadBalancer, existing.ID, loadbalancers.DeleteOpts{Cascade: true}).ExtractErr()
	if err != nil {
		return false, errors.Wrapf(err, "deleting LB %s", existing.ID)
	}

	return true, nil
}

// SyncListenersAndMembers reconciles the listener/pool/member set on
// the LB to match the Service's ports and the tenant's worker node
// IPs. The reconcile is single-step per call: at most one mutation is
// performed, and if anything was changed we return pending=true so the
// caller requeues. This avoids the 409 cascade that occurs when
// chained mutations hit Octavia while the LB is still PENDING_UPDATE.
//
// Layout:
//
//   - one listener per Service.spec.ports[] (protocol must be TCP/UDP/SCTP;
//     OVN rejects HTTP/HTTPS so we error out cleanly)
//   - one pool per listener, algorithm SOURCE_IP_PORT (the only one OVN
//     supports), no health monitor (kube-proxy NodePort handles
//     liveness from inside the cluster)
//   - members = (nodeIP, port.NodePort) for every Ready worker, set as
//     a single declarative batch via BatchUpdateMembers
//
// memberIPs may be empty; that flushes the pool of stale members
// rather than skipping the sync.
func SyncListenersAndMembers(
	ctx context.Context,
	c *Clients,
	lb *loadbalancers.LoadBalancer,
	svc *corev1.Service,
	memberIPs []string,
) (pending bool, err error) {
	if lb.ProvisioningStatus != statusActive {
		// Don't try to mutate while Octavia is still settling.
		return true, nil
	}

	desiredPorts := make([]corev1.ServicePort, 0, len(svc.Spec.Ports))

	for _, p := range svc.Spec.Ports {
		switch p.Protocol {
		case corev1.ProtocolTCP, corev1.ProtocolUDP, corev1.ProtocolSCTP:
			desiredPorts = append(desiredPorts, p)
		default:
			return false, errors.Newf("port %d has unsupported protocol %q (OVN listener requires TCP/UDP/SCTP)", p.Port, p.Protocol)
		}

		if p.NodePort == 0 {
			return false, errors.Newf("port %d has no nodePort allocated; ensure Service.spec.type=LoadBalancer triggered kube-apiserver to assign one", p.Port)
		}
	}

	existing, err := listListenersForLB(ctx, c, lb.ID)
	if err != nil {
		return false, errors.Wrap(err, "listing existing listeners")
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
			if _, err := createListener(ctx, c, lb.ID, listenerName, p); err != nil {
				return false, errors.Wrapf(err, "creating listener %s", listenerName)
			}

			// Mutation initiated; LB now in PENDING_UPDATE. Yield.
			return true, nil
		}

		pool, created, err := ensurePool(ctx, c, lb.ID, listener.ID, p)
		if err != nil {
			return false, errors.Wrapf(err, "ensuring pool for listener %s", listenerName)
		}

		if created {
			return true, nil
		}

		changed, err := syncMembers(ctx, c, pool.ID, p.NodePort, memberIPs, lb.VipSubnetID)
		if err != nil {
			return false, errors.Wrapf(err, "syncing members for pool %s", pool.ID)
		}

		if changed {
			return true, nil
		}
	}

	for name, listener := range byName {
		if _, ok := keep[name]; ok {
			continue
		}

		// Listener no longer in the Service spec — drop it. Octavia
		// cascade also drops its pool + members.
		if err := listeners.Delete(ctx, c.LoadBalancer, listener.ID).ExtractErr(); err != nil {
			return false, errors.Wrapf(err, "deleting stale listener %s", listener.ID)
		}

		return true, nil
	}

	return false, nil
}

// ---- helpers below ----

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
		return nil, err //nolint:wrapcheck
	}

	return found, nil
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
		Protocol:       listeners.Protocol(string(port.Protocol)),
		ProtocolPort:   int(port.Port),
	}).Extract()
}

// ensurePool returns the pool bound to the given listener. created is
// true when this call performed the create — caller should requeue to
// let the LB return to ACTIVE before further mutations.
func ensurePool(ctx context.Context, c *Clients, lbID, listenerID string, port corev1.ServicePort) (p *pools.Pool, created bool, err error) {
	existing, err := findPoolForListener(ctx, c, lbID, listenerID)
	if err != nil {
		return nil, false, err
	}

	if existing != nil {
		return existing, false, nil
	}

	out, err := pools.Create(ctx, c.LoadBalancer, pools.CreateOpts{
		Name:        fmt.Sprintf("pool-%s", listenerID),
		LBMethod:    pools.LBMethodSourceIpPort,
		Protocol:    pools.Protocol(string(port.Protocol)),
		ListenerID:  listenerID,
		Description: "Managed by Cozystack loadbalancer-controller",
	}).Extract()
	if err != nil {
		return nil, false, errors.Wrap(err, "creating pool")
	}

	return out, true, nil
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

// syncMembers reconciles the pool's members to match memberIPs. Uses
// Octavia's BatchUpdateMembers (declarative pool-member API): a single
// atomic call replaces the entire member set, so there is no
// PENDING_UPDATE cascade between member additions and deletions and
// therefore no 409 race. Returns changed=true when the call was
// actually issued (i.e. there was a diff); callers should then
// requeue and let the LB return to ACTIVE.
func syncMembers(ctx context.Context, c *Clients, poolID string, nodePort int32, memberIPs []string, subnetID string) (changed bool, err error) {
	existing, err := listMembers(ctx, c, poolID)
	if err != nil {
		return false, errors.Wrap(err, "listing pool members")
	}

	desired := make(map[string]struct{}, len(memberIPs))
	for _, ip := range memberIPs {
		desired[ip] = struct{}{}
	}

	currentByIP := make(map[string]pools.Member, len(existing))
	for _, m := range existing {
		currentByIP[m.Address] = m
	}

	// Detect drift: any new IP, any removed IP, or any port mismatch
	// counts as a change. If nothing's different, skip the API call
	// to avoid pointless PENDING_UPDATE churn.
	drift := len(currentByIP) != len(desired)

	if !drift {
		for ip := range desired {
			cur, ok := currentByIP[ip]
			if !ok || cur.ProtocolPort != int(nodePort) || cur.SubnetID != subnetID {
				drift = true

				break
			}
		}
	}

	if !drift {
		return false, nil
	}

	opts := make([]pools.BatchUpdateMemberOpts, 0, len(memberIPs))

	for _, ip := range memberIPs {
		memberName := fmt.Sprintf("worker-%s", strings.ReplaceAll(ip, ".", "-"))
		subnet := subnetID
		opts = append(opts, pools.BatchUpdateMemberOpts{
			Name:         &memberName,
			Address:      ip,
			ProtocolPort: int(nodePort),
			SubnetID:     &subnet,
		})
	}

	if err := pools.BatchUpdateMembers(ctx, c.LoadBalancer, poolID, opts).ExtractErr(); err != nil {
		return false, errors.Wrapf(err, "batch-updating pool %s members", poolID)
	}

	return true, nil
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
