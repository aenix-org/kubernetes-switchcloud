package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

var (
	openStackServerGVR = schema.GroupVersionResource{
		Group:    "infrastructure.cluster.x-k8s.io",
		Version:  "v1alpha1",
		Resource: "openstackservers",
	}
	ipAddressGVR = schema.GroupVersionResource{
		Group:    "ipam.cluster.x-k8s.io",
		Version:  "v1beta1",
		Resource: "ipaddresses",
	}
	ipAddressClaimGVR = schema.GroupVersionResource{
		Group:    "ipam.cluster.x-k8s.io",
		Version:  "v1beta1",
		Resource: "ipaddressclaims",
	}
)

type Reconciler struct {
	kube           dynamic.Interface
	cloudsYAMLPath string
	cloudName      string
	namespace      string
	log            *slog.Logger
}

func (r *Reconciler) Reconcile(ctx context.Context) error {
	servers, err := r.kube.Resource(openStackServerGVR).Namespace(r.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list OpenStackServers: %w", err)
	}

	var toFix []unstructured.Unstructured
	var toNudge []unstructured.Unstructured
	for _, s := range servers.Items {
		if hasFloatingIPError(&s) {
			toFix = append(toFix, s)
		} else if isStuckWaitingForIPClaim(&s) {
			toNudge = append(toNudge, s)
		}
	}

	// Nudge CAPO for servers stuck waiting for IPAddressClaim allocation:
	// CAPO's cluster-scope watch for ipaddressclaims is forbidden in this environment,
	// so it never learns when the claim is bound. Patching the annotation re-triggers reconcile.
	for i := range toNudge {
		s := &toNudge[i]
		if err := r.nudgeIfIPClaimBound(ctx, s); err != nil {
			r.log.Error("failed to nudge stuck server",
				"server", s.GetName(),
				"namespace", s.GetNamespace(),
				"err", err)
		}
	}

	if len(toFix) == 0 {
		r.log.Debug("no OpenStackServers with FloatingIPError")
		return nil
	}

	// Build neutron client only when there is work to do.
	neutron, err := newNeutronClient(r.cloudsYAMLPath, r.cloudName)
	if err != nil {
		return fmt.Errorf("create neutron client: %w", err)
	}

	for i := range toFix {
		s := &toFix[i]
		if err := r.reconcileServer(ctx, s, neutron); err != nil {
			r.log.Error("failed to reconcile server",
				"server", s.GetName(),
				"namespace", s.GetNamespace(),
				"err", err)
		}
	}
	return nil
}

// nudgeIfIPClaimBound checks whether the IPAddressClaim for the server is already bound
// and, if so, patches an annotation to trigger a fresh CAPO reconcile.
func (r *Reconciler) nudgeIfIPClaimBound(ctx context.Context, server *unstructured.Unstructured) error {
	name := server.GetName()
	ns := server.GetNamespace()

	ipClaimName := name + "-floating-ip-address"
	claim, err := r.kube.Resource(ipAddressClaimGVR).Namespace(ns).Get(ctx, ipClaimName, metav1.GetOptions{})
	if err != nil {
		r.log.Debug("IPAddressClaim not found, skipping nudge", "server", name, "claim", ipClaimName)
		return nil
	}

	addrRef, _, _ := unstructured.NestedString(claim.Object, "status", "addressRef", "name")
	if addrRef == "" {
		r.log.Debug("IPAddressClaim not yet bound, skipping nudge", "server", name)
		return nil
	}

	r.log.Info("nudging CAPO: IPAddressClaim bound but server stuck", "server", name, "addressRef", addrRef)
	return r.triggerCAPOReconcile(ctx, ns, name)
}

func (r *Reconciler) reconcileServer(ctx context.Context, server *unstructured.Unstructured, neutron *NeutronClient) error {
	name := server.GetName()
	ns := server.GetNamespace()
	log := r.log.With("server", name, "namespace", ns)

	portID, err := internalPortID(server)
	if err != nil {
		return fmt.Errorf("read internal port: %w", err)
	}
	if portID == "" {
		log.Info("no internal port in status yet, skipping")
		return nil
	}

	// IPAddressClaim/IPAddress naming convention in CAPO: {server-name}-floating-ip-address
	ipAddrName := name + "-floating-ip-address"
	ipAddrObj, err := r.kube.Resource(ipAddressGVR).Namespace(ns).Get(ctx, ipAddrName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get IPAddress %s: %w", ipAddrName, err)
	}

	fipIP, found, err := unstructured.NestedString(ipAddrObj.Object, "spec", "address")
	if err != nil || !found || fipIP == "" {
		log.Info("IPAddress not yet populated", "ipaddress", ipAddrName)
		return nil
	}

	log.Info("resolving FIP", "fip_ip", fipIP, "internal_port", portID)

	fipID, err := neutron.GetFIPByIP(ctx, fipIP)
	if err != nil {
		return fmt.Errorf("resolve FIP for %s: %w", fipIP, err)
	}

	already, err := neutron.IsFIPAssociated(ctx, fipID, portID)
	if err != nil {
		return fmt.Errorf("check FIP association: %w", err)
	}

	if !already {
		log.Info("associating FIP", "fip_id", fipID, "port_id", portID)
		if err := neutron.AssociateFIP(ctx, fipID, portID); err != nil {
			return fmt.Errorf("associate FIP %s with port %s: %w", fipID, portID, err)
		}
		log.Info("FIP associated")
	} else {
		log.Info("FIP already associated, nudging CAPO")
	}

	return r.triggerCAPOReconcile(ctx, ns, name)
}

// triggerCAPOReconcile patches a timestamp annotation on the OpenStackServer so
// CAPO's watch fires and it re-reads the (now-associated) FIP state.
func (r *Reconciler) triggerCAPOReconcile(ctx context.Context, ns, name string) error {
	ts := time.Now().UTC().Format(time.RFC3339)
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				"kubernetes-switchcloud.aenix.io/fip-reconciled-at": ts,
			},
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = r.kube.Resource(openStackServerGVR).Namespace(ns).Patch(
		ctx, name, types.MergePatchType, data, metav1.PatchOptions{},
	)
	return err
}

// isStuckWaitingForIPClaim returns true when an OpenStackServer has no conditions yet
// (CAPO hasn't progressed past the IPAddressClaim wait) and has no resources created.
// This happens when CAPO's cluster-scope watch for ipaddressclaims is forbidden and
// it never learns that the claim was bound.
func isStuckWaitingForIPClaim(server *unstructured.Unstructured) bool {
	conditions, found, _ := unstructured.NestedSlice(server.Object, "status", "conditions")
	if found && len(conditions) > 0 {
		return false
	}
	resources, found, _ := unstructured.NestedMap(server.Object, "status", "resources")
	if found && len(resources) > 0 {
		return false
	}
	// Server has floatingIPPoolRef (uses IPAddressClaim flow)
	_, hasFIPRef, _ := unstructured.NestedString(server.Object, "spec", "floatingIPPoolRef", "name")
	return hasFIPRef
}

func hasFloatingIPError(server *unstructured.Unstructured) bool {
	conditions, found, err := unstructured.NestedSlice(server.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}
	for _, c := range conditions {
		cond, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if cond["type"] == "FloatingAddressFromPoolReady" &&
			cond["status"] == "False" &&
			cond["reason"] == "FloatingIPError" {
			return true
		}
	}
	return false
}

// internalPortID extracts status.resources.ports[0].id from an OpenStackServer.
func internalPortID(server *unstructured.Unstructured) (string, error) {
	ports, found, err := unstructured.NestedSlice(server.Object, "status", "resources", "ports")
	if err != nil {
		return "", err
	}
	if !found || len(ports) == 0 {
		return "", nil
	}
	port, ok := ports[0].(map[string]any)
	if !ok {
		return "", fmt.Errorf("unexpected port element type %T", ports[0])
	}
	id, _ := port["id"].(string)
	return id, nil
}
