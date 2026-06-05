/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

// Package ksc resolves per-tenant configuration from the
// KubernetesSwitchcloud CR in the management cluster. The
// loadbalancer-controller never imports the
// `apps.cozystack.io/v1alpha1` types directly — they live in a
// separate repo and pulling them in would create a fragile dependency.
// Instead we read the CR as unstructured and pluck the few fields the
// controller cares about (`spec.openstack.*` plus the new
// `spec.openstack.loadBalancer.*` sub-tree).
package ksc

import (
	"context"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/openstack"
)

// TenantNamespace is the namespace Cozystack puts every
// KubernetesSwitchcloud CR (and its Secrets) into.
const TenantNamespace = "tenant-root"

// LoadBalancerConfig is the resolved view of
// `KubernetesSwitchcloud.spec.openstack.loadBalancer` plus the
// credentials needed to talk to OpenStack on that tenant's behalf.
type LoadBalancerConfig struct {
	Enabled               bool
	ProviderDriver        string
	VIPNetworkID          string
	FloatingNetworkID     string
	FloatingSubnetID      string
	WorkerSecurityGroupID string
	AllowedCIDRs          []string
	Creds                 openstack.Credentials

	// MisconfiguredReason is set when the CR opts into the feature
	// (loadBalancer.enabled=true) but a required field is missing.
	// In that case Enabled is forced back to false so the reconciler
	// short-circuits cleanly; the controller logs the reason once
	// when it observes a Service that would otherwise be managed,
	// instead of erroring (and re-erroring) on every reconcile of
	// every Service in the tenant.
	MisconfiguredReason string
}

var kscGVR = schema.GroupVersionResource{
	Group:    "apps.cozystack.io",
	Version:  "v1alpha1",
	Resource: "kubernetesswitchclouds",
}

// Resolve loads the named tenant's KubernetesSwitchcloud CR from the
// management cluster, resolves OpenStack credentials (inline values or
// referenced existingSecret), and returns a LoadBalancerConfig.
//
// The caller is expected to short-circuit when `cfg.Enabled` is false:
// that tenant has opted out of the LB feature and the controller must
// not provision OpenStack resources for any of its Services.
func Resolve(ctx context.Context, mgmtClient ctrlclient.Client, tenant string) (*LoadBalancerConfig, error) {
	ksc := &unstructured.Unstructured{}
	ksc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   kscGVR.Group,
		Version: kscGVR.Version,
		Kind:    "KubernetesSwitchcloud",
	})

	err := mgmtClient.Get(ctx, types.NamespacedName{Namespace: TenantNamespace, Name: tenant}, ksc)
	if err != nil {
		return nil, errors.Wrapf(err, "fetching KubernetesSwitchcloud %s/%s", TenantNamespace, tenant)
	}

	cfg := &LoadBalancerConfig{}

	enabled, _, _ := unstructured.NestedBool(ksc.Object, "spec", "openstack", "loadBalancer", "enabled")
	cfg.Enabled = enabled

	if !cfg.Enabled {
		return cfg, nil
	}

	providerDriver, _, _ := unstructured.NestedString(ksc.Object, "spec", "openstack", "loadBalancer", "providerDriver")
	cfg.ProviderDriver = providerDriver

	vipNetID, _, _ := unstructured.NestedString(ksc.Object, "spec", "openstack", "loadBalancer", "vipNetworkID")
	if vipNetID == "" {
		// Soft failure: surface the misconfiguration to the
		// reconciler via MisconfiguredReason and treat the feature
		// as disabled. Avoids an error-and-retry storm on every
		// Service in the tenant when an operator forgets to set
		// the field.
		cfg.Enabled = false
		cfg.MisconfiguredReason = "spec.openstack.loadBalancer.vipNetworkID is required when loadBalancer.enabled=true " +
			"(set it to a tenant-owned Neutron network ID, typically the same as spec.openstack.network.id)"

		return cfg, nil
	}

	cfg.VIPNetworkID = vipNetID

	floatingNetID, _, _ := unstructured.NestedString(ksc.Object, "spec", "openstack", "loadBalancer", "floatingNetworkID")
	cfg.FloatingNetworkID = floatingNetID

	floatingSubnetID, _, _ := unstructured.NestedString(ksc.Object, "spec", "openstack", "loadBalancer", "floatingSubnetID")
	cfg.FloatingSubnetID = floatingSubnetID

	workerSGID, _, _ := unstructured.NestedString(ksc.Object, "spec", "openstack", "loadBalancer", "workerSecurityGroupID")
	cfg.WorkerSecurityGroupID = workerSGID

	rawCIDRs, _, _ := unstructured.NestedStringSlice(ksc.Object, "spec", "openstack", "loadBalancer", "allowedCIDRs")
	if len(rawCIDRs) == 0 {
		// Match the openAPI default — open to the world. Operators
		// who want narrower exposure set explicit CIDRs in the CR.
		cfg.AllowedCIDRs = []string{"0.0.0.0/0"}
	} else {
		cfg.AllowedCIDRs = rawCIDRs
	}

	creds, err := resolveCredentials(ctx, mgmtClient, ksc)
	if err != nil {
		return nil, errors.Wrapf(err, "resolving OpenStack credentials for tenant %q", tenant)
	}

	cfg.Creds = creds

	return cfg, nil
}

// resolveCredentials picks between the inline shape
// (`applicationCredentialID` + `applicationCredentialSecret`) and the
// `existingSecret` shape (Secret in tenant-root with a `clouds.yaml`
// key in the standard OpenStack format). authURL + regionName come
// from the inline fields regardless — they're not part of the Secret.
func resolveCredentials(ctx context.Context, mgmtClient ctrlclient.Client, ksc *unstructured.Unstructured) (openstack.Credentials, error) {
	authURL, _, _ := unstructured.NestedString(ksc.Object, "spec", "openstack", "authURL")
	regionName, _, _ := unstructured.NestedString(ksc.Object, "spec", "openstack", "regionName")
	existingSecret, _, _ := unstructured.NestedString(ksc.Object, "spec", "openstack", "existingSecret")

	if existingSecret != "" {
		credID, credSecret, err := readCloudsYAML(ctx, mgmtClient, existingSecret)
		if err != nil {
			return openstack.Credentials{}, err
		}

		return openstack.Credentials{
			AuthURL:                     authURL,
			RegionName:                  regionName,
			ApplicationCredentialID:     credID,
			ApplicationCredentialSecret: credSecret,
		}, nil
	}

	credID, _, _ := unstructured.NestedString(ksc.Object, "spec", "openstack", "applicationCredentialID")
	credSecret, _, _ := unstructured.NestedString(ksc.Object, "spec", "openstack", "applicationCredentialSecret")

	if credID == "" || credSecret == "" {
		return openstack.Credentials{}, errors.New(
			"openstack credentials missing: set either spec.openstack.existingSecret " +
				"or both spec.openstack.applicationCredentialID and applicationCredentialSecret")
	}

	return openstack.Credentials{
		AuthURL:                     authURL,
		RegionName:                  regionName,
		ApplicationCredentialID:     credID,
		ApplicationCredentialSecret: credSecret,
	}, nil
}

// cloudsYAML mirrors the on-disk format of the os-client-config
// clouds.yaml file that OpenStack tooling expects. Only the fields we
// pull are declared here; sigs.k8s.io/yaml ignores the rest.
type cloudsYAML struct {
	Clouds map[string]struct {
		Auth struct {
			ApplicationCredentialID     string `json:"application_credential_id"`
			ApplicationCredentialSecret string `json:"application_credential_secret"`
		} `json:"auth"`
	} `json:"clouds"`
}

func readCloudsYAML(ctx context.Context, mgmtClient ctrlclient.Client, secretName string) (id, secret string, err error) {
	s := &corev1.Secret{}

	err = mgmtClient.Get(ctx, types.NamespacedName{Namespace: TenantNamespace, Name: secretName}, s)
	if err != nil {
		return "", "", errors.Wrapf(err, "fetching existingSecret %s/%s", TenantNamespace, secretName)
	}

	raw, ok := s.Data["clouds.yaml"]
	if !ok {
		return "", "", errors.Newf("Secret %s/%s missing required key clouds.yaml", TenantNamespace, secretName)
	}

	var parsed cloudsYAML
	if err := yaml.Unmarshal(raw, &parsed); err != nil {
		return "", "", errors.Wrap(err, "parsing clouds.yaml")
	}

	// Cozystack convention is a single cloud entry; take the first one
	// rather than requiring callers to know its name (matches OpenStack
	// CLI behaviour with OS_CLOUD unset on a single-entry file).
	for _, c := range parsed.Clouds {
		if c.Auth.ApplicationCredentialID != "" {
			return c.Auth.ApplicationCredentialID, c.Auth.ApplicationCredentialSecret, nil
		}
	}

	return "", "", errors.Newf("clouds.yaml in Secret %s/%s has no application-credential entry", TenantNamespace, secretName)
}

// IsNotFound returns true when err comes from a Get that hit a missing
// KubernetesSwitchcloud — used by the reconciler to skip cleanly when
// a CR is being deleted out from under us.
func IsNotFound(err error) bool {
	return apierrors.IsNotFound(errors.UnwrapAll(err))
}
