package main

import (
	"context"
	"fmt"
	"os"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/layer3/floatingips"
	"gopkg.in/yaml.v3"
)

// NeutronClient wraps a gophercloud networking service client.
type NeutronClient struct {
	svc *gophercloud.ServiceClient
}

type cloudsYAML struct {
	Clouds map[string]cloudEntry `yaml:"clouds"`
}

type cloudEntry struct {
	Auth       cloudAuth `yaml:"auth"`
	AuthType   string    `yaml:"auth_type"`
	RegionName string    `yaml:"region_name"`
}

type cloudAuth struct {
	AuthURL                     string `yaml:"auth_url"`
	ApplicationCredentialID     string `yaml:"application_credential_id"`
	ApplicationCredentialSecret string `yaml:"application_credential_secret"`
}

func newNeutronClient(cloudsYAMLPath, cloudName string) (*NeutronClient, error) {
	raw, err := os.ReadFile(cloudsYAMLPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", cloudsYAMLPath, err)
	}

	var clouds cloudsYAML
	if err := yaml.Unmarshal(raw, &clouds); err != nil {
		return nil, fmt.Errorf("parse clouds.yaml: %w", err)
	}

	cloud, ok := clouds.Clouds[cloudName]
	if !ok {
		return nil, fmt.Errorf("cloud %q not found in clouds.yaml", cloudName)
	}

	provider, err := openstack.AuthenticatedClient(context.Background(), gophercloud.AuthOptions{
		IdentityEndpoint:            cloud.Auth.AuthURL,
		ApplicationCredentialID:     cloud.Auth.ApplicationCredentialID,
		ApplicationCredentialSecret: cloud.Auth.ApplicationCredentialSecret,
	})
	if err != nil {
		return nil, fmt.Errorf("authenticate with OpenStack: %w", err)
	}

	svc, err := openstack.NewNetworkV2(provider, gophercloud.EndpointOpts{
		Region: cloud.RegionName,
	})
	if err != nil {
		return nil, fmt.Errorf("create network client: %w", err)
	}

	return &NeutronClient{svc: svc}, nil
}

// GetFIPByIP returns the UUID of the floating IP with the given address.
func (n *NeutronClient) GetFIPByIP(ctx context.Context, ip string) (string, error) {
	pages, err := floatingips.List(n.svc, floatingips.ListOpts{FloatingIP: ip}).AllPages(ctx)
	if err != nil {
		return "", fmt.Errorf("list floatingips (ip=%s): %w", ip, err)
	}
	fips, err := floatingips.ExtractFloatingIPs(pages)
	if err != nil {
		return "", fmt.Errorf("extract floatingips: %w", err)
	}
	if len(fips) == 0 {
		return "", fmt.Errorf("floating IP %q not found in project", ip)
	}
	return fips[0].ID, nil
}

// IsFIPAssociated returns true if the FIP is already mapped to portID.
func (n *NeutronClient) IsFIPAssociated(ctx context.Context, fipID, portID string) (bool, error) {
	fip, err := floatingips.Get(ctx, n.svc, fipID).Extract()
	if err != nil {
		return false, fmt.Errorf("get floatingip %s: %w", fipID, err)
	}
	return fip.PortID == portID, nil
}

// AssociateFIP maps the floating IP to the given Neutron port.
func (n *NeutronClient) AssociateFIP(ctx context.Context, fipID, portID string) error {
	_, err := floatingips.Update(ctx, n.svc, fipID, floatingips.UpdateOpts{PortID: &portID}).Extract()
	if err != nil {
		return fmt.Errorf("update floatingip %s -> port %s: %w", fipID, portID, err)
	}
	return nil
}
