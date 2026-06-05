/*
Copyright 2026 The Aenix Authors.
*/

package openstack

import (
	"testing"
)

func TestIsClaimedByKnown(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		server  string
		known   []string
		claimed bool
	}{
		{
			name:    "exact known cluster matches its worker",
			server:  "kubernetes-switchcloud-mesh1-md0-tr6cd-lhj7k",
			known:   []string{"mesh1"},
			claimed: true,
		},
		{
			name:    "mesh1 must NOT claim mesh10 worker (anchor avoids prefix-collision)",
			server:  "kubernetes-switchcloud-mesh10-md0-aaaa-bbbb",
			known:   []string{"mesh1"},
			claimed: false,
		},
		{
			name:    "multi-token cluster name like prod-east is matched anchored",
			server:  "kubernetes-switchcloud-prod-east-md0-aaaa-bbbb",
			known:   []string{"prod-east"},
			claimed: true,
		},
		{
			name:    "unknown cluster is orphan even with kubernetes-switchcloud- prefix",
			server:  "kubernetes-switchcloud-mesh3-md0-aaaa-bbbb",
			known:   []string{"mesh1", "mesh2"},
			claimed: false,
		},
		{
			name:    "empty cluster entry in known set is skipped defensively",
			server:  "kubernetes-switchcloud--md0-aaaa-bbbb",
			known:   []string{""},
			claimed: false,
		},
		{
			name:    "empty known set",
			server:  "kubernetes-switchcloud-mesh1-md0-aaaa-bbbb",
			known:   []string{},
			claimed: false,
		},
		{
			name:    "non-KSC server name never claimed",
			server:  "some-other-instance",
			known:   []string{"mesh1"},
			claimed: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			known := make(map[string]struct{}, len(tc.known))
			for _, k := range tc.known {
				known[k] = struct{}{}
			}

			got := isClaimedByKnown(tc.server, known)
			if got != tc.claimed {
				t.Errorf("isClaimedByKnown(%q, %v) = %v, want %v", tc.server, tc.known, got, tc.claimed)
			}
		})
	}
}

func TestParseClusterFromLBName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		lbName string
		want   string
		wantOK bool
	}{
		{name: "standard", lbName: "cozystack:mesh3/lb-test/hello-lb", want: "mesh3", wantOK: true},
		{name: "multi-token cluster", lbName: "cozystack:prod-east/ns/svc", want: "prod-east", wantOK: true},
		{name: "non-managed name", lbName: "some-other-lb", want: "", wantOK: false},
		{name: "missing namespace separator", lbName: "cozystack:mesh3", want: "", wantOK: false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := parseClusterFromLBName(tc.lbName)
			if got != tc.want || ok != tc.wantOK {
				t.Errorf("parseClusterFromLBName(%q) = (%q, %v), want (%q, %v)", tc.lbName, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}
