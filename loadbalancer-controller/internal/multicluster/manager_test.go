/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

package multicluster

import (
	"testing"
)

func TestSecretNamePredicates(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		secretName string
		want       bool
		wantTenant string
	}{
		{"single-word tenant", "kubernetes-switchcloud-mesh1-admin-kubeconfig", true, "mesh1"},
		{"multi-dash tenant token", "kubernetes-switchcloud-mesh-prod-admin-kubeconfig", true, "mesh-prod"},
		{"empty tenant token", "kubernetes-switchcloud--admin-kubeconfig", false, ""},
		{"wrong prefix", "other-mesh1-admin-kubeconfig", false, ""},
		{"wrong suffix", "kubernetes-switchcloud-mesh1-admin", false, ""},
		{"helm release secret", "sh.helm.release.v1.kubernetes-switchcloud-mesh1.v1", false, ""},
		{"empty string", "", false, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := isKubeconfigSecretName(tc.secretName); got != tc.want {
				t.Fatalf("isKubeconfigSecretName(%q) = %v, want %v", tc.secretName, got, tc.want)
			}

			if got := tenantFromSecretName(tc.secretName); got != tc.wantTenant {
				t.Fatalf("tenantFromSecretName(%q) = %q, want %q", tc.secretName, got, tc.wantTenant)
			}
		})
	}
}

func TestSha256OfBytesDeterministic(t *testing.T) {
	t.Parallel()

	payload := []byte("apiVersion: v1\nkind: Config\n...")

	a := sha256OfBytes(payload)
	b := sha256OfBytes(payload)

	if a != b {
		t.Fatalf("sha256OfBytes is non-deterministic: %q vs %q", a, b)
	}

	if a == sha256OfBytes(append(payload, '!')) {
		t.Fatal("sha256OfBytes did not change when payload changed")
	}
}
