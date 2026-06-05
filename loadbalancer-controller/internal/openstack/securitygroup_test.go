/*
Copyright 2026 The Aenix Authors.
*/

package openstack

import (
	"testing"

	"github.com/gophercloud/gophercloud/v2/openstack/networking/v2/extensions/security/rules"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func makeSvc(ns, name string, ports ...corev1.ServicePort) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       corev1.ServiceSpec{Ports: ports},
	}
}

func TestBuildDesiredRules_SkipsZeroNodePort(t *testing.T) {
	svc := makeSvc("ns", "svc",
		corev1.ServicePort{Port: 80, NodePort: 0, Protocol: corev1.ProtocolTCP},
		corev1.ServicePort{Port: 443, NodePort: 32100, Protocol: corev1.ProtocolTCP},
	)

	got := buildDesiredRules("clu", svc, []string{"0.0.0.0/0"})

	if len(got) != 1 {
		t.Fatalf("want 1 rule (NodePort=0 skipped), got %d", len(got))
	}

	if _, ok := got[ruleKey{port: 32100, proto: "tcp", cidr: "0.0.0.0/0"}]; !ok {
		t.Fatalf("expected nodePort=32100 rule, got map=%v", got)
	}
}

func TestBuildDesiredRules_EtherTypeIPv4AndIPv6(t *testing.T) {
	svc := makeSvc("ns", "svc",
		corev1.ServicePort{Port: 80, NodePort: 31000, Protocol: corev1.ProtocolTCP},
	)

	got := buildDesiredRules("clu", svc, []string{"10.0.0.0/24", "2001:db8::/32"})

	if len(got) != 2 {
		t.Fatalf("want 2 rules (one per CIDR), got %d", len(got))
	}

	if et := got[ruleKey{port: 31000, proto: "tcp", cidr: "10.0.0.0/24"}].EtherType; et != rules.EtherType4 {
		t.Errorf("IPv4 CIDR expected EtherType4, got %v", et)
	}

	if et := got[ruleKey{port: 31000, proto: "tcp", cidr: "2001:db8::/32"}].EtherType; et != rules.EtherType6 {
		t.Errorf("IPv6 CIDR expected EtherType6, got %v", et)
	}
}

func TestBuildDesiredRules_MultiPortMultiCIDR(t *testing.T) {
	svc := makeSvc("ns", "svc",
		corev1.ServicePort{Port: 80, NodePort: 31000, Protocol: corev1.ProtocolTCP},
		corev1.ServicePort{Port: 53, NodePort: 31001, Protocol: corev1.ProtocolUDP},
	)

	got := buildDesiredRules("clu", svc, []string{"10.0.0.0/8", "192.168.0.0/16"})

	if want := 4; len(got) != want {
		t.Fatalf("ports*cidrs = 2*2 = %d, got %d: %v", want, len(got), got)
	}

	expect := []ruleKey{
		{port: 31000, proto: "tcp", cidr: "10.0.0.0/8"},
		{port: 31000, proto: "tcp", cidr: "192.168.0.0/16"},
		{port: 31001, proto: "udp", cidr: "10.0.0.0/8"},
		{port: 31001, proto: "udp", cidr: "192.168.0.0/16"},
	}

	for _, k := range expect {
		if _, ok := got[k]; !ok {
			t.Errorf("missing rule for key %s", k)
		}
	}
}

func TestRuleDescriptionPrefix_NoSubstringCollision(t *testing.T) {
	// Disambiguation: foo and foobar must not share a prefix that
	// would make foo's cleanup nuke foobar's rules.
	foo := ruleDescriptionPrefix("clu", makeSvc("ns", "foo"))
	foobar := ruleDescriptionPrefix("clu", makeSvc("ns", "foobar"))

	if foo == foobar {
		t.Fatal("foo and foobar produced identical prefix")
	}

	// Adjacent test: foobar's prefix must NOT start with foo's prefix
	// (it would, if we forgot the trailing ':').
	if len(foobar) > len(foo) && foobar[:len(foo)] == foo {
		t.Errorf("foobar prefix starts with foo prefix: %q vs %q", foobar, foo)
	}
}

func TestRuleDescription_RoundTripsInPrefix(t *testing.T) {
	// Any description generated for svc=X must start with the prefix
	// computed for that same svc, so listOwnedRules picks it up.
	svc := makeSvc("ns", "svc")
	k := ruleKey{port: 31000, proto: "tcp", cidr: "0.0.0.0/0"}

	desc := ruleDescription("clu", svc, k)
	prefix := ruleDescriptionPrefix("clu", svc)

	if len(desc) < len(prefix) || desc[:len(prefix)] != prefix {
		t.Fatalf("description %q does not start with prefix %q", desc, prefix)
	}
}

func TestEtherTypeFor(t *testing.T) {
	cases := map[string]rules.RuleEtherType{
		"0.0.0.0/0":     rules.EtherType4,
		"10.0.0.0/8":    rules.EtherType4,
		"::/0":          rules.EtherType6,
		"2001:db8::/32": rules.EtherType6,
		"fe80::/10":     rules.EtherType6,
	}

	for cidr, want := range cases {
		if got := etherTypeFor(cidr); got != want {
			t.Errorf("etherTypeFor(%q): want %v, got %v", cidr, want, got)
		}
	}
}
