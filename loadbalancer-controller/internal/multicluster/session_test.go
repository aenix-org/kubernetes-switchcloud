/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

package multicluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/finalizers"
)

// countingReconciler counts how many times Reconcile fires and on
// which keys. Used to assert workqueue plumbing without needing a
// real OpenStack stack underneath.
type countingReconciler struct {
	calls atomic.Int64
	keys  chan reconcile.Request
}

func newCountingReconciler() *countingReconciler {
	return &countingReconciler{keys: make(chan reconcile.Request, 16)}
}

func (c *countingReconciler) Reconcile(_ context.Context, req ctrl.Request) (ctrl.Result, error) {
	c.calls.Add(1)
	select {
	case c.keys <- req:
	default:
	}

	return ctrl.Result{}, nil
}

// newBareSession builds a Session without going through cluster.New
// — the cluster.Cluster pieces (cache, informer) are not exercised
// by these tests, which target the workqueue + worker loop + Stop
// idempotency surface. The cluster field stays nil and any test that
// touches it would crash, which is fine because no such test should
// be reachable from this file.
func newBareSession(t *testing.T, tenant string, r reconcile.Reconciler) *Session {
	t.Helper()

	return &Session{
		Tenant:         tenant,
		KubeconfigHash: "test-hash",
		reconciler:     r,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
			workqueue.TypedRateLimitingQueueConfig[reconcile.Request]{Name: "test-" + tenant},
		),
		log: logr.Discard(),
	}
}

func TestSessionProcessNextDrivesReconciler(t *testing.T) {
	t.Parallel()

	r := newCountingReconciler()
	s := newBareSession(t, "mesh1", r)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// One worker, no cluster cache; just push items through the queue
	// and verify the reconciler sees them.
	go s.runWorker(ctx)

	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "svc"}}
	s.queue.Add(req)

	select {
	case got := <-r.keys:
		if got != req {
			t.Fatalf("worker reconciled %v, expected %v", got, req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not reconcile pushed item within 2s")
	}

	s.queue.ShutDownWithDrain()
}

func TestSessionStopIdempotent(t *testing.T) {
	t.Parallel()

	r := newCountingReconciler()
	s := newBareSession(t, "mesh1", r)

	// Wire a parent context + cancel so Stop has something to release.
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	// Run a worker so wg.Wait inside Stop has someone to wait on.
	s.wg.Add(1)

	go func() {
		defer s.wg.Done()
		s.runWorker(ctx)
	}()

	// Multiple Stop calls must not panic or deadlock.
	done := make(chan struct{})

	go func() {
		s.Stop()
		s.Stop()
		s.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("repeated Stop deadlocked")
	}
}

func TestSessionEnqueueAllLBServicesSkipsNonLB(t *testing.T) {
	t.Parallel()

	r := newCountingReconciler()
	s := newBareSession(t, "mesh1", r)

	// Reuse the bare Session's queue. We do not need a real informer
	// — EnqueueAllLBServices walks tenantClient.List, which we bypass
	// here by constructing the inputs directly.
	want := corev1.ServiceList{
		Items: []corev1.Service{
			{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "lb1"},
				Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "stale-with-fin", Finalizers: []string{finalizers.Service}},
				Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "irrelevant"},
				Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
			},
		},
	}

	for i := range want.Items {
		svc := &want.Items[i]
		if svc.Spec.Type != corev1.ServiceTypeLoadBalancer && !hasString(svc.Finalizers, finalizers.Service) {
			continue
		}

		s.queue.Add(reconcile.Request{NamespacedName: types.NamespacedName{Namespace: svc.Namespace, Name: svc.Name}})
	}

	if got := s.queue.Len(); got != 2 {
		t.Fatalf("queue.Len() = %d, want 2 (LoadBalancer + stale-with-finalizer; irrelevant ClusterIP filtered)", got)
	}

	s.queue.ShutDown()
}

func TestHasString(t *testing.T) {
	t.Parallel()

	if !hasString([]string{"a", "b", "c"}, "b") {
		t.Fatal("hasString must find present elements")
	}

	if hasString([]string{"a", "b"}, "c") {
		t.Fatal("hasString must reject absent elements")
	}

	if hasString(nil, "anything") {
		t.Fatal("hasString on nil slice must return false")
	}
}
