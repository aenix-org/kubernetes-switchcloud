/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

package multicluster

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/finalizers"
)

// sessionWorkers is the per-tenant concurrency. One reconcile worker
// per tenant is the prior static design; raising this trades CPU and
// OpenStack-API rate budget for faster catch-up on bulk Service events.
// Keep at 1 for cost parity with the old behaviour; revisit if pool
// member resyncs back up on hot tenants.
const sessionWorkers = 1

// sessionStartTimeout caps how long Start blocks waiting for the
// tenant cache to deliver its first Sync. A tenant whose apiserver
// is unreachable should not be allowed to wedge the manager's
// reconcile loop indefinitely — bail out, let the Manager log the
// failure, and try again on the next Secret event (CA rotation,
// transient apiserver restart, etc.).
const sessionStartTimeout = 90 * time.Second

// resyncInterval keeps the workqueue moving on tenants whose Service
// objects do not generate events for a while. Mirrors the
// memberResyncAfter cadence of the per-Service reconciler, so a
// tenant whose pool members went stale (worker scale, node Ready
// flap) catches up within a minute even without an explicit event.
const resyncInterval = 60 * time.Second

// Session manages one tenant's reconciler lifecycle: its
// cluster.Cluster (cache+client), Service watch, workqueue, and
// worker goroutines. Sessions are created and torn down by Manager
// as the tenant kubeconfig set changes, without restarting the
// controller process.
type Session struct {
	Tenant         string
	KubeconfigHash string

	cluster    cluster.Cluster
	reconciler reconcile.Reconciler
	queue      workqueue.TypedRateLimitingInterface[reconcile.Request]
	cancel     context.CancelFunc
	stopOnce   sync.Once
	wg         sync.WaitGroup
	log        logr.Logger
}

// NewSession wires a tenant cluster.Cluster up to a user-provided
// reconciler. The session is inert until Start is called; Stop is
// idempotent and safe to call concurrently with Start in the error
// path.
func NewSession(tenant, kubeconfigHash string, c cluster.Cluster, r reconcile.Reconciler, log logr.Logger) *Session {
	return &Session{
		Tenant:         tenant,
		KubeconfigHash: kubeconfigHash,
		cluster:        c,
		reconciler:     r,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
			workqueue.TypedRateLimitingQueueConfig[reconcile.Request]{
				Name: "tenant-" + tenant,
			},
		),
		log: log.WithValues("tenant", tenant),
	}
}

// Start runs the tenant cluster cache + Service watch + worker loop
// until the parent context is cancelled or Stop is called. Returns
// once the cache is synced and workers are pumping; the caller is
// expected to invoke Stop to drain the workqueue gracefully.
func (s *Session) Start(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel

	// Run the cluster cache in its own goroutine. Errors here usually
	// mean the apiserver was unreachable mid-stream; we surface them
	// through the manager's restart machinery rather than block Start.
	s.wg.Add(1)

	go func() {
		defer s.wg.Done()

		if err := s.cluster.Start(ctx); err != nil && ctx.Err() == nil {
			s.log.Error(err, "tenant cluster cache exited")
		}
	}()

	// WaitForCacheSync blocks until the informers have done their
	// initial list. Worker goroutines started after this point see a
	// fully-warmed cache.
	syncCtx, syncCancel := context.WithTimeout(ctx, sessionStartTimeout)
	defer syncCancel()

	if !s.cluster.GetCache().WaitForCacheSync(syncCtx) {
		s.cancel()
		s.wg.Wait()

		return errors.Newf("tenant %q cache failed to sync within %s", s.Tenant, sessionStartTimeout)
	}

	// Wire the Service informer into our workqueue. We use the
	// informer directly rather than going through controller-runtime's
	// Controller because that abstraction's Watches are immutable
	// after manager Start — which is the very limitation Manager
	// exists to work around.
	svcInformer, err := s.cluster.GetCache().GetInformer(ctx, &corev1.Service{})
	if err != nil {
		s.cancel()
		s.wg.Wait()

		return errors.Wrapf(err, "getting Service informer for tenant %q", s.Tenant)
	}

	_, err = svcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { s.enqueueFromObject(obj) },
		UpdateFunc: func(_, obj interface{}) { s.enqueueFromObject(obj) },
		DeleteFunc: func(obj interface{}) { s.enqueueFromObject(obj) },
	})
	if err != nil {
		s.cancel()
		s.wg.Wait()

		return errors.Wrapf(err, "registering Service event handler for tenant %q", s.Tenant)
	}

	// Periodic resync so the reconciler catches up on Node-IP and
	// member-list drift even when no Service event fires.
	s.wg.Add(1)

	go func() {
		defer s.wg.Done()
		s.runResync(ctx)
	}()

	for i := 0; i < sessionWorkers; i++ {
		s.wg.Add(1)

		go func() {
			defer s.wg.Done()
			s.runWorker(ctx)
		}()
	}

	s.log.Info("tenant session started", "kubeconfigHash", s.KubeconfigHash)

	return nil
}

// Stop cancels the session context, drains the workqueue, and waits
// for the worker goroutines to exit. Idempotent and safe to call from
// any goroutine — Manager calls it during teardown and during the
// error path of Start.
func (s *Session) Stop() {
	s.stopOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}

		s.queue.ShutDownWithDrain()
		s.wg.Wait()
		s.log.Info("tenant session stopped")
	})
}

// EnqueueAllLBServices walks the tenant Service cache and enqueues
// every type=LoadBalancer Service (plus anything carrying our
// finalizer, in case a type change is in flight). Manager calls this
// from its KSC watch so that flipping spec.openstack.loadBalancer.enabled
// reconciles every tenant Service without waiting for an event on
// each individual Service object.
func (s *Session) EnqueueAllLBServices(ctx context.Context, finalizerName string) error {
	var list corev1.ServiceList
	if err := s.cluster.GetClient().List(ctx, &list); err != nil {
		return errors.Wrapf(err, "listing Services for tenant %q", s.Tenant)
	}

	for i := range list.Items {
		svc := &list.Items[i]

		if svc.Spec.Type != corev1.ServiceTypeLoadBalancer && !hasString(svc.Finalizers, finalizerName) {
			continue
		}

		s.queue.Add(reconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: svc.Namespace,
			Name:      svc.Name,
		}})
	}

	return nil
}

// Client returns the tenant cluster client. Exposed for the
// reconciler factory in Manager and for direct cache reads where
// the reconciler interface alone is not enough.
func (s *Session) Client() ctrlclient.Client {
	return s.cluster.GetClient()
}

func (s *Session) enqueueFromObject(obj interface{}) {
	svc, ok := obj.(*corev1.Service)
	if !ok {
		// DeletedFinalStateUnknown carries the last-seen object inside;
		// unwrap it so we still reconcile cleanly on delete.
		if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			if svc, ok = tomb.Obj.(*corev1.Service); !ok {
				return
			}
		} else {
			return
		}
	}

	s.queue.Add(reconcile.Request{NamespacedName: types.NamespacedName{
		Namespace: svc.Namespace,
		Name:      svc.Name,
	}})
}

func (s *Session) runWorker(ctx context.Context) {
	for s.processNext(ctx) {
	}
}

func (s *Session) processNext(ctx context.Context) bool {
	item, shutdown := s.queue.Get()
	if shutdown {
		return false
	}

	defer s.queue.Done(item)

	result, err := s.reconciler.Reconcile(ctx, item)
	switch {
	case err != nil:
		s.log.Error(err, "reconcile error", "namespace", item.Namespace, "name", item.Name)
		s.queue.AddRateLimited(item)
	case result.RequeueAfter > 0:
		s.queue.Forget(item)
		s.queue.AddAfter(item, result.RequeueAfter)
	default:
		s.queue.Forget(item)
	}

	return true
}

func (s *Session) runResync(ctx context.Context) {
	t := time.NewTicker(resyncInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.EnqueueAllLBServices(ctx, finalizerName); err != nil {
				s.log.Error(err, "periodic resync: enqueue failed")
			}
		}
	}
}

// finalizerName references the canonical finalizer constant via the
// leaf finalizers package, which both the controller and the
// multicluster package import. Keeps the string in exactly one place.
const finalizerName = finalizers.Service

func hasString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}

	return false
}
