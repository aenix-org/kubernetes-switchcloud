/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

package multicluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/finalizers"
)

// Tenant kubeconfig Secret naming convention. Exported so callers
// outside this package stay in sync without each maintaining a
// private copy of the strings; a drift here would silently miss
// real tenants or react to unrelated Secrets.
const (
	TenantNamespace      = "tenant-root"
	KubeconfigSuffix     = "-admin-kubeconfig"
	KubeconfigSecretKey  = "super-admin.conf"
	KubeconfigNamePrefix = "kubernetes-switchcloud-"
)

// ReconcilerFactory builds the per-tenant Reconciler given the
// tenant name and its cluster client. Pulled out as a function so
// internal/controller can construct its tenantServiceReconciler
// without internal/multicluster having to depend on the controller
// package (which would create an import cycle: the controller
// package imports multicluster).
type ReconcilerFactory func(tenant string, tenantClient ctrlclient.Client) reconcile.Reconciler

// managerResyncInterval is the belt against missed Secret-informer
// events. The primary trigger is the informer event handler; the
// timer just makes sure a manager that somehow drops an event still
// converges within one tick.
const managerResyncInterval = 30 * time.Second

// Manager owns the set of live per-tenant Sessions. It is registered
// with the controller-runtime manager via mgr.Add(); when Start runs
// it watches kubeconfig Secrets in tenant-root via the manager's
// cached informer and creates / replaces / stops Sessions as the
// secret set changes. Replaces the previous "static registry +
// restart-on-Secret-change" design so a tenant create/delete no
// longer takes the controller process down.
//
// Lifecycle:
//   - Start performs an initial reconcile against the current Secret
//     set, then loops on Secret events and a periodic safety tick.
//   - On a new kubeconfig Secret: build cluster.Cluster, build the
//     tenant Reconciler via ReconcilerFactory, start the Session.
//   - On a changed kubeconfig (different sha256 of Data): stop the
//     old Session, build and start a new one with the fresh REST
//     config — covers CA rotation after delete+recreate without
//     restarting the process.
//   - On a missing Secret: stop the Session and drop it from the map.
//   - On Stop (parent ctx cancelled): tear every Session down.
type Manager struct {
	// MgmtCache is the controller-runtime manager's cache, used to
	// register the Secret informer event handler so Secret create /
	// update / delete events in tenant-root trigger reconcile.
	MgmtCache ctrlcache.Cache

	// MgmtClient is the cached client backing MgmtCache. Used to
	// re-list Secrets on every reconcile pass.
	MgmtClient ctrlclient.Client

	// Scheme is the runtime scheme the per-tenant clusters share.
	// Must include corev1 + any Kinds the tenant reconciler reads.
	Scheme *runtime.Scheme

	// ReconcilerFactory builds the tenant Reconciler. Called once
	// per Session, never reused across sessions.
	ReconcilerFactory ReconcilerFactory

	Log logr.Logger

	sessions   map[string]*Session
	sessionsMu sync.Mutex
	wake       chan struct{}
}

// NeedLeaderElection ensures the dynamic manager runs on the leader
// only. Two replicas racing on Session lifecycle would deliver
// duplicate Service events and tear down each other's caches.
func (m *Manager) NeedLeaderElection() bool { return true }

// Start runs the reconcile loop until the parent context is
// cancelled. Implements controller-runtime's manager.Runnable.
func (m *Manager) Start(parent context.Context) error {
	m.sessions = make(map[string]*Session)

	m.wake = make(chan struct{}, 1)

	defer m.stopAll()

	// Register a Secret informer event handler that wakes the
	// reconcile loop on any tenant-root Secret create / update /
	// delete. The handler does not pass the event through — the
	// reconcile pass re-lists, so the event payload is irrelevant.
	informer, err := m.MgmtCache.GetInformer(parent, &corev1.Secret{})
	if err != nil {
		return errors.Wrap(err, "getting Secret informer")
	}

	handlerReg, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(_ interface{}) { m.signal() },
		UpdateFunc: func(_, _ interface{}) { m.signal() },
		DeleteFunc: func(_ interface{}) { m.signal() },
	})
	if err != nil {
		return errors.Wrap(err, "registering Secret event handler")
	}

	defer func() { _ = informer.RemoveEventHandler(handlerReg) }()

	if err := m.reconcileAll(parent); err != nil {
		return errors.Wrap(err, "initial Session reconcile")
	}

	t := time.NewTicker(managerResyncInterval)
	defer t.Stop()

	for {
		select {
		case <-parent.Done():
			return nil
		case <-m.wake:
			if err := m.reconcileAll(parent); err != nil {
				m.Log.Error(err, "reconciling tenant Sessions on Secret event")
			}
		case <-t.C:
			if err := m.reconcileAll(parent); err != nil {
				m.Log.Error(err, "reconciling tenant Sessions on periodic tick")
			}
		}
	}
}

func (m *Manager) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// reconcileAll diffs the current set of tenant Secrets against the
// live Sessions and applies the delta. Session.Start and Session.Stop
// can each take up to sessionStartTimeout to complete (cluster.Cluster
// cache sync) and are run **outside** the sessions lock in parallel
// so one unhealthy tenant does not block every other tenant's reconcile
// — and crucially does not block KSCWatcher reconciles that take the
// same lock through Manager.EnqueueAllForTenant. The lock is held only
// long enough to take a snapshot of the diff and, at the end, to swap
// the result of the parallel build into m.sessions.
func (m *Manager) reconcileAll(ctx context.Context) error {
	var secrets corev1.SecretList
	if err := m.MgmtClient.List(ctx, &secrets, ctrlclient.InNamespace(TenantNamespace)); err != nil {
		return errors.Wrap(err, "listing tenant Secrets")
	}

	desired := make(map[string]secretSnapshot, len(secrets.Items))

	for i := range secrets.Items {
		s := &secrets.Items[i]
		if !isKubeconfigSecretName(s.Name) {
			continue
		}

		tenant := tenantFromSecretName(s.Name)
		if tenant == "" {
			continue
		}

		kubeconfig, ok := s.Data[KubeconfigSecretKey]
		if !ok || len(kubeconfig) == 0 {
			// Kamaji writes the kubeconfig key in a later step after
			// creating the Secret. Skip this tenant for now; we will
			// be woken again on the next update event.
			continue
		}

		desired[tenant] = secretSnapshot{
			kubeconfig: kubeconfig,
			hash:       sha256OfBytes(kubeconfig),
		}
	}

	// Phase 1: under the lock, compute the diff and remove
	// to-be-stopped Sessions from the map. The Sessions themselves
	// are not stopped yet — we just stop *publishing* them via
	// EnqueueAllForTenant so a stuck Stop does not delay a KSC event
	// for a healthy tenant.
	m.sessionsMu.Lock()

	toStop := make([]*Session, 0)
	toBuild := make(map[string]secretSnapshot, len(desired))

	for tenant, sess := range m.sessions {
		want, present := desired[tenant]

		switch {
		case !present:
			m.Log.Info("stopping Session for removed tenant", "tenant", tenant)
			toStop = append(toStop, sess)
			delete(m.sessions, tenant)
		case want.hash != sess.KubeconfigHash:
			m.Log.Info("stopping Session for changed kubeconfig", "tenant", tenant, "oldHash", sess.KubeconfigHash, "newHash", want.hash)
			toStop = append(toStop, sess)
			delete(m.sessions, tenant)
		}
	}

	for tenant, want := range desired {
		if _, present := m.sessions[tenant]; present {
			continue
		}

		toBuild[tenant] = want
	}

	m.sessionsMu.Unlock()

	// Phase 2: tear down stale Sessions in parallel. Each Stop drains
	// its own workqueue and waits its own WaitGroup; serial execution
	// would compound timeouts across N tenants on shutdown.
	var stopWG sync.WaitGroup

	for _, sess := range toStop {
		stopWG.Add(1)

		go func(s *Session) {
			defer stopWG.Done()
			s.Stop()
		}(sess)
	}

	stopWG.Wait()

	// Phase 3: build and start new Sessions in parallel. Each call
	// is bounded by sessionStartTimeout; parallelism caps the worst
	// case at one timeout, not N timeouts.
	type buildResult struct {
		tenant string
		sess   *Session
		err    error
	}

	results := make(chan buildResult, len(toBuild))

	var startWG sync.WaitGroup

	for tenant, want := range toBuild {
		startWG.Add(1)

		go func(tenant string, want secretSnapshot) {
			defer startWG.Done()

			sess, err := m.buildSession(tenant, want.kubeconfig, want.hash)
			if err != nil {
				results <- buildResult{tenant: tenant, err: err}

				return
			}

			if err := sess.Start(ctx); err != nil {
				sess.Stop()
				results <- buildResult{tenant: tenant, err: err}

				return
			}

			results <- buildResult{tenant: tenant, sess: sess}
		}(tenant, want)
	}

	startWG.Wait()
	close(results)

	// Phase 4: swap successful Sessions into the map under the lock.
	// Failed builds are logged; the next reconcile pass will retry
	// because the desired tenant has no Session.
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()

	for r := range results {
		if r.err != nil {
			m.Log.Error(r.err, "Session build/start failed; will retry next reconcile", "tenant", r.tenant)

			continue
		}

		// Edge case: a concurrent reconcileAll pass may have started
		// its own Session for this tenant. We're behind the lock;
		// stop the duplicate and let the previous one win.
		if existing, present := m.sessions[r.tenant]; present {
			r.sess.Stop()
			_ = existing

			continue
		}

		m.sessions[r.tenant] = r.sess
	}

	return nil
}

func (m *Manager) buildSession(tenant string, kubeconfig []byte, hash string) (*Session, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, errors.Wrapf(err, "parsing kubeconfig for tenant %q", tenant)
	}

	c, err := cluster.New(cfg, func(o *cluster.Options) {
		o.Scheme = m.Scheme
	})
	if err != nil {
		return nil, errors.Wrapf(err, "building cluster.Cluster for tenant %q", tenant)
	}

	reconciler := m.ReconcilerFactory(tenant, c.GetClient())

	return NewSession(tenant, hash, c, reconciler, m.Log), nil
}

// EnqueueAllForTenant is the entry point for KSC-watch reconcilers
// that want to ripple a CR change into every tenant Service. A
// missing Session means we have no kubeconfig for the tenant yet —
// nothing to reconcile against. The next Secret event will build
// the Session and the new reconciler will see the current CR state
// on its first reconcile.
func (m *Manager) EnqueueAllForTenant(ctx context.Context, tenant string) error {
	m.sessionsMu.Lock()

	sess, present := m.sessions[tenant]

	m.sessionsMu.Unlock()

	if !present {
		return nil
	}

	return sess.EnqueueAllLBServices(ctx, finalizers.Service)
}

func (m *Manager) stopAll() {
	m.sessionsMu.Lock()

	snapshot := make([]*Session, 0, len(m.sessions))
	for tenant, sess := range m.sessions {
		snapshot = append(snapshot, sess)
		delete(m.sessions, tenant)
	}

	m.sessionsMu.Unlock()

	// Stop in parallel: on leader loss or SIGTERM controller-runtime
	// gives us gracefulShutdownTimeout (default 30s) to drain. Serial
	// Stops compound — each waits its own workqueue drain — and would
	// blow that budget on a fleet of tenants.
	var wg sync.WaitGroup

	for _, sess := range snapshot {
		wg.Add(1)

		go func(s *Session) {
			defer wg.Done()
			s.Stop()
		}(sess)
	}

	wg.Wait()
}

// Tenants returns the names of currently-active tenants. Used for
// startup logs and for the orphan-sweeper which needs the same view
// of which tenants the controller currently knows about.
func (m *Manager) Tenants() []string {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()

	names := make([]string, 0, len(m.sessions))
	for n := range m.sessions {
		names = append(names, n)
	}

	return names
}

// secretSnapshot pairs a kubeconfig payload with its hash so callers
// can compare without re-hashing.
type secretSnapshot struct {
	kubeconfig []byte
	hash       string
}

// isKubeconfigSecretName reports whether a Secret name follows the
// `kubernetes-switchcloud-<tenant>-admin-kubeconfig` convention.
func isKubeconfigSecretName(name string) bool {
	if !strings.HasPrefix(name, KubeconfigNamePrefix) || !strings.HasSuffix(name, KubeconfigSuffix) {
		return false
	}

	return tenantFromSecretName(name) != ""
}

// tenantFromSecretName strips the kubernetes-switchcloud- prefix and
// the -admin-kubeconfig suffix. Returns "" on names that do not
// match the convention.
func tenantFromSecretName(name string) string {
	if !strings.HasPrefix(name, KubeconfigNamePrefix) || !strings.HasSuffix(name, KubeconfigSuffix) {
		return ""
	}

	return strings.TrimSuffix(strings.TrimPrefix(name, KubeconfigNamePrefix), KubeconfigSuffix)
}

func sha256OfBytes(b []byte) string {
	h := sha256.Sum256(b)

	return hex.EncodeToString(h[:])
}
