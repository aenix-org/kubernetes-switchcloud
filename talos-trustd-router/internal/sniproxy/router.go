package sniproxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	// LabelTrustd marks a Service as a talos-csr-signer backend.
	LabelTrustd = "talos.aenix.io/trustd"
	// AnnotationHostname is the cluster FQDN this signer handles.
	AnnotationHostname = "talos.aenix.io/cluster-hostname"

	dialTimeout = 10 * time.Second
)

// Router watches Kubernetes Services labelled talos.aenix.io/trustd=true and
// routes incoming TCP connections to the correct backend based on TLS SNI.
type Router struct {
	mu       sync.RWMutex
	backends map[string]string // hostname → host:port
	log      *slog.Logger
}

// NewRouter creates a Router and starts watching Services via client.
func NewRouter(ctx context.Context, client kubernetes.Interface, log *slog.Logger) (*Router, error) {
	r := &Router{
		backends: make(map[string]string),
		log:      log,
	}

	factory := informers.NewSharedInformerFactory(client, 30*time.Second)

	svcInformer := factory.Core().V1().Services().Informer()
	svcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { r.upsert(obj) },
		UpdateFunc: func(_, obj interface{}) { r.upsert(obj) },
		DeleteFunc: func(obj interface{}) { r.delete(obj) },
	})

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), svcInformer.HasSynced) {
		return nil, fmt.Errorf("timed out waiting for service cache sync")
	}

	log.Info("service cache synced", "backends", r.count())
	return r, nil
}

func (r *Router) upsert(obj interface{}) {
	svc, ok := toService(obj)
	if !ok {
		return
	}
	if !isTrustdService(svc) {
		return
	}
	hostname, ok := svc.Annotations[AnnotationHostname]
	if !ok || hostname == "" {
		return
	}
	backend := fmt.Sprintf("%s.%s.svc:%d", svc.Name, svc.Namespace, trustdPort(svc))

	r.mu.Lock()
	r.backends[hostname] = backend
	r.mu.Unlock()

	r.log.Info("registered backend", "hostname", hostname, "backend", backend)
}

func (r *Router) delete(obj interface{}) {
	svc, ok := toService(obj)
	if !ok {
		return
	}
	hostname := svc.Annotations[AnnotationHostname]
	if hostname == "" {
		return
	}
	r.mu.Lock()
	delete(r.backends, hostname)
	r.mu.Unlock()

	r.log.Info("removed backend", "hostname", hostname)
}

// Backend returns the host:port for the given SNI hostname.
func (r *Router) Backend(hostname string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.backends[hostname]
	return b, ok
}

func (r *Router) count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.backends)
}

// Handle accepts a TCP connection, reads SNI, and proxies to the backend.
func (r *Router) Handle(conn net.Conn) {
	defer conn.Close()

	sni, peekConn, err := ReadSNI(conn)
	if err != nil {
		r.log.Warn("failed to read SNI", "peer", conn.RemoteAddr(), "err", err)
		return
	}

	backend, ok := r.Backend(sni)
	if !ok {
		r.log.Warn("no backend for SNI", "sni", sni, "peer", conn.RemoteAddr())
		return
	}

	upstream, err := net.DialTimeout("tcp", backend, dialTimeout)
	if err != nil {
		r.log.Error("dial backend failed", "backend", backend, "sni", sni, "err", err)
		return
	}
	defer upstream.Close()

	r.log.Info("proxying connection", "sni", sni, "peer", conn.RemoteAddr(), "backend", backend)

	// Bidirectional copy.
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, peekConn); done <- struct{}{} }()
	go func() { _, _ = io.Copy(peekConn, upstream); done <- struct{}{} }()
	<-done
}

func isTrustdService(svc *corev1.Service) bool {
	return labels.Set(svc.Labels).Has(LabelTrustd) &&
		svc.Labels[LabelTrustd] == "true"
}

func trustdPort(svc *corev1.Service) int32 {
	for _, p := range svc.Spec.Ports {
		if p.Port == 50001 {
			return p.Port
		}
	}
	if len(svc.Spec.Ports) > 0 {
		return svc.Spec.Ports[0].Port
	}
	return 50001
}

func toService(obj interface{}) (*corev1.Service, bool) {
	svc, ok := obj.(*corev1.Service)
	if ok {
		return svc, true
	}
	// Handle DeletedFinalStateUnknown tombstone.
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}
	svc, ok = tombstone.Obj.(*corev1.Service)
	return svc, ok
}
