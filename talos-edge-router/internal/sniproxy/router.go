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
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	// AnnotationHostname is the cluster FQDN this backend handles.
	AnnotationHostname = "talos.aenix.io/cluster-hostname"

	dialTimeout = 10 * time.Second
)

// Listener describes one TCP listener and the label / port it routes for.
type Listener struct {
	// Name is a human-readable id used in logs and as the default target
	// port name on backend Services.
	Name string
	// Addr is the listen address (e.g. ":50001").
	Addr string
	// Label is the Service label key whose value "true" opts a Service
	// into routing for this Listener.
	Label string
	// TargetPort is the port name on the backend Service. Defaults to Name.
	TargetPort string
}

func (l Listener) targetPortName() string {
	if l.TargetPort != "" {
		return l.TargetPort
	}
	return l.Name
}

// Router watches Kubernetes Services and maintains a per-label routing table
// keyed by TLS SNI hostname.
type Router struct {
	mu sync.RWMutex
	// backends is keyed by label, then by SNI hostname.
	backends map[string]map[string]string
	// portByLabel is the target port name to look up on backend Services
	// for that label.
	portByLabel map[string]string
	log         *slog.Logger
}

// NewRouter starts informers and returns a Router that tracks Services for
// each of the listeners' labels. listeners must be non-empty.
func NewRouter(ctx context.Context, client kubernetes.Interface, listeners []Listener, log *slog.Logger) (*Router, error) {
	if len(listeners) == 0 {
		return nil, fmt.Errorf("at least one listener required")
	}

	r := &Router{
		backends:    make(map[string]map[string]string),
		portByLabel: make(map[string]string),
		log:         log,
	}
	for _, l := range listeners {
		r.backends[l.Label] = make(map[string]string)
		r.portByLabel[l.Label] = l.targetPortName()
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
	hostname := svc.Annotations[AnnotationHostname]
	if hostname == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for label, table := range r.backends {
		if svc.Labels[label] != "true" {
			delete(table, hostname)
			continue
		}
		portName := r.portByLabel[label]
		port := servicePort(svc, portName)
		if port == 0 {
			r.log.Warn("backend matches label but has no matching port", "service", svc.Name, "namespace", svc.Namespace, "label", label, "expectedPort", portName)
			delete(table, hostname)
			continue
		}
		backend := fmt.Sprintf("%s.%s.svc:%d", svc.Name, svc.Namespace, port)
		if existing, ok := table[hostname]; !ok || existing != backend {
			table[hostname] = backend
			r.log.Info("registered backend", "label", label, "hostname", hostname, "backend", backend)
		}
	}
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
	defer r.mu.Unlock()
	for label, table := range r.backends {
		if _, ok := table[hostname]; ok {
			delete(table, hostname)
			r.log.Info("removed backend", "label", label, "hostname", hostname)
		}
	}
}

// Backend returns the upstream host:port for the given listener label and SNI.
func (r *Router) Backend(label, hostname string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	table, ok := r.backends[label]
	if !ok {
		return "", false
	}
	b, ok := table[hostname]
	return b, ok
}

func (r *Router) count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	total := 0
	for _, t := range r.backends {
		total += len(t)
	}
	return total
}

// Handle accepts a TCP connection, reads SNI, and proxies to the backend
// resolved by the given listener label.
func (r *Router) Handle(listenerName, label string, conn net.Conn) {
	defer conn.Close()

	sni, peekConn, err := ReadSNI(conn)
	if err != nil {
		r.log.Warn("failed to read SNI", "listener", listenerName, "peer", conn.RemoteAddr(), "err", err)
		return
	}

	backend, ok := r.Backend(label, sni)
	if !ok {
		r.log.Warn("no backend for SNI", "listener", listenerName, "label", label, "sni", sni, "peer", conn.RemoteAddr())
		return
	}

	upstream, err := net.DialTimeout("tcp", backend, dialTimeout)
	if err != nil {
		r.log.Error("dial backend failed", "listener", listenerName, "backend", backend, "sni", sni, "err", err)
		return
	}
	defer upstream.Close()

	r.log.Info("proxying connection", "listener", listenerName, "sni", sni, "peer", conn.RemoteAddr(), "backend", backend)

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, peekConn); done <- struct{}{} }()
	go func() { _, _ = io.Copy(peekConn, upstream); done <- struct{}{} }()
	<-done
}

func servicePort(svc *corev1.Service, name string) int32 {
	for _, p := range svc.Spec.Ports {
		if p.Name == name {
			return p.Port
		}
	}
	return 0
}

func toService(obj interface{}) (*corev1.Service, bool) {
	svc, ok := obj.(*corev1.Service)
	if ok {
		return svc, true
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}
	svc, ok = tombstone.Obj.(*corev1.Service)
	return svc, ok
}
