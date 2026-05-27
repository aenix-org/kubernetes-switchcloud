package sniproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// AnnotationExtraIPs holds additional IPs (e.g. cloud public IPs) that are not
// present in Node.status.addresses. Comma-separated list.
// Example: talos.aenix.io/extra-ips: "129.153.33.233,150.136.153.190"
const AnnotationExtraIPs = "talos.aenix.io/extra-ips"

// SyncDebounceInterval is the minimum spacing between Service externalIPs
// patches. Cloud-controller-managers that flap node addresses every few
// seconds would otherwise churn the Service and force Cilium to reprogram
// its BPF rules continuously, tearing down long-lived gRPC streams. New
// nodes join rarely enough that 15s lag is fine.
const SyncDebounceInterval = 30 * time.Second

// RunIPSyncer watches Node objects and keeps the named Service's externalIPs
// in sync with the union of all node InternalIP, ExternalIP, and extra-IPs annotations.
// Updates are rate-limited to at most one Service patch per SyncDebounceInterval.
// It blocks until ctx is cancelled.
func RunIPSyncer(ctx context.Context, client kubernetes.Interface, namespace, serviceName string, log *slog.Logger) error {
	factory := informers.NewSharedInformerFactory(client, 30*time.Second)
	nodeLister := factory.Core().V1().Nodes().Lister()
	nodeInformer := factory.Core().V1().Nodes().Informer()

	// Coalesce node events through a 1-slot trigger channel: many events
	// during a flap collapse into a single sync; the goroutine below enforces
	// the minimum interval between actual Service patches.
	trigger := make(chan struct{}, 1)
	enqueue := func(_ interface{}) {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}

	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    enqueue,
		UpdateFunc: func(_, obj interface{}) { enqueue(obj) },
		DeleteFunc: enqueue,
	})

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), nodeInformer.HasSynced) {
		return fmt.Errorf("timed out waiting for node cache sync")
	}

	doSync := func() {
		if err := syncExternalIPs(ctx, client, nodeLister, namespace, serviceName, log); err != nil {
			log.Error("failed to sync externalIPs", "err", err)
		}
	}

	doSync()
	lastSync := time.Now()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-trigger:
			elapsed := time.Since(lastSync)
			if elapsed < SyncDebounceInterval {
				timer := time.NewTimer(SyncDebounceInterval - elapsed)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil
				case <-timer.C:
				}
				// Drain any additional events that piled up during the wait so
				// the next iteration acts on a fresh state.
				select {
				case <-trigger:
				default:
				}
			}
			doSync()
			lastSync = time.Now()
		}
	}
}

func syncExternalIPs(ctx context.Context, client kubernetes.Interface, lister listersv1.NodeLister, namespace, serviceName string, log *slog.Logger) error {
	nodes, err := lister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}

	seen := map[string]struct{}{}
	var ips []string

	// Include both InternalIP and ExternalIP. Dropping ExternalIP makes
	// Cilium fail to advertise Service.spec.externalIPs on the host's
	// public NIC (verified empirically: external probes returned
	// ECONNREFUSED on port 8132 with InternalIP-only). The cloud-controller-
	// manager flaps NodeExternalIP every couple of seconds on this cluster,
	// which still churns Service.spec.externalIPs and disrupts long-lived
	// streams — accepted as a separate problem to solve.
	for _, node := range nodes {
		for _, addr := range node.Status.Addresses {
			if addr.Type != corev1.NodeInternalIP && addr.Type != corev1.NodeExternalIP {
				continue
			}
			if _, ok := seen[addr.Address]; !ok {
				seen[addr.Address] = struct{}{}
				ips = append(ips, addr.Address)
			}
		}
		if extra := node.Annotations[AnnotationExtraIPs]; extra != "" {
			for _, ip := range strings.Split(extra, ",") {
				ip = strings.TrimSpace(ip)
				if ip == "" {
					continue
				}
				if _, ok := seen[ip]; !ok {
					seen[ip] = struct{}{}
					ips = append(ips, ip)
				}
			}
		}
	}
	sort.Strings(ips)

	svc, err := client.CoreV1().Services(namespace).Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get service: %w", err)
	}

	// LoadBalancer Services have their public IPs managed by an external LB
	// controller (MetalLB, a cloud LB). Writing to spec.externalIPs from here
	// would fight that controller, churn the Service every reconcile, and on
	// MetalLB IP-sharing setups would silently break the shared-IP guard.
	// The operator opts into this mode by setting service.type=LoadBalancer
	// in the chart values.
	if svc.Spec.Type == corev1.ServiceTypeLoadBalancer {
		log.Debug("service is LoadBalancer, skipping externalIPs sync",
			"service", serviceName, "lb-ingress", svc.Status.LoadBalancer.Ingress)
		return nil
	}

	current := make([]string, len(svc.Spec.ExternalIPs))
	copy(current, svc.Spec.ExternalIPs)
	sort.Strings(current)

	if reflect.DeepEqual(current, ips) {
		return nil
	}

	raw, err := json.Marshal(ips)
	if err != nil {
		return fmt.Errorf("marshal ips: %w", err)
	}
	patch := fmt.Sprintf(`{"spec":{"externalIPs":%s}}`, raw)

	if _, err = client.CoreV1().Services(namespace).Patch(ctx, serviceName, types.MergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("patch service: %w", err)
	}

	log.Info("synced service externalIPs", "service", serviceName, "ips", ips)
	return nil
}
