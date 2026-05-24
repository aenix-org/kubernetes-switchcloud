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

// RunIPSyncer watches Node objects and keeps the named Service's externalIPs
// in sync with the union of all node InternalIP, ExternalIP, and extra-IPs annotations.
// It blocks until ctx is cancelled.
func RunIPSyncer(ctx context.Context, client kubernetes.Interface, namespace, serviceName string, log *slog.Logger) error {
	factory := informers.NewSharedInformerFactory(client, 30*time.Second)
	nodeLister := factory.Core().V1().Nodes().Lister()
	nodeInformer := factory.Core().V1().Nodes().Informer()

	sync := func(_ interface{}) {
		if err := syncExternalIPs(ctx, client, nodeLister, namespace, serviceName, log); err != nil {
			log.Error("failed to sync externalIPs", "err", err)
		}
	}

	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sync,
		UpdateFunc: func(_, obj interface{}) { sync(obj) },
		DeleteFunc: sync,
	})

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), nodeInformer.HasSynced) {
		return fmt.Errorf("timed out waiting for node cache sync")
	}

	// Initial sync after cache is warm.
	sync(nil)

	<-ctx.Done()
	return nil
}

func syncExternalIPs(ctx context.Context, client kubernetes.Interface, lister listersv1.NodeLister, namespace, serviceName string, log *slog.Logger) error {
	nodes, err := lister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}

	seen := map[string]struct{}{}
	var ips []string

	// Node.status NodeExternalIP flaps on this cluster — the cloud-controller-
	// manager updates it every couple of seconds, which churned
	// Service.spec.externalIPs and forced Cilium to reprogram BPF rules
	// continuously, tearing down active konnectivity gRPC streams. NodeInternalIP
	// is stable, and Cilium needs to see at least one IP that lives on the
	// node's NIC to advertise externalIPs there — so keep InternalIP and the
	// stable talos.aenix.io/extra-ips annotation, drop ExternalIP.
	for _, node := range nodes {
		for _, addr := range node.Status.Addresses {
			if addr.Type != corev1.NodeInternalIP {
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
