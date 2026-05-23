package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/aenix-org/kubernetes-switchcloud/talos-edge-router/internal/sniproxy"
)

// listenerFlag implements flag.Value to collect repeatable --listener entries.
//
// Format: name:port:label[:targetPort]
//
//	name        — listener id (also default target port name on backend Service)
//	port        — TCP listen port (listener binds on 0.0.0.0:<port>)
//	label       — Service label key that opts a Service into this listener
//	targetPort  — optional override of the port name on backend Services
type listenerFlag []sniproxy.Listener

func (l *listenerFlag) String() string {
	parts := make([]string, 0, len(*l))
	for _, lis := range *l {
		parts = append(parts, fmt.Sprintf("%s=%s/%s", lis.Name, lis.Addr, lis.Label))
	}
	return strings.Join(parts, ",")
}

func (l *listenerFlag) Set(v string) error {
	parts := strings.SplitN(v, ":", 4)
	if len(parts) < 3 {
		return fmt.Errorf("listener must be name:port:label[:targetPort], got %q", v)
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil || port <= 0 || port > 65535 {
		return fmt.Errorf("listener %q has invalid port %q", parts[0], parts[1])
	}
	lis := sniproxy.Listener{Name: parts[0], Addr: fmt.Sprintf(":%d", port), Label: parts[2]}
	if len(parts) == 4 {
		lis.TargetPort = parts[3]
	}
	if lis.Name == "" || lis.Label == "" {
		return fmt.Errorf("listener fields must be non-empty: %q", v)
	}
	*l = append(*l, lis)
	return nil
}

func main() {
	var listeners listenerFlag
	flag.Var(&listeners, "listener", "repeatable listener spec name:port:label[:targetPort]; e.g. trustd:50001:talos.aenix.io/trustd")
	kubeconfig := flag.String("kubeconfig", "", "path to kubeconfig (empty = in-cluster)")
	syncNamespace := flag.String("sync-namespace", "", "namespace of the Service to sync externalIPs (empty = disabled)")
	syncService := flag.String("sync-service", "talos-edge-router", "name of the Service to sync externalIPs")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if len(listeners) == 0 {
		log.Error("at least one --listener flag required")
		os.Exit(2)
	}

	cfg, err := buildConfig(*kubeconfig)
	if err != nil {
		log.Error("build kubeconfig", "err", err)
		os.Exit(1)
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Error("create k8s client", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	router, err := sniproxy.NewRouter(ctx, client, listeners, log)
	if err != nil {
		log.Error("create router", "err", err)
		os.Exit(1)
	}

	if *syncNamespace != "" {
		go func() {
			if err := sniproxy.RunIPSyncer(ctx, client, *syncNamespace, *syncService, log); err != nil {
				log.Error("ip syncer error", "err", err)
			}
		}()
	}

	var wg sync.WaitGroup
	for _, lis := range listeners {
		wg.Add(1)
		go func(lis sniproxy.Listener) {
			defer wg.Done()
			serve(ctx, lis, router, log)
		}(lis)
	}
	wg.Wait()
}

func serve(ctx context.Context, lis sniproxy.Listener, router *sniproxy.Router, log *slog.Logger) {
	l, err := net.Listen("tcp", lis.Addr)
	if err != nil {
		log.Error("listen failed", "listener", lis.Name, "addr", lis.Addr, "err", err)
		return
	}
	log.Info("listener starting", "name", lis.Name, "addr", lis.Addr, "label", lis.Label)

	go func() {
		<-ctx.Done()
		l.Close()
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				log.Error("accept error", "listener", lis.Name, "err", err)
				continue
			}
		}
		go router.Handle(lis.Name, lis.Label, conn)
	}
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}
