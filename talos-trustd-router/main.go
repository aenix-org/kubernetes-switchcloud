package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/aenix-org/kubernetes-switchcloud/talos-trustd-router/internal/sniproxy"
)

func main() {
	addr := flag.String("addr", ":50001", "TCP listen address")
	kubeconfig := flag.String("kubeconfig", "", "path to kubeconfig (empty = in-cluster)")
	syncNamespace := flag.String("sync-namespace", "", "namespace of the Service to sync externalIPs (empty = disabled)")
	syncService := flag.String("sync-service", "talos-trustd-router", "name of the Service to sync externalIPs")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

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

	router, err := sniproxy.NewRouter(ctx, client, log)
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

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("listen", "addr", *addr, "err", err)
		os.Exit(1)
	}

	log.Info("talos-trustd-router starting", "addr", *addr)

	go func() {
		<-ctx.Done()
		lis.Close()
	}()

	for {
		conn, err := lis.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				log.Error("accept error", "err", err)
				continue
			}
		}
		go router.Handle(conn)
	}
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}
