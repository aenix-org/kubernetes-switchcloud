package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/aenix-org/kubernetes-switchcloud/talos-csr-signer/internal/bootstrap"
	"github.com/aenix-org/kubernetes-switchcloud/talos-csr-signer/internal/ca"
	"github.com/aenix-org/kubernetes-switchcloud/talos-csr-signer/internal/server"
)

func main() {
	addr := flag.String("addr", ":50001", "gRPC listen address")
	bundleDir := flag.String("talos-bundle", "/secrets/bundle", "path to directory containing Talos secrets bundle file")
	clusterHostname := flag.String("cluster-hostname", "", "cluster API hostname included in TLS SAN (e.g. mycluster.example.org)")
	workloadKubeconfig := flag.String("workload-kubeconfig", "", "path to workload cluster kubeconfig for bootstrap token management (optional)")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	loader, err := ca.NewLoader(*bundleDir)
	if err != nil {
		log.Error("failed to load Talos CA", "err", err)
		os.Exit(1)
	}

	tlsCert, _, err := loader.TLSCredentials(*clusterHostname)
	if err != nil {
		log.Error("failed to build TLS credentials", "err", err)
		os.Exit(1)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		// Workers connect without client cert during bootstrap.
		ClientAuth: tls.NoClientCert,
		MinVersion: tls.VersionTLS12,
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("failed to listen", "addr", *addr, "err", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsCfg)),
	)

	svc := server.New(loader, log)
	svc.Register(grpcServer)

	log.Info("talos-csr-signer starting", "addr", *addr)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if *workloadKubeconfig != "" {
		if err := ensureBootstrapToken(ctx, *bundleDir, *workloadKubeconfig, log); err != nil {
			log.Warn("initial bootstrap token setup failed, will retry", "err", err)
		}
		go runTokenLoop(ctx, *bundleDir, *workloadKubeconfig, log)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("grpc serve error", "err", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	grpcServer.GracefulStop()
}

// runTokenLoop calls ensureBootstrapToken every minute so that if the workload
// cluster is recreated (and its bootstrap-token secret is wiped), the token is
// re-created automatically without restarting this pod.
func runTokenLoop(ctx context.Context, bundleDir, kubeconfigPath string, log *slog.Logger) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := ensureBootstrapToken(ctx, bundleDir, kubeconfigPath, log); err != nil {
				log.Warn("failed to ensure bootstrap token", "err", err)
			}
		}
	}
}

func ensureBootstrapToken(ctx context.Context, bundleDir, kubeconfigPath string, log *slog.Logger) error {
	token, err := bootstrap.TokenFromBundle(bundleDir)
	if err != nil {
		return err
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return fmt.Errorf("build kubeconfig: %w", err)
	}

	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("create k8s client: %w", err)
	}

	return bootstrap.EnsureToken(ctx, cs, token, log)
}
