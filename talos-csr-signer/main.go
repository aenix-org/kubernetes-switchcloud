package main

import (
	"context"
	"crypto/tls"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/aenix-org/kubernetes-switchcloud/talos-csr-signer/internal/ca"
	"github.com/aenix-org/kubernetes-switchcloud/talos-csr-signer/internal/server"
)

func main() {
	addr := flag.String("addr", ":50001", "gRPC listen address")
	bundleDir := flag.String("talos-bundle", "/secrets/bundle", "path to directory containing Talos secrets bundle file")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	loader, err := ca.NewLoader(*bundleDir)
	if err != nil {
		log.Error("failed to load Talos CA", "err", err)
		os.Exit(1)
	}

	tlsCert, _, err := loader.TLSCredentials()
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

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("grpc serve error", "err", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	grpcServer.GracefulStop()
}
