// Package server implements the Talos SecurityService gRPC server.
package server

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	securityapi "github.com/siderolabs/talos/pkg/machinery/api/security"

	"github.com/aenix-org/kubernetes-switchcloud/talos-csr-signer/internal/ca"
)

// Server implements securityapi.SecurityServiceServer.
type Server struct {
	securityapi.UnimplementedSecurityServiceServer
	loader *ca.Loader
	log    *slog.Logger
}

// New creates a new Server.
func New(loader *ca.Loader, log *slog.Logger) *Server {
	return &Server{loader: loader, log: log}
}

// Register registers the server on a gRPC server instance.
func (s *Server) Register(srv *grpc.Server) {
	securityapi.RegisterSecurityServiceServer(srv, s)
}

// Certificate signs the provided CSR with the Talos CA and returns the signed cert.
func (s *Server) Certificate(ctx context.Context, req *securityapi.CertificateRequest) (*securityapi.CertificateResponse, error) {
	peerInfo, _ := peer.FromContext(ctx)
	addr := ""
	if peerInfo != nil {
		addr = peerInfo.Addr.String()
	}

	s.log.Info("certificate signing request", "peer", addr)

	if len(req.Csr) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty CSR")
	}

	signed, err := s.loader.SignCSR(req.Csr)
	if err != nil {
		s.log.Error("failed to sign CSR", "peer", addr, "err", err)
		return nil, status.Errorf(codes.Internal, "sign CSR: %v", err)
	}

	s.log.Info("certificate signed successfully", "peer", addr)

	return &securityapi.CertificateResponse{
		Ca:  s.loader.CACertPEM(),
		Crt: signed,
	}, nil
}
