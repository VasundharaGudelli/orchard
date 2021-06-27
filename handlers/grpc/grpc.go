package grpchandlers

import (
	"context"

	"github.com/loupe-co/orchard/config"
	"google.golang.org/grpc"
)

type OrchardGRPCServer struct {
	cfg config.Config
}

func NewOrchardGRPCServer(cfg config.Config) *OrchardGRPCServer {
	return &OrchardGRPCServer{cfg: cfg}
}

func grpcError(ctx context.Context, err error) error {
	return grpc.Errorf(grpc.Code(err), "Error handling request: %v", err)
}
