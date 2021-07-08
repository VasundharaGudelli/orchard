package grpchandlers

import (
	"context"

	"github.com/loupe-co/orchard/clients"
	"github.com/loupe-co/orchard/config"
	"google.golang.org/grpc"
)

type OrchardGRPCServer struct {
	cfg          config.Config
	tenantClient *clients.TenantClient
	crmClient    *clients.CRMClient
}

func NewOrchardGRPCServer(cfg config.Config, tenantClient *clients.TenantClient, crmClient *clients.CRMClient) *OrchardGRPCServer {
	return &OrchardGRPCServer{cfg: cfg, tenantClient: tenantClient, crmClient: crmClient}
}

func grpcError(ctx context.Context, err error) error {
	return grpc.Errorf(grpc.Code(err), "Error handling request: %v", err)
}
