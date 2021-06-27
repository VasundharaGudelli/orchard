package grpchandlers

import (
	"context"

	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (server *OrchardGRPCServer) CreateSystemRole(ctx context.Context, in *servicePb.CreateSystemRoleRequest) (*servicePb.CreateSystemRoleResponse, error) {
	return nil, nil
}
