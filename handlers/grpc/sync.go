package grpchandlers

import (
	"context"

	"github.com/loupe-co/go-loupe-logger/log"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (server *OrchardGRPCServer) Sync(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "Sync")
	defer span.End()

	if _, err := server.SyncCrmRoles(spanCtx, in); err != nil {
		return nil, err
	}

	if _, err := server.SyncGroups(spanCtx, in); err != nil {
		return nil, err
	}

	if _, err := server.SyncUsers(spanCtx, in); err != nil {
		return nil, err
	}

	if _, err := server.UpdateGroupTypes(spanCtx, &servicePb.UpdateGroupTypesRequest{TenantId: in.TenantId}); err != nil {
		return nil, err
	}

	return &servicePb.SyncResponse{}, nil
}
