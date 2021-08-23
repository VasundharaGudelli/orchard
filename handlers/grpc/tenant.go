package grpchandlers

import (
	"context"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/db"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (server *OrchardGRPCServer) GetGroupSyncSettings(ctx context.Context, in *servicePb.GetGroupSyncSettingsRequest) (*servicePb.GetGroupSyncSettingsResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetGroupSyncSettings")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	svc := db.NewTenantService()

	tenant, err := svc.GetByID(spanCtx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error getting tenant from postgres")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	tenantProto, err := svc.ToProto(tenant)
	if err != nil {
		err := errors.Wrap(err, "error converting tenant model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.GetGroupSyncSettingsResponse{
		Status:   tenantProto.GroupSyncState,
		Metadata: tenantProto.GroupSyncMetadata,
	}, nil
}
