package handlers

import (
	"context"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (h *Handlers) GetGroupSyncSettings(ctx context.Context, in *servicePb.GetGroupSyncSettingsRequest) (*servicePb.GetGroupSyncSettingsResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetGroupSyncSettings")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	svc := h.db.NewTenantService()

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

func (h *Handlers) UpdateGroupSyncState(ctx context.Context, in *servicePb.UpdateGroupSyncStateRequest) (*servicePb.UpdateGroupSyncStateResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpdateGroupSyncState")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	svc := h.db.NewTenantService()

	err := svc.UpdateGroupSyncState(spanCtx, in.TenantId, in.Status)
	if err != nil {
		err := errors.Wrap(err, "error updating tenant group sync state")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpdateGroupSyncStateResponse{}, nil
}

func (h *Handlers) UpdateGroupSyncMetadata(ctx context.Context, in *servicePb.UpdateGroupSyncMetadataRequest) (*servicePb.UpdateGroupSyncMetadataResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpdateGroupSyncMetadata")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	svc := h.db.NewTenantService()

	err := svc.UpdateGroupSyncMetadata(spanCtx, in.TenantId, in.Metadata)
	if err != nil {
		err := errors.Wrap(err, "error updating tenant group sync metadata")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpdateGroupSyncMetadataResponse{}, nil
}
