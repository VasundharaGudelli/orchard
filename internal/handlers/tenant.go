package handlers

import (
	"context"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"google.golang.org/grpc/codes"
)

func (h *Handlers) GetGroupSyncSettings(ctx context.Context, in *servicePb.GetGroupSyncSettingsRequest) (*servicePb.GetGroupSyncSettingsResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	svc := h.db.NewTenantService()

	tenant, err := svc.GetByID(ctx, in.TenantId)
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
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	svc := h.db.NewTenantService()

	err := svc.UpdateGroupSyncState(ctx, in.TenantId, in.Status)
	if err != nil {
		err := errors.Wrap(err, "error updating tenant group sync state")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpdateGroupSyncStateResponse{}, nil
}

func (h *Handlers) UpdateGroupSyncMetadata(ctx context.Context, in *servicePb.UpdateGroupSyncMetadataRequest) (*servicePb.UpdateGroupSyncMetadataResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	svc := h.db.NewTenantService()

	err := svc.UpdateGroupSyncMetadata(ctx, in.TenantId, in.Metadata)
	if err != nil {
		err := errors.Wrap(err, "error updating tenant group sync metadata")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpdateGroupSyncMetadataResponse{}, nil
}

func (h *Handlers) GetTenantPersonCount(ctx context.Context, in *servicePb.GetTenantPersonCountRequest) (*servicePb.GetTenantPersonCountResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	svc := h.db.NewTenantService()

	counts, err := svc.GetTenantPersonCounts(ctx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error getting tenant person counts from db")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if counts == nil {
		return nil, errors.New("counts not found").WithCode(codes.NotFound).AsGRPC()
	}

	res := &servicePb.GetTenantPersonCountResponse{
		ActiveInGroup: counts.ActiveInGroup,
		Inactive:      counts.Inactive,
		Provisioned:   counts.Provisioned,
		Total:         counts.Total,
	}

	return res, nil
}
