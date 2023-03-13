package handlers

import (
	"context"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (h *Handlers) IsHierarchySynced(ctx context.Context, in *servicePb.IsHierarchySyncedRequest) (*servicePb.IsHierarchySyncedResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.String())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()

	isSynced, err := svc.IsCRMSynced(ctx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error checking if tenant crm roles are synced with groups")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.IsHierarchySyncedResponse{IsSynced: isSynced}, nil
}
