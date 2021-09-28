package handlers

import (
	"context"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (h *Handlers) IsHierarchySynced(ctx context.Context, in *servicePb.IsHierarchySyncedRequest) (*servicePb.IsHierarchySyncedResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "IsHierarchySynced")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.String())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()

	isSynced, err := svc.IsCRMSynced(spanCtx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error checking if tenant crm roles are synced with groups")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.IsHierarchySyncedResponse{IsSynced: isSynced}, nil
}
