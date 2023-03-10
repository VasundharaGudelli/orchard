package handlers

import (
	"context"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/db"
	tenantPb "github.com/loupe-co/protos/src/common/tenant"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"google.golang.org/grpc/codes"
)

func (h *Handlers) Sync(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	if _, err := h.SyncCrmRoles(ctx, in); err != nil {
		return nil, err
	}

	if _, err := h.SyncGroups(ctx, in); err != nil {
		return nil, err
	}

	if _, err := h.SyncUsers(ctx, in); err != nil {
		return nil, err
	}

	if _, err := h.UpdateGroupTypes(ctx, &servicePb.UpdateGroupTypesRequest{TenantId: in.TenantId}); err != nil {
		return nil, err
	}

	return &servicePb.SyncResponse{}, nil
}

func (h *Handlers) ReSyncCRM(ctx context.Context, in *servicePb.ReSyncCRMRequest) (*servicePb.ReSyncCRMResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	logger.Info("Re-Syncing CRM for tenant")

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error creating transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	groupSvc := h.db.NewGroupService()
	groupSvc.SetTransaction(tx)
	tenantSvc := h.db.NewTenantService()
	tenantSvc.SetTransaction(tx)

	fullSynced, err := groupSvc.IsCRMSynced(ctx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error checking current hierarchy sync state")
		logger.Error(err)
		groupSvc.Rollback() // Haven't really done anything yet, so not handling error
		return nil, err.AsGRPC()
	}
	if fullSynced { // Attempt to bypass other processes if we're already in a full synced state
		if _, err := h.Sync(ctx, &servicePb.SyncRequest{TenantId: in.TenantId}); err != nil {
			err := errors.Wrap(err, "error syncing crm data")
			logger.Error(err)
			groupSvc.Rollback() // Haven't really done anything yet, so not handling error
			return nil, err.AsGRPC()
		}
		return &servicePb.ReSyncCRMResponse{Status: tenantPb.GroupSyncStatus_Active}, nil
	}

	// Check to make sure all the tenant's groups are currently delete before resyncing, because apparently that's an issue?
	groupCount, err := groupSvc.GetTenantGroupCount(ctx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error getting count of tenant's groups")
		logger.Error(err)
		if err := groupSvc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}
	if groupCount > 0 {
		err := errors.New("hierarchy must be reset before switching back to full sync").WithCode(codes.FailedPrecondition)
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	// Delete all the tenant's groups just in case
	if err := groupSvc.DeleteAllTenantGroups(ctx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error deleting existing tenant groups in sql")
		logger.Error(err)
		if err := groupSvc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	if err := groupSvc.RemoveAllGroupMembers(ctx, in.TenantId, db.DefaultTenantID); err != nil {
		err := errors.Wrap(err, "error removing all group members for tenant")
		logger.Error(err)
		if err := groupSvc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	if err := tenantSvc.UpdateGroupSyncState(ctx, in.TenantId, tenantPb.GroupSyncStatus_Active); err != nil {
		err := errors.Wrap(err, "error updating tenant group sync state in sql")
		logger.Error(err)
		if err := groupSvc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	if err := groupSvc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting transaction")
		logger.Error(err)
		if err := groupSvc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	if _, err := h.Sync(ctx, &servicePb.SyncRequest{TenantId: in.TenantId}); err != nil {
		err := errors.Wrap(err, "error syncing crm data")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.ReSyncCRMResponse{Status: tenantPb.GroupSyncStatus_Active}, nil
}
