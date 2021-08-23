package grpchandlers

import (
	"context"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/db"
	tenantPb "github.com/loupe-co/protos/src/common/tenant"
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

func (server *OrchardGRPCServer) ReSyncCRM(ctx context.Context, in *servicePb.ReSyncCRMRequest) (*servicePb.ReSyncCRMResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "ReSyncCRM")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	groupSvc := db.NewGroupService()
	tenantSvc := db.NewTenantService()

	if err := groupSvc.WithTransaction(spanCtx); err != nil {
		err := errors.Wrap(err, "error creating transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}
	tenantSvc.WithTransaction(spanCtx, groupSvc.GetTX()) // Don't need to handle error, as the we bypass error case when passing tx manually

	fullSynced, err := groupSvc.IsCRMSynced(spanCtx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error checking current hierarchy sync state")
		logger.Error(err)
		groupSvc.Rollback() // Haven't really done anything yet, so not handling error
		return nil, err.AsGRPC()
	}
	if fullSynced { // Attempt to bypass other processes if we're already in a full synced state
		if _, err := server.Sync(spanCtx, &servicePb.SyncRequest{TenantId: in.TenantId}); err != nil {
			err := errors.Wrap(err, "error syncing crm data")
			logger.Error(err)
			groupSvc.Rollback() // Haven't really done anything yet, so not handling error
			return nil, err.AsGRPC()
		}
		return &servicePb.ReSyncCRMResponse{Status: tenantPb.GroupSyncStatus_Active}, nil
	}

	if err := groupSvc.DeleteAllTenantGroups(spanCtx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error deleting existing tenant groups in sql")
		logger.Error(err)
		if err := groupSvc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	if err := groupSvc.RemoveAllGroupMembers(spanCtx, in.TenantId, "00000000-0000-0000-0000-000000000000"); err != nil {
		err := errors.Wrap(err, "error removing all group members for tenant")
		logger.Error(err)
		if err := groupSvc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	if err := tenantSvc.UpdateGroupSyncState(spanCtx, in.TenantId, tenantPb.GroupSyncStatus_Active); err != nil {
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

	if _, err := server.Sync(spanCtx, &servicePb.SyncRequest{TenantId: in.TenantId}); err != nil {
		err := errors.Wrap(err, "error syncing crm data")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.ReSyncCRMResponse{Status: tenantPb.GroupSyncStatus_Active}, nil
}
