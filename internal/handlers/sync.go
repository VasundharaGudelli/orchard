package handlers

import (
	"context"
	"strings"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/db"
	tenantPb "github.com/loupe-co/protos/src/common/tenant"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"google.golang.org/grpc/codes"
)

func (h *Handlers) Sync(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "Sync")
	defer span.End()

	if strings.Contains(in.TenantId, "create_and_close") || in.LicenseType == tenantPb.LicenseType_LICENSE_TYPE_CREATE_AND_CLOSE || in.LicenseType == tenantPb.LicenseType_LICENSE_TYPE_CREATE_AND_CLOSE_HYBRID {
		if _, err := h.SyncUsers(spanCtx, in); err != nil {
			return nil, err
		}
		return &servicePb.SyncResponse{}, nil
	}

	if _, err := h.SyncCrmRoles(spanCtx, in); err != nil {
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

	// Get the tenant record so we know the license type
	tenantSvc := h.db.NewTenantService()
	tenantDB, err := tenantSvc.GetByID(ctx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error getting tenant record")
		logger.Error(err)
		return nil, err.AsGRPC()
	}
	tenant, err := tenantSvc.ToProto(tenantDB)
	if err != nil {
		err := errors.Wrap(err, "error converting tenant db model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error creating transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	groupSvc := h.db.NewGroupService()
	groupSvc.SetTransaction(tx)
	tenantSvc.SetTransaction(tx)

	fullSynced, err := groupSvc.IsCRMSynced(ctx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error checking current hierarchy sync state")
		logger.Error(err)
		groupSvc.Rollback() // Haven't really done anything yet, so not handling error
		return nil, err.AsGRPC()
	}
	if fullSynced { // Attempt to bypass other processes if we're already in a full synced state
		logger.Info("already fully synced, running sync")
		if _, err := h.Sync(ctx, &servicePb.SyncRequest{TenantId: in.TenantId, UpdatePersonGroups: true, LicenseType: tenant.LicenseType}); err != nil {
			err := errors.Wrap(err, "error syncing crm data")
			logger.Error(err)
			groupSvc.Rollback() // Haven't really done anything yet, so not handling error
			return nil, err.AsGRPC()
		}
		return &servicePb.ReSyncCRMResponse{Status: tenantPb.GroupSyncStatus_Active}, nil
	}

	logger.Info("not fully synced, re-syncing")

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

	// If c&c customer, do another c&c sync run
	if tenant.LicenseType == tenantPb.LicenseType_LICENSE_TYPE_CREATE_AND_CLOSE || tenant.LicenseType == tenantPb.LicenseType_LICENSE_TYPE_CREATE_AND_CLOSE_HYBRID {
		if _, err := h.Sync(ctx, &servicePb.SyncRequest{TenantId: in.TenantId, UpdatePersonGroups: true, LicenseType: tenant.LicenseType}); err != nil {
			err := errors.Wrap(err, "error syncing C&C crm data")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
	}

	return &servicePb.ReSyncCRMResponse{Status: tenantPb.GroupSyncStatus_Active}, nil
}

func parseSyncLicense(in *servicePb.SyncRequest) (tenantID string, license tenantPb.LicenseType) {
	license = in.LicenseType

	idParts := strings.Split(in.TenantId, "::")
	tenantID = idParts[0]
	if len(idParts) > 1 {
		switch strings.ToLower(idParts[1]) {
		case "create_and_close":
			license = tenantPb.LicenseType_LICENSE_TYPE_CREATE_AND_CLOSE
		case "create_and_close_hybrid":
			license = tenantPb.LicenseType_LICENSE_TYPE_CREATE_AND_CLOSE_HYBRID
		default:
			license = tenantPb.LicenseType_LICENSE_TYPE_COMMIT_STANDALONE
		}
	}

	return
}
