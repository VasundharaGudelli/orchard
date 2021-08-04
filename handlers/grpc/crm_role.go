package grpchandlers

import (
	"context"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/db"
	"github.com/loupe-co/orchard/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (server *OrchardGRPCServer) SyncCrmRoles(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "SyncCrmRoles")
	defer span.End()

	syncSince := in.SyncSince.AsTime()

	logger := log.WithTenantID(in.TenantId).WithCustom("syncSince", syncSince)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	latestCRMRoles, err := server.crmClient.GetLatestCRMRoles(spanCtx, in.TenantId, in.SyncSince)
	if err != nil {
		err := errors.Wrap(err, "error getting latest crm roles from crm-data-access")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if len(latestCRMRoles) == 0 {
		logger.Info("no new crm_roles to sync")
		return &servicePb.SyncResponse{}, nil
		// return nil, errors.Error("no latest crm_roles returned from crm-data-access")
	}

	svc := db.NewCRMRoleService()
	if err := svc.WithTransaction(spanCtx); err != nil {
		err := errors.Wrap(err, "error starting sync_crm_roles transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	ids := make([]interface{}, len(latestCRMRoles))
	dbCRMRoles := make([]*models.CRMRole, len(latestCRMRoles))
	for i, role := range latestCRMRoles {
		ids[i] = role.Id
		dbCRMRoles[i] = svc.FromProto(role)
	}

	if err := svc.UpsertAll(spanCtx, dbCRMRoles); err != nil {
		err := errors.Wrap(err, "error upserting latest crm roles for sync")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := svc.DeleteUnSynced(spanCtx, in.TenantId, ids...); err != nil {
		err := errors.Wrap(err, "error deleting unsynced crm_roles")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting sync_crm_roles transaction")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	return &servicePb.SyncResponse{}, nil
}

func (server *OrchardGRPCServer) UpsertCRMRoles(ctx context.Context, in *servicePb.UpsertCRMRolesRequest) (*servicePb.UpsertCRMRolesResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpsertCRMRoles")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if len(in.CrmRoles) == 0 {
		return &servicePb.UpsertCRMRolesResponse{}, nil
	}

	svc := db.NewCRMRoleService()

	crmRoles := make([]*models.CRMRole, len(in.CrmRoles))
	for i, role := range in.CrmRoles {
		if role.TenantId == "" {
			role.TenantId = in.TenantId
		}
		crmRoles[i] = svc.FromProto(role)
	}

	if err := svc.UpsertAll(spanCtx, crmRoles); err != nil {
		err := errors.Wrap(err, "error upserting crmRoles in sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpsertCRMRolesResponse{}, nil
}

func (server *OrchardGRPCServer) GetCRMRoleById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.CRMRole, error) {
	spanCtx, span := log.StartSpan(ctx, "GetCRMRoleById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("id", in.Id)

	if in.Id == "" {
		err := ErrBadRequest.New("id can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := db.NewCRMRoleService()

	cr, err := svc.GetByID(spanCtx, in.Id, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error getting crmRole from sql by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	crmRole, err := svc.ToProto(cr)
	if err != nil {
		err := errors.Wrap(err, "error converting crmRole from db model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return crmRole, nil
}

func (server *OrchardGRPCServer) GetCRMRoles(ctx context.Context, in *servicePb.GetCRMRolesRequest) (*servicePb.GetCRMRolesResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetCRMRoles")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("search", in.Search)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := db.NewCRMRoleService()

	crs, err := svc.Search(spanCtx, in.TenantId, in.Search)
	if err != nil {
		err := errors.Wrap(err, "error getting crmRole from sql by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	crmRoles := make([]*orchardPb.CRMRole, len(crs))
	for i, cr := range crs {
		role, err := svc.ToProto(cr)
		if err != nil {
			err := errors.Wrap(err, "error converting crmRole from db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		crmRoles[i] = role
	}

	return &servicePb.GetCRMRolesResponse{CrmRoles: crmRoles}, nil
}

func (server *OrchardGRPCServer) GetUnsyncedCRMRoles(ctx context.Context, in *servicePb.GetUnsyncedCRMRolesRequest) (*servicePb.GetUnsyncedCRMRolesResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetCRMRoles")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := db.NewCRMRoleService()

	crs, err := svc.GetUnsynced(spanCtx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error getting unsynced crmRoles from sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	crmRoles := make([]*orchardPb.CRMRole, len(crs))
	for i, cr := range crs {
		role, err := svc.ToProto(cr)
		if err != nil {
			err := errors.Wrap(err, "error converting crmRole from db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		crmRoles[i] = role
	}

	return &servicePb.GetUnsyncedCRMRolesResponse{CrmRoles: crmRoles}, nil
}

func (server *OrchardGRPCServer) DeleteCRMRoleById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	spanCtx, span := log.StartSpan(ctx, "DeleteCRMRoleById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("id", in.Id)

	if in.Id == "" {
		err := ErrBadRequest.New("id can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := db.NewCRMRoleService()

	err := svc.DeleteByID(spanCtx, in.Id, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error deleting crmRole from sql by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.Empty{}, nil
}
