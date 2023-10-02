package handlers

import (
	"context"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (h *Handlers) SyncCrmRoles(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	ctx, span := log.StartSpan(ctx, "SyncCrmRoles")
	defer span.End()

	syncSince := in.SyncSince.AsTime()

	logger := log.WithContext(ctx).
		WithTenantID(in.TenantId).
		WithCustom("syncSince", syncSince)

	logger.Info("begin SyncCrmRoles")

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error starting sync_crm_roles transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}
	svc := h.db.NewCRMRoleService()
	svc.SetTransaction(tx)

	var (
		batchSize = h.cfg.SyncRolesBatchSize
		total     int
		nextToken string
	)

	for {
		logger.Debug("getting next page of crm roles")
		var latestCRMRoles []*orchardPb.CRMRole
		latestCRMRoles, total, nextToken, err = h.crmClient.GetLatestCRMRoles(ctx, in.TenantId, in.SyncSince, batchSize, nextToken)
		if err != nil {
			err := errors.Wrap(err, "error getting latest crm roles from crm-data-access")
			logger.Error(err)
			return nil, err.Clean().AsGRPC()
		}

		if len(latestCRMRoles) == 0 {
			break
		}

		dbCRMRoles := make([]*models.CRMRole, len(latestCRMRoles))
		for i, role := range latestCRMRoles {
			dbCRMRoles[i] = svc.FromProto(role)
		}

		if err := svc.UpsertAll(ctx, dbCRMRoles); err != nil {
			err := errors.Wrap(err, "error upserting latest crm roles for sync")
			logger.Error(err)
			svc.Rollback()
			return nil, err.Clean().AsGRPC()
		}

		if nextToken == "" {
			break
		}
	}

	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting sync_crm_roles transaction")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	logger.WithCustom("total", total).Info("finish SyncCrmRoles")

	return &servicePb.SyncResponse{}, nil
}

func (h *Handlers) UpsertCRMRoles(ctx context.Context, in *servicePb.UpsertCRMRolesRequest) (*servicePb.UpsertCRMRolesResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpsertCRMRoles")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if len(in.CrmRoles) == 0 {
		return &servicePb.UpsertCRMRolesResponse{}, nil
	}

	svc := h.db.NewCRMRoleService()

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

func (h *Handlers) GetCRMRoleById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.CRMRole, error) {
	spanCtx, span := log.StartSpan(ctx, "GetCRMRoleById")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId).WithCustom("id", in.Id)

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

	svc := h.db.NewCRMRoleService()

	cr, err := svc.GetByID(spanCtx, in.Id, in.TenantId, in.IsOutreach)
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

func (h *Handlers) GetCRMRolesByIds(ctx context.Context, in *servicePb.IdsRequest) (*servicePb.GetCRMRolesByIdsResponse, error) {
	ctx, span := log.StartSpan(ctx, "GetCRMRolesByIds")
	defer span.End()

	logger := log.WithContext(ctx).
		WithTenantID(in.TenantId).
		WithCustom("ids", in.Ids)

	if len(in.Ids) == 0 {
		err := ErrBadRequest.New("ids can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewCRMRoleService()

	crs, err := svc.GetByIDs(ctx, in.TenantId, in.IsOutreach, in.Ids...)
	if err != nil {
		err := errors.Wrap(err, "error getting crmRoles from sql by ids")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	res := &servicePb.GetCRMRolesByIdsResponse{
		Roles: make([]*orchardPb.CRMRole, len(crs)),
	}
	for i, cr := range crs {
		crmRole, err := svc.ToProto(cr)
		if err != nil {
			err := errors.Wrap(err, "error converting crmRole from db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		res.Roles[i] = crmRole
	}

	return res, nil
}

func (h *Handlers) GetCRMRoles(ctx context.Context, in *servicePb.GetCRMRolesRequest) (*servicePb.GetCRMRolesResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetCRMRoles")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId).WithCustom("search", in.Search)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	limit := 20
	if in.PageSize > 0 {
		limit = int(in.PageSize)
	}
	offset := 0
	if in.Page > 0 {
		offset = (int(in.Page) - 1) * limit
	}

	svc := h.db.NewCRMRoleService()

	crs, total, err := svc.Search(spanCtx, in.TenantId, in.Search, limit, offset, in.IsOutreach)
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

	return &servicePb.GetCRMRolesResponse{CrmRoles: crmRoles, Total: int32(total)}, nil
}

func (h *Handlers) GetUnsyncedCRMRoles(ctx context.Context, in *servicePb.GetUnsyncedCRMRolesRequest) (*servicePb.GetUnsyncedCRMRolesResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetCRMRoles")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewCRMRoleService()

	crs, err := svc.GetUnsynced(spanCtx, in.TenantId, in.IsOutreach)
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

func (h *Handlers) DeleteCRMRoleById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	spanCtx, span := log.StartSpan(ctx, "DeleteCRMRoleById")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId).WithCustom("id", in.Id)

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

	svc := h.db.NewCRMRoleService()

	err := svc.DeleteByID(spanCtx, in.Id, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error deleting crmRole from sql by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.Empty{}, nil
}
