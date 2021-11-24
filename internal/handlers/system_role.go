package handlers

import (
	"context"

	strUtil "github.com/loupe-co/go-common/data-structures/slice/string"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/db"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	bouncerPb "github.com/loupe-co/protos/src/services/bouncer"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (h *Handlers) CreateSystemRole(ctx context.Context, in *servicePb.CreateSystemRoleRequest) (*servicePb.CreateSystemRoleResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "CreateSystemRole")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.SystemRole == nil {
		err := ErrBadRequest.New("system role can't be null")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.SystemRole.Id != "" {
		err := ErrBadRequest.New("can't insert record with existing id")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	in.SystemRole.Id = db.MakeID()

	if in.TenantId != "" {
		in.SystemRole.TenantId = in.TenantId
	}

	svc := h.db.NewSystemRoleService()

	sr := svc.FromProto(in.SystemRole)

	if err := svc.Insert(spanCtx, sr); err != nil {
		err := errors.Wrap(err, "error inserting system role into sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	systemRole, err := svc.ToProto(sr)
	if err != nil {
		err := errors.Wrap(err, "error converting systemRole db model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.CreateSystemRoleResponse{SystemRole: systemRole}, nil
}

func (h *Handlers) CloneSystemRole(ctx context.Context, in *servicePb.CloneSystemRoleRequest) (*servicePb.CloneSystemRoleResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "CloneSystemRole")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.BaseRoleId == "" {
		err := ErrBadRequest.New("baseRoleId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.NewSystemRole == nil {
		err := ErrBadRequest.New("new system role can't be null")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.NewSystemRole.Id != "" {
		err := ErrBadRequest.New("can't insert record with existing id")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewSystemRoleService()

	br, err := svc.GetByID(spanCtx, in.BaseRoleId)
	if err != nil {
		err := errors.Wrap(err, "error getting base system role")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if br == nil {
		err := ErrBadRequest.New("base role not found")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if br.TenantID != db.DefaultTenantID {
		err := ErrBadRequest.New("base role can only belong to default tenant")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	in.NewSystemRole.Id = db.MakeID()

	if in.TenantId != "" {
		in.NewSystemRole.TenantId = in.TenantId
	}

	sr := svc.FromProto(in.NewSystemRole)
	sr.Permissions = br.Permissions

	if err := svc.Insert(spanCtx, sr); err != nil {
		err := errors.Wrap(err, "error inserting system role into sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	systemRole, err := svc.ToProto(sr)
	if err != nil {
		err := errors.Wrap(err, "error converting systemRole db model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.CloneSystemRoleResponse{SystemRole: systemRole}, nil
}

func (h *Handlers) GetSystemRoleById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.SystemRole, error) {
	spanCtx, span := log.StartSpan(ctx, "GetSystemRoleById")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId).WithCustom("id", in.Id)

	if in.Id == "" {
		err := ErrBadRequest.New("id can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewSystemRoleService()

	sr, err := svc.GetByID(spanCtx, in.Id)
	if err != nil {
		err := errors.Wrap(err, "error getting systemRole by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	systemRole, err := svc.ToProto(sr)
	if err != nil {
		err := errors.Wrap(err, "error converting systemRole db model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return systemRole, nil
}

func (h *Handlers) GetSystemRoles(ctx context.Context, in *servicePb.GetSystemRolesRequest) (*servicePb.GetSystemRolesResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetSystemRoles")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId).WithCustom("search", in.Search)

	svc := h.db.NewSystemRoleService()

	srs, err := svc.Search(spanCtx, in.TenantId, in.Search)
	if err != nil {
		err := errors.Wrap(err, "error searching systemRoles")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	systemRoles := make([]*orchardPb.SystemRole, len(srs))
	for i, sr := range srs {
		systemRoles[i], err = svc.ToProto(sr)
		if err != nil {
			err := errors.Wrap(err, "error converting systemRole db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
	}

	return &servicePb.GetSystemRolesResponse{
		SystemRoles: systemRoles,
		Total:       int32(len(systemRoles)),
	}, nil
}

func (h *Handlers) UpdateSystemRole(ctx context.Context, in *servicePb.UpdateSystemRoleRequest) (*servicePb.UpdateSystemRoleResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpdateSystemRole")
	defer span.End()

	if in.SystemRole == nil {
		err := ErrBadRequest.New("systemRole can't be null")
		log.WithContext(spanCtx).Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.SystemRole.Id == "" {
		in.SystemRole.Id = in.Id
	}
	if in.SystemRole.Id == "" {
		err := ErrBadRequest.New("id can't be empty")
		log.WithContext(spanCtx).Warn(err.Error())
		return nil, err.AsGRPC()
	}

	logger := log.WithContext(spanCtx).WithTenantID(in.SystemRole.TenantId).WithCustom("id", in.Id)

	tx, err := h.db.NewTransaction(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error creating update system role transaction")
		logger.Error(err)
		return nil, err
	}

	svc := h.db.NewSystemRoleService()
	svc.SetTransaction(tx)

	sr := svc.FromProto(in.SystemRole)

	if err := svc.Update(spanCtx, sr, in.OnlyFields); err != nil {
		err := errors.Wrap(err, "error updating systemRole")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if len(in.OnlyFields) == 0 || strUtil.Strings(in.OnlyFields).Has("permissions") {
		// TODO: eventually, probably want to check the tenantID on the deleted system_role to see if we can be more specific with our cache bust
		if _, err := h.bouncerClient.BustAuthCache(spanCtx, &bouncerPb.BustAuthCacheRequest{}); err != nil {
			err := errors.Wrap(err, "error busting auth data cache in bouncer")
			logger.Error(err)
			if err := svc.Rollback(); err != nil {
				logger.Error(errors.Wrap(err, "error rolling back transaction"))
			}
			return nil, err.AsGRPC()
		}
	}

	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting update system role transaction")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	systemRole, err := svc.ToProto(sr)
	if err != nil {
		err := errors.Wrap(err, "error converting systemRole db model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpdateSystemRoleResponse{SystemRole: systemRole}, nil
}

func (h *Handlers) DeleteSystemRoleById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	spanCtx, span := log.StartSpan(ctx, "DeleteSystemRoleById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("id", in.Id)

	if in.Id == "" {
		err := ErrBadRequest.New("id can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.UserId == "" {
		in.UserId = db.DefaultTenantID
	}

	tx, err := h.db.NewTransaction(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error creating delete system role transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	svc := h.db.NewSystemRoleService()
	svc.SetTransaction(tx)

	if err := svc.SoftDeleteByID(spanCtx, in.Id, in.UserId); err != nil {
		err := errors.Wrap(err, "error deleting systemRole by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// TODO: eventually, probably want to check the tenantID on the deleted system_role to see if we can be more specific with our cache bust
	if _, err := h.bouncerClient.BustAuthCache(spanCtx, &bouncerPb.BustAuthCacheRequest{}); err != nil {
		err := errors.Wrap(err, "error busting auth data cache in bouncer")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting delete system role transaction")
		logger.Error(err)
		if err := svc.Rollback(); err != nil {
			logger.Error(errors.Wrap(err, "error rolling back transaction"))
		}
		return nil, err.AsGRPC()
	}

	return &servicePb.Empty{}, nil
}
