package grpchandlers

import (
	"context"

	strUtil "github.com/loupe-co/go-common/data-structures/slice/string"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/db"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	bouncerPb "github.com/loupe-co/protos/src/services/bouncer"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (server *OrchardGRPCServer) CreateSystemRole(ctx context.Context, in *servicePb.CreateSystemRoleRequest) (*servicePb.CreateSystemRoleResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "CreateSystemRole")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

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

	svc := db.NewSystemRoleService()

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

func (server *OrchardGRPCServer) GetSystemRoleById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.SystemRole, error) {
	spanCtx, span := log.StartSpan(ctx, "GetSystemRoleById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("id", in.Id)

	if in.Id == "" {
		err := ErrBadRequest.New("id can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := db.NewSystemRoleService()

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

func (server *OrchardGRPCServer) GetSystemRoles(ctx context.Context, in *servicePb.GetSystemRolesRequest) (*servicePb.GetSystemRolesResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetSystemRoles")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("search", in.Search)

	svc := db.NewSystemRoleService()

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

func (server *OrchardGRPCServer) UpdateSystemRole(ctx context.Context, in *servicePb.UpdateSystemRoleRequest) (*servicePb.UpdateSystemRoleResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpdateSystemRole")
	defer span.End()

	if in.SystemRole == nil {
		err := ErrBadRequest.New("systemRole can't be null")
		log.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.SystemRole.Id == "" {
		in.SystemRole.Id = in.Id
	}
	if in.SystemRole.Id == "" {
		err := ErrBadRequest.New("id can't be empty")
		log.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	logger := log.WithTenantID(in.SystemRole.TenantId).WithCustom("id", in.Id)
	svc := db.NewSystemRoleService()
	if err := svc.WithTransaction(spanCtx); err != nil {
		err := errors.Wrap(err, "error creating update system role transaction")
		logger.Error(err)
		return nil, err
	}

	sr := svc.FromProto(in.SystemRole)

	err := svc.Update(spanCtx, sr, in.OnlyFields)
	if err != nil {
		err := errors.Wrap(err, "error updating systemRole")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if len(in.OnlyFields) == 0 || strUtil.Strings(in.OnlyFields).Has("permissions") {
		// TODO: eventually, probably want to check the tenantID on the deleted system_role to see if we can be more specific with our cache bust
		if _, err := server.bouncerClient.BustAuthCache(spanCtx, &bouncerPb.BustAuthCacheRequest{}); err != nil {
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

func (server *OrchardGRPCServer) DeleteSystemRoleById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	spanCtx, span := log.StartSpan(ctx, "DeleteSystemRoleById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("id", in.Id)

	if in.Id == "" {
		err := ErrBadRequest.New("id can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.UserId == "" {
		in.UserId = "00000000-0000-0000-0000-000000000000"
	}

	svc := db.NewSystemRoleService()
	if err := svc.WithTransaction(spanCtx); err != nil {
		err := errors.Wrap(err, "error creating delete system role transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	err := svc.SoftDeleteByID(spanCtx, in.Id, in.UserId)
	if err != nil {
		err := errors.Wrap(err, "error deleting systemRole by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// TODO: eventually, probably want to check the tenantID on the deleted system_role to see if we can be more specific with our cache bust
	if _, err := server.bouncerClient.BustAuthCache(spanCtx, &bouncerPb.BustAuthCacheRequest{}); err != nil {
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
