package grpchandlers

import (
	"context"
	"fmt"

	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/db"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (server *OrchardGRPCServer) CreateSystemRole(ctx context.Context, in *servicePb.CreateSystemRoleRequest) (*servicePb.CreateSystemRoleResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "CreateSystemRole")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.SystemRole.Id != "" {
		logger.Warn("Bad Request: can't insert new record with existing id")
		return nil, grpcError(spanCtx, fmt.Errorf("Bad Request: can't insert new systemRole with non-empty id"))
	}

	in.SystemRole.Id = db.MakeID()

	if in.TenantId != "" {
		in.SystemRole.TenantId = in.TenantId
	}

	svc := db.NewSystemRoleService()

	sr := svc.FromProto(in.SystemRole)

	if err := svc.Insert(spanCtx, sr); err != nil {
		logger.Errorf("error inserting system role into sql: %s", err.Error())
		return nil, err
	}

	return nil, nil
}

func (server *OrchardGRPCServer) GetSystemRoleById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.SystemRole, error) {
	spanCtx, span := log.StartSpan(ctx, "GetSystemRoleById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("id", in.Id)

	if in.Id == "" {
		logger.Warn("Bad Request: Id can't be empty")
		return nil, fmt.Errorf("Bad Request: Id can't be empty")
	}

	svc := db.NewSystemRoleService()

	sr, err := svc.GetByID(spanCtx, in.Id)
	if err != nil {
		logger.Errorf("error getting systemRole by id: %s", err.Error())
		return nil, err
	}

	systemRole, err := svc.ToProto(sr)
	if err != nil {
		logger.Errorf("error converting systemRole db model to proto: %s", err.Error())
		return nil, err
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
		logger.Errorf("error searching systemRoles: %s", err.Error())
		return nil, err
	}

	systemRoles := make([]*orchardPb.SystemRole, len(srs))
	for i, sr := range srs {
		systemRoles[i], err = svc.ToProto(sr)
		if err != nil {
			logger.Errorf("error converting systemRole db model to proto: %s", err.Error())
			return nil, err
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
		log.Warn("Bad Request: SystemRole can't be null")
		return nil, fmt.Errorf("Bad Request: SystemRole can't be null")
	}

	if in.SystemRole.Id == "" {
		in.SystemRole.Id = in.Id
	}

	logger := log.WithTenantID(in.SystemRole.TenantId).WithCustom("id", in.Id)
	svc := db.NewSystemRoleService()

	sr := svc.FromProto(in.SystemRole)

	err := svc.Update(spanCtx, sr, in.OnlyFields)
	if err != nil {
		logger.Errorf("error updating systemRole: %s", err.Error())
		return nil, err
	}

	systemRole, err := svc.ToProto(sr)
	if err != nil {
		logger.Errorf("error converting systemRole db model to proto: %s", err.Error())
		return nil, err
	}

	return &servicePb.UpdateSystemRoleResponse{SystemRole: systemRole}, nil
}

func (server *OrchardGRPCServer) DeleteSystemRoleById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	spanCtx, span := log.StartSpan(ctx, "DeleteSystemRoleById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("id", in.Id)

	if in.Id == "" {
		logger.Warn("Bad Request: Id can't be empty")
		return nil, fmt.Errorf("Bad Request: Id can't be empty")
	}

	svc := db.NewSystemRoleService()

	err := svc.DeleteByID(spanCtx, in.Id)
	if err != nil {
		logger.Errorf("error deleting systemRole by id: %s", err.Error())
		return nil, err
	}

	return &servicePb.Empty{}, nil
}
