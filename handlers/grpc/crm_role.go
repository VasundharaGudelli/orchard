package grpchandlers

import (
	"context"
	"fmt"

	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/db"
	"github.com/loupe-co/orchard/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (server *OrchardGRPCServer) UpsertCRMRoles(ctx context.Context, in *servicePb.UpsertCRMRolesRequest) (*servicePb.UpsertCRMRolesResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpsertCRMRoles")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		logger.Warn("Bad Request: tenantId can't be empty")
		return nil, fmt.Errorf("Bad Request: tenantId can't be empty")
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
		logger.Errorf("error upserting crmRoles in sql: %s", err.Error())
		return nil, err
	}

	return &servicePb.UpsertCRMRolesResponse{}, nil
}

func (server *OrchardGRPCServer) GetCRMRoleById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.CRMRole, error) {
	spanCtx, span := log.StartSpan(ctx, "GetCRMRoleById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("id", in.Id)

	if in.Id == "" {
		logger.Warn("Bad Request: id can't be empty")
		return nil, fmt.Errorf("Bad Request: id can't be empty")
	}

	if in.TenantId == "" {
		logger.Warn("Bad Request: tenantId can't be empty")
		return nil, fmt.Errorf("Bad Request: tenantId can't be empty")
	}

	svc := db.NewCRMRoleService()

	cr, err := svc.GetByID(spanCtx, in.Id, in.TenantId)
	if err != nil {
		logger.Errorf("error getting crmRole from sql by id: %s", err.Error())
		return nil, err
	}

	crmRole, err := svc.ToProto(cr)
	if err != nil {
		logger.Errorf("error converting crmRole from db model to proto: %s", err.Error())
		return nil, err
	}

	return crmRole, nil
}

func (server *OrchardGRPCServer) GetCRMRoles(ctx context.Context, in *servicePb.GetCRMRolesRequest) (*servicePb.GetCRMRolesResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetCRMRoles")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("search", in.Search)

	if in.TenantId == "" {
		logger.Warn("Bad Request: tenantId can't be empty")
		return nil, fmt.Errorf("Bad Request: tenantId can't be empty")
	}

	svc := db.NewCRMRoleService()

	crs, err := svc.Search(spanCtx, in.TenantId, in.Search)
	if err != nil {
		logger.Errorf("error getting crmRole from sql by id: %s", err.Error())
		return nil, err
	}

	crmRoles := make([]*orchardPb.CRMRole, len(crs))
	for i, cr := range crs {
		role, err := svc.ToProto(cr)
		if err != nil {
			logger.Errorf("error converting crmRole from db model to proto: %s", err.Error())
			return nil, err
		}
		crmRoles[i] = role
	}

	return &servicePb.GetCRMRolesResponse{CrmRoles: crmRoles}, nil
}

func (server *OrchardGRPCServer) DeleteCRMRoleById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	spanCtx, span := log.StartSpan(ctx, "DeleteCRMRoleById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("id", in.Id)

	if in.Id == "" {
		logger.Warn("Bad Request: id can't be empty")
		return nil, fmt.Errorf("Bad Request: id can't be empty")
	}

	if in.TenantId == "" {
		logger.Warn("Bad Request: tenantId can't be empty")
		return nil, fmt.Errorf("Bad Request: tenantId can't be empty")
	}

	svc := db.NewCRMRoleService()

	err := svc.DeleteByID(spanCtx, in.Id, in.TenantId)
	if err != nil {
		logger.Errorf("error deleting crmRole from sql by id: %s", err.Error())
		return nil, err
	}

	return &servicePb.Empty{}, nil
}
