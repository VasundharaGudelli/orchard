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
	"google.golang.org/grpc/codes"
)

func (h *Handlers) CreateSystemRole(ctx context.Context, in *servicePb.CreateSystemRoleRequest) (*servicePb.CreateSystemRoleResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

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

	if err := svc.Insert(ctx, sr); err != nil {
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
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

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

	br, err := svc.GetByID(ctx, in.BaseRoleId)
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

	basePermissions := br.Permissions
	roleType := br.Type
	roleStatus := br.Status

	// if base role not empty move up a level
	if br.BaseRoleID.Valid && !br.BaseRoleID.IsZero() {
		br, err = svc.GetByID(ctx, br.BaseRoleID.String)
		if err != nil {
			err := errors.Wrap(err, "error getting base system role")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		in.BaseRoleId = br.ID
	}

	if br.TenantID != db.DefaultTenantID {
		err := ErrBadRequest.New("base role can only belong to default tenant")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	in.NewSystemRole.Id = db.MakeID()

	// make sure tenant id is set
	if in.TenantId != "" {
		in.NewSystemRole.TenantId = in.TenantId
	}

	// make sure base role id is set
	if in.BaseRoleId != "" {
		in.NewSystemRole.BaseRoleId = in.BaseRoleId
	}

	sr := svc.FromProto(in.NewSystemRole)

	// add data from base role
	sr.Permissions = basePermissions
	sr.Type = roleType
	sr.Status = roleStatus

	if err := svc.Insert(ctx, sr); err != nil {
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
	logger := log.WithContext(ctx).WithTenantID(in.TenantId).WithCustom("id", in.Id)

	if in.Id == "" {
		err := ErrBadRequest.New("id can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewSystemRoleService()

	sr, err := svc.GetByID(ctx, in.Id)
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

func (h *Handlers) GetSystemRoleWithBaseRole(ctx context.Context, in *servicePb.IdRequest) (*servicePb.GetSystemRoleWithBaseRoleResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId).WithCustom("id", in.Id)

	if in.Id == "" {
		err := ErrBadRequest.New("id can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewSystemRoleService()

	srs, err := svc.GetByIDWithBaseRole(ctx, in.Id)
	if err != nil {
		err := errors.Wrap(err, "error getting systemRole with base role")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	res := &servicePb.GetSystemRoleWithBaseRoleResponse{}

	// only one role, return as system role
	if len(srs) == 1 {
		role, err := svc.ToProto(srs[0])
		if err != nil {
			err := errors.Wrap(err, "error converting systemRole db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		res.SystemRole = role
		return res, nil
	}

	// sort out system/base roles
	for _, sr := range srs {
		role, err := svc.ToProto(sr)
		if err != nil {
			err := errors.Wrap(err, "error converting systemRole db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}

		if len(role.BaseRoleId) == 0 {
			res.BaseRole = role
			continue
		}

		res.SystemRole = role
	}

	return res, nil
}

func (h *Handlers) GetSystemRoles(ctx context.Context, in *servicePb.GetSystemRolesRequest) (*servicePb.GetSystemRolesResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId).WithCustom("search", in.Search)

	svc := h.db.NewSystemRoleService()

	srs, err := svc.Search(ctx, in.TenantId, in.Search)
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
	if in.SystemRole == nil {
		err := ErrBadRequest.New("systemRole can't be null")
		log.WithContext(ctx).Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.SystemRole.Id == "" {
		in.SystemRole.Id = in.Id
	}
	if in.SystemRole.Id == "" {
		err := ErrBadRequest.New("id can't be empty")
		log.WithContext(ctx).Warn(err.Error())
		return nil, err.AsGRPC()
	}

	logger := log.WithContext(ctx).WithTenantID(in.SystemRole.TenantId).WithCustom("id", in.Id)

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error creating update system role transaction")
		logger.Error(err)
		return nil, err
	}

	svc := h.db.NewSystemRoleService()
	svc.SetTransaction(tx)

	sr := svc.FromProto(in.SystemRole)

	if err := svc.Update(ctx, sr, in.OnlyFields); err != nil {
		err := errors.Wrap(err, "error updating systemRole")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if len(in.OnlyFields) == 0 || strUtil.Strings(in.OnlyFields).Has("permissions") {
		// TODO: eventually, probably want to check the tenantID on the deleted system_role to see if we can be more specific with our cache bust
		if _, err := h.bouncerClient.BustAuthCache(ctx, &bouncerPb.BustAuthCacheRequest{}); err != nil {
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
	logger := log.WithTenantID(in.TenantId).WithCustom("id", in.Id)

	if in.Id == "" {
		err := ErrBadRequest.New("id can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.UserId == "" {
		in.UserId = db.DefaultTenantID
	}

	personSvc := h.db.NewPersonService()
	numPeople, err := personSvc.CountPeopleByRoleId(ctx, in.TenantId, in.Id)
	if err != nil {
		err := errors.Wrap(err, "error checking if role has users attached")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if numPeople > 0 {
		err := errors.New("cannot delete role, users are currently attached").WithCode(codes.FailedPrecondition)
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error creating delete system role transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	svc := h.db.NewSystemRoleService()
	svc.SetTransaction(tx)

	if err := svc.SoftDeleteByID(ctx, in.Id, in.TenantId, in.UserId); err != nil {
		err := errors.Wrap(err, "error deleting systemRole by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// TODO: eventually, probably want to check the tenantID on the deleted system_role to see if we can be more specific with our cache bust
	if _, err := h.bouncerClient.BustAuthCache(ctx, &bouncerPb.BustAuthCacheRequest{}); err != nil {
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
