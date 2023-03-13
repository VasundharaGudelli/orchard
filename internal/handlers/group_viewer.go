package handlers

import (
	"context"
	"time"

	perm "github.com/loupe-co/bouncer/pkg/permissions"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	authPb "github.com/loupe-co/protos/src/common/auth"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	bouncerPb "github.com/loupe-co/protos/src/services/bouncer"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (h *Handlers) InsertGroupViewer(ctx context.Context, in *servicePb.InsertGroupViewerRequest) (*servicePb.InsertGroupViewerResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if in.GroupViewer == nil {
		err := ErrBadRequest.New("groupViewer can't be null")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.GroupViewer.TenantId == "" || in.GroupViewer.GroupId == "" || in.GroupViewer.PersonId == "" {
		err := ErrBadRequest.New("tenantId, groupId and personId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupViewerService()

	gv := svc.FromProto(in.GroupViewer)

	if err := svc.Insert(ctx, gv); err != nil {
		err := errors.Wrap(err, "error inserting group viewer into sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	groupViewer, err := svc.ToProto(gv)
	if err != nil {
		err := errors.Wrap(err, "error converting groupViewer db model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.InsertGroupViewerResponse{GroupViewer: groupViewer}, nil
}

func (h *Handlers) GetGroupViewers(ctx context.Context, in *servicePb.IdRequest) (*servicePb.GetGroupViewersResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		err := ErrBadRequest.New("tenantId and groupId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupViewerService()

	peeps, err := svc.GetGroupViewers(ctx, in.TenantId, in.GroupId)
	if err != nil {
		err := errors.Wrap(err, "error getting group viewers from sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	personSvc := h.db.NewPersonService()
	groupViewers := make([]*orchardPb.Person, len(peeps))
	ids := make([]string, len(peeps))
	for i, peep := range peeps {
		ids[i] = peep.ID
		groupViewers[i], err = personSvc.ToProto(peep)
		if err != nil {
			err := errors.Wrap(err, "error converting person db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
	}

	return &servicePb.GetGroupViewersResponse{
		GroupId:   in.GroupId,
		ViewerIds: ids,
		Viewers:   groupViewers,
	}, nil
}

func (h *Handlers) GetPersonViewableGroups(ctx context.Context, in *servicePb.IdRequest) (*servicePb.GetPersonViewableGroupsResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId).WithCustom("personId", in.PersonId)

	if in.TenantId == "" || in.PersonId == "" {
		err := ErrBadRequest.New("tenantId and personId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupViewerService()

	groups, err := svc.GetPersonViewableGroups(ctx, in.TenantId, in.PersonId)
	if err != nil {
		err := errors.Wrap(err, "error getting person viewable groups from sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	groupSvc := h.db.NewGroupService()
	viewableGroups := make([]*orchardPb.Group, len(groups))
	ids := make([]string, len(groups))
	for i, group := range groups {
		ids[i] = group.ID
		viewableGroups[i], err = groupSvc.ToProto(group)
		if err != nil {
			err := errors.Wrap(err, "error converting group db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
	}

	return &servicePb.GetPersonViewableGroupsResponse{
		PersonId: in.PersonId,
		GroupIds: ids,
		Groups:   viewableGroups,
	}, nil
}

func (h *Handlers) SetPersonViewableGroups(ctx context.Context, in *servicePb.SetPersonViewableGroupsRequest) (*servicePb.SetPersonViewableGroupsResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if in.TenantId == "" || in.PersonId == "" {
		err := ErrBadRequest.New("tenantId and personId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	permissionSet := authPb.PermissionSet_Group

	perms := []perm.Permission{
		perm.NewPermission(permissionSet, authPb.Permission_Access),
		perm.NewPermission(permissionSet, authPb.Permission_Read),
	}

	p := perm.NewPermissions()
	p = p.WithPermissions(perms...)

	permissions := p[permissionSet]

	svc := h.db.NewGroupViewerService()
	viewableGroups, err := svc.GetPersonViewableGroups(ctx, in.TenantId, in.PersonId)
	if err != nil {
		err := errors.Wrap(err, "error getting person viewable groups from sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	var groupIds = make(map[string]struct{}, len(viewableGroups))
	for _, vg := range viewableGroups {
		groupIds[vg.ID] = struct{}{}
	}

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error starting setpersonviewable groups transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}
	svc.SetTransaction(tx)

	now := timestamppb.New(time.Now().UTC())

	for _, gvId := range in.GroupViewerIds {
		if _, ok := groupIds[gvId]; !ok {
			gvProto := &orchardPb.GroupViewer{
				GroupId:     gvId,
				TenantId:    in.TenantId,
				PersonId:    in.PersonId,
				CreatedBy:   in.CreatedBy,
				UpdatedBy:   in.UpdatedBy,
				UpdatedAt:   now,
				CreatedAt:   now,
				Permissions: permissions,
			}
			gv := svc.FromProto(gvProto)

			if err := svc.Insert(ctx, gv); err != nil {
				err := errors.Wrap(err, "error inserting new groupviewers in sql")
				logger.Error(err)
				svc.Rollback()
				return nil, err.AsGRPC()
			}
		}
	}

	var groupViewerIds = make(map[string]struct{}, len(in.GroupViewerIds))
	for _, vg := range in.GroupViewerIds {
		groupViewerIds[vg] = struct{}{}
	}

	for gId := range groupIds {
		if _, ok := groupViewerIds[gId]; !ok {
			if err := svc.DeleteByID(ctx, in.TenantId, gId, in.PersonId); err != nil {
				err := errors.Wrap(err, "error deleting group viewer in sql")
				logger.Error(err)
				svc.Rollback()
				return nil, err.AsGRPC()
			}
		}
	}

	groups, err := svc.GetPersonViewableGroups(ctx, in.TenantId, in.PersonId)
	if err != nil {
		err := errors.Wrap(err, "error getting person viewable groups from sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting transaction, rolling back")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	groupSvc := h.db.NewGroupService()
	updatedViewableGroups := make([]*orchardPb.Group, len(groups))
	ids := make([]string, len(groups))
	for i, group := range groups {
		ids[i] = group.ID
		updatedViewableGroups[i], err = groupSvc.ToProto(group)
		if err != nil {
			err := errors.Wrap(err, "error converting group db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
	}

	if _, err := h.bouncerClient.BustAuthCache(ctx, &bouncerPb.BustAuthCacheRequest{TenantId: in.TenantId, UserId: in.PersonId}); err != nil {
		err := errors.Wrap(err, "error busting auth data cache for user")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.SetPersonViewableGroupsResponse{
		Groups: updatedViewableGroups,
	}, nil
}

func (h *Handlers) UpdateGroupViewer(ctx context.Context, in *servicePb.UpdateGroupViewerRequest) (*servicePb.UpdateGroupViewerResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if in.GroupViewer == nil {
		err := ErrBadRequest.New("groupViewer can't be null")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.GroupViewer.TenantId == "" || in.GroupViewer.GroupId == "" || in.GroupViewer.PersonId == "" {
		err := ErrBadRequest.New("tenantId, groupId and personId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupViewerService()

	gv := svc.FromProto(in.GroupViewer)

	if err := svc.Update(ctx, gv); err != nil {
		err := errors.Wrap(err, "error updating group viewer in sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	groupViewer, err := svc.ToProto(gv)
	if err != nil {
		err := errors.Wrap(err, "error converting groupViewer db model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if _, err := h.bouncerClient.BustAuthCache(ctx, &bouncerPb.BustAuthCacheRequest{TenantId: in.TenantId, UserId: in.PersonId}); err != nil {
		err := errors.Wrap(err, "error busting auth data cache for user")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpdateGroupViewerResponse{GroupViewer: groupViewer}, nil
}

func (h *Handlers) DeleteGroupViewerById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if in.TenantId == "" || in.GroupId == "" || in.PersonId == "" {
		err := ErrBadRequest.New("tenantId, groupId and personId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupViewerService()

	if err := svc.DeleteByID(ctx, in.TenantId, in.GroupId, in.PersonId); err != nil {
		err := errors.Wrap(err, "error deleting group viewer in sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if _, err := h.bouncerClient.BustAuthCache(ctx, &bouncerPb.BustAuthCacheRequest{TenantId: in.TenantId, UserId: in.PersonId}); err != nil {
		err := errors.Wrap(err, "error busting auth data cache for user")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.Empty{}, nil
}
