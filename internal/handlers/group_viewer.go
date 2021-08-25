package handlers

import (
	"context"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (h *Handlers) InsertGroupViewer(ctx context.Context, in *servicePb.InsertGroupViewerRequest) (*servicePb.InsertGroupViewerResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "InsertGroupViewer")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

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

	if err := svc.Insert(spanCtx, gv); err != nil {
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
	spanCtx, span := log.StartSpan(ctx, "GetGroupViewers")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		err := ErrBadRequest.New("tenantId and groupId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupViewerService()

	peeps, err := svc.GetGroupViewers(spanCtx, in.TenantId, in.GroupId)
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
	spanCtx, span := log.StartSpan(ctx, "GetPersonViewableGroups")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("personId", in.PersonId)

	if in.TenantId == "" || in.GroupId == "" {
		err := ErrBadRequest.New("tenantId and personId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupViewerService()

	groups, err := svc.GetPersonViewableGroups(spanCtx, in.TenantId, in.GroupId)
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

func (h *Handlers) UpdateGroupViewer(ctx context.Context, in *servicePb.UpdateGroupViewerRequest) (*servicePb.UpdateGroupViewerResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpdateGroupViewer")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

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

	if err := svc.Update(spanCtx, gv); err != nil {
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

	return &servicePb.UpdateGroupViewerResponse{GroupViewer: groupViewer}, nil
}

func (h *Handlers) DeleteGroupViewerById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	spanCtx, span := log.StartSpan(ctx, "UpdateGroupViewer")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" || in.GroupId == "" || in.PersonId == "" {
		err := ErrBadRequest.New("tenantId, groupId and personId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupViewerService()

	if err := svc.DeleteByID(spanCtx, in.TenantId, in.GroupId, in.PersonId); err != nil {
		err := errors.Wrap(err, "error deleting group viewer in sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.Empty{}, nil
}
