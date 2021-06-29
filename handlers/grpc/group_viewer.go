package grpchandlers

import (
	"context"
	"fmt"

	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/db"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (server *OrchardGRPCServer) InsertGroupViewer(ctx context.Context, in *servicePb.InsertGroupViewerRequest) (*servicePb.InsertGroupViewerResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "InsertGroupViewer")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.GroupViewer == nil {
		logger.Warn("Bad Request: inserting groupViewer requires GroupViewer to be non-null")
		return nil, grpcError(spanCtx, fmt.Errorf("Bad Request: inserting groupViewer requires GroupViewer to be non-null"))
	}

	if in.GroupViewer.TenantId == "" || in.GroupViewer.GroupId == "" || in.GroupViewer.PersonId == "" {
		logger.Warn("Bad Request: inserting groupViewer requires tenantId, groupId and personId to be non-empty")
		return nil, grpcError(spanCtx, fmt.Errorf("Bad Request: inserting groupViewer requires tenantId, groupId and personId to be non-empty"))
	}

	svc := db.NewGroupViewerService()

	gv := svc.FromProto(in.GroupViewer)

	if err := svc.Insert(spanCtx, gv); err != nil {
		logger.Errorf("error inserting group viewer into sql: %s", err.Error())
		return nil, err
	}

	groupViewer, err := svc.ToProto(gv)
	if err != nil {
		logger.Errorf("error converting groupViewer db model to proto: %s", err.Error())
		return nil, err
	}

	return &servicePb.InsertGroupViewerResponse{GroupViewer: groupViewer}, nil
}

func (server *OrchardGRPCServer) GetGroupViewers(ctx context.Context, in *servicePb.IdRequest) (*servicePb.GetGroupViewersResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetGroupViewers")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		logger.Warn("Bad Request: getting groupViewers requires tenantId, groupId to be non-empty")
		return nil, grpcError(spanCtx, fmt.Errorf("Bad Request: getting groupViewers requires tenantId, groupId to be non-empty"))
	}

	svc := db.NewGroupViewerService()

	peeps, err := svc.GetGroupViewers(spanCtx, in.TenantId, in.GroupId)
	if err != nil {
		logger.Errorf("error getting group viewers from sql: %s", err.Error())
		return nil, err
	}

	personSvc := db.NewPersonService()
	groupViewers := make([]*orchardPb.Person, len(peeps))
	ids := make([]string, len(peeps))
	for i, peep := range peeps {
		ids[i] = peep.ID
		groupViewers[i], err = personSvc.ToProto(peep)
		if err != nil {
			logger.Errorf("error converting person db model to proto: %s", err.Error())
			return nil, err
		}
	}

	return &servicePb.GetGroupViewersResponse{
		GroupId:   in.GroupId,
		ViewerIds: ids,
		Viewers:   groupViewers,
	}, nil
}

func (server *OrchardGRPCServer) GetPersonViewableGroups(ctx context.Context, in *servicePb.IdRequest) (*servicePb.GetPersonViewableGroupsResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetPersonViewableGroups")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("personId", in.PersonId)

	if in.TenantId == "" || in.GroupId == "" {
		logger.Warn("Bad Request: getting person viewable groups requires tenantId, personId to be non-empty")
		return nil, grpcError(spanCtx, fmt.Errorf("Bad Request: getting person viewable groups requires tenantId, personId to be non-empty"))
	}

	svc := db.NewGroupViewerService()

	groups, err := svc.GetPersonViewableGroups(spanCtx, in.TenantId, in.GroupId)
	if err != nil {
		logger.Errorf("error getting person viewable groups from sql: %s", err.Error())
		return nil, err
	}

	groupSvc := db.NewGroupService()
	viewableGroups := make([]*orchardPb.Group, len(groups))
	ids := make([]string, len(groups))
	for i, group := range groups {
		ids[i] = group.ID
		viewableGroups[i], err = groupSvc.ToProto(group)
		if err != nil {
			logger.Errorf("error converting group db model to proto: %s", err.Error())
			return nil, err
		}
	}

	return &servicePb.GetPersonViewableGroupsResponse{
		PersonId: in.PersonId,
		GroupIds: ids,
		Groups:   viewableGroups,
	}, nil
}

func (server *OrchardGRPCServer) UpdateGroupViewer(ctx context.Context, in *servicePb.UpdateGroupViewerRequest) (*servicePb.UpdateGroupViewerResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpdateGroupViewer")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.GroupViewer == nil {
		logger.Warn("Bad Request: updating groupViewer requires GroupViewer to be non-null")
		return nil, grpcError(spanCtx, fmt.Errorf("Bad Request: updating groupViewer requires GroupViewer to be non-null"))
	}

	if in.GroupViewer.TenantId == "" || in.GroupViewer.GroupId == "" || in.GroupViewer.PersonId == "" {
		logger.Warn("Bad Request: updating groupViewer requires tenantId, groupId and personId to be non-empty")
		return nil, grpcError(spanCtx, fmt.Errorf("Bad Request: updating groupViewer requires tenantId, groupId and personId to be non-empty"))
	}

	svc := db.NewGroupViewerService()

	gv := svc.FromProto(in.GroupViewer)

	if err := svc.Update(spanCtx, gv); err != nil {
		logger.Errorf("error updating group viewer in sql: %s", err.Error())
		return nil, err
	}

	groupViewer, err := svc.ToProto(gv)
	if err != nil {
		logger.Errorf("error converting groupViewer db model to proto: %s", err.Error())
		return nil, err
	}

	return &servicePb.UpdateGroupViewerResponse{GroupViewer: groupViewer}, nil
}

func (server *OrchardGRPCServer) DeleteGroupViewerById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	spanCtx, span := log.StartSpan(ctx, "UpdateGroupViewer")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" || in.GroupId == "" || in.PersonId == "" {
		logger.Warn("Bad Request: deleting groupViewer requires tenantId, groupId and personId to be non-empty")
		return nil, grpcError(spanCtx, fmt.Errorf("Bad Request: deleting groupViewer requires tenantId, groupId and personId to be non-empty"))
	}

	svc := db.NewGroupViewerService()

	if err := svc.DeleteByID(spanCtx, in.TenantId, in.GroupId, in.PersonId); err != nil {
		logger.Errorf("error deleting group viewer in sql: %s", err.Error())
		return nil, err
	}

	return &servicePb.Empty{}, nil
}
