package grpchandlers

import (
	"context"
	"fmt"

	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/db"
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
	return nil, nil
}

func (server *OrchardGRPCServer) GetPersonViewableGroups(ctx context.Context, in *servicePb.IdRequest) (*servicePb.GetPersonViewableGroupsResponse, error) {
	return nil, nil
}

func (server *OrchardGRPCServer) UpdateGroupViewer(ctx context.Context, in *servicePb.UpdateGroupViewerRequest) (*servicePb.UpdateGroupViewerResponse, error) {
	return nil, nil
}

func (server *OrchardGRPCServer) DeleteGroupViewerById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	return nil, nil
}
