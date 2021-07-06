package grpchandlers

import (
	"context"
	"fmt"

	strUtils "github.com/loupe-co/go-common/data-structures/slice/string"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/db"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (server *OrchardGRPCServer) CreateGroup(ctx context.Context, in *servicePb.CreateGroupRequest) (*servicePb.CreateGroupResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "CreateGroup")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		logger.Warn("Bad Request: tenantId can't be empty")
		return nil, fmt.Errorf("Bad Request: tenantId can't be empty")
	}

	if in.Group == nil {
		logger.Warn("Bad Request: group is null")
		return nil, fmt.Errorf("Bad Request: group is null")
	}

	if in.Group.Id != "" {
		logger.Warn("Bad Request: can't create new group with existing id")
		return nil, fmt.Errorf("Bad Request: can't create new group with existing id")
	}

	in.Group.Id = db.MakeID()

	svc := db.NewGroupService()
	if err := svc.WithTransaction(spanCtx); err != nil {
		logger.Errorf("error getting sql transaction for inserting group: %s", err.Error())
		return nil, err
	}

	insertableGroup := svc.FromProto(in.Group)

	if err := svc.Insert(spanCtx, insertableGroup); err != nil {
		logger.Errorf("error inserting group into sql: %s", err.Error())
		svc.Rollback()
		return nil, err
	}

	if err := svc.UpdateGroupPaths(spanCtx, in.TenantId); err != nil {
		logger.Errorf("error updating group paths in sql: %s", err.Error())
		svc.Rollback()
		return nil, err
	}

	group, err := svc.ToProto(insertableGroup)
	if err != nil {
		logger.Errorf("error converting group db model to proto: %s", err.Error())
		svc.Rollback()
		return nil, err
	}

	if err := svc.Commit(); err != nil {
		logger.Errorf("error commiting transaction for inserting group: %s", err.Error())
		return nil, err
	}

	return &servicePb.CreateGroupResponse{Group: group}, nil
}

func (server *OrchardGRPCServer) GetGroupById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.Group, error) {
	spanCtx, span := log.StartSpan(ctx, "GetGroupById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		logger.Warn("Bad Request: tenantId and GroupId can't be empty")
		return nil, fmt.Errorf("Bad Request: tenantId and groupId can't be empty")
	}

	svc := db.NewGroupService()

	g, err := svc.GetByID(spanCtx, in.GroupId, in.TenantId)
	if err != nil {
		logger.Errorf("error getting group by id: %s", err.Error())
		return nil, err
	}

	group, err := svc.ToProto(g)
	if err != nil {
		logger.Errorf("error converting group db model to proto: %s", err.Error())
		return nil, err
	}

	return group, nil
}

func (server *OrchardGRPCServer) GetGroups(ctx context.Context, in *servicePb.GetGroupsRequest) (*servicePb.GetGroupsResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetGroups")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		logger.Warn("Bad Request: tenantId can't be empty")
		return nil, fmt.Errorf("Bad Request: tenantId can't be empty")
	}

	svc := db.NewGroupService()

	gs, err := svc.Search(spanCtx, in.TenantId, in.Search)
	if err != nil {
		logger.Errorf("error getting groups: %s", err.Error())
		return nil, err
	}

	groups := make([]*orchardPb.Group, len(gs))
	for i, g := range gs {
		groups[i], err = svc.ToProto(g)
		if err != nil {
			logger.Errorf("error converting group from db model to proto: %s", err.Error())
			return nil, err
		}
	}

	return &servicePb.GetGroupsResponse{
		Groups: groups,
	}, nil
}

func (server *OrchardGRPCServer) GetGroupSubTree(ctx context.Context, in *servicePb.GetGroupSubTreeRequest) (*servicePb.GetGroupSubTreeResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetGroupSubTree")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		logger.Warn("Bad Request: tenantId and GroupId can't be empty")
		return nil, fmt.Errorf("Bad Request: tenantId and groupId can't be empty")
	}

	svc := db.NewGroupService()
	personSvc := db.NewPersonService()

	flatGroups, err := svc.GetGroupSubTree(spanCtx, in.TenantId, in.GroupId, int(in.MaxDepth), in.HydrateUsers)
	if err != nil {
		logger.Errorf("error getting group and all subtrees from sql: %s", err.Error())
		return nil, err
	}

	// Convert db models to protos
	flatProtos := make([]*servicePb.GroupWithMembers, len(flatGroups))
	for i, g := range flatGroups {
		group, err := svc.ToProto(&g.Group)
		if err != nil {
			logger.Errorf("error converting group db model to proto: %s", err.Error())
			return nil, err
		}
		members := []*orchardPb.Person{}
		if in.HydrateUsers {
			members = make([]*orchardPb.Person, len(g.Members))
			for j, p := range g.Members {
				members[j], err = personSvc.ToProto(&p)
				if err != nil {
					logger.Errorf("error converting person db model to proto: %s", err.Error())
					return nil, err
				}
			}
		}
		flatProtos[i] = &servicePb.GroupWithMembers{
			Group:    group,
			Members:  members,
			Children: []*servicePb.GroupWithMembers{},
		}
	}

	// Form tree structure
	var root *servicePb.GroupWithMembers
	for _, g := range flatProtos {
		if g.Group.Id == in.GroupId {
			root = g
		}
	}
	depth := recursivelyGetGroupChildren(root, flatProtos, 0)

	return &servicePb.GetGroupSubTreeResponse{
		GroupId: in.GroupId,
		Depth:   int32(depth),
		SubTree: root,
	}, nil
}

func recursivelyGetGroupChildren(node *servicePb.GroupWithMembers, groups []*servicePb.GroupWithMembers, depth int) int {
	maxDepth := depth
	for _, g := range groups {
		if g.Group.ParentId == node.Group.Id {
			maxDepth = max(recursivelyGetGroupChildren(g, groups, depth+1), maxDepth)
			node.Children = append(node.Children, g)
		}
	}
	return maxDepth
}

func max(x, y int) int {
	if y > x {
		return y
	}
	return x
}

func (server *OrchardGRPCServer) UpdateGroup(ctx context.Context, in *servicePb.UpdateGroupRequest) (*servicePb.UpdateGroupResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpdateGroup")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		logger.Warn("Bad Request: tenantId can't be empty")
		return nil, fmt.Errorf("Bad Request: tenantId can't be empty")
	}

	if in.Group == nil {
		logger.Warn("Bad Request: group is null")
		return nil, fmt.Errorf("Bad Request: group is null")
	}

	if in.Group.Id == "" && in.TenantId == "" {
		logger.Warn("Bad Request: can't update group empty id")
		return nil, fmt.Errorf("Bad Request: can't update group empty id")
	}

	svc := db.NewGroupService()
	if err := svc.WithTransaction(spanCtx); err != nil {
		logger.Errorf("error getting sql transaction for updating group: %s", err.Error())
		return nil, err
	}

	updateableGroup := svc.FromProto(in.Group)

	if err := svc.Update(spanCtx, updateableGroup, in.OnlyFields); err != nil {
		logger.Errorf("error updating group into sql: %s", err.Error())
		svc.Rollback()
		return nil, err
	}

	if len(in.OnlyFields) == 0 || strUtils.Strings(in.OnlyFields).Filter(func(_ int, v string) bool { return v == "parent_id" }).Len() > 0 {
		if err := svc.UpdateGroupPaths(spanCtx, in.TenantId); err != nil {
			logger.Errorf("error updating group paths in sql: %s", err.Error())
			svc.Rollback()
			return nil, err
		}
	}

	group, err := svc.ToProto(updateableGroup)
	if err != nil {
		logger.Errorf("error converting group db model to proto: %s", err.Error())
		svc.Rollback()
		return nil, err
	}

	if err := svc.Commit(); err != nil {
		logger.Errorf("error commiting transaction for updating group: %s", err.Error())
		return nil, err
	}

	return &servicePb.UpdateGroupResponse{Group: group}, nil
}

func (server *OrchardGRPCServer) DeleteGroup(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	spanCtx, span := log.StartSpan(ctx, "DeleteGroupById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		logger.Warn("Bad Request: tenantId and GroupId can't be empty")
		return nil, fmt.Errorf("Bad Request: tenantId and groupId can't be empty")
	}

	svc := db.NewGroupService()

	if err := svc.DeleteByID(spanCtx, in.GroupId, in.TenantId); err != nil {
		logger.Errorf("error deleting group by id: %s", err.Error())
		return nil, err
	}

	return nil, nil
}
