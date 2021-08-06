package grpchandlers

import (
	"context"
	"sync"
	"time"

	strUtils "github.com/loupe-co/go-common/data-structures/slice/string"
	"github.com/loupe-co/go-common/errors"
	commonSync "github.com/loupe-co/go-common/sync"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/db"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (server *OrchardGRPCServer) SyncGroups(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "SyncGroups")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.String())
		return nil, err.AsGRPC()
	}

	svc := db.NewGroupService()

	isSynced, err := svc.IsCRMSynced(spanCtx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error checking if tenant crm roles are synced with groups")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if !isSynced {
		logger.Info("tenant crm roles are not synced with groups, skipping group sync.")
		return &servicePb.SyncResponse{}, nil
	}

	if err := svc.WithTransaction(spanCtx); err != nil {
		err := errors.Wrap(err, "error starting transaction for syncing groups")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if err := svc.SyncGroups(spanCtx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error syncing groups with crm roles")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := svc.DeleteUnSyncedGroups(spanCtx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error deleting unsynced groups")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := svc.UpdateGroupPaths(spanCtx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error updating group paths after sync")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting transaction, rolling back")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	return &servicePb.SyncResponse{}, nil
}

func (server *OrchardGRPCServer) CreateGroup(ctx context.Context, in *servicePb.CreateGroupRequest) (*servicePb.CreateGroupResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "CreateGroup")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.Group == nil {
		err := ErrBadRequest.New("group can't be nil")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.Group.Id != "" {
		err := ErrBadRequest.New("can't create new group with existing id")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	in.Group.Id = db.MakeID()

	svc := db.NewGroupService()
	if err := svc.WithTransaction(spanCtx); err != nil {
		err := errors.Wrap(err, "error getting sql transaction for inserting group")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	insertableGroup := svc.FromProto(in.Group)
	insertableGroup.CreatedAt = time.Now().UTC()
	insertableGroup.UpdatedAt = time.Now().UTC()

	if err := svc.Insert(spanCtx, insertableGroup); err != nil {
		err := errors.Wrap(err, "error inserting group into sql")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := svc.UpdateGroupPaths(spanCtx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error updating group paths in sql")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	group, err := svc.ToProto(insertableGroup)
	if err != nil {
		err := errors.Wrap(err, "error converting group db model to proto")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting transaction for inserting group")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.CreateGroupResponse{Group: group}, nil
}

func (server *OrchardGRPCServer) GetGroupById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.Group, error) {
	spanCtx, span := log.StartSpan(ctx, "GetGroupById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		err := ErrBadRequest.New("tenantId and groupId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := db.NewGroupService()

	g, err := svc.GetByID(spanCtx, in.GroupId, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error getting group by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	group, err := svc.ToProto(g)
	if err != nil {
		err := errors.Wrap(err, "error converting group db model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return group, nil
}

func (server *OrchardGRPCServer) GetGroups(ctx context.Context, in *servicePb.GetGroupsRequest) (*servicePb.GetGroupsResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetGroups")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := db.NewGroupService()

	gs, err := svc.Search(spanCtx, in.TenantId, in.Search)
	if err != nil {
		err := errors.Wrap(err, "error getting groups")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	groups := make([]*orchardPb.Group, len(gs))
	for i, g := range gs {
		groups[i], err = svc.ToProto(g)
		if err != nil {
			err := errors.Wrap(err, "error converting group from db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
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

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := db.NewGroupService()

	flatGroups, err := svc.GetGroupSubTree(spanCtx, in.TenantId, in.GroupId, int(in.MaxDepth), in.HydrateUsers)
	if err != nil {
		err := errors.Wrap(err, "error getting group and all subtrees from sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// Convert db models to protos
	parGroup, _ := commonSync.NewWorkerPool(spanCtx, 10)
	flatProtos := make([]*servicePb.GroupWithMembers, len(flatGroups))
	for i, g := range flatGroups {
		parGroup.Go(runGroupTreeProtoConversion(spanCtx, i, g, flatProtos, in.TenantId, in.HydrateUsers, in.HydrateCrmRoles))
	}
	if err := parGroup.Close(); err != nil {
		logger.Error(err)
		return nil, err
	}

	// Form tree structure
	roots := []*servicePb.GroupWithMembers{}
	for _, g := range flatProtos {
		if g.Group.ParentId == "" {
			roots = append(roots, g)
		}
	}
	if len(roots) == 0 {
		return &servicePb.GetGroupSubTreeResponse{
			Roots: []*servicePb.GroupSubtreeRoot{},
		}, nil
	}

	finalRoots := make([]*servicePb.GroupSubtreeRoot, len(roots))
	wg := sync.WaitGroup{}
	for i, root := range roots {
		wg.Add(1)
		go func(w *sync.WaitGroup, r *servicePb.GroupWithMembers, all []*servicePb.GroupWithMembers, idx int) {
			depth := recursivelyGetGroupChildren(r, all, 1)
			finalRoots[idx] = &servicePb.GroupSubtreeRoot{
				GroupId: r.Group.Id,
				Depth:   int32(depth),
				SubTree: r,
			}
			w.Done()
		}(&wg, root, flatProtos, i)
	}
	wg.Wait()

	return &servicePb.GetGroupSubTreeResponse{
		Roots: finalRoots,
	}, nil
}

func runGroupTreeProtoConversion(ctx context.Context, idx int, g *db.GroupTreeNode, results []*servicePb.GroupWithMembers, tenantID string, hydrateUsers, hydrateRoles bool) func() error {
	return func() error {
		// Parse group
		svc := db.NewGroupService()
		group, err := svc.ToProto(&g.Group)
		if err != nil {
			err := errors.Wrap(err, "error converting group db model to proto")
			return err.AsGRPC()
		}

		// Parse members
		personSvc := db.NewPersonService()
		members := make([]*orchardPb.Person, len(g.Members))
		for j, p := range g.Members {
			if !hydrateUsers {
				members[j] = &orchardPb.Person{Id: p.ID}
				continue
			}
			members[j], err = personSvc.ToProto(&p)
			if err != nil {
				err := errors.Wrap(err, "error converting person db model to proto")
				return err.AsGRPC()
			}
		}

		// If requested, get full crm_roles and put them onto group object
		if hydrateRoles {
			crmSvc := db.NewCRMRoleService()
			crmRoles, err := crmSvc.GetByIDs(ctx, tenantID, group.CrmRoleIds...)
			if err != nil {
				return errors.Wrap(err, "error getting crm_roles for group").AsGRPC()
			}
			group.CrmRoles = make([]*orchardPb.CRMRole, len(crmRoles))
			for j, cr := range crmRoles {
				crmRole, err := crmSvc.ToProto(cr)
				if err != nil {
					return errors.Wrap(err, "error converting crm role to proto for group").AsGRPC()
				}
				group.CrmRoles[j] = crmRole
			}
		}

		// Assign final groupWithMembers struct
		results[idx] = &servicePb.GroupWithMembers{
			Group:    group,
			Members:  members,
			Children: []*servicePb.GroupWithMembers{},
		}

		return nil
	}
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
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.Group == nil {
		err := ErrBadRequest.New("group is null")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.Group.Id == "" && in.TenantId == "" {
		err := ErrBadRequest.New("can't update group with empty id")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := db.NewGroupService()
	if err := svc.WithTransaction(spanCtx); err != nil {
		err := errors.Wrap(err, "error getting sql transaction for updating group")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	updateableGroup := svc.FromProto(in.Group)

	if err := svc.Update(spanCtx, updateableGroup, in.OnlyFields); err != nil {
		err := errors.Wrap(err, "error updating group into sql")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if len(in.OnlyFields) == 0 || strUtils.Strings(in.OnlyFields).Filter(func(_ int, v string) bool { return v == "parent_id" }).Len() > 0 {
		if err := svc.UpdateGroupPaths(spanCtx, in.TenantId); err != nil {
			err := errors.Wrap(err, "error updating group paths in sql")
			logger.Error(err)
			svc.Rollback()
			return nil, err.AsGRPC()
		}
	}

	if err := svc.Reload(spanCtx, updateableGroup); err != nil {
		err := errors.Wrap(err, "error reloading group from sql")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	group, err := svc.ToProto(updateableGroup)
	if err != nil {
		err := errors.Wrap(err, "error converting group db model to proto")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting transaction for updating group")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpdateGroupResponse{Group: group}, nil
}

func (server *OrchardGRPCServer) UpdateGroupTypes(ctx context.Context, in *servicePb.UpdateGroupTypesRequest) (*servicePb.UpdateGroupTypesResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpdateGroupTypes")
	defer span.End()

	logger := log.WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := db.NewGroupService()

	if err := svc.UpdateGroupTypes(spanCtx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error updating group types")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpdateGroupTypesResponse{}, nil
}

func (server *OrchardGRPCServer) DeleteGroupById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	spanCtx, span := log.StartSpan(ctx, "DeleteGroupById")
	defer span.End()

	logger := log.WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		err := ErrBadRequest.New("tenantId and GroupId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.UserId == "" {
		in.UserId = "00000000-0000-0000-0000-000000000000"
	}

	svc := db.NewGroupService()
	if err := svc.WithTransaction(spanCtx); err != nil {
		err := errors.Wrap(err, "error starting delete group transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if err := svc.TransferGroupChildrenParent(spanCtx, in.GroupId, in.TenantId, in.UserId); err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error transferring deleted group's children's parent")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if err := svc.RemoveGroupMembers(spanCtx, in.GroupId, in.TenantId, in.UserId); err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error removing deleted group's members")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if err := svc.SoftDeleteByID(spanCtx, in.GroupId, in.TenantId, in.UserId); err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error soft deleting group by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if err := svc.Commit(); err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error commiting delete group transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.Empty{}, nil
}
