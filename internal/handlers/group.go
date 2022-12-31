package handlers

import (
	"context"
	"database/sql"
	"sync"
	"time"

	timestamppb "github.com/golang/protobuf/ptypes/timestamp"
	act "github.com/loupe-co/bouncer/pkg/context"
	strUtils "github.com/loupe-co/go-common/data-structures/slice/string"
	"github.com/loupe-co/go-common/errors"
	commonSync "github.com/loupe-co/go-common/sync"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/db"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	tenantPb "github.com/loupe-co/protos/src/common/tenant"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"google.golang.org/grpc/codes"
)

func (h *Handlers) SyncGroups(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "SyncGroups")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.String())
		return nil, err.AsGRPC()
	}

	tx, err := h.db.NewTransaction(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error starting transaction for syncing groups")
		logger.Error(err)
		return nil, err.AsGRPC()
	}
	svc := h.db.NewGroupService()
	svc.SetTransaction(tx)

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

func (h *Handlers) CreateGroup(ctx context.Context, in *servicePb.CreateGroupRequest) (*servicePb.CreateGroupResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "CreateGroup")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

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

	tx, err := h.db.NewTransaction(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error getting sql transaction for inserting group")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()
	svc.SetTransaction(tx)

	insertableGroup := svc.FromProto(in.Group)
	insertableGroup.CreatedAt = time.Now().UTC()
	insertableGroup.UpdatedAt = time.Now().UTC()

	if hasDups, err := svc.CheckDuplicateCRMRoleIDs(spanCtx, in.Group.Id, in.TenantId, in.Group.CrmRoleIds); err != nil {
		err := errors.Wrap(err, "error checking for duplicate crm_role_ids before write")
		logger.Error(err)
		return nil, err.AsGRPC()
	} else if hasDups {
		err := errors.Error("given group has crm_role_ids that already exist in another group").WithCode(codes.InvalidArgument)
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

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

	if err := svc.UpdateGroupTypes(spanCtx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error updating group types")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := h.ensureTenantGroupSyncState(spanCtx, in.TenantId, svc.GetTransaction()); err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error ensuring tenant group sync state")
		logger.Error(err)
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

func (h *Handlers) GetGroupById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.Group, error) {
	spanCtx, span := log.StartSpan(ctx, "GetGroupById")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		err := ErrBadRequest.New("tenantId and groupId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()

	g, err := svc.GetByID(spanCtx, in.GroupId, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error getting group by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if g == nil {
		return nil, errors.New("group not found").WithCode(codes.NotFound).AsGRPC()
	}

	group, err := svc.ToProto(g)
	if err != nil {
		err := errors.Wrap(err, "error converting group db model to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return group, nil
}

func (h *Handlers) GetGroups(ctx context.Context, in *servicePb.GetGroupsRequest) (*servicePb.GetGroupsResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetGroups")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()

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

func (h *Handlers) GetGroupSubTree(ctx context.Context, in *servicePb.GetGroupSubTreeRequest) (*servicePb.GetGroupSubTreeResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetGroupSubTree")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()

	flatGroups, err := svc.GetGroupSubTree(spanCtx, in.TenantId, in.GroupId, int(in.MaxDepth), in.HydrateUsers, in.Simplify, in.ActiveUsers, in.UseManagerNames, in.ExcludeManagerUsers, in.ViewableGroups...)
	if err != nil {
		err := errors.Wrap(err, "error getting group and all subtrees from sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// Convert db models to protos
	parGroup, _ := commonSync.NewWorkerPool(spanCtx, 10)
	flatProtos := make([]*servicePb.GroupWithMembers, len(flatGroups))
	for i, g := range flatGroups {
		parGroup.Go(h.runGroupTreeProtoConversion(spanCtx, i, g, flatProtos, in.TenantId, in.HydrateUsers, in.HydrateCrmRoles))
	}
	if err := parGroup.Close(); err != nil {
		logger.Error(err)
		return nil, err
	}

	vgMap := map[string]bool{}
	for _, item := range in.ViewableGroups {
		vgMap[item] = true
	}

	// Form tree structure
	roots := []*servicePb.GroupWithMembers{}
	for _, g := range flatProtos {
		if g == nil {
			logger.Warn("flatProtos.g is nil")
			continue
		} else if g.Group == nil {
			logger.Warn("flatProtos.g.group is nil")
			continue
		}
		if (in.GroupId != "" && g.Group.Id == in.GroupId) || (in.GroupId == "" && g.Group.ParentId == "") || vgMap[g.Group.Id] {
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
			depth := recursivelyGetGroupChildren(r, all, 1, in.Simplify)
			finalRoots[idx] = &servicePb.GroupSubtreeRoot{
				GroupId: r.Group.Id,
				Depth:   int32(depth),
				SubTree: r,
			}
			w.Done()
		}(&wg, root, flatProtos, i)
	}
	wg.Wait()

	// b, _ := json.Marshal(finalRoots)
	// fmt.Println(string(b), "finalRoots")

	return &servicePb.GetGroupSubTreeResponse{
		Roots: finalRoots,
	}, nil
}

func (h *Handlers) runGroupTreeProtoConversion(ctx context.Context, idx int, g *db.GroupTreeNode, results []*servicePb.GroupWithMembers, tenantID string, hydrateUsers, hydrateRoles bool) func() error {
	return func() error {
		// Parse group
		svc := h.db.NewGroupService()
		group, err := svc.ToProto(&g.Group)
		if err != nil {
			err := errors.Wrap(err, "error converting group db model to proto")
			return err.AsGRPC()
		}

		// Parse members
		personSvc := h.db.NewPersonService()
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
			crmSvc := h.db.NewCRMRoleService()
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

func recursivelyGetGroupChildren(node *servicePb.GroupWithMembers, groups []*servicePb.GroupWithMembers, depth int, simplify bool) int {
	maxDepth := depth
	for _, g := range groups {
		if g.Group.ParentId == node.Group.Id {
			maxDepth = max(recursivelyGetGroupChildren(g, groups, depth+1, simplify), maxDepth)
			node.Children = append(node.Children, g)
		}
	}
	if simplify {
		if len(node.Children) == 1 && node.Children[0].Group.Type == orchardPb.SystemRoleType_IC && len(node.Children[0].Members) > 0 && len(node.Children[0].Members) <= 25 {
			node.Members = append(node.Members, node.Children[0].Members...)
			node.Children = []*servicePb.GroupWithMembers{}
			node.Group.Type = orchardPb.SystemRoleType_IC
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

func (h *Handlers) UpdateGroup(ctx context.Context, in *servicePb.UpdateGroupRequest) (*servicePb.UpdateGroupResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpdateGroup")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

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

	tx, err := h.db.NewTransaction(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error getting sql transaction for updating group")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()
	svc.SetTransaction(tx)

	updateableGroup := svc.FromProto(in.Group)

	if hasDups, err := svc.CheckDuplicateCRMRoleIDs(spanCtx, in.Group.Id, in.TenantId, in.Group.CrmRoleIds); err != nil {
		err := errors.Wrap(err, "error checking for duplicate crm_role_ids before write")
		logger.Error(err)
		return nil, err.AsGRPC()
	} else if hasDups {
		err := errors.Error("given group has crm_role_ids that already exist in another group").WithCode(codes.InvalidArgument)
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if err := svc.Update(spanCtx, updateableGroup, in.OnlyFields); err != nil {
		err := errors.Wrap(err, "error updating group into sql")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if len(in.OnlyFields) == 0 || strUtils.Strings(in.OnlyFields).Has("parent_id") {
		if err := svc.UpdateGroupPaths(spanCtx, in.TenantId); err != nil {
			err := errors.Wrap(err, "error updating group paths in sql")
			logger.Error(err)
			svc.Rollback()
			return nil, err.AsGRPC()
		}
	}

	// re-sync users into groups if the group's crm_role_ids changed
	if len(in.OnlyFields) == 0 || strUtils.Strings(in.OnlyFields).Has("crm_role_ids") {
		if err := h.updatePersonGroups(spanCtx, in.TenantId, svc.GetTransaction()); err != nil {
			err := errors.Wrap(err, "error updating person groups")
			logger.Error(err)
			svc.Rollback()
			return nil, err.AsGRPC()
		}
	}

	// Make sure group types are updated correctly
	if err := svc.UpdateGroupTypes(spanCtx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error updating group types")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	// If the crm_role_ids changed or the status changed, then make sure to re-calculate/set the tenant's sync state
	if len(in.OnlyFields) == 0 || strUtils.Strings(in.OnlyFields).Intersects([]string{"crm_role_ids", "status"}) {
		if err := h.ensureTenantGroupSyncState(spanCtx, in.TenantId, svc.GetTransaction()); err != nil {
			svc.Rollback()
			err := errors.Wrap(err, "error ensuring tenant group sync state")
			logger.Error(err)
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

func (h *Handlers) UpdateGroupTypes(ctx context.Context, in *servicePb.UpdateGroupTypesRequest) (*servicePb.UpdateGroupTypesResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "UpdateGroupTypes")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()

	if err := svc.UpdateGroupTypes(spanCtx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error updating group types")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.UpdateGroupTypesResponse{}, nil
}

func (h *Handlers) DeleteGroupById(ctx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {
	spanCtx, span := log.StartSpan(ctx, "DeleteGroupById")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		err := ErrBadRequest.New("tenantId and GroupId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.UserId == "" {
		in.UserId = db.DefaultTenantID
	}

	tx, err := h.db.NewTransaction(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error starting delete group transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()
	svc.SetTransaction(tx)

	if err := svc.SoftDeleteGroupChildren(spanCtx, in.GroupId, in.TenantId, in.UserId); err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error soft deleting group children groups")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if err := svc.SoftDeleteByID(spanCtx, in.GroupId, in.TenantId, in.UserId); err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error soft deleting group by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// Check the tenant's remaining group count and reset hierarchy if there are 0 active groups left
	groupCount, err := svc.GetTenantActiveGroupCount(spanCtx, in.TenantId)
	if err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error getting tenant groups count")
		logger.Error(err)
		return nil, err.AsGRPC()
	}
	if groupCount == 0 {
		if err := h.resetHierarchy(spanCtx, in.TenantId, in.UserId, tx); err != nil {
			// resetHierarchy already takes care of commiting/rolling back transaction, so need to handle that here
			err := errors.Wrap(err, "error resetting tenant hierarchy")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		return &servicePb.Empty{}, nil
	}

	if err := svc.UpdateGroupPaths(spanCtx, in.TenantId); err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error updating group paths")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if err := h.ensureTenantGroupSyncState(spanCtx, in.TenantId, svc.GetTransaction()); err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error ensuring tenant group sync state")
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

func (h *Handlers) ResetHierarchy(ctx context.Context, in *servicePb.ResetHierarchyRequest) (*servicePb.ResetHierarchyResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "ResetHierarchy")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	userID := act.GetUserID(ctx)

	if userID == "" {
		userID = db.DefaultTenantID
	}

	if err := h.resetHierarchy(spanCtx, in.TenantId, userID, nil); err != nil {
		err := errors.Wrap(err, "error resetting tenant hierarchy")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.ResetHierarchyResponse{}, nil
}

func (h *Handlers) resetHierarchy(ctx context.Context, tenantID, userID string, tx *sql.Tx) error {
	spanCtx, span := log.StartSpan(ctx, "resetHierarchy")
	defer span.End()

	if tx == nil {
		_tx, err := h.db.NewTransaction(spanCtx)
		if err != nil {
			return errors.Wrap(err, "error starting reset hierarchy transaction")
		}
		tx = _tx
	}

	svc := h.db.NewGroupService()
	svc.SetTransaction(tx)
	tenantSvc := h.db.NewTenantService()
	tenantSvc.SetTransaction(tx)

	if err := svc.RemoveAllGroupMembers(spanCtx, tenantID, userID); err != nil {
		svc.Rollback()
		return errors.Wrap(err, "error removing all groups members for tenant")
	}

	if err := svc.DeleteAllTenantGroups(spanCtx, tenantID); err != nil {
		svc.Rollback()
		return errors.Wrap(err, "error soft deleting tenant groups")
	}

	if err := tenantSvc.UpdateGroupSyncState(spanCtx, tenantID, tenantPb.GroupSyncStatus_Inactive); err != nil {
		svc.Rollback()
		return errors.Wrap(err, "error updating tenant group sync state")
	}

	if err := svc.Commit(); err != nil {
		svc.Rollback()
		return errors.Wrap(err, "error commiting reset hierarchy transaction")
	}

	return nil
}

func (h *Handlers) ensureTenantGroupSyncState(ctx context.Context, tenantID string, tx *sql.Tx) error {
	logger := log.WithContext(ctx).WithTenantID(tenantID)

	if tx == nil {
		_tx, err := h.db.NewTransaction(ctx)
		if err != nil {
			return err
		}
		tx = _tx
	}

	tenantSvc := h.db.NewTenantService()
	tenantSvc.SetTransaction(tx)
	groupSvc := h.db.NewGroupService()
	groupSvc.SetTransaction(tx)

	currentSyncState, err := tenantSvc.GetGroupSyncState(ctx, tenantID)
	if err != nil {
		return errors.Wrap(err, "error checking current tenant group sync state")
	}

	logger.WithCustom("currentSyncState", currentSyncState)

	isSynced, err := groupSvc.IsCRMSynced(ctx, tenantID)
	if err != nil {
		return errors.Wrap(err, "error checking if hierarchy is synced")
	}

	logger.WithCustom("isSynced", isSynced)

	if isSynced {
		if currentSyncState != tenantPb.GroupSyncStatus_Active {
			logger.Debug("wasn't active, now is active")
			return tenantSvc.UpdateGroupSyncState(ctx, tenantID, tenantPb.GroupSyncStatus_Active)
		}
		logger.Debug("state not changing from active")
		return nil
	}

	peopleSynced, err := tenantSvc.CheckPeopleSyncState(ctx, tenantID)
	if err != nil {
		return errors.Wrap(err, "error checking people sync state")
	}

	logger.WithCustom("peopleSynced", peopleSynced)

	if peopleSynced {
		if currentSyncState != tenantPb.GroupSyncStatus_PeopleOnly {
			logger.Debug("wasn't people_only, now is people_only")
			return tenantSvc.UpdateGroupSyncState(ctx, tenantID, tenantPb.GroupSyncStatus_PeopleOnly)
		}
		logger.Debug("state not changing from people_only")
		return nil
	}

	if currentSyncState != tenantPb.GroupSyncStatus_Inactive {
		logger.Debug("state changing to inactive")
		return tenantSvc.UpdateGroupSyncState(ctx, tenantID, tenantPb.GroupSyncStatus_Inactive)
	}

	logger.Debug("no changes detected for group sync state")

	return nil
}

func (h *Handlers) GetTenantGroupsLastModifiedTS(ctx context.Context, in *servicePb.GetTenantGroupsLastModifiedTSRequest) (*servicePb.GetTenantGroupsLastModifiedTSResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetTenantGroupsLastModifiedTS")
	defer span.End()

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		return nil, ErrBadRequest.New("tenantId is empty")
	}

	svc := h.db.NewGroupService()
	ts, err := svc.GetLatestModifiedTS(spanCtx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error getting tenant groups last modified ts")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	protoTS := timestamppb.Timestamp{Seconds: ts.Unix(), Nanos: int32(ts.Nanosecond())}
	if err != nil {
		err := errors.Wrap(err, "error converting timestamp to proto")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.GetTenantGroupsLastModifiedTSResponse{LastModifiedTs: &protoTS}, nil
}
