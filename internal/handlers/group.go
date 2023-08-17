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
	"github.com/volatiletech/null/v8"
	"google.golang.org/grpc/codes"
)

func (h *Handlers) SyncGroups(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.String())
		return nil, err.AsGRPC()
	}

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error starting transaction for syncing groups")
		logger.Error(err)
		return nil, err.AsGRPC()
	}
	svc := h.db.NewGroupService()
	svc.SetTransaction(tx)

	isSynced, err := svc.IsCRMSynced(ctx, in.TenantId)
	if err != nil {
		err := errors.Wrap(err, "error checking if tenant crm roles are synced with groups")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if !isSynced {
		logger.Info("tenant crm roles are not synced with groups, skipping group sync.")
		return &servicePb.SyncResponse{}, nil
	}

	if err := svc.SyncGroups(ctx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error syncing groups with crm roles")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := svc.DeleteUnSyncedGroups(ctx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error deleting unsynced groups")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := svc.UpdateGroupPaths(ctx, in.TenantId); err != nil {
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
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

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

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error getting sql transaction for inserting group")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()
	crmSVC := h.db.NewCRMRoleService()

	svc.SetTransaction(tx)

	var outreachToCommitMapping map[string]string
	commitToOutreachMapping := map[string]string{}

	if in.IsOutreach && len(in.Group.CrmRoleIds) > 0 {
		outreachToCommitMapping, commitToOutreachMapping, err = crmSVC.GetOutreachCommitMappingsByOutreachIDs(ctx, in.TenantId, in.Group.CrmRoleIds...)
		if err != nil {
			err := errors.Wrap(err, "error getting getting outreach commit mappings by ids")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		for idx, item := range in.Group.CrmRoleIds {
			in.Group.CrmRoleIds[idx] = outreachToCommitMapping[item]
		}
	}

	insertableGroup := svc.FromProto(in.Group)
	insertableGroup.CreatedAt = time.Now().UTC()
	insertableGroup.UpdatedAt = time.Now().UTC()
	if insertableGroup.ParentID.String == "" {
		insertableGroup.ParentID = null.NewString("", false)
	}

	if hasDups, err := svc.CheckDuplicateCRMRoleIDs(ctx, in.Group.Id, in.TenantId, in.Group.CrmRoleIds); err != nil {
		err := errors.Wrap(err, "error checking for duplicate crm_role_ids before write")
		logger.Error(err)
		return nil, err.AsGRPC()
	} else if hasDups {
		err := errors.Error("given group has crm_role_ids that already exist in another group").WithCode(codes.InvalidArgument)
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if err := svc.Insert(ctx, insertableGroup); err != nil {
		err := errors.Wrap(err, "error inserting group into sql")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := svc.UpdateGroupPaths(ctx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error updating group paths in sql")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := svc.UpdateGroupTypes(ctx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error updating group types")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := h.ensureTenantGroupSyncState(ctx, in.TenantId, svc.GetTransaction()); err != nil {
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

	if in.IsOutreach && len(group.CrmRoleIds) > 0 {
		for idx, item := range group.CrmRoleIds {
			group.CrmRoleIds[idx] = commitToOutreachMapping[item]
		}
	}

	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting transaction for inserting group")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.CreateGroupResponse{Group: group}, nil
}

func (h *Handlers) GetGroupById(ctx context.Context, in *servicePb.IdRequest) (*orchardPb.Group, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		err := ErrBadRequest.New("tenantId and groupId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()

	g, err := svc.GetByID(ctx, in.GroupId, in.TenantId)
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

	crmSVC := h.db.NewCRMRoleService()

	if in.IsOutreach && len(group.CrmRoleIds) > 0 {
		_, commitToOutreachMapping, err := crmSVC.GetOutreachCommitMappingsByCommitIDs(ctx, in.TenantId, group.CrmRoleIds...)
		if err != nil {
			err := errors.Wrap(err, "error getting getting outreach commit mappings by ids")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		for idx, item := range group.CrmRoleIds {
			group.CrmRoleIds[idx] = commitToOutreachMapping[item]
		}
	}

	return group, nil
}

func (h *Handlers) GetGroups(ctx context.Context, in *servicePb.GetGroupsRequest) (*servicePb.GetGroupsResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()

	gs, err := svc.Search(ctx, in.TenantId, in.Search)
	if err != nil {
		err := errors.Wrap(err, "error getting groups")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	groups := make([]*orchardPb.Group, len(gs))

	crmRoleIDs := []string{}
	for i, g := range gs {
		groups[i], err = svc.ToProto(g)
		crmRoleIDs = append(crmRoleIDs, groups[i].CrmRoleIds...)
		if err != nil {
			err := errors.Wrap(err, "error converting group from db model to proto")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
	}

	crmSVC := h.db.NewCRMRoleService()

	if in.IsOutreach && len(crmRoleIDs) > 0 {
		_, commitToOutreachMapping, err := crmSVC.GetOutreachCommitMappingsByCommitIDs(ctx, in.TenantId, crmRoleIDs...)
		if err != nil {
			err := errors.Wrap(err, "error getting getting outreach commit mappings by ids")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		for _, g := range groups {
			for idx, item := range g.CrmRoleIds {
				g.CrmRoleIds[idx] = commitToOutreachMapping[item]
			}
		}
	}

	return &servicePb.GetGroupsResponse{
		Groups: groups,
	}, nil
}

func (h *Handlers) GetGroupSubTree(ctx context.Context, in *servicePb.GetGroupSubTreeRequest) (*servicePb.GetGroupSubTreeResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()

	flatGroups, err := svc.GetGroupSubTree(ctx, in.TenantId, in.GroupId, int(in.MaxDepth), in.HydrateUsers, in.Simplify, in.ActiveUsers, in.UseManagerNames, in.ExcludeManagerUsers, in.ViewableGroups...)
	if err != nil {
		err := errors.Wrap(err, "error getting group and all subtrees from sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// Convert db models to protos
	parGroup, _ := commonSync.NewWorkerPool(ctx, 10)
	flatProtos := make([]*servicePb.GroupWithMembers, len(flatGroups))
	forceKeepLevelMap := map[string]bool{}
	for i, g := range flatGroups {
		if g.ActiveMemberCount > 0 && g.Type == "manager" {
			forceKeepLevelMap[g.ID] = true
		}
		parGroup.Go(h.runGroupTreeProtoConversion(ctx, i, g, flatProtos, in.TenantId, in.HydrateUsers, in.HydrateCrmRoles, in.IsOutreach))
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
			depth, _ := recursivelyGetGroupChildren(r, all, 1, in.Simplify, forceKeepLevelMap)
			finalRoots[idx] = &servicePb.GroupSubtreeRoot{
				GroupId: r.Group.Id,
				Depth:   int32(depth),
				SubTree: r,
			}
			w.Done()
		}(&wg, root, flatProtos, i)
	}
	wg.Wait()

	if in.IsOutreach {
		crmSVC := h.db.NewCRMRoleService()
		crmRoleIDs := recursivelyGetCRMRolesStart(finalRoots)

		_, commitToOutreachMapping, err := crmSVC.GetOutreachCommitMappingsByCommitIDs(ctx, in.TenantId, crmRoleIDs...)
		if err != nil {
			err := errors.Wrap(err, "error getting getting outreach commit mappings by ids")
			logger.Error(err)
			return nil, err.AsGRPC()
		}

		logger.WithCustom("crmRoleIDs", crmRoleIDs).WithCustom("commitToOutreachMapping", commitToOutreachMapping).Debug("id mappings")

		recursivelySetCRMRolesStart(finalRoots, commitToOutreachMapping)
	}

	return &servicePb.GetGroupSubTreeResponse{
		Roots: finalRoots,
	}, nil
}

func recursivelyGetCRMRolesStart(level []*servicePb.GroupSubtreeRoot) []string {
	crmRoleIDs := []string{}
	for _, g := range level {
		appendIDs := recursivelyGetCRMRoles([]*servicePb.GroupWithMembers{g.SubTree})
		if len(appendIDs) > 0 {
			crmRoleIDs = append(crmRoleIDs, appendIDs...)
		}
	}
	return crmRoleIDs
}

func recursivelyGetCRMRoles(level []*servicePb.GroupWithMembers) []string {
	crmRoleIDs := []string{}
	for _, g := range level {
		for _, item := range g.Group.CrmRoleIds {
			if item == "" {
				continue
			}
			log.Debugf("appending id: %s", item)
			crmRoleIDs = append(crmRoleIDs, item)
		}
		for _, item := range g.Group.CrmRoles {
			if item == nil {
				continue
			}
			log.Debugf("appending id 2: %s", item.Id)
			crmRoleIDs = append(crmRoleIDs, item.Id)
		}
		if len(g.Children) > 0 {
			appendIDs := recursivelyGetCRMRoles(g.Children)
			if len(appendIDs) > 0 {
				crmRoleIDs = append(crmRoleIDs, appendIDs...)
			}
		}
	}
	return crmRoleIDs
}

func recursivelySetCRMRolesStart(level []*servicePb.GroupSubtreeRoot, roleMap map[string]string) {
	for _, g := range level {
		recursivelySetCRMRoles([]*servicePb.GroupWithMembers{g.SubTree}, roleMap)
	}
}

func recursivelySetCRMRoles(level []*servicePb.GroupWithMembers, roleMap map[string]string) {
	for _, g := range level {
		for idx, item := range g.Group.CrmRoleIds {
			if item == "" {
				continue
			}
			log.Debugf("setting id: %s -> %s", item, roleMap[item])
			g.Group.CrmRoleIds[idx] = roleMap[item]
		}
		for idx, item := range g.Group.CrmRoles {
			if item == nil {
				continue
			}
			log.Debugf("setting id: %s -> %s", g.Group.CrmRoles[idx].Id, roleMap[g.Group.CrmRoles[idx].Id])
			g.Group.CrmRoles[idx].Id = roleMap[g.Group.CrmRoles[idx].Id]
		}
		if len(g.Children) > 0 {
			recursivelySetCRMRoles(g.Children, roleMap)
		}
	}
}

func (h *Handlers) runGroupTreeProtoConversion(ctx context.Context, idx int, g *db.GroupTreeNode, results []*servicePb.GroupWithMembers, tenantID string, hydrateUsers, hydrateRoles, isOutreach bool) func() error {
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
			crmRoles, err := crmSvc.GetByIDs(ctx, tenantID, isOutreach, group.CrmRoleIds...)
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

func recursivelyGetGroupChildren(node *servicePb.GroupWithMembers, groups []*servicePb.GroupWithMembers, depth int, simplify bool, forceKeepLevelMap map[string]bool) (int, map[string]bool) {
	if forceKeepLevelMap == nil {
		forceKeepLevelMap = map[string]bool{}
	}
	maxDepth := depth
	simplifiedGroups := map[string]bool{}
	for _, g := range groups {
		if g.Group.ParentId == node.Group.Id {
			currentDepth, sfg := recursivelyGetGroupChildren(g, groups, depth+1, simplify, forceKeepLevelMap)
			maxDepth = max(currentDepth, maxDepth)
			node.Children = append(node.Children, g)
			for k, v := range sfg {
				simplifiedGroups[k] = v
			}
		}
	}
	if simplify {
		if len(node.Children) == 1 && node.Children[0].Group.Type == orchardPb.SystemRoleType_IC && len(node.Children[0].Members) > 0 && len(node.Children[0].Members) <= 25 {
			if !forceKeepLevelMap[node.Children[0].Group.Id] {
				node.Members = append(node.Members, node.Children[0].Members...)
				node.Children = []*servicePb.GroupWithMembers{}
				node.Group.Type = orchardPb.SystemRoleType_IC
				simplifiedGroups[node.Group.Id] = true
			} else if simplifiedGroups[node.Children[0].Group.Id] {
				log.WithTenantID(node.Group.TenantId).Debugf("simplifying hierarchy additional redundancy: parent::%s , child::%s", node.Group.Id, node.Children[0].Group.Id)
			}
		}
	}
	return maxDepth, simplifiedGroups
}

func max(x, y int) int {
	if y > x {
		return y
	}
	return x
}

func (h *Handlers) UpdateGroup(ctx context.Context, in *servicePb.UpdateGroupRequest) (*servicePb.UpdateGroupResponse, error) {
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

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

	if len(in.OnlyFields) == 0 || (strUtils.Strings(in.OnlyFields).Has("parent_id") && in.Group.ParentId == in.Group.Id) {
		err := ErrBadRequest.New("can't use group's id as its parent id")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error getting sql transaction for updating group")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()
	crmSVC := h.db.NewCRMRoleService()

	svc.SetTransaction(tx)

	var outreachToCommitMapping map[string]string
	commitToOutreachMapping := map[string]string{}

	if in.IsOutreach && len(in.Group.CrmRoleIds) > 0 {
		outreachToCommitMapping, commitToOutreachMapping, err = crmSVC.GetOutreachCommitMappingsByOutreachIDs(ctx, in.TenantId, in.Group.CrmRoleIds...)
		if err != nil {
			err := errors.Wrap(err, "error getting getting outreach commit mappings by ids")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		for idx, item := range in.Group.CrmRoleIds {
			in.Group.CrmRoleIds[idx] = outreachToCommitMapping[item]
		}
	}

	updateableGroup := svc.FromProto(in.Group)

	if updateableGroup.ParentID.String == "" {
		updateableGroup.ParentID = null.NewString("", false)
	}

	if updateableGroup.ID == "" {
		updateableGroup.ID = in.GroupId
	}
	if updateableGroup.TenantID == "" {
		updateableGroup.TenantID = in.TenantId
	}

	if hasDups, err := svc.CheckDuplicateCRMRoleIDs(ctx, updateableGroup.ID, in.TenantId, in.Group.CrmRoleIds); err != nil {
		err := errors.Wrap(err, "error checking for duplicate crm_role_ids before write")
		logger.Error(err)
		return nil, err.AsGRPC()
	} else if hasDups {
		err := errors.Error("given group has crm_role_ids that already exist in another group").WithCode(codes.InvalidArgument)
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if err := svc.Update(ctx, updateableGroup, in.OnlyFields); err != nil {
		err := errors.Wrap(err, "error updating group into sql")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if len(in.OnlyFields) == 0 || strUtils.Strings(in.OnlyFields).Has("parent_id") {
		if err := svc.UpdateGroupPaths(ctx, in.TenantId); err != nil {
			err := errors.Wrap(err, "error updating group paths in sql")
			logger.Error(err)
			svc.Rollback()
			return nil, err.AsGRPC()
		}
	}

	// re-sync users into groups if the group's crm_role_ids changed
	if len(in.OnlyFields) == 0 || strUtils.Strings(in.OnlyFields).Has("crm_role_ids") {
		if err := h.updatePersonGroups(ctx, in.TenantId, svc.GetTransaction()); err != nil {
			err := errors.Wrap(err, "error updating person groups")
			logger.Error(err)
			svc.Rollback()
			return nil, err.AsGRPC()
		}
	}

	// Make sure group types are updated correctly
	if err := svc.UpdateGroupTypes(ctx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error updating group types")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	// If the crm_role_ids changed or the status changed, then make sure to re-calculate/set the tenant's sync state
	if len(in.OnlyFields) == 0 || strUtils.Strings(in.OnlyFields).Intersects([]string{"crm_role_ids", "status"}) {
		if err := h.ensureTenantGroupSyncState(ctx, in.TenantId, svc.GetTransaction()); err != nil {
			svc.Rollback()
			err := errors.Wrap(err, "error ensuring tenant group sync state")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
	}

	if err := svc.Reload(ctx, updateableGroup); err != nil {
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

	if in.IsOutreach && len(group.CrmRoleIds) > 0 {
		for idx, item := range group.CrmRoleIds {
			group.CrmRoleIds[idx] = commitToOutreachMapping[item]
		}
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
	logger := log.WithContext(ctx).WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		err := ErrBadRequest.New("tenantId and GroupId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if in.UserId == "" {
		in.UserId = db.DefaultTenantID
	}

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error starting delete group transaction")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()
	svc.SetTransaction(tx)

	if err := svc.SoftDeleteGroupChildren(ctx, in.GroupId, in.TenantId, in.UserId); err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error soft deleting group children groups")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if err := svc.SoftDeleteByID(ctx, in.GroupId, in.TenantId, in.UserId); err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error soft deleting group by id")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// Check the tenant's remaining group count and reset hierarchy if there are 0 active groups left
	groupCount, err := svc.GetTenantActiveGroupCount(ctx, in.TenantId)
	if err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error getting tenant groups count")
		logger.Error(err)
		return nil, err.AsGRPC()
	}
	if groupCount == 0 {
		if err := h.resetHierarchy(ctx, in.TenantId, in.UserId, tx); err != nil {
			// resetHierarchy already takes care of commiting/rolling back transaction, so need to handle that here
			err := errors.Wrap(err, "error resetting tenant hierarchy")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
		return &servicePb.Empty{}, nil
	}

	if err := svc.UpdateGroupPaths(ctx, in.TenantId); err != nil {
		svc.Rollback()
		err := errors.Wrap(err, "error updating group paths")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	// Make sure group types are updated correctly
	if err := svc.UpdateGroupTypes(ctx, in.TenantId); err != nil {
		err := errors.Wrap(err, "error updating group types")
		logger.Error(err)
		svc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := h.ensureTenantGroupSyncState(ctx, in.TenantId, svc.GetTransaction()); err != nil {
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
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	userID := act.GetUserID(ctx)

	if userID == "" {
		userID = db.DefaultTenantID
	}

	if err := h.resetHierarchy(ctx, in.TenantId, userID, nil); err != nil {
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
	logger := log.WithContext(ctx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		return nil, ErrBadRequest.New("tenantId is empty")
	}

	svc := h.db.NewGroupService()
	ts, err := svc.GetLatestModifiedTS(ctx, in.TenantId)
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
