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
	"github.com/loupe-co/orchard/internal/helpers"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	tenantPb "github.com/loupe-co/protos/src/common/tenant"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"github.com/volatiletech/null/v8"
	"google.golang.org/grpc/codes"
)

var rollback bool = true

func (h *Handlers) SyncGroups(spanCtx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		return nil, helpers.BadRequest(logger, "tenantId can't be empty")
	}

	svc := h.db.NewGroupService()

	isSynced, err := svc.IsCRMSynced(spanCtx, in.TenantId)
	if err != nil {
		return nil, helpers.ErrorHandler(logger, svc, err, "error error checking if tenant crm roles are synced with groups", false)
	}

	if !isSynced {
		logger.Info("tenant crm roles are not synced with groups, skipping group sync.")
		return &servicePb.SyncResponse{}, nil
	}

	if err := helpers.CreateTransaction(h.db, logger, spanCtx, svc, "error starting transaction for syncing groups"); err != nil {
		return nil, err
	}

	if err := svc.SyncGroups(spanCtx, in.TenantId); err != nil {
		return nil, helpers.ErrorHandler(logger, svc, err, "error syncing groups with crm roles", rollback)
	}

	if err := svc.DeleteUnSyncedGroups(spanCtx, in.TenantId); err != nil {
		return nil, helpers.ErrorHandler(logger, svc, err, "error deleting unsynced groups", rollback)
	}

	if err := svc.UpdateGroupPaths(spanCtx, in.TenantId); err != nil {
		return nil, helpers.ErrorHandler(logger, svc, err, "error updating group paths after sync", rollback)
	}

	return &servicePb.SyncResponse{}, helpers.CommitTransaction(logger, svc, "sync Groups")
}

func (h *Handlers) CreateGroup(spanCtx context.Context, in *servicePb.CreateGroupRequest) (*servicePb.CreateGroupResponse, error) {

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		return nil, helpers.BadRequest(logger, "tenantId can't be empty")
	}

	if in.Group == nil {
		return nil, helpers.BadRequest(logger, "group can't be nil")
	}

	if in.Group.Id != "" {
		return nil, helpers.BadRequest(logger, "can't create new group with existing id")
	}

	in.Group.Id = db.MakeID()

	svc := h.db.NewGroupService()
	crmSVC := h.db.NewCRMRoleService()

	var outreachToCommitMapping map[string]string
	commitToOutreachMapping := map[string]string{}
	var err error

	if in.IsOutreach && len(in.Group.CrmRoleIds) > 0 {
		outreachToCommitMapping, commitToOutreachMapping, err = crmSVC.GetOutreachCommitMappingsByOutreachIDs(spanCtx, in.TenantId, in.Group.CrmRoleIds...)
		if err != nil {
			return nil, helpers.ErrorHandler(logger, nil, err, "error getting getting outreach commit mappings by id", false)
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

	if hasDups, err := svc.CheckDuplicateCRMRoleIDs(spanCtx, in.Group.Id, in.TenantId, in.Group.CrmRoleIds); err != nil {
		return nil, helpers.ErrorHandler(logger, nil, err, "error checking for duplicate crm_role_ids before write", false)

	} else if hasDups {
		err := errors.Error("given group has crm_role_ids that already exist in another group").WithCode(codes.InvalidArgument)
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if err := helpers.CreateTransaction(h.db, logger, spanCtx, svc, "sql transaction for inserting group"); err != nil {
		return nil, err
	}

	if err := svc.Insert(spanCtx, insertableGroup); err != nil {
		return nil, helpers.ErrorHandler(logger, nil, err, "error inserting group into sql", rollback)
	}

	if err := svc.UpdateGroupPaths(spanCtx, in.TenantId); err != nil {
		return nil, helpers.ErrorHandler(logger, nil, err, "error updating group paths in sql", rollback)
	}

	if err := svc.UpdateGroupTypes(spanCtx, in.TenantId); err != nil {
		return nil, helpers.ErrorHandler(logger, nil, err, "error updating group types", rollback)
	}

	if err := helpers.CommitTransaction(logger, svc, "create group transaction"); err != nil {
		return nil, err
	}

	if err := h.ensureTenantGroupSyncState(spanCtx, in.TenantId, nil); err != nil {
		return nil, helpers.ErrorHandler(logger, nil, err, "error ensuring tenant group sync state", rollback)
	}

	group, err := svc.ToProto(insertableGroup)
	if err != nil {
		return nil, helpers.ErrorHandler(logger, nil, err, "error converting group db model to proto", rollback)
	}

	if in.IsOutreach && len(group.CrmRoleIds) > 0 {
		for idx, item := range group.CrmRoleIds {
			group.CrmRoleIds[idx] = commitToOutreachMapping[item]
		}
	}

	return &servicePb.CreateGroupResponse{Group: group}, helpers.CommitTransaction(logger, svc, "transaction for creating group")
}

func (h *Handlers) GetGroupById(spanCtx context.Context, in *servicePb.IdRequest) (*orchardPb.Group, error) {

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		return nil, helpers.BadRequest(logger, "tenantId and groupId can't be empty")
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

	crmSVC := h.db.NewCRMRoleService()

	if in.IsOutreach && len(group.CrmRoleIds) > 0 {
		_, commitToOutreachMapping, err := crmSVC.GetOutreachCommitMappingsByCommitIDs(spanCtx, in.TenantId, group.CrmRoleIds...)
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

func (h *Handlers) GetGroups(spanCtx context.Context, in *servicePb.GetGroupsRequest) (*servicePb.GetGroupsResponse, error) {

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
		_, commitToOutreachMapping, err := crmSVC.GetOutreachCommitMappingsByCommitIDs(spanCtx, in.TenantId, crmRoleIDs...)
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
func (h *Handlers) GetManagerAndParentIDs(ctx context.Context, in *servicePb.GetManagerAndParentIDsRequest) (*servicePb.GetManagerAndParentIDsResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "GetManagerAndParentIDs")
	defer span.End()

	tenantID := in.GetTenantId()
	personID := in.GetPersonId()

	logger := log.WithContext(spanCtx).WithTenantID(tenantID).WithCustom("personId", personID)

	if tenantID == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if personID == "" {
		err := ErrBadRequest.New("personId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	svc := h.db.NewGroupService()

	managerID, parentID, err := svc.GetManagerAndParentIDs(ctx, tenantID, personID)
	if err != nil {
		err := errors.Wrap(err, "error getting manager and parent IDs from sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	return &servicePb.GetManagerAndParentIDsResponse{
		ManagerId: managerID,
		ParentId:  parentID,
	}, nil
}
func (h *Handlers) GetGroupSubTree(spanCtx context.Context, in *servicePb.GetGroupSubTreeRequest) (*servicePb.GetGroupSubTreeResponse, error) {

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
	forceKeepLevelMap := map[string]bool{}
	for i, g := range flatGroups {
		if g.ActiveMemberCount > 0 && g.Type == "manager" {
			forceKeepLevelMap[g.ID] = true
		}
		parGroup.Go(h.runGroupTreeProtoConversion(spanCtx, i, g, flatProtos, in.TenantId, in.HydrateUsers, in.HydrateCrmRoles, in.IsOutreach))
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

		_, commitToOutreachMapping, err := crmSVC.GetOutreachCommitMappingsByCommitIDs(spanCtx, in.TenantId, crmRoleIDs...)
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

func (h *Handlers) UpdateGroup(spanCtx context.Context, in *servicePb.UpdateGroupRequest) (*servicePb.UpdateGroupResponse, error) {

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		return nil, helpers.BadRequest(logger, "tenantId can't be empty")
	}

	if in.Group == nil {
		return nil, helpers.BadRequest(logger, "group is null")
	}

	if in.Group.Id == "" && in.GroupId != "" {
		in.Group.Id = in.GroupId
	}

	if in.Group.Id == "" && in.TenantId == "" {
		return nil, helpers.BadRequest(logger, "can't update group with empty id")
	}

	if (len(in.OnlyFields) == 0 || strUtils.Strings(in.OnlyFields).Has("parent_id")) && in.Group.ParentId == in.Group.Id {
		return nil, helpers.BadRequest(logger, "can't use group's id as its parent id")
	}

	svc := h.db.NewGroupService()
	crmSVC := h.db.NewCRMRoleService()

	var outreachToCommitMapping map[string]string
	commitToOutreachMapping := map[string]string{}
	var err error

	if in.IsOutreach && len(in.Group.CrmRoleIds) > 0 {
		outreachToCommitMapping, commitToOutreachMapping, err = crmSVC.GetOutreachCommitMappingsByOutreachIDs(spanCtx, in.TenantId, in.Group.CrmRoleIds...)
		if err != nil {
			return nil, helpers.ErrorHandler(logger, nil, err, "error getting getting outreach commit mappings by ids", false)
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

	if hasDups, err := svc.CheckDuplicateCRMRoleIDs(spanCtx, updateableGroup.ID, in.TenantId, in.Group.CrmRoleIds); err != nil {
		return nil, helpers.ErrorHandler(logger, nil, err, "error checking for duplicate crm_role_ids before write", false)
	} else if hasDups {
		err := errors.Error("given group has crm_role_ids that already exist in another group").WithCode(codes.InvalidArgument)
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	if err := helpers.CreateTransaction(h.db, logger, spanCtx, svc, "sql transaction for updating group"); err != nil {
		return nil, err
	}

	if err := svc.Update(spanCtx, updateableGroup, in.OnlyFields); err != nil {
		return nil, helpers.ErrorHandler(logger, nil, err, "error updating group into sql", rollback)
	}

	if len(in.OnlyFields) == 0 || strUtils.Strings(in.OnlyFields).Has("parent_id") {
		if err := svc.UpdateGroupPaths(spanCtx, in.TenantId); err != nil {
			return nil, helpers.ErrorHandler(logger, nil, err, "error updating group paths in sql", rollback)
		}
	}

	// re-sync users into groups if the group's crm_role_ids changed
	if len(in.OnlyFields) == 0 || strUtils.Strings(in.OnlyFields).Has("crm_role_ids") {
		if err := h.updatePersonGroups(spanCtx, in.TenantId, svc.GetTransaction()); err != nil {
			return nil, helpers.ErrorHandler(logger, nil, err, "error updating person groups", rollback)
		}
	}

	// Make sure group types are updated correctly
	if err := svc.UpdateGroupTypes(spanCtx, in.TenantId); err != nil {
		return nil, helpers.ErrorHandler(logger, nil, err, "error updating group types", rollback)
	}

	if err := helpers.CommitTransaction(logger, svc, "update group transaction"); err != nil {
		return nil, err
	}
	// If the crm_role_ids changed or the status changed, then make sure to re-calculate/set the tenant's sync state
	if len(in.OnlyFields) == 0 || strUtils.Strings(in.OnlyFields).Intersects([]string{"crm_role_ids", "status"}) {
		if err := h.ensureTenantGroupSyncState(spanCtx, in.TenantId, nil); err != nil {
			return nil, helpers.ErrorHandler(logger, nil, err, "error ensuring tenant group sync state", rollback)
		}
	}

	if err := svc.Reload(spanCtx, updateableGroup); err != nil {
		return nil, helpers.ErrorHandler(logger, nil, err, "error reloading group from sql", rollback)
	}

	group, err := svc.ToProto(updateableGroup)
	if err != nil {
		return nil, helpers.ErrorHandler(logger, nil, err, "error converting group db model to proto", rollback)
	}

	if in.IsOutreach && len(group.CrmRoleIds) > 0 {
		for idx, item := range group.CrmRoleIds {
			group.CrmRoleIds[idx] = commitToOutreachMapping[item]
		}
	}

	return &servicePb.UpdateGroupResponse{Group: group}, helpers.CommitTransaction(logger, svc, "transaction for updating group")
}

func (h *Handlers) UpdateGroupTypes(spanCtx context.Context, in *servicePb.UpdateGroupTypesRequest) (*servicePb.UpdateGroupTypesResponse, error) {

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

func (h *Handlers) DeleteGroupById(spanCtx context.Context, in *servicePb.IdRequest) (*servicePb.Empty, error) {

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId).WithCustom("groupId", in.GroupId)

	if in.TenantId == "" || in.GroupId == "" {
		return nil, helpers.BadRequest(logger, "tenantId and GroupId can't be empty")
	}

	if in.UserId == "" {
		in.UserId = db.DefaultTenantID
	}
	svc := h.db.NewGroupService()

	if err := helpers.CreateTransaction(h.db, logger, spanCtx, svc, "delete group"); err != nil {
		return nil, err
	}

	if err := svc.SoftDeleteGroupChildren(spanCtx, in.GroupId, in.TenantId, in.UserId); err != nil {
		return nil, helpers.ErrorHandler(logger, svc, err, "error soft deleting group children groups", rollback)
	}

	if err := svc.SoftDeleteByID(spanCtx, in.GroupId, in.TenantId, in.UserId); err != nil {
		return nil, helpers.ErrorHandler(logger, svc, err, "error soft deleting group by id", rollback)
	}

	if err := helpers.CommitTransaction(logger, svc, "delete group transaction"); err != nil {
		return nil, err
	}

	// Check the tenant's remaining group count and reset hierarchy if there are 0 active groups left
	groupCount, err := svc.GetTenantActiveGroupCount(spanCtx, in.TenantId)
	if err != nil {
		return nil, helpers.ErrorHandler(logger, svc, err, "error getting tenant groups count", rollback)
	}
	if groupCount == 0 {
		if err := h.resetHierarchy(spanCtx, in.TenantId, in.UserId, nil); err != nil {
			// resetHierarchy already takes care of commiting/rolling back transaction, so need to handle that here
			return nil, helpers.ErrorHandler(logger, svc, err, "error resetting tenant hierarchy", rollback)
		}
		return &servicePb.Empty{}, nil
	}

	if err := helpers.CreateTransaction(h.db, logger, spanCtx, svc, "delete group"); err != nil {
		return nil, err
	}

	if err := svc.UpdateGroupPaths(spanCtx, in.TenantId); err != nil {
		return nil, helpers.ErrorHandler(logger, svc, err, "error updating group paths", rollback)
	}

	// Make sure group types are updated correctly
	if err := svc.UpdateGroupTypes(spanCtx, in.TenantId); err != nil {
		return nil, helpers.ErrorHandler(logger, svc, err, "error updating group types", rollback)
	}

	if err := helpers.CommitTransaction(logger, svc, "delete group transaction"); err != nil {
		return nil, err
	}

	if err := h.ensureTenantGroupSyncState(spanCtx, in.TenantId, nil); err != nil {
		return nil, helpers.ErrorHandler(logger, svc, err, "error ensuring tenant group sync state", rollback)
	}
	return &servicePb.Empty{}, helpers.CommitTransaction(logger, svc, "delete group transaction")
}

func (h *Handlers) ResetHierarchy(spanCtx context.Context, in *servicePb.ResetHierarchyRequest) (*servicePb.ResetHierarchyResponse, error) {

	logger := log.WithContext(spanCtx).WithTenantID(in.TenantId)

	if in.TenantId == "" {
		err := ErrBadRequest.New("tenantId can't be empty")
		logger.Warn(err.Error())
		return nil, err.AsGRPC()
	}

	userID := act.GetUserID(spanCtx)

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

func (h *Handlers) resetHierarchy(spanCtx context.Context, tenantID, userID string, tx *sql.Tx) error {

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

func (h *Handlers) GetTenantGroupsLastModifiedTS(spanCtx context.Context, in *servicePb.GetTenantGroupsLastModifiedTSRequest) (*servicePb.GetTenantGroupsLastModifiedTSResponse, error) {

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
