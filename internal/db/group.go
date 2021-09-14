package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	null "github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
	"github.com/volatiletech/sqlboiler/v4/types"
)

type GroupService struct {
	*DBService
}

func (db *DB) NewGroupService() *GroupService {
	return &GroupService{
		DBService: db.NewDBService(),
	}
}

func (svc *GroupService) FromProto(g *orchardPb.Group) *models.Group {
	createdAt := g.CreatedAt.AsTime()
	updatedAt := g.UpdatedAt.AsTime()

	return &models.Group{
		ID:                g.Id,
		TenantID:          g.TenantId,
		Name:              g.Name,
		Type:              strings.ToLower(g.Type.String()),
		Status:            strings.ToLower(g.Status.String()),
		RoleIds:           types.StringArray(g.RoleIds),
		CRMRoleIds:        types.StringArray(g.CrmRoleIds),
		ParentID:          null.NewString(g.ParentId, g.ParentId != ""),
		GroupPath:         strings.ReplaceAll(g.GroupPath, "-", "_"),
		Order:             int(g.Order),
		SyncFilter:        null.NewString(g.SyncFilter, g.SyncFilter != ""),
		OpportunityFilter: null.NewString(g.OpportunityFilter, g.OpportunityFilter != ""),
		CreatedAt:         createdAt,
		CreatedBy:         g.CreatedBy,
		UpdatedAt:         updatedAt,
		UpdatedBy:         g.UpdatedBy,
	}
}

func (svc *GroupService) ToProto(g *models.Group) (*orchardPb.Group, error) {
	createdAt, err := ptypes.TimestampProto(g.CreatedAt)
	if err != nil {
		return nil, err
	}

	updatedAt, err := ptypes.TimestampProto(g.UpdatedAt)
	if err != nil {
		return nil, err
	}

	status := orchardPb.BasicStatus_Inactive
	switch g.Status {
	case strings.ToLower(orchardPb.BasicStatus_Active.String()):
		status = orchardPb.BasicStatus_Active
	}

	typ := orchardPb.SystemRoleType_Unknown
	switch g.Type {
	case strings.ToLower(orchardPb.SystemRoleType_IC.String()):
		typ = orchardPb.SystemRoleType_IC
	case strings.ToLower(orchardPb.SystemRoleType_Internal.String()):
		typ = orchardPb.SystemRoleType_Internal
	case strings.ToLower(orchardPb.SystemRoleType_Manager.String()):
		typ = orchardPb.SystemRoleType_Manager
	}

	return &orchardPb.Group{
		Id:                g.ID,
		TenantId:          g.TenantID,
		Name:              g.Name,
		Type:              typ,
		Status:            status,
		RoleIds:           []string(g.RoleIds),
		CrmRoleIds:        []string(g.CRMRoleIds),
		ParentId:          g.ParentID.String,
		GroupPath:         strings.ReplaceAll(g.GroupPath, "_", "-"),
		Order:             int32(g.Order),
		SyncFilter:        g.SyncFilter.String,
		OpportunityFilter: g.OpportunityFilter.String,
		CreatedAt:         createdAt,
		CreatedBy:         g.CreatedBy,
		UpdatedAt:         updatedAt,
		UpdatedBy:         g.UpdatedBy,
	}, nil
}

var (
	groupInsertWhitelist = []string{
		"id", "tenant_id", "name", "type", "status",
		"role_ids", "crm_role_ids", "parent_id", "group_path",
		"order", "sync_filter", "opportunity_filter",
		"created_at", "created_by", "updated_at", "updated_by",
	}
)

func (svc *GroupService) Insert(ctx context.Context, g *models.Group) error {
	spanCtx, span := log.StartSpan(ctx, "Group.Insert")
	defer span.End()
	g.GroupPath = strings.ReplaceAll(g.GroupPath, "-", "_")
	return g.Insert(spanCtx, svc.GetContextExecutor(), boil.Whitelist(groupInsertWhitelist...))
}

func (svc *GroupService) GetByID(ctx context.Context, id, tenantID string) (*models.Group, error) {
	spanCtx, span := log.StartSpan(ctx, "Group.GetById")
	defer span.End()
	group, err := models.Groups(qm.Where("id=$1 AND tenant_id=$2", id, tenantID)).One(spanCtx, svc.GetContextExecutor())
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if err == sql.ErrNoRows || group == nil {
		return nil, nil
	}

	return group, nil
}

const (
	checkDuplicateCRMIDsQuery = `SELECT SUM(1) > 0 as has_dups
	FROM "group" g
	WHERE g.crm_role_ids && $1 AND tenant_id = $2 AND id <> $3
	GROUP BY tenant_id`
)

type HasDupsResult struct {
	HasDups bool `json:"has_dups" boil:"has_dups"`
}

func (svc *GroupService) CheckDuplicateCRMRoleIDs(ctx context.Context, id, tenantID string, crmRolesIDs []string) (bool, error) {
	spanCtx, span := log.StartSpan(ctx, "Group.CheckDuplicateCRMRoleIDs")
	defer span.End()
	result := HasDupsResult{}
	err := queries.Raw(checkDuplicateCRMIDsQuery, types.StringArray(crmRolesIDs), tenantID, id).Bind(spanCtx, svc.GetContextExecutor(), &result)
	if err != nil && err != sql.ErrNoRows {
		log.WithTenantID(tenantID).WithCustom("query", checkDuplicateCRMIDsQuery).Error(err)
		return false, err
	}
	if err == sql.ErrNoRows {
		return false, nil
	}
	return result.HasDups, nil
}

func (svc *GroupService) Search(ctx context.Context, tenantID, query string) ([]*models.Group, error) {
	spanCtx, span := log.StartSpan(ctx, "Group.Search")
	defer span.End()

	queryParts := []qm.QueryMod{}

	queryParts = append(queryParts, qm.Where("tenant_id=$1", tenantID))

	if query != "" {
		searchClause := "LOWER(name) LIKE $2"
		queryParts = append(queryParts, qm.Where(searchClause, "%"+strings.ToLower(query)+"%"))
	}

	queryParts = append(queryParts, qm.OrderBy("\"order\", name"))

	groups, err := models.Groups(queryParts...).All(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return nil, err
	}

	return groups, nil
}

type GroupTreeNode struct {
	models.Group `boil:",bind"`
	MembersRaw   types.StringArray `boil:"members_raw"`
	Members      []models.Person   `boil:"-"`
	Children     []*GroupTreeNode  `boil:"-"`
}

const (
	getGroupSubTreeQuery = `SELECT
	"group".id as "group.id", "group".tenant_id as "group.tenant_id", "group".name as "group.name", "group".type as "group.type",
	"group".status as "group.status", "group".role_ids as "group.role_ids", "group".crm_role_ids as "group.crm_role_ids", "group".parent_id as "group.parent_id",
	"group".group_path as "group.group_path", "group".order as "group.order", "group".sync_filter as "group.sync_filter", "group".opportunity_filter as "group.opportunity_filter",
	"group".created_at as "group.created_at", "group".created_by as "group.created_by", "group".updated_at as "group.updated_at", "group".updated_by as "group.updated_by",
	ARRAY_REMOVE(
		ARRAY_AGG(
			CASE WHEN p.id IS NULL THEN NULL ELSE
				{PERSON_SELECT}
			END
		),
		NULL
	) as "members_raw"
FROM "group"
LEFT OUTER JOIN person p ON p.group_id = "group".id AND p.tenant_id = "group".tenant_id {STATUS_PART}
WHERE {GROUP_SELECT} AND "group".tenant_id = $2 AND "group".status = 'active'
GROUP BY
	"group".id, "group".tenant_id, "group".name, "group".type, "group".status, "group".role_ids, "group".crm_role_ids, "group".parent_id,
	"group".group_path, "group".order, "group".sync_filter, "group".opportunity_filter, "group".created_at, "group".created_by, "group".updated_at, "group".updated_by
ORDER BY "group.name"`

	fullPersonSelectClause = `JSONB_BUILD_OBJECT(
		'id', p.id, 'tenant_id', p.tenant_id, 'name', p."name", 'first_name', p.first_name, 'last_name', p.last_name, 'email', p.email, 'manager_id', p.manager_id,
		'role_ids', p.role_ids, 'crm_role_ids', p.crm_role_ids, 'is_provisioned', p.is_provisioned, 'is_synced', p.is_synced, 'status', p.status,
		'created_at', TO_CHAR(p.created_at, 'YYYY-MM-DD"T"HH:MI:SS"Z"'), 'created_by', p.created_by,
		'updated_at', TO_CHAR(p.updated_at, 'YYYY-MM-DD"T"HH:MI:SS"Z"'), 'updated_by', p.updated_by,
		'type', p."type"
	)`

	rootGroupSelectorClause = `(
		"group".id = $1 OR (
			"group".group_path <@ (SELECT group_path FROM "group" WHERE id = $1 AND tenant_id = $2)
			AND (nlevel(group_path) - (SELECT nlevel(group_path) FROM "group" WHERE id = $1 AND tenant_id = $2)) <= $3
		)
	)`

	allTenantGroupsSelectorClause = `(
		"group".parent_id = '' OR (
			"group".group_path <@ $1::ltree AND (nlevel(group_path)) - (nlevel($1::ltree)) <= ($3 + 1)
		)
	)`

	simplieHierarchyWrapperQuery = `SELECT
	"group.id",
	"group.tenant_id",
	CASE WHEN "group.type" = 'manager' AND array_length(members_raw, 1) = 1 AND members_raw[1]->>'status' = 'active'  THEN (members_raw[1]->>'name')
	ELSE "group.name" END AS "group.name",
	"group.type",
	"group.status",
	"group.role_ids",
	"group.crm_role_ids",
	"group.parent_id",
	"group.group_path",
	"group.order",
	"group.sync_filter",
	"group.opportunity_filter",
	"group.created_at",
	"group.created_by",
	"group.updated_at",
	"group.updated_by",
	"members_raw"
FROM (
	{INNER_QUERY}
) x`
)

func (svc *GroupService) GetGroupSubTree(ctx context.Context, tenantID, groupID string, maxDepth int, hydrateUsers bool, simplify bool, activeUsers bool, useManagerNames bool) ([]*GroupTreeNode, error) {
	spanCtx, span := log.StartSpan(ctx, "Group.GetGroupSubTree")
	defer span.End()

	if maxDepth < 0 {
		maxDepth = 1000000
	}

	params := []interface{}{
		groupID,
		tenantID,
		maxDepth,
	}

	personSelect := "p.id"
	if hydrateUsers {
		personSelect = fullPersonSelectClause
	} else if useManagerNames {
		personSelect = `JSONB_BUILD_OBJECT(
			'id', p.id, 'name', p."name", 'status', p."status",	'type', p."type"
		)`
	}
	groupSelect := rootGroupSelectorClause
	if groupID == "" {
		groupSelect = allTenantGroupsSelectorClause
		params[0] = strings.ReplaceAll(tenantID, "-", "_")
	}

	statusPart := ""

	if activeUsers {
		statusPart = `AND p."status" = 'active'`
	}

	query := strings.ReplaceAll(getGroupSubTreeQuery, "{PERSON_SELECT}", personSelect)
	query = strings.ReplaceAll(query, "{STATUS_PART}", statusPart)
	query = strings.ReplaceAll(query, "{GROUP_SELECT}", groupSelect)

	if useManagerNames {
		query = strings.ReplaceAll(simplieHierarchyWrapperQuery, "{INNER_QUERY}", query)
	}

	results := []*GroupTreeNode{}
	if err := queries.Raw(query, params...).Bind(spanCtx, svc.GetContextExecutor(), &results); err != nil {
		log.WithTenantID(tenantID).WithCustom("groupId", groupID).WithCustom("maxDepth", maxDepth).WithCustom("hydrateUsers", hydrateUsers).WithCustom("query", query).Error(err)
		return nil, err
	}

	for i, node := range results {
		results[i].Members = make([]models.Person, len(node.MembersRaw))
		for j, memberRaw := range node.MembersRaw {
			member := models.Person{}
			if !hydrateUsers && !useManagerNames {
				member.ID = memberRaw
				results[i].Members[j] = member
				continue
			}
			if err := json.Unmarshal([]byte(memberRaw), &member); err != nil {
				return nil, err
			}
			results[i].Members[j] = member
		}
	}
	return results, nil
}

const (
	getFullTenantTreeQuery = `SELECT
	"group".id as "group.id", "group".tenant_id as "group.tenant_id", "group".name as "group.name", "group".type as "group.type",
	"group".status as "group.status", "group".role_ids as "group.role_ids", "group".crm_role_ids as "group.crm_role_ids", "group".parent_id as "group.parent_id",
	"group".group_path as "group.group_path", "group".order as "group.order", "group".sync_filter as "group.sync_filter", "group".opportunity_filter as "group.opportunity_filter",
	"group".created_at as "group.created_at", "group".created_by as "group.created_by", "group".updated_at as "group.updated_at", "group".updated_by as "group.updated_by",
	ARRAY_REMOVE(
		ARRAY_AGG(
			CASE WHEN p.id IS NULL THEN NULL ELSE
				{PERSON_SELECT}
			END
		),
		NULL
	) as "members_raw"
FROM "group"
LEFT OUTER JOIN person p ON p.group_id = "group".id AND p.tenant_id = "group".tenant_id
WHERE "group".tenant_id = $1 AND "group".status = 'active'
GROUP BY
	"group".id, "group".tenant_id, "group".name, "group".type, "group".status, "group".role_ids, "group".crm_role_ids, "group".parent_id,
	"group".group_path, "group".order, "group".sync_filter, "group".opportunity_filter, "group".created_at, "group".created_by, "group".updated_at, "group".updated_by
ORDER BY "group.name"`
)

func (svc *GroupService) GetFullTenantTree(ctx context.Context, tenantID string, hydrateUsers bool) ([]*GroupTreeNode, error) {
	spanCtx, span := log.StartSpan(ctx, "Group.GetFullTenantTree")
	defer span.End()

	personSelect := "p.id"
	if hydrateUsers {
		personSelect = `JSONB_BUILD_OBJECT(
			'id', p.id, 'tenant_id', p.tenant_id, 'name', p."name", 'first_name', p.first_name, 'last_name', p.last_name, 'email', p.email, 'manager_id', p.manager_id,
			'role_ids', p.role_ids, 'crm_role_ids', p.crm_role_ids, 'is_provisioned', p.is_provisioned, 'is_synced', p.is_synced, 'status', p.status,
			'created_at', TO_CHAR(p.created_at, 'YYYY-MM-DD"T"HH:MI:SS"Z"'), 'created_by', p.created_by,
			'updated_at', TO_CHAR(p.updated_at, 'YYYY-MM-DD"T"HH:MI:SS"Z"'), 'updated_by', p.updated_by
		)`
	}
	query := strings.ReplaceAll(getFullTenantTreeQuery, "{PERSON_SELECT}", personSelect)

	results := []*GroupTreeNode{}
	if err := queries.Raw(query, tenantID).Bind(spanCtx, svc.GetContextExecutor(), &results); err != nil {
		log.WithTenantID(tenantID).WithCustom("hydrateUsers", hydrateUsers).WithCustom("query", query).Error(err)
		return nil, err
	}

	for i, node := range results {
		results[i].Members = make([]models.Person, len(node.MembersRaw))
		for j, memberRaw := range node.MembersRaw {
			member := models.Person{}
			if !hydrateUsers {
				member.ID = memberRaw
				results[i].Members[j] = member
				continue
			}
			if err := json.Unmarshal([]byte(memberRaw), &member); err != nil {
				return nil, err
			}
			results[i].Members[j] = member
		}
	}

	return results, nil
}

var (
	defaultGroupUpdateWhitelist = []string{
		"name", "type", "status", "role_ids", "crm_role_ids",
		"parent_id", "group_path", "order", "sync_filter",
		"opportunity_filter", "updated_at", "updated_by",
	}
)

func (svc *GroupService) Update(ctx context.Context, g *models.Group, onlyFields []string) error {
	spanCtx, span := log.StartSpan(ctx, "Group.Update")
	defer span.End()

	whitelist := defaultGroupUpdateWhitelist
	if len(onlyFields) > 0 {
		whitelist = onlyFields
	}
	var hasUpdatedAt bool
	for _, f := range whitelist {
		if f == "updated_at" {
			hasUpdatedAt = true
		}
	}
	if !hasUpdatedAt {
		whitelist = append(whitelist, "updated_at")
	}

	g.GroupPath = strings.ReplaceAll(g.GroupPath, "-", "_")

	numAffected, err := g.Update(spanCtx, svc.GetContextExecutor(), boil.Whitelist(whitelist...))
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error updating group: update affected 0 rows")
	}

	return nil
}

const (
	updateGroupPathsQuery = `WITH RECURSIVE group_tree AS (
		SELECT id, tenant_id, parent_id, CONCAT(REPLACE($1, '-', '_'), '.', REPLACE(id, '-', '_')) as group_path
		FROM "group"
		WHERE tenant_id::TEXT = $1 AND parent_id IS NULL AND status = 'active'
		UNION
		SELECT g.id, g.tenant_id, g.parent_id, CONCAT(gt.group_path, '.', REPLACE(g.id, '-', '_')) as group_path
		FROM "group" g
		INNER JOIN group_tree gt ON gt.id = g.parent_id AND gt.tenant_id = g.tenant_id
		WHERE g.status = 'active' AND g.tenant_id::TEXT = $1
	)
	UPDATE "group"
	SET group_path = gt.group_path::ltree
	FROM group_tree gt
	WHERE "group".tenant_id = gt.tenant_id AND "group".id = gt.id`
)

func (svc *GroupService) UpdateGroupPaths(ctx context.Context, tenantID string) error {
	spanCtx, span := log.StartSpan(ctx, "Group.UpdateGroupPaths")
	defer span.End()
	if _, err := queries.Raw(updateGroupPathsQuery, tenantID).ExecContext(spanCtx, svc.GetContextExecutor()); err != nil {
		log.WithTenantID(tenantID).WithCustom("query", updateGroupPathsQuery).Error(err)
		return err
	}
	return nil
}

func (svc *GroupService) Reload(ctx context.Context, group *models.Group) error {
	spanCtx, span := log.StartSpan(ctx, "Group.Reload")
	defer span.End()
	return group.Reload(spanCtx, svc.GetContextExecutor())
}

func (svc *GroupService) DeleteByID(ctx context.Context, id, tenantID string) error {
	spanCtx, span := log.StartSpan(ctx, "Group.DeleteById")
	defer span.End()
	group := &models.Group{ID: id, TenantID: tenantID}
	numAffected, err := group.Delete(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error deleting group: delete affected 0 rows")
	}
	return nil
}

func (svc *GroupService) SoftDeleteByID(ctx context.Context, id, tenantID, userID string) error {
	spanCtx, span := log.StartSpan(ctx, "Group.SoftDeleteById")
	defer span.End()
	group := &models.Group{ID: id, TenantID: tenantID, UpdatedBy: userID, Status: "inactive", UpdatedAt: time.Now().UTC(), CRMRoleIds: types.StringArray{}}
	numAffected, err := group.Update(spanCtx, svc.GetContextExecutor(), boil.Whitelist("updated_at", "updated_by", "status", "crm_role_ids"))
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error soft deleting group: delete affected 0 rows")
	}
	return nil
}

const (
	softDeleteGroupChildrenQuery = `UPDATE "group"
	SET parent_id = NULL, status = 'inactive', crm_role_ids = '{}', updated_at = CURRENT_TIMESTAMP, updated_by = $3
	FROM (
		SELECT id, tenant_id
		FROM "group" g
		WHERE g.tenant_id = $2 AND g.group_path <@ (SELECT group_path FROM "group" WHERE id = $1 AND tenant_id = $2)
	) g
	WHERE "group".id = g.id AND "group".tenant_id = g.tenant_id`

	removeGroupMembersRecursiveQuery = `UPDATE person
	SET group_id = NULL, updated_at = CURRENT_TIMESTAMP, updated_by = $3
	FROM (
		SELECT p.id, p.tenant_id
		FROM person p
		INNER JOIN (
			SELECT id, tenant_id
			FROM "group" g
			WHERE g.tenant_id = $2 AND g.group_path <@ (SELECT group_path FROM "group" WHERE id = $1 AND tenant_id = $2)
		) g ON p.group_id = g.id AND p.tenant_id = g.tenant_id
		WHERE p.tenant_id = $2
	) p
	WHERE person.id = p.id AND person.tenant_id = p.tenant_id`
)

func (svc *GroupService) SoftDeleteGroupChildren(ctx context.Context, id, tenantID, userID string) error {
	spanCtx, span := log.StartSpan(ctx, "Group.SoftDeleteGroupChildren")
	defer span.End()

	log.WithTenantID(tenantID).WithCustom("groupId", id).WithCustom("userId", userID).WithCustom("query", softDeleteGroupChildrenQuery).Debug("removeGroupMembersRecursiveQuery")
	if _, err := queries.Raw(removeGroupMembersRecursiveQuery, id, tenantID, userID).ExecContext(spanCtx, svc.GetContextExecutor()); err != nil {
		return err
	}

	log.WithTenantID(tenantID).WithCustom("groupId", id).WithCustom("userId", userID).WithCustom("query", softDeleteGroupChildrenQuery).Debug("softDeleteGroupChildrenQuery")
	if _, err := queries.Raw(softDeleteGroupChildrenQuery, id, tenantID, userID).ExecContext(spanCtx, svc.GetContextExecutor()); err != nil {
		return err
	}

	return nil
}

const (
	softDeleteTenantGroupsQuery = `UPDATE "group"
	SET status = 'inactive', crm_role_ids = '{}', updated_by = $1, updated_at = CURRENT_TIMESTAMP
	WHERE tenant_id = $2`
)

func (svc *GroupService) SoftDeleteTenantGroups(ctx context.Context, tenantID, userID string) error {
	spanCtx, span := log.StartSpan(ctx, "Group.SoftDeleteTenantGroups")
	defer span.End()
	_, err := queries.Raw(softDeleteTenantGroupsQuery, userID, tenantID).ExecContext(spanCtx, svc.GetContextExecutor())
	return err
}

const (
	transferGroupChildrenParentQuery = `UPDATE "group"
	SET parent_id = (SELECT parent_id FROM "group" WHERE id = $1 AND tenant_id = $2 LIMIT 1), updated_by = $3, updated_at = CURRENT_TIMESTAMP
	WHERE parent_id = $1 AND tenant_id = $2`
)

func (svc *GroupService) TransferGroupChildrenParent(ctx context.Context, groupID, tenantID, userID string) error {
	spanCtx, span := log.StartSpan(ctx, "Group.TransferGroupChildrenParent")
	defer span.End()
	_, err := queries.Raw(transferGroupChildrenParentQuery, groupID, tenantID, userID).ExecContext(spanCtx, svc.GetContextExecutor())
	return err
}

const (
	removeGroupMembersQuery = `UPDATE person
	SET group_id = NULL, updated_by = $3, updated_at = CURRENT_TIMESTAMP
	WHERE group_id = $1 AND tenant_id = $2`
)

func (svc *GroupService) RemoveGroupMembers(ctx context.Context, groupID, tenantID, userID string) error {
	spanCtx, span := log.StartSpan(ctx, "Group.RemoveGroupMembers")
	defer span.End()
	_, err := queries.Raw(removeGroupMembersQuery, groupID, tenantID, userID).ExecContext(spanCtx, svc.GetContextExecutor())
	return err
}

const (
	removeAllGroupMembersQuery = `UPDATE person
	SET group_id = NULL, updated_by = $1, updated_at = CURRENT_TIMESTAMP
	WHERE tenant_id = $2`
)

func (svc *GroupService) RemoveAllGroupMembers(ctx context.Context, tenantID, userID string) error {
	spanCtx, span := log.StartSpan(ctx, "Group.RemoveAllGroupMembers")
	defer span.End()
	_, err := queries.Raw(removeAllGroupMembersQuery, userID, tenantID).ExecContext(spanCtx, svc.GetContextExecutor())
	return err
}

const (
	isCRMSyncedQuery = `WITH UserCreated AS (
		SELECT SUM(CASE WHEN created_by <> '00000000-0000-0000-0000-000000000000' THEN 1 ELSE 0 END) as user_created_count
		FROM "group"
		WHERE tenant_id = $1
		GROUP BY tenant_id
	)
	SELECT
		COALESCE((SELECT user_created_count > 0 FROM UserCreated), FALSE)
		OR
		(SUM(CASE WHEN cr.id IS NULL THEN 1 ELSE 0 END) > 0)
		OR
		(SUM(CASE WHEN g.id IS NOT NULL AND (g.status = 'inactive' OR ARRAY_LENGTH(g.crm_role_ids, 1) > 1) THEN 1 ELSE 0 END)) > 1
		AS is_not_synced
	FROM crm_role cr
	FULL OUTER JOIN "group" g ON cr.id = ANY(g.crm_role_ids) AND cr.tenant_id = g.tenant_id
	WHERE cr.tenant_id = $1 OR g.tenant_id = $1`
)

type IsCRMSyncedResult struct {
	IsNotSynced sql.NullBool `boil:"is_not_synced" json:"is_not_synced"`
}

func (svc *GroupService) IsCRMSynced(ctx context.Context, tenantID string) (bool, error) {
	spanCtx, span := log.StartSpan(ctx, "Group.IsCRMSynced")
	defer span.End()
	result := &IsCRMSyncedResult{}
	err := queries.Raw(isCRMSyncedQuery, tenantID).Bind(spanCtx, svc.GetContextExecutor(), result)
	if err != nil && err != sql.ErrNoRows {
		log.WithTenantID(tenantID).WithCustom("query", isCRMSyncedQuery).Error(err)
		return false, err
	}
	if err == sql.ErrNoRows { // This should only happen if the tenant has no crm_roles and no groups in postgres, in which case there is nothing to sync anyway
		return false, nil
	}
	isSynced := false
	if !result.IsNotSynced.Valid || !result.IsNotSynced.Bool {
		isSynced = true
	}
	return isSynced, nil
}

const (
	syncGroupsQuery = `WITH SyncedGroups AS (
		SELECT DISTINCT
			COALESCE(g.id, uuid_generate_v1()::TEXT) AS id,
			cr.tenant_id,
			COALESCE(cr."name", g."name", 'Unknown') AS "name",
			COALESCE(
				(CASE WHEN cr2.parent_id IS NULL THEN 'ic' ELSE 'manager' END)::GROUP_TYPE,
				g."type",
				'ic'
			) AS "type",
			COALESCE(g.status, 'active') AS status,
			COALESCE(g.role_ids, '{}') AS role_ids,
			COALESCE(ARRAY[cr.id], g.crm_role_ids, '{}') AS crm_role_ids,
			cr.parent_id AS crm_parent_id, -- Used for identifying actual group parent later
			cr.id AS crm_role_id, --  Used for identifying actual group parent later
			COALESCE(g.group_path, ''::ltree) AS group_path,
			COALESCE(g."order", 0) AS "order",
			COALESCE(g.sync_filter, NULL) AS sync_filter,
			COALESCE(g.opportunity_filter, NULL) AS opportunity_filter,
			'00000000-0000-0000-0000-000000000000' AS created_by,
			CURRENT_TIMESTAMP AS created_at,
			'00000000-0000-0000-0000-000000000000' AS updated_by,
			CURRENT_TIMESTAMP AS updated_at
		FROM crm_role cr
		LEFT OUTER JOIN "group" g ON cr.id = ANY(g.crm_role_ids) AND cr.tenant_id = g.tenant_id
		LEFT OUTER JOIN (
			SELECT DISTINCT parent_id, tenant_id
			FROM crm_role
		) cr2 ON cr.id = cr2.parent_id AND cr.tenant_id = cr2.tenant_id
		WHERE cr.tenant_id = $1
	)
	INSERT INTO "group"
	(id, tenant_id, "name", "type", status, role_ids, crm_role_ids, parent_id, group_path, "order", sync_filter, opportunity_filter, created_by, created_at, updated_by, updated_at)
	SELECT
		sg1.id, sg1.tenant_id, sg1."name", sg1."type", sg1.status, sg1.role_ids, sg1.crm_role_ids, sg2.id AS parent_id,
		sg1.group_path, sg1."order", sg1.sync_filter, sg1.opportunity_filter, sg1.created_by, sg1.created_at, sg1.updated_by, sg1.updated_at
	FROM SyncedGroups sg1
	LEFT OUTER JOIN SyncedGroups sg2 ON sg1.crm_parent_id = sg2.crm_role_id
	ON CONFLICT (id, tenant_id) DO UPDATE
	SET "name" = EXCLUDED."name", "type" = EXCLUDED."type", status = EXCLUDED.status, role_ids = EXCLUDED.role_ids, crm_role_ids = EXCLUDED.crm_role_ids,
		parent_id = EXCLUDED.parent_id, group_path = EXCLUDED.group_path, "order" = EXCLUDED."order", sync_filter = EXCLUDED.sync_filter, opportunity_filter = EXCLUDED.opportunity_filter,
		created_by = EXCLUDED.created_by, created_at = EXCLUDED.created_at, updated_by = EXCLUDED.updated_by, updated_at = EXCLUDED.updated_at;`
)

func (svc *GroupService) SyncGroups(ctx context.Context, tenantID string) error {
	spanCtx, span := log.StartSpan(ctx, "Group.SyncGroups")
	defer span.End()
	if _, err := queries.Raw(syncGroupsQuery, tenantID).ExecContext(spanCtx, svc.GetContextExecutor()); err != nil {
		log.WithTenantID(tenantID).WithCustom("query", syncGroupsQuery).Error(err)
		return err
	}
	return nil
}

const (
	deleteUnSyncedGroupsQuery = `DELETE FROM "group"
	WHERE tenant_id = $1 AND id IN (
		SELECT g.id
		FROM "group" g
		LEFT OUTER JOIN crm_role cr ON cr.id = ANY(g.crm_role_ids) AND cr.tenant_id = g.tenant_id
		WHERE cr.id IS NULL AND g.tenant_id = $1
	)`
)

func (svc *GroupService) DeleteUnSyncedGroups(ctx context.Context, tenantID string) error {
	spanCtx, span := log.StartSpan(ctx, "Group.DeleteUnSyncedGroups")
	defer span.End()
	if _, err := queries.Raw(deleteUnSyncedGroupsQuery, tenantID).ExecContext(spanCtx, svc.GetContextExecutor()); err != nil {
		log.WithTenantID(tenantID).WithCustom("query", deleteUnSyncedGroupsQuery).Error(err)
		return err
	}
	return nil
}

const (
	updateGroupTypesQuery = `UPDATE "group"
	SET "type" = groups."type"::GROUP_TYPE, updated_by = '00000000-0000-0000-0000-000000000000', updated_at = CURRENT_TIMESTAMP
	FROM (
		SELECT g.id, g.tenant_id, CASE WHEN g2.id IS NULL THEN 'ic' ELSE 'manager' END AS "type"
		FROM "group" g
		LEFT OUTER JOIN "group" g2 ON g.tenant_id = g2.tenant_id AND g.id = g2.parent_id
		WHERE g.tenant_id = $1
	) groups
	WHERE "group".id = groups.id AND "group".tenant_id = groups.tenant_id
	`

	updatePersonTypesQuery = `UPDATE person
	SET "type" = pg."type"::person_type, updated_by = '00000000-0000-0000-0000-000000000000', updated_at = CURRENT_TIMESTAMP
	FROM (
		SELECT p.id, p.tenant_id, COALESCE(g."type"::text, 'ic') AS "type"
		FROM person p
		LEFT OUTER JOIN "group" g ON p.group_id = g.id AND p.tenant_id = g.tenant_id
		WHERE p.tenant_id = $1
	) pg
	WHERE person.id = pg.id AND person.tenant_id = pg.tenant_id`
)

func (svc *GroupService) UpdateGroupTypes(ctx context.Context, tenantID string) error {
	spanCtx, span := log.StartSpan(ctx, "Group.UpdateGroupTypes")
	defer span.End()
	if _, err := queries.Raw(updateGroupTypesQuery, tenantID).ExecContext(spanCtx, svc.GetContextExecutor()); err != nil {
		log.WithTenantID(tenantID).WithCustom("query", updateGroupTypesQuery).Error(err)
		return err
	}
	if _, err := queries.Raw(updatePersonTypesQuery, tenantID).ExecContext(spanCtx, svc.GetContextExecutor()); err != nil {
		log.WithTenantID(tenantID).WithCustom("query", updatePersonTypesQuery).Error(err)
		return err
	}
	return nil
}

const (
	deleteAllTenantGroupQuery = `DELETE FROM "group" WHERE tenant_id = $1`
)

func (svc *GroupService) DeleteAllTenantGroups(ctx context.Context, tenantID string) error {
	spanCtx, span := log.StartSpan(ctx, "Group.DeleteAllTenantGroups")
	defer span.End()
	if _, err := queries.Raw(deleteAllTenantGroupQuery, tenantID).ExecContext(spanCtx, svc.GetContextExecutor()); err != nil {
		log.WithTenantID(tenantID).WithCustom("query", deleteAllTenantGroupQuery).Error(err)
		return err
	}
	return nil
}

const (
	getLastModifiedTSQuery = `SELECT MAX(updated_at) as latest_modified_ts
	FROM "group"
	WHERE tenant_id = $1
	GROUP BY tenant_id`
)

type GetLatestModifiedTSResult struct {
	LatestModifiedTS time.Time `json:"latestModifiedTS" boil:"latest_modified_ts"`
}

func (svc *GroupService) GetLatestModifiedTS(ctx context.Context, tenantID string) (time.Time, error) {
	spanCtx, span := log.StartSpan(ctx, "Group.GetLatestModifiedTS")
	defer span.End()
	res := GetLatestModifiedTSResult{}
	err := queries.Raw(getLastModifiedTSQuery, tenantID).Bind(spanCtx, svc.GetContextExecutor(), &res)
	if err != nil && err != sql.ErrNoRows {
		log.WithTenantID(tenantID).WithCustom("query", getLastModifiedTSQuery).Error(err)
		return time.Time{}, err
	}
	if err == sql.ErrNoRows {
		return time.Time{}, nil
	}
	return res.LatestModifiedTS, nil
}

func (svc *GroupService) GetTenantGroupCount(ctx context.Context, tenantID string) (int64, error) {
	spanCtx, span := log.StartSpan(ctx, "Group.GetTenantGroupCount")
	defer span.End()
	count, err := models.Groups(qm.Where("tenant_id = $1", tenantID)).Count(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (svc *GroupService) GetTenantActiveGroupCount(ctx context.Context, tenantID string) (int64, error) {
	spanCtx, span := log.StartSpan(ctx, "Group.GetTenantActiveGroupCount")
	defer span.End()
	count, err := models.Groups(qm.Where("tenant_id = $1 AND status = 'active'", tenantID)).Count(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return 0, err
	}
	return count, nil
}
