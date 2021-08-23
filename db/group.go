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
	"github.com/loupe-co/orchard/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	null "github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
	"github.com/volatiletech/sqlboiler/v4/types"
)

// TODO: Add tracing

type GroupService struct {
	tx *sql.Tx
}

func NewGroupService() *GroupService {
	return &GroupService{}
}

func (svc *GroupService) WithTransaction(ctx context.Context) error {
	tx, err := Global.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	svc.tx = tx
	return nil
}

func (svc *GroupService) Rollback() error {
	if svc.tx == nil {
		return nil
	}
	return svc.tx.Rollback()
}

func (svc *GroupService) Commit() error {
	if svc.tx == nil {
		return nil
	}
	return svc.tx.Commit()
}

func (svc *GroupService) GetTX() *sql.Tx {
	return svc.tx
}

func (svc *GroupService) SetTx(tx *sql.Tx) {
	svc.tx = tx
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
	g.GroupPath = strings.ReplaceAll(g.GroupPath, "-", "_")
	if svc.tx != nil {
		return g.Insert(ctx, svc.tx, boil.Whitelist(groupInsertWhitelist...))
	}
	return g.Insert(ctx, Global, boil.Whitelist(groupInsertWhitelist...))
}

func (svc *GroupService) GetByID(ctx context.Context, id, tenantID string) (*models.Group, error) {
	group, err := models.Groups(qm.Where("id=$1 AND tenant_id=$2", id, tenantID)).One(ctx, Global)
	if err != nil {
		return nil, err
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
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	result := HasDupsResult{}
	err := queries.Raw(checkDuplicateCRMIDsQuery, types.StringArray(crmRolesIDs), tenantID, id).Bind(ctx, x, &result)
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
	queryParts := []qm.QueryMod{}

	queryParts = append(queryParts, qm.Where("tenant_id=$1", tenantID))

	if query != "" {
		searchClause := "LOWER(name) LIKE $2"
		queryParts = append(queryParts, qm.Where(searchClause, "%"+strings.ToLower(query)+"%"))
	}

	queryParts = append(queryParts, qm.OrderBy("order, name"))

	groups, err := models.Groups(queryParts...).All(ctx, Global)
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
LEFT OUTER JOIN person p ON p.group_id = "group".id AND p.tenant_id = "group".tenant_id
WHERE {GROUP_SELECT} AND "group".tenant_id = $2 AND "group".status = 'active'
GROUP BY
	"group".id, "group".tenant_id, "group".name, "group".type, "group".status, "group".role_ids, "group".crm_role_ids, "group".parent_id,
	"group".group_path, "group".order, "group".sync_filter, "group".opportunity_filter, "group".created_at, "group".created_by, "group".updated_at, "group".updated_by
ORDER BY "group.name"`

	fullPersonSelectClause = `JSONB_BUILD_OBJECT(
		'id', p.id, 'tenant_id', p.tenant_id, 'name', p."name", 'first_name', p.first_name, 'last_name', p.last_name, 'email', p.email, 'manager_id', p.manager_id,
		'role_ids', p.role_ids, 'crm_role_ids', p.crm_role_ids, 'is_provisioned', p.is_provisioned, 'is_synced', p.is_synced, 'status', p.status,
		'created_at', TO_CHAR(p.created_at, 'YYYY-MM-DD"T"HH:MI:SS"Z"'), 'created_by', p.created_by,
		'updated_at', TO_CHAR(p.updated_at, 'YYYY-MM-DD"T"HH:MI:SS"Z"'), 'updated_by', p.updated_by
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
)

func (svc *GroupService) GetGroupSubTree(ctx context.Context, tenantID, groupID string, maxDepth int, hydrateUsers bool) ([]*GroupTreeNode, error) {
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
	}
	groupSelect := rootGroupSelectorClause
	if groupID == "" {
		groupSelect = allTenantGroupsSelectorClause
		params[0] = strings.ReplaceAll(tenantID, "-", "_")
	}

	query := strings.ReplaceAll(getGroupSubTreeQuery, "{PERSON_SELECT}", personSelect)
	query = strings.ReplaceAll(query, "{GROUP_SELECT}", groupSelect)

	results := []*GroupTreeNode{}
	if err := queries.Raw(query, params...).Bind(ctx, Global, &results); err != nil {
		log.WithTenantID(tenantID).WithCustom("groupId", groupID).WithCustom("maxDepth", maxDepth).WithCustom("hydrateUsers", hydrateUsers).WithCustom("query", query).Error(err)
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
	if err := queries.Raw(query, tenantID).Bind(ctx, Global, &results); err != nil {
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

	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	numAffected, err := g.Update(ctx, x, boil.Whitelist(whitelist...))
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
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	if _, err := queries.Raw(updateGroupPathsQuery, tenantID).ExecContext(ctx, x); err != nil {
		log.WithTenantID(tenantID).WithCustom("query", updateGroupPathsQuery).Error(err)
		return err
	}
	return nil
}

func (svc *GroupService) Reload(ctx context.Context, group *models.Group) error {
	if svc.tx != nil {
		return group.Reload(ctx, svc.tx)
	}
	return group.Reload(ctx, Global)
}

func (svc *GroupService) DeleteByID(ctx context.Context, id, tenantID string) error {
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	group := &models.Group{ID: id, TenantID: tenantID}
	numAffected, err := group.Delete(ctx, x)
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error deleting group: delete affected 0 rows")
	}
	return nil
}

func (svc *GroupService) SoftDeleteByID(ctx context.Context, id, tenantID, userID string) error {
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	group := &models.Group{ID: id, TenantID: tenantID, UpdatedBy: userID, Status: "inactive", UpdatedAt: time.Now().UTC()}
	numAffected, err := group.Update(ctx, x, boil.Whitelist("updated_at", "updated_by", "status"))
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error soft deleting group: delete affected 0 rows")
	}
	return nil
}

const (
	softDeleteTenantGroupsQuery = `UPDATE "group"
	SET status = 'inactive', updated_by = $1, updated_at = CURRENT_TIMESTAMP
	WHERE tenant_id = $2`
)

func (svc *GroupService) SoftDeleteTenantGroups(ctx context.Context, tenantID, userID string) error {
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	_, err := queries.Raw(softDeleteTenantGroupsQuery, userID, tenantID).ExecContext(ctx, x)
	return err
}

const (
	transferGroupChildrenParentQuery = `UPDATE "group"
	SET parent_id = (SELECT parent_id FROM "group" WHERE id = $1 AND tenant_id = $2 LIMIT 1), updated_by = $3, updated_at = CURRENT_TIMESTAMP
	WHERE parent_id = $1 AND tenant_id = $2`
)

func (svc *GroupService) TransferGroupChildrenParent(ctx context.Context, groupID, tenantID, userID string) error {
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	_, err := queries.Raw(transferGroupChildrenParentQuery, groupID, tenantID, userID).ExecContext(ctx, x)
	return err
}

const (
	removeGroupMembersQuery = `UPDATE person
	SET group_id = NULL, updated_by = $3, updated_at = CURRENT_TIMESTAMP
	WHERE group_id = $1 AND tenant_id = $2`
)

func (svc *GroupService) RemoveGroupMembers(ctx context.Context, groupID, tenantID, userID string) error {
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	_, err := queries.Raw(removeGroupMembersQuery, groupID, tenantID, userID).ExecContext(ctx, x)
	return err
}

const (
	removeAllGroupMembersQuery = `UPDATE person
	SET group_id = NULL, updated_by = $1, updated_at = CURRENT_TIMESTAMP
	WHERE tenant_id = $2`
)

func (svc *GroupService) RemoveAllGroupMembers(ctx context.Context, tenantID, userID string) error {
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	_, err := queries.Raw(removeAllGroupMembersQuery, userID, tenantID).ExecContext(ctx, x)
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
	WHERE cr.tenant_id = $1
	GROUP BY cr.tenant_id`
)

type IsCRMSyncedResult struct {
	IsNotSynced bool `boil:"is_not_synced" json:"is_not_synced"`
}

func (svc *GroupService) IsCRMSynced(ctx context.Context, tenantID string) (bool, error) {
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	result := &IsCRMSyncedResult{}
	err := queries.Raw(isCRMSyncedQuery, tenantID).Bind(ctx, x, result)
	if err != nil && err != sql.ErrNoRows {
		log.WithTenantID(tenantID).WithCustom("query", isCRMSyncedQuery).Error(err)
		return false, err
	}
	if err == sql.ErrNoRows { // This should only happen if the tenant has no crm_roles and no groups in postgres, in which case there is nothing to sync anyway
		return false, nil
	}
	return !result.IsNotSynced, nil
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
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	if _, err := queries.Raw(syncGroupsQuery, tenantID).ExecContext(ctx, x); err != nil {
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
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	if _, err := queries.Raw(deleteUnSyncedGroupsQuery, tenantID).ExecContext(ctx, x); err != nil {
		log.WithTenantID(tenantID).WithCustom("query", deleteUnSyncedGroupsQuery).Error(err)
		return err
	}
	return nil
}

const (
	updateGroupTypesQuery = `UPDATE "group"
	SET "type" = groups."type"::GROUP_TYPE
	FROM (
		SELECT g.id, g.tenant_id, CASE WHEN g2.id IS NULL THEN 'ic' ELSE 'manager' END AS "type"
		FROM "group" g
		LEFT OUTER JOIN "group" g2 ON g.tenant_id = g2.tenant_id AND g.id = g2.parent_id
		WHERE g.tenant_id = $1
	) groups
	WHERE "group".id = groups.id AND "group".tenant_id = groups.tenant_id`
)

func (svc *GroupService) UpdateGroupTypes(ctx context.Context, tenantID string) error {
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	if _, err := queries.Raw(updateGroupTypesQuery, tenantID).ExecContext(ctx, x); err != nil {
		log.WithTenantID(tenantID).WithCustom("query", updateGroupTypesQuery).Error(err)
		return err
	}
	return nil
}

const (
	deleteAllTenantGroupQuery = `DELETE FROM "group" WHERE tenant_id = $1`
)

func (svc *GroupService) DeleteAllTenantGroups(ctx context.Context, tenantID string) error {
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	if _, err := queries.Raw(deleteAllTenantGroupQuery, tenantID).ExecContext(ctx, x); err != nil {
		log.WithTenantID(tenantID).WithCustom("query", deleteAllTenantGroupQuery).Error(err)
		return err
	}
	return nil
}
