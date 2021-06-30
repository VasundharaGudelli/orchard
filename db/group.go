package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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

func (svc *GroupService) FromProto(g *orchardPb.Group) *models.Group {
	createdAt := g.CreatedAt.AsTime()
	updatedAt := g.UpdatedAt.AsTime()

	return &models.Group{
		ID:         g.Id,
		TenantID:   g.TenantId,
		Name:       g.Name,
		Type:       strings.ToLower(g.Type.String()),
		Status:     strings.ToLower(g.Status.String()),
		RoleIds:    types.StringArray(g.RoleIds),
		CRMRoleIds: types.StringArray(g.CrmRoleIds),
		ParentID:   null.NewString(g.ParentId, g.ParentId != ""),
		GroupPath:  g.GroupPath,
		Order:      int(g.Order),
		CreatedAt:  createdAt,
		CreatedBy:  g.CreatedBy,
		UpdatedAt:  updatedAt,
		UpdatedBy:  g.UpdatedBy,
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
		Id:         g.ID,
		TenantId:   g.TenantID,
		Name:       g.Name,
		Type:       typ,
		Status:     status,
		RoleIds:    []string(g.RoleIds),
		CrmRoleIds: []string(g.CRMRoleIds),
		ParentId:   g.ParentID.String,
		GroupPath:  g.GroupPath,
		Order:      int32(g.Order),
		CreatedAt:  createdAt,
		CreatedBy:  g.CreatedBy,
		UpdatedAt:  updatedAt,
		UpdatedBy:  g.UpdatedBy,
	}, nil
}

var (
	groupInsertWhitelist = []string{
		"id", "tenant_id", "name", "type", "status",
		"role_ids", "crm_role_ids", "parent_id", "group_path",
		"order", "created_at", "created_by", "updated_at", "updated_by",
	}
)

func (svc *GroupService) Insert(ctx context.Context, g *models.Group) error {
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
	Group    *models.Group    `boil:"group,bind"`
	Members  []*models.Person `boil:"members,bind"`
	Children []*GroupTreeNode `boil:"-"`
}

const (
	getGroupSubTreeQuery = `SELECT g as "group", ARRAY_AGG({PERSON_SELECT}) as "members"
	FROM "group" g INNER JOIN person p ON p.group_id = g.id AND p.tenant_id = g.tenant_id
	WHERE (
		g.id = $1 OR (
			g.group_path <@ (SELECT group_path FROM "group" WHERE id = $1 AND tenant_id = $2)
			AND (nlevel(group_path) - (SELECT nlevel(group_path) FROM "group" WHERE id = $1 AND tenant_id = $2)) <= $3
		)
	) AND g.tenant_id = $2
	GROUP BY "group"`
)

func (svc *GroupService) GetGroupSubTree(ctx context.Context, tenantID, groupID string, maxDepth int, hydrateUsers bool) ([]*GroupTreeNode, error) {
	if maxDepth < 0 {
		maxDepth = 1000000
	}
	personSelect := "(p.id)"
	if hydrateUsers {
		personSelect = "p"
	}
	query := strings.ReplaceAll(getGroupSubTreeQuery, "{PERSON_SELECT}", personSelect)

	results := []*GroupTreeNode{}
	if err := queries.Raw(query, groupID, tenantID, maxDepth).Bind(ctx, Global, results); err != nil {
		log.WithTenantID(tenantID).WithCustom("groupId", groupID).WithCustom("maxDepth", maxDepth).WithCustom("hydrateUsers", hydrateUsers).WithCustom("query", query).Error(err)
		return nil, err
	}

	return results, nil
}

var (
	defaultGroupUpdateWhitelist = []string{
		"name", "type", "status", "role_ids", "crm_role_ids",
		"parent_id", "group_path", "order", "updated_at", "updated_by",
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
		SELECT id, tenant_id, parent_id, CONCAT($1, '.', id) as group_path
		FROM "group"
		WHERE tenant_id = $1 AND parent_id IS NULL
		UNION
		SELECT g.id, g.tenant_id, g.parent_id, CONCAT(gt.group_path, '.', g.id) as group_path
		FROM "group" g
		INNER JOIN group_tree gt ON gt.id = g.parent_id AND gt.tenant_id = g.tenant_id
		WHERE g.tenant_id = $1
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

func (svc *GroupService) DeleteByID(ctx context.Context, id, tenantID string) error {
	group := &models.Group{ID: id, TenantID: tenantID}
	numAffected, err := group.Delete(ctx, Global)
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error deleting group: delete affected 0 rows")
	}
	return nil
}
