package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang/protobuf/ptypes"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	null "github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
)

type CRMRoleService struct {
	*DBService
}

func (db *DB) NewCRMRoleService() *CRMRoleService {
	return &CRMRoleService{
		DBService: db.NewDBService(),
	}
}

func (svc *CRMRoleService) FromProto(cr *orchardPb.CRMRole) *models.CRMRole {
	updatedAt := cr.UpdatedAt.AsTime()

	return &models.CRMRole{
		ID:          cr.Id,
		TenantID:    cr.TenantId,
		Name:        cr.Name,
		Description: null.NewString(cr.Description, cr.Description != ""),
		ParentID:    null.NewString(cr.ParentId, cr.ParentId != ""),
		UpdatedAt:   updatedAt,
	}
}

func (svc *CRMRoleService) ToProto(cr *models.CRMRole) (*orchardPb.CRMRole, error) {
	updatedAt, err := ptypes.TimestampProto(cr.UpdatedAt)
	if err != nil {
		return nil, err
	}

	return &orchardPb.CRMRole{
		Id:          cr.ID,
		TenantId:    cr.TenantID,
		Name:        cr.Name,
		Description: cr.Description.String,
		ParentId:    cr.ParentID.String,
		UpdatedAt:   updatedAt,
	}, nil
}

var (
	crmRoleInsertWhitelist = []string{"id", "tenant_id", "name", "description", "parent_id", "updated_at"}
)

func (svc *CRMRoleService) Insert(ctx context.Context, cr *models.CRMRole) error {
	spanCtx, span := log.StartSpan(ctx, "CRMRole.Insert")
	defer span.End()
	return cr.Insert(spanCtx, svc.GetContextExecutor(), boil.Whitelist(crmRoleInsertWhitelist...))
}

const (
	crmRoleUpsertAllQuery = "INSERT INTO crm_role (id, tenant_id, name, description, parent_id, updated_at) VALUES\n\t{SUBS}\nON CONFLICT (tenant_id, id) DO\n\tUPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, parent_id = EXCLUDED.parent_id, updated_at = EXCLUDED.updated_at;"
)

func (svc *CRMRoleService) UpsertAll(ctx context.Context, crmRoles []*models.CRMRole) error {
	spanCtx, span := log.StartSpan(ctx, "CRMRole.UpsertAll")
	defer span.End()

	subs := []string{}
	vals := []interface{}{}

	paramIdx := 1
	for _, role := range crmRoles {
		subs = append(subs, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", paramIdx, paramIdx+1, paramIdx+2, paramIdx+3, paramIdx+4, paramIdx+5))
		paramIdx += 6
		vals = append(vals, role.ID, role.TenantID, role.Name, role.Description, role.ParentID, role.UpdatedAt)
	}

	query := strings.ReplaceAll(crmRoleUpsertAllQuery, "{SUBS}", strings.Join(subs, ",\n"))

	_, err := queries.Raw(query, vals...).ExecContext(spanCtx, svc.GetContextExecutor())
	if err != nil {
		argsRaw, _ := json.Marshal(vals)
		fmt.Println("QUERY", query)
		fmt.Println("ARGS", argsRaw)
		return err
	}

	return nil
}

func (svc *CRMRoleService) GetByID(ctx context.Context, id, tenantID string) (*models.CRMRole, error) {
	spanCtx, span := log.StartSpan(ctx, "CRMRole.GetByID")
	defer span.End()

	cr, err := models.CRMRoles(qm.Where("id=$1 AND tenant_id=$2", id, tenantID)).One(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return nil, err
	}
	return cr, nil
}

func (svc *CRMRoleService) GetByIDs(ctx context.Context, tenantID string, ids ...string) ([]*models.CRMRole, error) {
	spanCtx, span := log.StartSpan(ctx, "CRMRole.GetByIDs")
	defer span.End()

	idsParam := make([]interface{}, len(ids))
	for i, id := range ids {
		idsParam[i] = id
	}
	crs, err := models.CRMRoles(qm.WhereIn("id IN ?", idsParam...), qm.And(fmt.Sprintf("tenant_id::TEXT = $%d", len(ids)+1), tenantID)).All(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return nil, err
	}

	return crs, nil
}

const (
	getUnsyncedCRMRolesQuery = `SELECT cr.*
	FROM crm_role cr
	LEFT OUTER JOIN "group" g ON cr.id = ANY(g.crm_role_ids) AND cr.tenant_id = g.tenant_id
	WHERE g.id IS NULL AND cr.tenant_id = $1`
)

func (svc *CRMRoleService) GetUnsynced(ctx context.Context, tenantID string) ([]*models.CRMRole, error) {
	spanCtx, span := log.StartSpan(ctx, "CRMRole.GetUnsynced")
	defer span.End()

	results := []*models.CRMRole{}
	if err := queries.Raw(getUnsyncedCRMRolesQuery, tenantID).Bind(spanCtx, svc.GetContextExecutor(), &results); err != nil {
		log.WithTenantID(tenantID).WithCustom("query", getUnsyncedCRMRolesQuery).Error(err)
		return nil, err
	}
	return results, nil
}

func (svc *CRMRoleService) Search(ctx context.Context, tenantID, query string, limit, offset int) ([]*models.CRMRole, int64, error) {
	spanCtx, span := log.StartSpan(ctx, "CRMRole.Search")
	defer span.End()

	queryParts := []qm.QueryMod{}
	paramIdx := 1

	if tenantID != "" {
		queryParts = append(queryParts, qm.Where(fmt.Sprintf("tenant_id=$%d", paramIdx), tenantID))
		paramIdx++
	}

	if query != "" {
		queryParts = append(queryParts, qm.Where(fmt.Sprintf("LOWER(name) LIKE $%d", paramIdx), "%"+strings.ToLower(query)+"%"))
		paramIdx++ // NOTE: not actually necessary, but just in case we add any more params
	}

	total, err := models.CRMRoles(queryParts...).Count(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return nil, 0, err
	}

	queryParts = append(queryParts, qm.OrderBy("name"), qm.Offset(offset), qm.Limit(limit))

	crmRoles, err := models.CRMRoles(queryParts...).All(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return nil, total, err
	}

	return crmRoles, total, nil
}

func (svc *CRMRoleService) DeleteByID(ctx context.Context, id, tenantID string) error {
	spanCtx, span := log.StartSpan(ctx, "CRMRole.DeleteByID")
	defer span.End()

	cr := &models.CRMRole{ID: id, TenantID: tenantID}
	numAffected, err := cr.Delete(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error deleting crmRole: delete affected 0 rows")
	}
	return nil
}

func (svc *CRMRoleService) DeleteUnSynced(ctx context.Context, tenantID string, syncedIDs ...interface{}) error {
	spanCtx, span := log.StartSpan(ctx, "CRMRole.DeleteUnSynced")
	defer span.End()

	_, err := models.CRMRoles(qm.WhereNotIn("id NOT IN ?", syncedIDs...), qm.And(fmt.Sprintf("tenant_id::TEXT = $%d", len(syncedIDs)+1), tenantID)).DeleteAll(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return err
	}
	return nil
}
