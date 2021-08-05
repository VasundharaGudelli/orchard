package db

import (
	"context"
	"database/sql"
	"encoding/json"
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
)

// TODO: Add tracing

type CRMRoleService struct {
	tx *sql.Tx
}

func NewCRMRoleService() *CRMRoleService {
	return &CRMRoleService{}
}

func (svc *CRMRoleService) WithTransaction(ctx context.Context) error {
	tx, err := Global.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	svc.tx = tx
	return nil
}

func (svc *CRMRoleService) Rollback() error {
	if svc.tx == nil {
		return nil
	}
	return svc.tx.Rollback()
}

func (svc *CRMRoleService) Commit() error {
	if svc.tx == nil {
		return nil
	}
	return svc.tx.Commit()
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
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	return cr.Insert(ctx, x, boil.Whitelist(crmRoleInsertWhitelist...))
}

const (
	crmRoleUpsertAllQuery = "INSERT INTO crm_role (id, tenant_id, name, description, parent_id, updated_at) VALUES\n\t{SUBS}\nON CONFLICT (tenant_id, id) DO\n\tUPDATE SET name = EXCLUDED.name, description = EXCLUDED.description, parent_id = EXCLUDED.parent_id, updated_at = EXCLUDED.updated_at;"
)

func (svc *CRMRoleService) UpsertAll(ctx context.Context, crmRoles []*models.CRMRole) error {
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}

	subs := []string{}
	vals := []interface{}{}

	paramIdx := 1
	for _, role := range crmRoles {
		subs = append(subs, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", paramIdx, paramIdx+1, paramIdx+2, paramIdx+3, paramIdx+4, paramIdx+5))
		paramIdx += 6
		vals = append(vals, role.ID, role.TenantID, role.Name, role.Description, role.ParentID, role.UpdatedAt)
	}

	query := strings.ReplaceAll(crmRoleUpsertAllQuery, "{SUBS}", strings.Join(subs, ",\n"))

	_, err := queries.Raw(query, vals...).ExecContext(ctx, x)
	if err != nil {
		argsRaw, _ := json.Marshal(vals)
		fmt.Println("QUERY", query)
		fmt.Println("ARGS", argsRaw)
		return err
	}

	return nil
}

func (svc *CRMRoleService) GetByID(ctx context.Context, id, tenantID string) (*models.CRMRole, error) {
	cr, err := models.CRMRoles(qm.Where("id=$1 AND tenant_id=$2", id, tenantID)).One(ctx, Global)
	if err != nil {
		return nil, err
	}

	return cr, nil
}

const (
	getUnsyncedCRMRolesQuery = `SELECT cr.*
	FROM crm_role cr
	LEFT OUTER JOIN "group" g ON cr.id = ANY(g.crm_role_ids) AND cr.tenant_id = g.tenant_id
	WHERE g.id IS NULL AND cr.tenant_id = $1`
)

func (svc *CRMRoleService) GetUnsynced(ctx context.Context, tenantID string) ([]*models.CRMRole, error) {
	results := []*models.CRMRole{}
	if err := queries.Raw(getUnsyncedCRMRolesQuery, tenantID).Bind(ctx, Global, &results); err != nil {
		log.WithTenantID(tenantID).WithCustom("query", getUnsyncedCRMRolesQuery).Error(err)
		return nil, err
	}

	return results, nil
}

func (svc *CRMRoleService) Search(ctx context.Context, tenantID, query string) ([]*models.CRMRole, error) {
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

	crmRoles, err := models.CRMRoles(queryParts...).All(ctx, Global)
	if err != nil {
		return nil, err
	}

	return crmRoles, nil
}

func (svc *CRMRoleService) DeleteByID(ctx context.Context, id, tenantID string) error {
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	cr := &models.CRMRole{ID: id, TenantID: tenantID}
	numAffected, err := cr.Delete(ctx, x)
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error deleting crmRole: delete affected 0 rows")
	}
	return nil
}

func (svc *CRMRoleService) DeleteUnSynced(ctx context.Context, tenantID string, syncedIDs ...interface{}) error {
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	_, err := models.CRMRoles(qm.Where("tenant_id::TEXT = $1"), qm.AndNotIn("id NOT IN ?", syncedIDs...)).DeleteAll(ctx, x)
	if err != nil {
		return err
	}
	return nil
}
