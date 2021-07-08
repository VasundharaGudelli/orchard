package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang/protobuf/ptypes"
	"github.com/loupe-co/orchard/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	null "github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
	"github.com/volatiletech/sqlboiler/v4/types"
)

// TODO: Add tracing

type PersonService struct {
	tx *sql.Tx
}

func NewPersonService() *PersonService {
	return &PersonService{}
}

func (svc *PersonService) WithTransaction(ctx context.Context) error {
	tx, err := Global.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	svc.tx = tx
	return nil
}

func (svc *PersonService) Rollback() error {
	if svc.tx == nil {
		return nil
	}
	return svc.tx.Rollback()
}

func (svc *PersonService) Commit() error {
	if svc.tx == nil {
		return nil
	}
	return svc.tx.Commit()
}

func (svc *PersonService) FromProto(p *orchardPb.Person) *models.Person {
	createdAt := p.CreatedAt.AsTime()
	updatedAt := p.UpdatedAt.AsTime()

	return &models.Person{
		ID:            p.Id,
		TenantID:      p.TenantId,
		Name:          null.NewString(p.Name, p.Name != ""),
		FirstName:     null.NewString(p.FirstName, p.FirstName != ""),
		LastName:      null.NewString(p.LastName, p.LastName != ""),
		Email:         null.NewString(p.Email, p.Email != ""),
		ManagerID:     null.NewString(p.ManagerId, p.ManagerId != ""),
		GroupID:       null.NewString(p.GroupId, p.GroupId != ""),
		RoleIds:       types.StringArray(p.RoleIds),
		CRMRoleIds:    types.StringArray(p.CrmRoleIds),
		IsProvisioned: p.IsProvisioned,
		IsSynced:      p.IsSynced,
		Status:        strings.ToLower(p.Status.String()),
		CreatedAt:     createdAt,
		CreatedBy:     p.CreatedBy,
		UpdatedAt:     updatedAt,
		UpdatedBy:     p.UpdatedBy,
	}
}

func (svc *PersonService) ToProto(p *models.Person) (*orchardPb.Person, error) {
	createdAt, err := ptypes.TimestampProto(p.CreatedAt)
	if err != nil {
		return nil, err
	}

	updatedAt, err := ptypes.TimestampProto(p.UpdatedAt)
	if err != nil {
		return nil, err
	}

	status := orchardPb.BasicStatus_Inactive
	switch p.Status {
	case strings.ToLower(orchardPb.BasicStatus_Active.String()):
		status = orchardPb.BasicStatus_Active
	}

	return &orchardPb.Person{
		Id:            p.ID,
		TenantId:      p.TenantID,
		Name:          p.Name.String,
		FirstName:     p.FirstName.String,
		LastName:      p.LastName.String,
		Email:         p.Email.String,
		ManagerId:     p.ManagerID.String,
		GroupId:       p.GroupID.String,
		RoleIds:       []string(p.RoleIds),
		CrmRoleIds:    []string(p.CRMRoleIds),
		IsProvisioned: p.IsProvisioned,
		IsSynced:      p.IsSynced,
		Status:        status,
		CreatedAt:     createdAt,
		CreatedBy:     p.CreatedBy,
		UpdatedAt:     updatedAt,
		UpdatedBy:     p.UpdatedBy,
	}, nil
}

var (
	personInsertWhitelist = []string{
		"id", "tenant_id", "name", "first_name", "last_name", "email",
		"manager_id", "group_id", "role_ids", "crm_role_ids",
		"is_provisioned", "is_synced", "status",
		"created_at", "created_by", "updated_at", "updated_by",
	}
)

func (svc *PersonService) Insert(ctx context.Context, p *models.Person) error {
	return p.Insert(ctx, Global, boil.Whitelist(personInsertWhitelist...))
}

const (
	personUpsertAllQuery = `INSERT INTO person (id, tenant_id, "name", first_name, last_name, email, manager_id, group_id, role_ids, crm_role_ids, is_provisioned, is_synced, status, created_at, created_by, updated_at, updated_by) VALUES
	{SUBS}
ON CONFLICT (tenant_id, id) DO
	UPDATE SET name = EXCLUDED.name, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, email = EXCLUDED.email, manager_id = EXCLUDED.manager_id, group_id = EXCLUDED.group_id, role_ids = EXCLUDED.role_ids, crm_role_ids = EXCLUDED.crm_role_ids, is_provisioned = EXCLUDED.is_provisioned, is_synced = EXCLUDED.is_synced, status = EXCLUDED.status, updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by;`
)

func (svc *PersonService) UpsertAll(ctx context.Context, people []*models.Person) error {
	subs := []string{}
	vals := []interface{}{}

	paramIdx := 1
	for _, p := range people {
		subs = append(
			subs,
			fmt.Sprintf(
				"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
				paramIdx, paramIdx+1, paramIdx+2, paramIdx+3, paramIdx+4, paramIdx+5,
				paramIdx+6, paramIdx+7, paramIdx+8, paramIdx+9, paramIdx+10, paramIdx+11,
				paramIdx+12, paramIdx+13, paramIdx+14, paramIdx+15, paramIdx+16,
			),
		)
		paramIdx += 17
		vals = append(vals, p.ID, p.TenantID, p.Name, p.FirstName, p.LastName, p.Email, p.ManagerID, p.GroupID, p.RoleIds, p.CRMRoleIds, p.IsProvisioned, p.IsSynced, p.Status, p.CreatedAt, p.CreatedBy, p.UpdatedAt, p.UpdatedBy)
	}

	query := strings.ReplaceAll(personUpsertAllQuery, "{SUBS}", strings.Join(subs, ",\n"))

	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	_, err := queries.Raw(query, vals...).ExecContext(ctx, x)
	if err != nil {
		argsRaw, _ := json.Marshal(vals)
		fmt.Println("QUERY", query)
		fmt.Println("ARGS", string(argsRaw))
		return err
	}

	return nil
}

func (svc *PersonService) GetByID(ctx context.Context, id, tenantID string) (*models.Person, error) {
	person, err := models.People(qm.Where("id=$1 AND tenant_id=$2", id, tenantID)).One(ctx, Global)
	if err != nil {
		return nil, err
	}

	return person, nil
}

func (svc *PersonService) GetByIDs(ctx context.Context, tenantID string, ids ...string) ([]*models.Person, error) {
	people, err := models.People(qm.Where("tenant_id = $1"), qm.AndIn("id IN ?", ids)).All(ctx, Global)
	if err != nil {
		return nil, err
	}
	return people, nil
}

func (svc *PersonService) Search(ctx context.Context, tenantID, query string, limit, offset int) ([]*models.Person, int64, error) {
	queryParts := []qm.QueryMod{}

	queryParts = append(queryParts, qm.Where("tenant_id=$1", tenantID))

	if query != "" {
		searchClause := "LOWER(name) LIKE $2 OR LOWER(email) LIKE $2"
		queryParts = append(queryParts, qm.Where(searchClause, "%"+strings.ToLower(query)+"%"))
	}

	total, err := models.People(queryParts...).Count(ctx, Global)
	if err != nil {
		return nil, 0, err
	}

	queryParts = append(queryParts, qm.OrderBy("name"), qm.Limit(limit), qm.Offset(offset))

	people, err := models.People(queryParts...).All(ctx, Global)
	if err != nil {
		return nil, total, err
	}

	return people, total, nil
}

func (svc *PersonService) GetPeopleByGroupId(ctx context.Context, tenantID, groupID string) ([]*models.Person, error) {
	people, err := models.People(qm.Where("tenant_id = $1 AND group_id = $2", tenantID, groupID)).All(ctx, Global)
	if err != nil {
		return nil, err
	}
	return people, nil
}

var (
	defaultPersonUpdateWhitelist = []string{
		"name", "first_name", "last_name", "email",
		"manager_id", "group_id", "role_ids", "crm_role_ids",
		"is_provisioned", "is_synced", "status", "updated_at", "updated_by",
	}
)

func (svc *PersonService) Update(ctx context.Context, p *models.Person, onlyFields []string) error {
	whitelist := defaultPersonUpdateWhitelist
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
	numAffected, err := p.Update(ctx, x, boil.Whitelist(whitelist...))
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error updating person: update affected 0 rows")
	}

	return nil
}

const (
	updatedPersonGroupsQuery = `WITH UpdatedPersonGroups AS (
		SELECT
			p.id, p.tenant_id, g.id as group_id
		FROM person p
		INNER JOIN "group" g ON p.crm_role_ids && g.crm_role_ids AND p.tenant_id = g.tenant_id
		WHERE p.tenant_id = $1 AND p.is_synced
	)
	UPDATE person
	SET group_id = g.group_id
	FROM UpdatedPersonGroups g
	WHERE person.id = g.id AND person.tenant_id = g.tenant_id`
)

func (svc *PersonService) UpdatePersonGroups(ctx context.Context, tenantID string) error {
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	_, err := queries.Raw(updatedPersonGroupsQuery, tenantID).ExecContext(ctx, x)
	return err
}

func (svc *PersonService) DeleteByID(ctx context.Context, id, tenantID string) error {
	person := &models.Person{ID: id, TenantID: tenantID}
	numAffected, err := person.Delete(ctx, Global)
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error deleting person: delete affected 0 rows")
	}
	return nil
}
