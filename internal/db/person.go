package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	null "github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
	"github.com/volatiletech/sqlboiler/v4/types"
)

type PersonService struct {
	*DBService
}

func (db *DB) NewPersonService() *PersonService {
	return &PersonService{
		DBService: db.NewDBService(),
	}
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
		PhotoURL:      null.NewString(p.PhotoUrl, p.PhotoUrl != ""),
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
		Type:          p.Type,
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
		PhotoUrl:      p.PhotoURL.String,
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
		Type:          p.Type,
	}, nil
}

var (
	personInsertWhitelist = []string{
		"id", "tenant_id", "name", "first_name", "last_name", "email",
		"photo_url", "manager_id", "group_id", "role_ids", "crm_role_ids",
		"is_provisioned", "is_synced", "status",
		"created_at", "created_by", "updated_at", "updated_by",
	}
)

func (svc *PersonService) Insert(ctx context.Context, p *models.Person) error {
	spanCtx, span := log.StartSpan(ctx, "Person.Insert")
	defer span.End()
	return p.Insert(spanCtx, svc.GetContextExecutor(), boil.Whitelist(personInsertWhitelist...))
}

const (
	personUpsertAllQuery = `INSERT INTO person (id, tenant_id, "name", first_name, last_name, email, photo_url, manager_id, group_id, role_ids, crm_role_ids, is_provisioned, is_synced, status, created_at, created_by, updated_at, updated_by) VALUES
	{SUBS}
ON CONFLICT (tenant_id, id) DO
	UPDATE SET name = EXCLUDED.name, first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, email = EXCLUDED.email, photo_url = EXCLUDED.photo_url, manager_id = EXCLUDED.manager_id, group_id = EXCLUDED.group_id, role_ids = EXCLUDED.role_ids, crm_role_ids = EXCLUDED.crm_role_ids, is_provisioned = EXCLUDED.is_provisioned, is_synced = EXCLUDED.is_synced, status = EXCLUDED.status, updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by;`
)

func (svc *PersonService) UpsertAll(ctx context.Context, people []*models.Person) error {
	spanCtx, span := log.StartSpan(ctx, "Person.UpsertAll")
	defer span.End()

	subs := []string{}
	vals := []interface{}{}

	paramIdx := 1
	for _, p := range people {
		if p == nil {
			continue
		}
		subs = append(
			subs,
			fmt.Sprintf(
				"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
				paramIdx, paramIdx+1, paramIdx+2, paramIdx+3, paramIdx+4, paramIdx+5,
				paramIdx+6, paramIdx+7, paramIdx+8, paramIdx+9, paramIdx+10, paramIdx+11,
				paramIdx+12, paramIdx+13, paramIdx+14, paramIdx+15, paramIdx+16, paramIdx+17,
			),
		)
		paramIdx += 18
		vals = append(vals, p.ID, p.TenantID, p.Name, p.FirstName, p.LastName, p.Email, p.PhotoURL, p.ManagerID, p.GroupID, p.RoleIds, p.CRMRoleIds, p.IsProvisioned, p.IsSynced, p.Status, p.CreatedAt, p.CreatedBy, p.UpdatedAt, p.UpdatedBy)
	}

	query := strings.ReplaceAll(personUpsertAllQuery, "{SUBS}", strings.Join(subs, ",\n"))

	_, err := queries.Raw(query, vals...).ExecContext(spanCtx, svc.GetContextExecutor())
	if err != nil {
		log.WithCustom("query", query).Debug("error running upsert person query")
		return err
	}

	return nil
}

func (svc *PersonService) GetByID(ctx context.Context, id, tenantID string) (*models.Person, error) {
	spanCtx, span := log.StartSpan(ctx, "Person.GetByID")
	defer span.End()
	person, err := models.People(qm.Where("id=$1 AND tenant_id=$2", id, tenantID)).One(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return nil, err
	}

	return person, nil
}

func (svc *PersonService) GetByIDs(ctx context.Context, tenantID string, ids ...interface{}) ([]*models.Person, error) {
	spanCtx, span := log.StartSpan(ctx, "Person.GetByIDs")
	defer span.End()
	people, err := models.People(qm.WhereIn("id IN ?", ids...), qm.And(fmt.Sprintf("tenant_id::TEXT = $%d", len(ids)+1), tenantID)).All(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return nil, err
	}
	return people, nil
}

func (svc *PersonService) GetAllActiveNonVirtualByEmails(ctx context.Context, tenantID string, emails ...interface{}) ([]*models.Person, error) {
	spanCtx, span := log.StartSpan(ctx, "Person.GetAllActiveNonVirtualByEmails")
	defer span.End()
	people, err := models.People(
		qm.WhereIn("email IN ?", emails...),
		qm.And(fmt.Sprintf("tenant_id::TEXT = $%d", len(emails)+1), tenantID),
		qm.And(fmt.Sprintf("created_by = $%d", len(emails)+2), DefaultTenantID),
		qm.And("status = 'active'"),
	).All(spanCtx, svc.GetContextExecutor())

	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	return people, nil
}

func (svc *PersonService) GetByEmail(ctx context.Context, tenantID, email string) (*models.Person, error) {
	spanCtx, span := log.StartSpan(ctx, "Person.GetByEmail")
	defer span.End()
	person, err := models.People(qm.Where("tenant_id = $1 AND email = $2", tenantID, email)).One(spanCtx, svc.GetContextExecutor())
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if person == nil || person.ID == "" || err == sql.ErrNoRows {
		return nil, nil
	}
	return person, nil
}

func (svc *PersonService) GetAllByEmailForProvisioning(ctx context.Context, email string) ([]*models.Person, error) {
	spanCtx, span := log.StartSpan(ctx, "Person.GetAllByEmailForProvisioning")
	defer span.End()

	// clean email so that we capture all person records
	email = svc.CleanEmail(email)
	emailQuery := fmt.Sprintf("%%%s%%", email)

	people, err := models.People(qm.Where("email LIKE $1 AND status = 'active' AND is_provisioned", emailQuery), qm.OrderBy("created_at ASC")).All(spanCtx, svc.GetContextExecutor())
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if people == nil || (err != nil && err == sql.ErrNoRows) {
		return nil, nil
	}

	// clean the email so that we always send the right one to auth0
	for _, person := range people {
		if person.Email.Valid && !person.Email.IsZero() {
			person.Email.String = svc.CleanEmail(person.Email.String)
		}
	}

	return people, nil
}

type PersonFilter struct {
	Field  string
	Op     string
	Values []interface{}
}

func (svc *PersonService) Search(ctx context.Context, tenantID, query string, limit, offset int, filters ...PersonFilter) ([]*models.Person, int64, error) {
	spanCtx, span := log.StartSpan(ctx, "Person.Search")
	defer span.End()
	queryParts := []qm.QueryMod{}
	queryParts = append(queryParts, qm.Where("tenant_id=$1", tenantID))
	paramIdx := 2
	if len(strings.TrimSpace(query)) >= 3 {
		paramIdx = 3
		searchClause := "LOWER(name) LIKE $2 OR LOWER(email) LIKE $2"
		queryParts = append(queryParts, qm.And(searchClause, "%"+strings.ToLower(query)+"%"))
	}

	for _, filter := range filters {
		switch filter.Op {
		case "EQ":
			queryParts = append(queryParts, qm.And(fmt.Sprintf("%s = $%d", filter.Field, paramIdx), filter.Values...))
		case "NEQ":
			queryParts = append(queryParts, qm.And(fmt.Sprintf("%s <> $%d", filter.Field, paramIdx), filter.Values...))
		case "IN":
			queryParts = append(queryParts, qm.AndIn(fmt.Sprintf("%s IN ?", filter.Field), filter.Values...))
			paramIdx += len(filters) - 1
		case "NIN":
			queryParts = append(queryParts, qm.AndNotIn(fmt.Sprintf("%s NOT IN ?", filter.Field), filter.Values...))
			paramIdx += len(filters) - 1
		case "GT":
			queryParts = append(queryParts, qm.And(fmt.Sprintf("%s > $%d", filter.Field, paramIdx), filter.Values...))
		case "GTE":
			queryParts = append(queryParts, qm.And(fmt.Sprintf("%s >= $%d", filter.Field, paramIdx), filter.Values...))
		case "LT":
			queryParts = append(queryParts, qm.And(fmt.Sprintf("%s < $%d", filter.Field, paramIdx), filter.Values...))
		case "LTE":
			queryParts = append(queryParts, qm.And(fmt.Sprintf("%s <= $%d", filter.Field, paramIdx), filter.Values...))
		case "EQANY":
			queryParts = append(queryParts, qm.And(fmt.Sprintf("$%d = ANY (%s)", paramIdx, filter.Field), filter.Values...))
		default:
			queryParts = append(queryParts, qm.And(fmt.Sprintf("%s = $%d", filter.Field, paramIdx), filter.Values...))
		}
		paramIdx++
	}

	total, err := models.People(queryParts...).Count(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return nil, 0, err
	}

	queryParts = append(queryParts, qm.OrderBy("name"), qm.Limit(limit), qm.Offset(offset))

	people, err := models.People(queryParts...).All(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return nil, total, err
	}

	return people, total, nil
}

func (svc *PersonService) GetPeopleByGroupId(ctx context.Context, tenantID, groupID string) ([]*models.Person, error) {
	spanCtx, span := log.StartSpan(ctx, "Person.GetPeopleByGroupId")
	defer span.End()
	people, err := models.People(qm.Where("tenant_id = $1 AND group_id = $2", tenantID, groupID)).All(spanCtx, svc.GetContextExecutor())
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return people, nil
}

func (svc *PersonService) GetPeopleByRoleId(ctx context.Context, tenantID, roleID string, limit, offset int) ([]*models.Person, error) {
	spanCtx, span := log.StartSpan(ctx, "Person.GetPeopleByRoleId")
	defer span.End()
	people, err := models.People(
		qm.Where("tenant_id = $1 AND $2 = ANY (role_ids)", tenantID, roleID),
		qm.Limit(limit),
		qm.Offset(offset),
		qm.OrderBy("last_name, first_name DESC"),
	).All(spanCtx, svc.GetContextExecutor())
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return people, nil
}

func (svc *PersonService) CountPeopleByRoleId(ctx context.Context, tenantID, roleID string) (int64, error) {
	spanCtx, span := log.StartSpan(ctx, "Person.GetPeopleByRoleId")
	defer span.End()
	numPeople, err := models.People(
		qm.Where("tenant_id = $1 AND $2 = ANY (role_ids)", tenantID, roleID),
	).Count(spanCtx, svc.GetContextExecutor())
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	return numPeople, nil
}

func (svc *PersonService) GetVirtualUsers(ctx context.Context, tenantID string) ([]*models.Person, error) {
	spanCtx, span := log.StartSpan(ctx, "Person.GetVirtualUsers")
	defer span.End()
	people, err := models.People(qm.Where("tenant_id = $1 AND created_by <> $2", tenantID, DefaultTenantID)).All(spanCtx, svc.GetContextExecutor())
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return people, nil
}

var (
	defaultPersonUpdateWhitelist = []string{
		"name", "first_name", "last_name", "email", "photo_url",
		"manager_id", "group_id", "role_ids", "crm_role_ids",
		"is_provisioned", "is_synced", "status", "updated_at", "updated_by",
	}
)

func (svc *PersonService) Update(ctx context.Context, p *models.Person, onlyFields []string) error {
	spanCtx, span := log.StartSpan(ctx, "Person.Update")
	defer span.End()
	whitelist := defaultPersonUpdateWhitelist
	if len(onlyFields) > 0 {
		whitelist = onlyFields
	}
	var hasUpdatedAt, hasUpdatedBy bool
	for _, f := range whitelist {
		if f == "updated_at" {
			hasUpdatedAt = true
		}
		if f == "updated_by" {
			hasUpdatedBy = true
		}
	}
	if !hasUpdatedAt {
		whitelist = append(whitelist, "updated_at")
	}
	if !hasUpdatedBy {
		whitelist = append(whitelist, "updated_by")
	}

	numAffected, err := p.Update(spanCtx, svc.GetContextExecutor(), boil.Whitelist(whitelist...))
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error updating person: update affected 0 rows")
	}

	return nil
}

const (
	updatedPersonGroupsQuery = `WITH UpdatedSyncedPersonGroups AS (
		SELECT
			p.id, p.tenant_id, g.id as group_id
		FROM person p
		INNER JOIN "group" g ON p.crm_role_ids && g.crm_role_ids AND p.tenant_id = g.tenant_id
		WHERE p.tenant_id = $1 AND p.is_synced
	)
	, UpdatedUnSyncedPersonGroups AS (
		SELECT
			p.id, p.tenant_id, g.id as group_id
		FROM person p
		LEFT OUTER JOIN "group" g ON g.id = p.group_id AND g.tenant_id = p.tenant_id
		WHERE p.tenant_id = $1 AND (NOT p.is_synced OR p.created_by <> '00000000-0000-0000-0000-000000000000')
	)
	UPDATE person
	SET group_id = g.group_id
	FROM (
		SELECT * FROM UpdatedSyncedPersonGroups
		UNION ALL
		SELECT * FROM UpdatedUnSyncedPersonGroups
	) g
	WHERE person.id = g.id AND person.tenant_id = g.tenant_id`
)

func (svc *PersonService) UpdatePersonGroups(ctx context.Context, tenantID string) error {
	spanCtx, span := log.StartSpan(ctx, "Person.UpdatePersonGroups")
	defer span.End()
	_, err := queries.Raw(updatedPersonGroupsQuery, tenantID).ExecContext(spanCtx, svc.GetContextExecutor())
	return err
}

func (svc *PersonService) DeleteByID(ctx context.Context, id, tenantID string) error {
	spanCtx, span := log.StartSpan(ctx, "Person.DeleteById")
	defer span.End()
	person := &models.Person{ID: id, TenantID: tenantID}
	numAffected, err := person.Delete(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error deleting person: delete affected 0 rows")
	}
	return nil
}

func (svc *PersonService) SoftDeleteByID(ctx context.Context, id, tenantID, userID string) error {
	spanCtx, span := log.StartSpan(ctx, "Person.SoftDeleteByID")
	defer span.End()
	person := &models.Person{ID: id, TenantID: tenantID, UpdatedBy: userID, UpdatedAt: time.Now().UTC(), Status: "inactive"}
	numAffected, err := person.Update(spanCtx, svc.GetContextExecutor(), boil.Whitelist("updated_by", "updated_at", "status"))
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error soft deleting person: delete affected 0 rows")
	}
	return nil
}

const (
	getPersonGroupIDsQuery = `SELECT id, group_id FROM person WHERE tenant_id = $1 AND is_provisioned AND status = 'active'`
)

type getPersonGroupIDsResponse struct {
	ID      string         `boil:"id" json:"id"`
	GroupID sql.NullString `boil:"group_id" json:"group_id"`
}

func (svc *PersonService) GetPersonGroupIDs(ctx context.Context, tenantID string) (map[string]string, error) {
	spanCtx, span := log.StartSpan(ctx, "Person.GetPersonGroupIDs")
	defer span.End()
	personGroupIDs := []*getPersonGroupIDsResponse{}
	if err := queries.Raw(getPersonGroupIDsQuery, tenantID).Bind(spanCtx, svc.GetContextExecutor(), &personGroupIDs); err != nil {
		return nil, errors.Wrap(err, "error querying sql for person group ids")
	}
	res := make(map[string]string, len(personGroupIDs))
	for _, person := range personGroupIDs {
		if person.GroupID.Valid {
			res[person.ID] = person.GroupID.String
		}
	}
	return res, nil
}

func (svc *PersonService) CleanEmail(email string) string {
	// remove sf sandbox suffix
	email = strings.ReplaceAll(email, ".invalid", "")

	return email
}
