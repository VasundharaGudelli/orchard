package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	null "github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SystemRoleService struct {
	*DBService
}

func (db *DB) NewSystemRoleService() *SystemRoleService {
	return &SystemRoleService{
		DBService: db.NewDBService(),
	}
}

func (svc *SystemRoleService) FromProto(sr *orchardPb.SystemRole) *models.SystemRole {
	tenantID := DefaultTenantID
	if sr.TenantId != "" {
		tenantID = sr.TenantId
	}

	createdAt := sr.CreatedAt.AsTime()
	updatedAt := sr.UpdatedAt.AsTime()

	return &models.SystemRole{
		ID:          sr.Id,
		TenantID:    tenantID,
		Name:        sr.Name,
		Description: null.NewString(sr.Description, sr.Description != ""),
		Type:        strings.ToLower(sr.Type.String()),
		Permissions: sr.Permissions,
		Status:      strings.ToLower(sr.Status.String()),
		Priority:    int(sr.Priority),
		BaseRoleID:  null.NewString(sr.BaseRoleId, sr.BaseRoleId != ""),
		CreatedBy:   sr.CreatedBy,
		CreatedAt:   createdAt,
		UpdatedBy:   sr.UpdatedBy,
		UpdatedAt:   updatedAt,
	}
}

func (svc *SystemRoleService) ToProto(sr *models.SystemRole) (*orchardPb.SystemRole, error) {
	createdAt := timestamppb.New(sr.CreatedAt)

	updatedAt := timestamppb.New(sr.UpdatedAt)

	typ := orchardPb.SystemRoleType_Unknown
	switch sr.Type {
	case "ic":
		typ = orchardPb.SystemRoleType_IC
	case "manager":
		typ = orchardPb.SystemRoleType_Manager
	case "internal":
		typ = orchardPb.SystemRoleType_Internal
	}

	status := orchardPb.BasicStatus_Inactive
	switch sr.Status {
	case "active":
		status = orchardPb.BasicStatus_Active
	}

	var isCustom bool
	if !strings.EqualFold(DefaultTenantID, sr.TenantID) || (sr.BaseRoleID.Valid && len(sr.BaseRoleID.String) > 0) {
		isCustom = true
	}

	return &orchardPb.SystemRole{
		Id:          sr.ID,
		TenantId:    sr.TenantID,
		Name:        sr.Name,
		Description: sr.Description.String,
		Type:        typ,
		Permissions: sr.Permissions,
		Priority:    int32(sr.Priority),
		Status:      status,
		IsCustom:    isCustom,
		BaseRoleId:  sr.BaseRoleID.String,
		CreatedAt:   createdAt,
		CreatedBy:   sr.CreatedBy,
		UpdatedAt:   updatedAt,
		UpdatedBy:   sr.UpdatedBy,
	}, nil
}

var (
	systemRoleInsertWhitelist = []string{
		"id", "tenant_id", "name", "description",
		"type", "permissions", "priority", "status", "base_role_id",
		"created_at", "created_by", "updated_at", "updated_by",
	}
)

func (svc *SystemRoleService) Insert(ctx context.Context, sr *models.SystemRole) error {
	spanCtx, span := log.StartSpan(ctx, "SystemRole.Insert")
	defer span.End()
	now := time.Now().UTC()
	sr.CreatedAt = now
	sr.UpdatedAt = now
	return sr.Insert(spanCtx, svc.GetContextExecutor(), boil.Whitelist(systemRoleInsertWhitelist...))
}

func (svc *SystemRoleService) GetByID(ctx context.Context, id string) (*models.SystemRole, error) {
	spanCtx, span := log.StartSpan(ctx, "SystemRole.GetByID")
	defer span.End()
	sr, err := models.FindSystemRole(spanCtx, svc.GetContextExecutor(), id)
	if err != nil {
		return nil, err
	}
	return sr, nil
}

func (svc *SystemRoleService) GetByIDs(ctx context.Context, ids ...string) ([]*models.SystemRole, error) {
	spanCtx, span := log.StartSpan(ctx, "SystemRole.GetByIDs")
	defer span.End()

	idsParam := make([]interface{}, len(ids))
	for i, id := range ids {
		idsParam[i] = id
	}
	srs, err := models.SystemRoles(qm.WhereIn("id IN ?", idsParam...)).All(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return nil, err
	}
	return srs, nil
}

const (
	systemRoleWithBaseQuery = `
	WITH RECURSIVE system_role_and_base AS (
	  SELECT *
	    FROM system_role
	   WHERE id = $1
	   UNION ALL
	  SELECT srA.*
	    FROM system_role srA, system_role_and_base srB
	   WHERE srB.base_role_id IS NOT NULL
	     AND srA.id = srB.base_role_id
	)
	SELECT * FROM system_role_and_base;
`
)

func (svc *SystemRoleService) GetByIDWithBaseRole(ctx context.Context, id string) ([]*models.SystemRole, error) {
	spanCtx, span := log.StartSpan(ctx, "SystemRole.GetByIDWithBaseRole")
	defer span.End()
	systemRoles := []*models.SystemRole{}
	if err := queries.Raw(systemRoleWithBaseQuery, id).Bind(spanCtx, svc.GetContextExecutor(), &systemRoles); err != nil {
		return nil, errors.Wrap(err, "error querying sql for system role with base role")
	}
	return systemRoles, nil
}

func (svc *SystemRoleService) Search(ctx context.Context, tenantID, query string) ([]*models.SystemRole, error) {
	spanCtx, span := log.StartSpan(ctx, "SystemRole.Search")
	defer span.End()

	queryParts := []qm.QueryMod{qm.Where("type <> 'internal'")}

	paramIdx := 1
	if tenantID == "" {
		queryParts = append(queryParts, qm.Where("status = 'active' AND tenant_id IN ($1)", DefaultTenantID))
		paramIdx++
	} else {
		queryParts = append(queryParts, qm.Where("status = 'active' AND tenant_id IN ($1, $2)", DefaultTenantID, tenantID))
		paramIdx += 2
	}

	if query != "" {
		queryParts = append(queryParts, qm.Where(fmt.Sprintf("LOWER(name) LIKE $%d", paramIdx), "%"+strings.ToLower(query)+"%"))
		paramIdx++ // NOTE: not actually necessary, but just in case we add any more params
	}

	systemRoles, err := models.SystemRoles(queryParts...).All(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return nil, err
	}

	return systemRoles, nil
}

func (svc *SystemRoleService) GetInternalRoleIDs(ctx context.Context) (map[string]struct{}, error) {
	spanCtx, span := log.StartSpan(ctx, "SystemRole.GetInternalRoleIDs")
	defer span.End()

	queryParts := []qm.QueryMod{qm.Where("type = 'internal'")}

	systemRoles, err := models.SystemRoles(queryParts...).All(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return nil, err
	}

	ids := map[string]struct{}{}

	for _, sr := range systemRoles {
		ids[sr.ID] = struct{}{}
	}

	return ids, nil
}

var (
	defaultSystemRoleUpdateWhitelist = []string{
		"name", "description", "type", "permissions", "priority",
		"status", "updated_at", "updated_by", "base_role_id",
	}
)

func (svc *SystemRoleService) Update(ctx context.Context, sr *models.SystemRole, onlyFields []string) error {
	spanCtx, span := log.StartSpan(ctx, "SystemRole.Update")
	defer span.End()

	whitelist := defaultSystemRoleUpdateWhitelist
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

	sr.UpdatedAt = time.Now().UTC()

	numAffected, err := sr.Update(spanCtx, svc.GetContextExecutor(), boil.Whitelist(whitelist...))
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error updating systemRole: update affected 0 rows")
	}

	return nil
}

func (svc *SystemRoleService) DeleteByID(ctx context.Context, id string) error {
	spanCtx, span := log.StartSpan(ctx, "SystemRole.DeleteByID")
	defer span.End()
	sr := &models.SystemRole{ID: id}
	numAffected, err := sr.Delete(spanCtx, svc.GetContextExecutor())
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error deleting systemRole: delete affected 0 rows")
	}
	return nil
}

func (svc *SystemRoleService) SoftDeleteByID(ctx context.Context, id, tenantID, userID string) error {
	spanCtx, span := log.StartSpan(ctx, "SystemRole.SoftDeleteByID")
	defer span.End()
	sr := &models.SystemRole{ID: id, TenantID: tenantID, UpdatedBy: userID, UpdatedAt: time.Now().UTC(), Status: "inactive"}
	numAffected, err := sr.Update(spanCtx, svc.GetContextExecutor(), boil.Whitelist("updated_by", "updated_at", "status"))
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error soft deleting systemRole: delete affected 0 rows")
	}
	return nil
}
