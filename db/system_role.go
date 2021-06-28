package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/golang/protobuf/ptypes"
	"github.com/loupe-co/orchard/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	null "github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
)

// TODO: Add tracing

type SystemRoleService struct{}

func NewSystemRoleService() *SystemRoleService {
	return &SystemRoleService{}
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
		CreatedBy:   sr.CreatedBy,
		CreatedAt:   createdAt,
		UpdatedBy:   sr.UpdatedBy,
		UpdatedAt:   updatedAt,
	}
}

func (svc *SystemRoleService) ToProto(sr *models.SystemRole) (*orchardPb.SystemRole, error) {
	createdAt, err := ptypes.TimestampProto(sr.CreatedAt)
	if err != nil {
		return nil, err
	}

	updatedAt, err := ptypes.TimestampProto(sr.UpdatedAt)
	if err != nil {
		return nil, err
	}

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

	return &orchardPb.SystemRole{
		Id:          sr.ID,
		TenantId:    sr.TenantID,
		Name:        sr.Name,
		Description: sr.Description.String,
		Type:        typ,
		Permissions: sr.Permissions,
		Priority:    int32(sr.Priority),
		Status:      status,
		CreatedAt:   createdAt,
		CreatedBy:   sr.CreatedBy,
		UpdatedAt:   updatedAt,
		UpdatedBy:   sr.UpdatedBy,
	}, nil
}

var (
	systemRoleInsertWhitelist = []string{
		"id", "tenant_id", "name", "description",
		"type", "permissions", "priority", "status",
		"created_at", "created_by", "updated_at", "updated_by",
	}
)

func (svc *SystemRoleService) Insert(ctx context.Context, sr *models.SystemRole) error {
	return sr.Insert(ctx, Global, boil.Whitelist(systemRoleInsertWhitelist...))
}

func (svc *SystemRoleService) GetByID(ctx context.Context, id string) (*models.SystemRole, error) {
	sr, err := models.FindSystemRole(ctx, Global, id)
	if err != nil {
		return nil, err
	}
	return sr, nil
}

func (svc *SystemRoleService) Search(ctx context.Context, tenantID, query string) ([]*models.SystemRole, error) {
	queryParts := []qm.QueryMod{}

	paramIdx := 1
	if tenantID == "" {
		queryParts = append(queryParts, qm.Where("tenant_id IN ($1)", DefaultTenantID))
		paramIdx++
	} else {
		queryParts = append(queryParts, qm.Where("tenant_id IN ($1, $2)", DefaultTenantID, tenantID))
		paramIdx += 2
	}

	if query != "" {
		queryParts = append(queryParts, qm.Where(fmt.Sprintf("LOWER(name) LIKE $%d", paramIdx), "%"+strings.ToLower(query)+"%"))
		paramIdx++ // NOTE: not actually necessary, but just in case we add any more params
	}

	systemRoles, err := models.SystemRoles(queryParts...).All(ctx, Global)
	if err != nil {
		return nil, err
	}

	return systemRoles, nil
}

var (
	defaultSystemRoleUpdateWhitelist = []string{
		"name", "description", "type", "permissions", "priority",
		"status", "updated_at", "updated_by",
	}
)

func (svc *SystemRoleService) Update(ctx context.Context, sr *models.SystemRole, onlyFields []string) error {
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

	numAffected, err := sr.Update(ctx, Global, boil.Whitelist(whitelist...))
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error updating systemRole: update affected 0 rows")
	}

	return nil
}

func (svc *SystemRoleService) DeleteByID(ctx context.Context, id string) error {
	sr := &models.SystemRole{ID: id}
	numAffected, err := sr.Delete(ctx, Global)
	if err != nil {
		return err
	}
	if numAffected != 1 {
		return fmt.Errorf("error deleting systemRole: delete affected 0 rows")
	}
	return nil
}
