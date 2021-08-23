package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/golang/protobuf/ptypes"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/orchard/models"
	tenantPb "github.com/loupe-co/protos/src/common/tenant"
	null "github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/types"
)

// TODO: Add tracing

type TenantService struct {
	tx *sql.Tx
}

func NewTenantService() *TenantService {
	return &TenantService{}
}

func (svc *TenantService) WithTransaction(ctx context.Context) error {
	tx, err := Global.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	svc.tx = tx
	return nil
}

func (svc *TenantService) Rollback() error {
	if svc.tx == nil {
		return nil
	}
	return svc.tx.Rollback()
}

func (svc *TenantService) Commit() error {
	if svc.tx == nil {
		return nil
	}
	return svc.tx.Commit()
}

func (svc *TenantService) FromProto(t *tenantPb.Tenant) (*models.Tenant, error) {
	createdAt := t.CreatedAt.AsTime()
	updatedAt := t.UpdatedAt.AsTime()

	groupSyncMetadataRaw, err := json.Marshal(t.GroupSyncMetadata)
	if err != nil {
		return nil, errors.Wrap(err, "error marshaling group_sync_metadata")
	}

	return &models.Tenant{
		ID:                t.Id,
		Status:            t.Status,
		Name:              t.Name,
		CreatedAt:         createdAt,
		UpdatedAt:         updatedAt,
		CRMID:             null.String{String: t.CrmId, Valid: t.CrmId != ""},
		IsTestInstance:    null.BoolFrom(t.IsTestInstance),
		ParentTenantID:    null.String{String: t.ParentTenantId, Valid: t.ParentTenantId != ""},
		GroupSyncState:    strings.ToLower(t.GroupSyncState.String()),
		GroupSyncMetadata: types.JSON(groupSyncMetadataRaw),
		Permissions:       t.Permissions,
	}, nil
}

func (svc *TenantService) ToProto(t *models.Tenant) (*tenantPb.Tenant, error) {
	createdAt, err := ptypes.TimestampProto(t.CreatedAt)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing createdAt")
	}

	updatedAt, err := ptypes.TimestampProto(t.UpdatedAt)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing updatedAt")
	}

	groupSyncState := tenantPb.GroupSyncStatus_Inactive
	switch t.GroupSyncState {
	case "active":
		groupSyncState = tenantPb.GroupSyncStatus_Active
	case "people_only":
		groupSyncState = tenantPb.GroupSyncStatus_PeopleOnly
	}

	groupSyncMetadata := &tenantPb.GroupSyncMetadata{}
	if err := json.Unmarshal(t.GroupSyncMetadata, groupSyncMetadata); err != nil {
		return nil, errors.Wrap(err, "error unmarshaling groupSyncMetadata")
	}

	return &tenantPb.Tenant{
		Id:                t.ID,
		Status:            t.Status,
		Name:              t.Name,
		CrmId:             t.CRMID.String,
		IsTestInstance:    t.IsTestInstance.Bool,
		ParentTenantId:    t.ParentTenantID.String,
		GroupSyncState:    groupSyncState,
		GroupSyncMetadata: groupSyncMetadata,
		Permissions:       t.Permissions,
		CreatedAt:         createdAt,
		UpdatedAt:         updatedAt,
	}, nil
}

func (svc *TenantService) GetByID(ctx context.Context, tenantID string) (*models.Tenant, error) {
	return models.FindTenant(ctx, Global, tenantID)
}
