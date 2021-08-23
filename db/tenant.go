package db

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/golang/protobuf/ptypes"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/orchard/models"
	tenantPb "github.com/loupe-co/protos/src/common/tenant"
	null "github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/sqlboiler/v4/types"
)

// TODO: Add tracing

type TenantService struct {
	tx *sql.Tx
}

func NewTenantService() *TenantService {
	return &TenantService{}
}

func (svc *TenantService) WithTransaction(ctx context.Context, tx *sql.Tx) error {
	if tx != nil {
		svc.tx = tx
		return nil
	}
	_tx, err := Global.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	svc.tx = _tx
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

func (svc *TenantService) GetTx() *sql.Tx {
	return svc.tx
}

func (svc *TenantService) FromProto(t *tenantPb.Tenant) (*models.Tenant, error) {
	createdAt := t.CreatedAt.AsTime()
	updatedAt := t.UpdatedAt.AsTime()

	groupSyncMetadataRaw, err := json.Marshal(t.GroupSyncMetadata)
	if err != nil {
		return nil, errors.Wrap(err, "error marshaling group_sync_metadata")
	}

	groupSyncState := "inactive"
	switch t.GroupSyncState {
	case tenantPb.GroupSyncStatus_Active:
		groupSyncState = "active"
	case tenantPb.GroupSyncStatus_PeopleOnly:
		groupSyncState = "people_only"
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
		GroupSyncState:    groupSyncState,
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
	case "people_only", "peopleonly":
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

type GetGroupSyncStateResponse struct {
	GroupSyncState string `boil:"group_sync_state" json:"groupSyncState"`
}

func (svc *TenantService) GetGroupSyncState(ctx context.Context, tenantID string) (tenantPb.GroupSyncStatus, error) {
	res := &GetGroupSyncStateResponse{}
	err := queries.Raw("SELECT group_sync_state FROM tenant WHERE id = $1", tenantID).Bind(ctx, Global, res)
	if err != nil {
		return tenantPb.GroupSyncStatus_Inactive, errors.Wrap(err, "error getting tenant group sync state")
	}
	state := tenantPb.GroupSyncStatus_Inactive
	switch res.GroupSyncState {
	case "active":
		state = tenantPb.GroupSyncStatus_Active
	case "people_only", "peopleonly":
		state = tenantPb.GroupSyncStatus_PeopleOnly
	}

	return state, nil
}

const (
	checkPeopleSyncStateQuery = `SELECT SUM(CARDINALITY(COALESCE(g.crm_role_ids, ARRAY[]::TEXT[]))) > 0 AS is_people_synced
	FROM "group" g
	WHERE g.status = 'active' AND tenant_id = $1
	GROUP BY tenant_id;`
)

type CheckPeopleSyncedStateResponse struct {
	IsPeopleSynced bool `boil:"is_people_synced" json:"isPeopleSynced"`
}

func (svc *TenantService) CheckPeopleSyncState(ctx context.Context, tenantID string) (peopleSynced bool, err error) {
	res := &CheckPeopleSyncedStateResponse{}
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	if err := queries.Raw(checkPeopleSyncStateQuery, tenantID).Bind(ctx, x, res); err != nil {
		return false, errors.Wrap(err, "error checking people sync state in sql")
	}
	return res.IsPeopleSynced, nil
}

func (svc *TenantService) UpdateGroupSyncState(ctx context.Context, tenantID string, state tenantPb.GroupSyncStatus) error {
	newState := "inactive"
	switch state {
	case tenantPb.GroupSyncStatus_Active:
		newState = "active"
	case tenantPb.GroupSyncStatus_PeopleOnly:
		newState = "people_only"
	}
	t := models.Tenant{ID: tenantID, GroupSyncState: newState}
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	_, err := t.Update(ctx, x, boil.Whitelist("group_sync_state"))
	if err != nil {
		return errors.Wrap(err, "error updating tenant in sql")
	}
	return nil
}

func (svc *TenantService) UpdateGroupSyncMetadata(ctx context.Context, tenantID string, metadata *tenantPb.GroupSyncMetadata) error {
	metadataRaw, err := json.Marshal(metadata)
	if err != nil {
		return errors.Wrap(err, "error marshaling metadata")
	}
	t := models.Tenant{ID: tenantID, GroupSyncMetadata: types.JSON(metadataRaw)}
	x := boil.ContextExecutor(Global)
	if svc.tx != nil {
		x = svc.tx
	}
	_, err = t.Update(ctx, x, boil.Whitelist("group_sync_metadata"))
	if err != nil {
		return errors.Wrap(err, "error updating tenant in sql")
	}
	return nil
}
