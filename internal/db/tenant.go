package db

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/golang/protobuf/ptypes"
	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/models"
	tenantPb "github.com/loupe-co/protos/src/common/tenant"
	null "github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
	"github.com/volatiletech/sqlboiler/v4/types"
)

type TenantService struct {
	*DBService
}

func (db *DB) NewTenantService() *TenantService {
	return &TenantService{
		DBService: db.NewDBService(),
	}
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
	spanCtx, span := log.StartSpan(ctx, "Tenant.GetByID")
	defer span.End()
	return models.FindTenant(spanCtx, svc.GetContextExecutor(), tenantID)
}

type GetGroupSyncStateResponse struct {
	GroupSyncState string `boil:"group_sync_state" json:"groupSyncState"`
}

func (svc *TenantService) GetGroupSyncState(ctx context.Context, tenantID string) (tenantPb.GroupSyncStatus, error) {
	spanCtx, span := log.StartSpan(ctx, "Tenant.GetGroupSyncState")
	defer span.End()
	res := &GetGroupSyncStateResponse{}
	err := queries.Raw("SELECT group_sync_state FROM tenant WHERE id = $1", tenantID).Bind(spanCtx, svc.GetContextExecutor(), res)
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
	spanCtx, span := log.StartSpan(ctx, "Tenant.CheckPeopleSyncState")
	defer span.End()
	res := &CheckPeopleSyncedStateResponse{}
	if err := queries.Raw(checkPeopleSyncStateQuery, tenantID).Bind(spanCtx, svc.GetContextExecutor(), res); err != nil && err != sql.ErrNoRows {
		err := errors.Wrap(err, "error checking people sync state in sql")
		log.Debug(err.Error())
		return false, err
	}
	return res.IsPeopleSynced, nil
}

func (svc *TenantService) UpdateGroupSyncState(ctx context.Context, tenantID string, state tenantPb.GroupSyncStatus) error {
	spanCtx, span := log.StartSpan(ctx, "Tenant.UpdateGroupSyncState")
	defer span.End()
	newState := "inactive"
	switch state {
	case tenantPb.GroupSyncStatus_Active:
		newState = "active"
	case tenantPb.GroupSyncStatus_PeopleOnly:
		newState = "people_only"
	}
	t := models.Tenant{ID: tenantID, GroupSyncState: newState}
	_, err := t.Update(spanCtx, svc.GetContextExecutor(), boil.Whitelist("group_sync_state"))
	if err != nil {
		return errors.Wrap(err, "error updating tenant in sql")
	}
	return nil
}

func (svc *TenantService) UpdateGroupSyncMetadata(ctx context.Context, tenantID string, metadata *tenantPb.GroupSyncMetadata) error {
	spanCtx, span := log.StartSpan(ctx, "Tenant.UpdateGroupSyncMetadata")
	defer span.End()
	metadataRaw, err := json.Marshal(metadata)
	if err != nil {
		return errors.Wrap(err, "error marshaling metadata")
	}
	t := models.Tenant{ID: tenantID, GroupSyncMetadata: types.JSON(metadataRaw)}
	_, err = t.Update(spanCtx, svc.GetContextExecutor(), boil.Whitelist("group_sync_metadata"))
	if err != nil {
		return errors.Wrap(err, "error updating tenant in sql")
	}
	return nil
}

func (svc *TenantService) GetActiveTenants(ctx context.Context) ([]*models.Tenant, error) {
	spanCtx, span := log.StartSpan(ctx, "Tenant.GetActiveTenants")
	defer span.End()
	tenants, err := models.Tenants(qm.Where("status NOT IN ('expired', 'deleted')")).All(spanCtx, svc.db)
	if err != nil {
		return nil, errors.Wrap(err, "error querying for active tenants")
	}
	return tenants, nil
}

const (
	getTenantPersonCountQuery = `
SELECT count(*) FILTER (WHERE p.status = 'active' AND p.group_id IS NOT NULL AND g.status = 'active') AS active_in_group,
       count(*) FILTER (WHERE p.status = 'active' AND p.group_id IS NULL) AS active_no_group,
       count(*) FILTER (WHERE p.status = 'inactive') AS inactive,
       count(*) FILTER (WHERE p.status = 'active' AND p.is_provisioned = TRUE) as provisioned,
       count(*) AS total
  FROM person p
  LEFT JOIN "group" g
    ON p.group_id = g.id
 WHERE p.tenant_id = $1`
)

type TenantPersonCountResponse struct {
	ActiveInGroup int64 `boil:"active_in_group" json:"activeInGroup"`
	Inactive      int64 `boil:"inactive" json:"inactive"`
	Provisioned   int64 `boil:"provisioned" json:"provisioned"`
	Total         int64 `boil:"total" json:"total"`
}

func (svc *TenantService) GetTenantPersonCounts(ctx context.Context, tenantID string) (*TenantPersonCountResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "Tenant.GetTenantPersonCounts")
	defer span.End()
	res := &TenantPersonCountResponse{}
	if err := queries.Raw(getTenantPersonCountQuery, tenantID).Bind(spanCtx, svc.GetContextExecutor(), res); err != nil && err != sql.ErrNoRows {
		err := errors.Wrap(err, "error getting person counts for tenant")
		log.Debug(err.Error())
		return nil, err
	}
	return res, nil
}
