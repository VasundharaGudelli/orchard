package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	_ "embed"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/db"
	"github.com/loupe-co/orchard/internal/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	tenantPb "github.com/loupe-co/protos/src/common/tenant"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/queries"
)

func (h *Handlers) SyncUsers(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	ctx, span := log.StartSpan(ctx, "SyncUsers")
	defer span.End()

	logger := log.WithContext(ctx).
		WithTenantID(in.TenantId).
		WithCustom("syncSince", in.SyncSince.AsTime()).
		WithCustom("licenseType", in.LicenseType).
		WithCustom("updatePersonGroups", in.UpdatePersonGroups)

	tenantID, license := parseSyncLicense(in)

	if license == tenantPb.LicenseType_LICENSE_TYPE_CREATE_AND_CLOSE || license == tenantPb.LicenseType_LICENSE_TYPE_CREATE_AND_CLOSE_HYBRID {
		logger.Info("begin SyncUsers for c&c tenant")

		if err := h.cleanupCNCUsers(ctx, tenantID); err != nil {
			err := errors.Wrap(err, "error running cleanupCNCUsers")
			logger.Error(err)
			return nil, err.Clean().AsGRPC()
		}

		if license == tenantPb.LicenseType_LICENSE_TYPE_CREATE_AND_CLOSE {
			logger.Debug("making hierarchy adjustments for c&c tenant")
			if err := h.makeHierarchyAdjustments(ctx, tenantID); err != nil {
				err := errors.Wrap(err, "error running makeHierarchyAdjustments")
				logger.Error(err)
				return nil, err.Clean().AsGRPC()
			}
		}

		if in.UpdatePersonGroups {
			if err := h.updatePersonGroups(ctx, tenantID, nil); err != nil {
				err := errors.Wrap(err, "failed to update person groups")
				logger.Error(err)
				return nil, err
			}
		}

		return &servicePb.SyncResponse{}, nil
	}

	logger.Info("begin SyncUsers")

	var (
		batchSize = h.cfg.SyncUsersBatchSize
		total     int
		nextToken string
		err       error
	)

	for {
		var latestCRMUsers []*orchardPb.Person
		latestCRMUsers, total, nextToken, err = h.crmClient.GetLatestChangedPeople(ctx, in.TenantId, in.SyncSince, batchSize, nextToken)
		if err != nil {
			err := errors.Wrap(err, "error getting person data from crm-data-access")
			logger.Error(err)
			return nil, err.Clean().AsGRPC()
		}
		data, _ := json.Marshal(latestCRMUsers)
		s1 := fmt.Sprintf("latestCRMUsers, Data: %v, total: %v", string(data), total)
		logger.WithTenantID(in.TenantId).Info(s1)

		if len(latestCRMUsers) == 0 {
			break
		}

		batch, err := h.createPeopleBatch(ctx, in.TenantId, latestCRMUsers)
		if err != nil {
			err := errors.Wrap(err, "error creating people batch")
			logger.Error(err)
			return nil, err.Clean().AsGRPC()
		}
		data, _ = json.Marshal(batch)
		s1 = fmt.Sprintf("batch, Data: %v", string(data))
		logger.WithTenantID(in.TenantId).Info(s1)

		if err := h.batchUpsertUsers(ctx, batch); err != nil {
			err := errors.Wrap(err, "error upserting batch users")
			logger.Error(err)
			return nil, err.Clean().AsGRPC()
		}

		if nextToken == "" {
			break
		}
	}

	svc := h.db.NewPersonService()
	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		err := errors.Wrap(err, "error creating transaction")
		logger.Error(err)
		return nil, err.Clean().AsGRPC()
	}
	svc.SetTransaction(tx)
	defer svc.Rollback()

	if err := h.updatePersonGroups(ctx, in.TenantId, svc.GetTransaction()); err != nil {
		err := errors.Wrap(err, "error updating person groups")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	if err := svc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting sync users transactions")
		logger.Error(err)
		return nil, err.Clean().AsGRPC()
	}

	logger.WithCustom("total", total).Info("finish SyncUsers")

	return &servicePb.SyncResponse{}, nil
}

func calculateBatchCount(total, batchSize int) int {
	batchCount := float64(total) / float64(batchSize)
	return int(math.Ceil(batchCount))
}

func (h *Handlers) createPeopleBatch(ctx context.Context, tenantID string, people []*orchardPb.Person) ([]*models.Person, error) {
	ctx, span := log.StartSpan(ctx, "createPeopleBatch")
	defer span.End()

	svc := h.db.NewPersonService()

	ids := make([]interface{}, len(people))
	for i, person := range people {
		ids[i] = person.Id
	}

	currentPeople, err := svc.GetByIDs(ctx, tenantID, ids...)
	if err != nil {
		return nil, errors.Wrap(err, "error getting existing person records from sql")
	}
	data, _ := json.Marshal(currentPeople)
	s1 := fmt.Sprintf("currentPeople, Data: %v", string(data))
	log.WithTenantID(tenantID).Info(s1)

	existingPeople := make(map[string]*models.Person, len(currentPeople))
	for _, person := range currentPeople {
		existingPeople[person.ID] = person
	}

	batch := make([]*models.Person, len(people))
	for i, person := range people {
		p := svc.FromProto(person)

		data, _ := json.Marshal(p)
		s1 := fmt.Sprintf("range people1, Data: %v", string(data))
		log.WithTenantID(tenantID).Info(s1)

		p.TenantID = tenantID
		p.UpdatedBy = db.DefaultTenantID
		p.UpdatedAt = time.Now().UTC()
		if current, ok := existingPeople[person.Id]; ok {
			if current.CreatedBy != db.DefaultTenantID && !(current.CreatedBy == db.DefaultOutreachSyncID && current.ID != current.OutreachGUID.String) {
				batch[i] = nil
				continue
			}
			if len(p.RoleIds) == 0 {
				p.RoleIds = current.RoleIds
			}
			if !p.GroupID.Valid || p.GroupID.String == "" {
				p.GroupID = current.GroupID
			}
			p.IsSynced = current.IsSynced
			p.IsProvisioned = current.IsProvisioned
		} else {
			p.CreatedBy = db.DefaultTenantID
			p.CreatedAt = time.Now().UTC()
			p.IsSynced = true
		}
		if strings.EqualFold(p.Status, orchardPb.BasicStatus_Inactive.String()) {
			p.IsProvisioned = false
			p.Email = null.String{String: "", Valid: false}
		}

		data, _ = json.Marshal(p)
		s1 = fmt.Sprintf("range people1 processed, Data: %v", string(data))
		log.WithTenantID(tenantID).Info(s1)
		batch[i] = p
	}

	return batch, nil
}

func (h *Handlers) batchUpsertUsers(ctx context.Context, people []*models.Person) error {
	ctx, span := log.StartSpan(ctx, "batchUpsertUsers")
	defer span.End()

	svc := h.db.NewPersonService()
	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		return errors.Wrap(err, "error creating transaction")
	}
	svc.SetTransaction(tx)
	defer svc.Rollback()

	if err := svc.UpsertAll(ctx, people); err != nil {
		return errors.Wrap(err, "error upserting people records batch")
	}

	if err := svc.Commit(); err != nil {
		return errors.Wrap(err, "error committing transaction")
	}

	return nil
}

//go:embed queries/cleanupCNCUsers.sql
var cleanupCNCUsersQuery string

type CNCUserCleanupResult struct {
	ID     sql.NullString `boil:"id" json:"id"`
	Action sql.NullString `boil:"action" json:"action"`
}

func (h *Handlers) cleanupCNCUsers(ctx context.Context, tenantID string) error {
	ctx, span := log.StartSpan(ctx, "batchUpsertUsers")
	defer span.End()

	logger := log.WithContext(ctx).WithTenantID(tenantID)

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		return err
	}

	pSVC := h.db.NewPersonService()

	defer tx.Rollback()
	result := []*CNCUserCleanupResult{}

	if err := queries.Raw(cleanupCNCUsersQuery, tenantID).Bind(ctx, tx, &result); err != nil {
		return errors.Wrap(err, "error committing transaction")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "error in cleanupCNCUsers")
	}

	for _, item := range result {
		log.WithContext(ctx).WithCustom("userId", item.ID.String).WithCustom("action", item.Action.String).Info("user impacted by cleanup")
		if !item.Action.Valid || !item.ID.Valid || item.Action.String != "swap" {
			continue
		}
		logger.DeepCopy().WithCustom("id", item.ID.String).Debug("reprovisioning swapped user")
		if _, err := updateUserProvisioning(ctx, tenantID, item.ID.String, "", pSVC, h.auth0Client); err != nil {
			return errors.Wrap(err, "error updating user provisioning")
		}
	}

	return nil
}

//go:embed queries/makeUsersHierarchicalQuery.sql
var makeUsersHierarchicalQuery string

func (h *Handlers) makeHierarchyAdjustments(ctx context.Context, tenantID string) error {
	ctx, span := log.StartSpan(ctx, "batchUpsertUsers")
	defer span.End()

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if _, err := queries.Raw(makeUsersHierarchicalQuery, tenantID).ExecContext(ctx, tx); err != nil {
		return errors.Wrap(err, "error committing transaction")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "error in makeHierarchyAdjustments")
	}
	return nil
}
