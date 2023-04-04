package handlers

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/db"
	"github.com/loupe-co/orchard/internal/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"github.com/volatiletech/null/v8"
)

func (h *Handlers) SyncUsers(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	ctx, span := log.StartSpan(ctx, "SyncUsers")
	defer span.End()

	logger := log.WithContext(ctx).
		WithTenantID(in.TenantId).
		WithCustom("syncSince", in.SyncSince.AsTime())

	logger.Info("begin SyncUsers")

	var (
		batchSize = 1000
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

		if len(latestCRMUsers) == 0 {
			break
		}

		batch, err := h.createPeopleBatch(ctx, in.TenantId, latestCRMUsers)
		if err != nil {
			err := errors.Wrap(err, "error creating people batch")
			logger.Error(err)
			return nil, err.Clean().AsGRPC()
		}

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

	existingPeople := make(map[string]*models.Person, len(currentPeople))
	for _, person := range currentPeople {
		existingPeople[person.ID] = person
	}

	batch := make([]*models.Person, len(people))
	for i, person := range people {
		p := svc.FromProto(person)
		p.TenantID = tenantID
		p.UpdatedBy = db.DefaultTenantID
		p.UpdatedAt = time.Now().UTC()
		if current, ok := existingPeople[person.Id]; ok {
			if current.CreatedBy != db.DefaultTenantID {
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
