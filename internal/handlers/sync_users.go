package handlers

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/loupe-co/go-common/errors"
	commonSync "github.com/loupe-co/go-common/sync"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/db"
	"github.com/loupe-co/orchard/internal/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
	"github.com/volatiletech/null/v8"
)

const DefaultBatchSize = 2000

func (h *Handlers) SyncUsers(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "SyncUsers")
	defer span.End()

	syncSince := in.SyncSince.AsTime()

	logger := log.WithTenantID(in.TenantId).WithCustom("syncSince", syncSince)

	latestCRMUsers, err := h.crmClient.GetLatestChangedPeople(spanCtx, in.TenantId, in.SyncSince)
	if err != nil {
		err := errors.Wrap(err, "error getting person data from crm-data-access")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	personSvc := h.db.NewPersonService()

	wp, _ := commonSync.NewWorkerPool(spanCtx, 10)
	l := len(latestCRMUsers)
	batchCount := calculateBatchCount(l, DefaultBatchSize)
	results := make([][]*models.Person, batchCount)
	for i := 0; i < batchCount; i++ {
		batchSize := DefaultBatchSize
		cursor := i * DefaultBatchSize
		if l < cursor+batchSize {
			batchSize = l - cursor
		}
		wp.Go(h.createPeopleBatch(spanCtx, in.TenantId, personSvc, latestCRMUsers[cursor:batchSize+cursor], results, i))
	}
	if err := wp.Wait(); err != nil {
		err := errors.Wrap(err, "error waiting for upsert person batches to create")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	tx, err := h.db.NewTransaction(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error creating transaction")
		logger.Error(err)
		return nil, err
	}
	personSvc.SetTransaction(tx)

	for _, batch := range results {
		if err := h.batchUpsertUsers(spanCtx, personSvc, batch)(); err != nil {
			err := errors.Wrap(err, "error running batch upsert people")
			logger.Error(err)
			return nil, err.AsGRPC()
		}
	}

	if err := h.updatePersonGroups(spanCtx, in.TenantId, personSvc.GetTransaction()); err != nil {
		err := errors.Wrap(err, "error updating person groups")
		logger.Error(err)
		personSvc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := personSvc.Commit(); err != nil {
		err := errors.Wrap(err, "error commiting sync users transactions in sql")
		logger.Error(err)
		personSvc.Rollback()
		return nil, err.AsGRPC()
	}

	return &servicePb.SyncResponse{}, nil
}

func calculateBatchCount(total, batchSize int) int {
	batchCount := float64(total) / float64(batchSize)
	return int(math.Ceil(batchCount))
}

func (h *Handlers) createPeopleBatch(ctx context.Context, tenantID string, svc *db.PersonService, people []*orchardPb.Person, results [][]*models.Person, resultIdx int) func() error {
	return func() error {
		spanCtx, span := log.StartSpan(ctx, "createPeopleBatch")
		defer span.End()

		ids := make([]interface{}, len(people))
		for i, person := range people {
			ids[i] = person.Id
		}

		currentPeople, err := svc.GetByIDs(spanCtx, tenantID, ids...)
		if err != nil {
			err := errors.Wrap(err, "error getting existing person records from sql")
			return err
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

		results[resultIdx] = batch

		return nil
	}
}

func (h *Handlers) batchUpsertUsers(ctx context.Context, svc *db.PersonService, people []*models.Person) func() error {
	return func() error {
		spanCtx, span := log.StartSpan(ctx, "batchUpsertUsers")
		defer span.End()
		if err := svc.UpsertAll(spanCtx, people); err != nil {
			err := errors.Wrap(err, "error upserting people records batch")
			return err
		}
		return nil
	}
}
