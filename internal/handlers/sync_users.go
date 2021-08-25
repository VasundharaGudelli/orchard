package handlers

import (
	"context"
	"time"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/internal/db"
	"github.com/loupe-co/orchard/internal/models"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

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

	ids := make([]interface{}, len(latestCRMUsers))
	for i, person := range latestCRMUsers {
		ids[i] = person.Id
	}

	tx, err := h.db.NewTransaction(spanCtx)
	if err != nil {
		err := errors.Wrap(err, "error creating transaction")
		logger.Error(err)
		return nil, err
	}

	personSvc := h.db.NewPersonService()
	personSvc.SetTransaction(tx)

	currentPeople, err := personSvc.GetByIDs(spanCtx, in.TenantId, ids...)
	if err != nil {
		err := errors.Wrap(err, "error getting existing person records from sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	existingPeople := make(map[string]*models.Person, len(currentPeople))
	for _, person := range currentPeople {
		existingPeople[person.ID] = person
	}

	upsertPeople := make([]*models.Person, len(latestCRMUsers))
	for i, person := range latestCRMUsers {
		p := personSvc.FromProto(person)
		p.TenantID = in.TenantId
		p.UpdatedBy = db.DefaultTenantID
		p.UpdatedAt = time.Now().UTC()
		if current, ok := existingPeople[person.Id]; ok {
			if len(p.RoleIds) == 0 {
				p.RoleIds = current.RoleIds
			}
			if !p.GroupID.Valid || p.GroupID.String == "" {
				p.GroupID = current.GroupID
			}
			p.IsSynced = current.IsSynced
			p.IsProvisioned = current.IsProvisioned
			p.Status = current.Status
		} else {
			p.CreatedBy = db.DefaultTenantID
			p.CreatedAt = time.Now().UTC()
			p.IsSynced = true
		}
		upsertPeople[i] = p
	}

	if err := personSvc.UpsertAll(spanCtx, upsertPeople); err != nil {
		err := errors.Wrap(err, "error upserting merged person records")
		logger.Error(err)
		personSvc.Rollback()
		return nil, err.AsGRPC()
	}

	if err := personSvc.UpdatePersonGroups(spanCtx, in.TenantId); err != nil {
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
