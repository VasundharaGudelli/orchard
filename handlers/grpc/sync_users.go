package grpchandlers

import (
	"context"

	"github.com/loupe-co/go-common/errors"
	"github.com/loupe-co/go-common/sync"
	"github.com/loupe-co/go-loupe-logger/log"
	"github.com/loupe-co/orchard/db"
	"github.com/loupe-co/orchard/models"
	orchardPb "github.com/loupe-co/protos/src/common/orchard"
	servicePb "github.com/loupe-co/protos/src/services/orchard"
)

func (server *OrchardGRPCServer) SyncUsers(ctx context.Context, in *servicePb.SyncUsersRequest) (*servicePb.SyncUsersResponse, error) {
	spanCtx, span := log.StartSpan(ctx, "SyncUsers")
	defer span.End()

	syncSince := in.SyncSince.AsTime()

	logger := log.WithTenantID(in.TenantId).WithCustom("syncSince", syncSince)

	provisionedUsers := []*orchardPb.Person{}
	latestCRMUsers := []*orchardPb.Person{}

	pool, _ := sync.NewWorkerPool(spanCtx, 2)
	pool.Go(func() error {
		var err error
		provisionedUsers, err = server.tenantClient.GetProvisionedUsers(spanCtx, in.TenantId)
		if err != nil {
			return err
		}
		return nil
	})

	pool.Go(func() error {
		var err error
		latestCRMUsers, err = server.crmClient.GetLatestChangedPeople(spanCtx, in.TenantId, in.SyncSince)
		if err != nil {
			return err
		}
		return nil
	})

	if err := pool.Wait(); err != nil {
		err := errors.Wrap(err, "error getting person data from disparate data sources")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	ids := make([]string, len(provisionedUsers)+len(latestCRMUsers))
	idx := 0
	for _, person := range provisionedUsers {
		ids[idx] = person.Id
		idx++
	}
	for _, person := range latestCRMUsers {
		ids[idx] = person.Id
		idx++
	}

	personSvc := db.NewPersonService()
	personSvc.WithTransaction(spanCtx)

	currentPeople, err := personSvc.GetByIDs(spanCtx, in.TenantId, ids...)
	if err != nil {
		err := errors.Wrap(err, "error getting existing person records from sql")
		logger.Error(err)
		return nil, err.AsGRPC()
	}

	mergedPeople := map[string]*models.Person{}
	for _, person := range currentPeople {
		mergedPeople[person.ID] = person
	}
	for _, person := range latestCRMUsers {
		p := personSvc.FromProto(person)
		p.TenantID = in.TenantId
		p.UpdatedBy = "00000000-0000-0000-0000-000000000000"
		if current, ok := mergedPeople[person.Id]; ok {
			current.Name = p.Name
			current.FirstName = p.FirstName
			current.LastName = p.LastName
			current.Email = p.Email
			current.ManagerID = p.ManagerID
			current.CRMRoleIds = p.CRMRoleIds
			current.Status = p.Status
			current.UpdatedAt = p.UpdatedAt
			continue
		}
		p.IsSynced = true
		mergedPeople[person.Id] = p
	}
	for _, person := range provisionedUsers {
		if current, ok := mergedPeople[person.Id]; ok {
			current.IsProvisioned = true
		}
	}

	upsertPeople := make([]*models.Person, len(mergedPeople))
	i := 0
	for _, person := range mergedPeople {
		upsertPeople[i] = person
		i++
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

	return &servicePb.SyncUsersResponse{}, nil
}
