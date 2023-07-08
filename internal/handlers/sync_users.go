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
	"github.com/volatiletech/sqlboiler/queries"
)

func (h *Handlers) SyncUsers(ctx context.Context, in *servicePb.SyncRequest) (*servicePb.SyncResponse, error) {
	ctx, span := log.StartSpan(ctx, "SyncUsers")
	defer span.End()

	logger := log.WithContext(ctx).
		WithTenantID(in.TenantId).
		WithCustom("syncSince", in.SyncSince.AsTime())

	if strings.Contains(in.TenantId, "create_and_close") {
		spl := strings.Split(in.TenantId, "::")
		tID := spl[0]
		licenseType := spl[1]
		if err := h.cleanupCNCUsers(ctx, tID); err != nil {
			err := errors.Wrap(err, "error running cleanupCNCUsers")
			logger.Error(err)
			return nil, err.Clean().AsGRPC()
		}
		if strings.EqualFold(licenseType, "create_and_close") {
			if err := h.makeHierarchyAdjustments(ctx, tID); err != nil {
				err := errors.Wrap(err, "error running makeHierarchyAdjustments")
				logger.Error(err)
				return nil, err.Clean().AsGRPC()
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

func (h *Handlers) cleanupCNCUsers(ctx context.Context, tenantID string) error {
	ctx, span := log.StartSpan(ctx, "batchUpsertUsers")
	defer span.End()

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if _, err := queries.Raw(`
	WITH
	cleanup_set AS (
		SELECT *,
		CASE WHEN rn > 1 THEN
			(
				CASE WHEN NOT is_provisioned AND id::TEXT = outreach_guid::TEXT THEN 'delete'
				ELSE 'unlink'
				END
			)
		ELSE 'do_nothing'
		END AS cleanup_action
		FROM (
			SELECT
			*,
			ROW_NUMBER() OVER(
				PARTITION BY outreach_guid ORDER BY
				CASE WHEN "status"::TEXT = 'active' THEN 1 ELSE 0 END DESC,
				CASE WHEN is_provisioned THEN 1 ELSE 0 END DESC,
				CASE WHEN created_by = '00000000-0000-0000-0000-000000000000' THEN 2 ELSE (CASE WHEN created_by = '00000000-0000-0000-0000-000000000001' THEN 1 ELSE 0 END) END DESC,
				CASE WHEN id::TEXT <> outreach_guid::TEXT AND created_by = '00000000-0000-0000-0000-000000000000' THEN 2 ELSE (CASE WHEN id::TEXT <> outreach_guid::TEXT THEN 1 ELSE 0 END) END DESC
			) AS rn
			FROM (
				SELECT *, COUNT(id) OVER(PARTITION BY email) AS emailCounter
				FROM person
				WHERE tenant_id = $1
				AND COALESCE(outreach_guid, '') <> ''
			) x
			WHERE emailCounter > 1
		) x
	),
	active_dupe_set AS (
		SELECT *
		FROM (
			SELECT *, COUNT(id) OVER(PARTITION BY email, "status") AS emailCounter
			FROM person WHERE tenant_id = $1
		) x
		WHERE emailCounter = 2 AND "status" = 'active'
		AND id NOT IN (SELECT id FROM cleanup_set)
	),
	delete_action AS (
		DELETE FROM person WHERE id IN (
			SELECT z.id FROM (
				SELECT x.id FROM
				(
					SELECT
						source.id
					FROM active_dupe_set target
					INNER JOIN active_dupe_set source ON source.email = target.email
					WHERE target.created_by = '00000000-0000-0000-0000-000000000001'
					AND source.created_by = '00000000-0000-0000-0000-000000000000'
				) x
				UNION ALL
				SELECT y.id FROM
				(
					SELECT id FROM cleanup_set WHERE cleanup_action = 'delete'
				) y
			) z
		)
		AND tenant_id = $1
		RETURNING id
	),
	swap_action AS (
		UPDATE person
		SET id = x.source_id
		FROM(
			SELECT
				source.id AS source_id,
				target.id AS target_id
			FROM active_dupe_set target
			INNER JOIN active_dupe_set source ON source.email = target.email
			WHERE target.created_by = '00000000-0000-0000-0000-000000000001'
			AND source.created_by = '00000000-0000-0000-0000-000000000000'
		) x
		WHERE person.id = x.target_id
		AND person.tenant_id = $1
		RETURNING id
	),
	unlink_action AS (
		UPDATE person
		SET outreach_id = NULL, outreach_is_admin = NULL, outreach_guid = NULL, outreach_role_id = NULL
		WHERE person.id IN (
			SELECT id FROM cleanup_set WHERE cleanup_action = 'unlink'
		)
		AND person.tenant_id = $1
		RETURNING id
	)

	SELECT COUNT(id), 'delete' AS "action" FROM delete_action
	UNION ALL
	SELECT COUNT(id), 'swap' AS "action" FROM swap_action
	UNION ALL
	SELECT COUNT(id), 'unlink' AS "action" FROM unlink_action
	`, tenantID).ExecContext(ctx, tx); err != nil {
		return errors.Wrap(err, "error committing transaction")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "error in cleanupCNCUsers")
	}
	return nil
}

func (h *Handlers) makeHierarchyAdjustments(ctx context.Context, tenantID string) error {
	ctx, span := log.StartSpan(ctx, "batchUpsertUsers")
	defer span.End()

	tx, err := h.db.NewTransaction(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if _, err := queries.Raw(`
		WITH crm_role_work AS (
			UPDATE crm_role
			SET parent_id = subquery.new_crm_role_parent_id
			FROM (
				SELECT crm_role_id, new_crm_role_parent_id FROM (
					SELECT
					target.id AS crm_role_id,
					source.id AS new_crm_role_parent_id,
					target.parent_id AS current_parent_id,
					ROW_NUMBER() OVER(PARTITION BY target.id ORDER BY CASE WHEN source.outreach_id <> source.id THEN 0 ELSE 1 END ASC) AS rn
					FROM crm_role target
					INNER JOIN crm_role source ON source.tenant_id = target.tenant_id AND source.outreach_id = target.outreach_parent_id
					WHERE target.tenant_id = $1
				) x WHERE rn = 1 AND COALESCE(current_parent_id, '') <> COALESCE(new_crm_role_parent_id, '')
			) AS subquery
			WHERE crm_role.id = subquery.crm_role_id
			AND crm_role.tenant_id = $1
			RETURNING id
		), person_role_work AS (
			UPDATE person
			SET crm_role_ids = CASE WHEN new_role_id IS NULL THEN NULL ELSE ARRAY[new_role_id] END
			FROM (
				SELECT DISTINCT person_id, new_role_id FROM (
					SELECT
					p.id AS person_id,
					c.id AS new_role_id,
					p.crm_role_ids AS current_role_ids,
					ROW_NUMBER() OVER(PARTITION BY p.id, c.outreach_id ORDER BY CASE WHEN c.outreach_id <> c.id THEN 0 ELSE 1 END ASC) AS rn
					FROM person p
					INNER JOIN crm_role c ON c.outreach_id = p.outreach_role_id
					WHERE p.tenant_id = $1 AND c.tenant_id = $1
				) x WHERE rn = 1 AND (NOT new_role_id = ANY(current_role_ids) OR (current_role_ids IS NULL AND new_role_id IS NOT NULL))
				UNION ALL
				SELECT
				p.id AS person_id,
				NULL AS new_role_id
				FROM person p WHERE crm_role_ids IS NOT NULL AND outreach_role_id IS NULL AND tenant_id = $1
			) AS subquery
			WHERE person.id = subquery.person_id
			AND person.tenant_id = $1
			RETURNING id
		)

		SELECT COUNT(*) AS changeCount, 'crm_role' AS changeType FROM crm_role_work
		UNION ALL
		SELECT COUNT(*) AS changeCount, 'person' AS changeType FROM person_role_work
		;
	`, tenantID).ExecContext(ctx, tx); err != nil {
		return errors.Wrap(err, "error committing transaction")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "error in makeHierarchyAdjustments")
	}
	return nil
}
