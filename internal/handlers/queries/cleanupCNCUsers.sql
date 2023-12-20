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
		SET
			id = x.source_id,
			photo_url = COALESCE(x.target_photo_url, x.source_photo_url),
			manager_id = COALESCE(x.target_manager_id, x.source_manager_id),
			group_id = COALESCE(x.target_group_id, x.source_group_id),
			role_ids = COALESCE(x.source_role_ids, x.target_role_ids),
			crm_role_ids = COALESCE(x.source_crm_role_ids, x.target_crm_role_ids),
			"type" = source_type,
			is_provisioned = x.target_is_provisioned OR x.source_is_provisioned,
			is_synced = x.source_is_synced,
			"status" = CASE WHEN x.source_status::TEXT = 'active' OR x.target_status::TEXT = 'active' THEN 'active'::person_status ELSE 'inactive'::person_status END
		FROM(
			SELECT
				source.id AS source_id,
				target.id AS target_id,
				source.photo_url AS source_photo_url,
				source.manager_id AS source_manager_id,
				source.group_id AS source_group_id,
				source.role_ids AS source_role_ids,
				source.crm_role_ids AS source_crm_role_ids,
				source."type" AS source_type,
				source.is_provisioned AS source_is_provisioned,
				source.is_synced AS source_is_synced,
				source."status" AS source_status,
				target.photo_url AS target_photo_url,
				target.manager_id AS target_manager_id,
				target.group_id AS target_group_id,
				target.role_ids AS target_role_ids,
				target.crm_role_ids AS target_crm_role_ids,
				target."type" AS target_type,
				target.is_provisioned AS target_is_provisioned,
				target.is_synced AS target_is_synced,
				target."status" AS target_status
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

	SELECT id, 'delete' AS "action" FROM delete_action
	UNION ALL
	SELECT id, 'swap' AS "action" FROM swap_action
	UNION ALL
	SELECT id, 'unlink' AS "action" FROM unlink_action