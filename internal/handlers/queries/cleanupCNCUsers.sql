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