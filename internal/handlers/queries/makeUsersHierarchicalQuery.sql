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
				SELECT DISTINCT person_id, new_role_id
				FROM (
					SELECT
						p.id AS person_id,
						c.id AS new_role_id,
						p.crm_role_ids AS current_role_ids,
						ROW_NUMBER() OVER(PARTITION BY p.id, c.outreach_id ORDER BY CASE WHEN c.outreach_id <> c.id THEN 0 ELSE 1 END ASC) AS rn
					FROM person p
					INNER JOIN crm_role c ON c.outreach_id = p.outreach_role_id
					WHERE p.tenant_id = $1 AND c.tenant_id = $1
				) x
				WHERE rn = 1 AND (NOT new_role_id = ANY(current_role_ids) OR (current_role_ids IS NULL AND new_role_id IS NOT NULL))
				UNION ALL
				SELECT
					p.id AS person_id,
					NULL AS new_role_id
				FROM person p
				WHERE crm_role_ids IS NOT NULL AND outreach_role_id IS NULL AND tenant_id = $1
			) AS subquery
			WHERE person.id = subquery.person_id AND person.tenant_id = $1
			RETURNING id
		)

		SELECT COUNT(*) AS changeCount, 'crm_role' AS changeType FROM crm_role_work
		UNION ALL
		SELECT COUNT(*) AS changeCount, 'person' AS changeType FROM person_role_work
		;