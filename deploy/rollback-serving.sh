#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required}"
: "${TARGET_ID:?TARGET_ID is required}"
: "${ROLLBACK_WORK_ID:?ROLLBACK_WORK_ID is required}"
: "${ROLLBACK_AUDIT_WORK_ID:?ROLLBACK_AUDIT_WORK_ID is required}"
: "${EXPECTED_URI:?EXPECTED_URI is required}"
: "${EXPECTED_VERSION:?EXPECTED_VERSION is required}"

psql "${DATABASE_URL}" -X -v ON_ERROR_STOP=1 \
  -v target_id="${TARGET_ID}" \
  -v rollback_work_id="${ROLLBACK_WORK_ID}" \
  -v rollback_audit_work_id="${ROLLBACK_AUDIT_WORK_ID}" \
  -v expected_uri="${EXPECTED_URI}" \
  -v expected_version="${EXPECTED_VERSION}" <<'SQL'
BEGIN;
WITH current_target AS (
    SELECT *
    FROM targets AS target
    WHERE target.target_id = :'target_id'::uuid
      AND target.served_lance_uri = :'expected_uri'
      AND target.served_lance_version = :'expected_version'::bigint
    FOR UPDATE
), prior AS (
    SELECT work.*
    FROM target_work AS work
    JOIN current_target AS target ON target.target_id = work.target_id
    WHERE work.work_id = :'rollback_work_id'::uuid
      AND work.state = 'SUCCEEDED'
      AND work.kind IN ('SERVE', 'REBUILD')
      AND work.candidate_lance_uri IS NOT NULL
      AND work.indexed_lance_version IS NOT NULL
      AND work.artifact_manifest_uri IS NOT NULL
      AND work.artifact_digest IS NOT NULL
), restored AS (
    UPDATE targets AS target
    SET served_lance_uri = prior.candidate_lance_uri,
        served_lance_version = prior.indexed_lance_version,
        updated_at = CURRENT_TIMESTAMP
    FROM prior
    WHERE target.target_id = prior.target_id
    RETURNING target.target_id
), audited AS (
    INSERT INTO target_work (
        work_id,
        target_id,
        kind,
        state,
        phase,
        attempt_count,
        next_attempt_at,
        data_lance_version,
        indexed_lance_version,
        candidate_lance_uri,
        expected_ingest_lance_uri,
        expected_served_lance_uri,
        expected_served_lance_version,
        artifact_manifest_uri,
        artifact_digest,
        created_at,
        updated_at
    )
    SELECT
        :'rollback_audit_work_id'::uuid,
        target.target_id,
        'SERVE',
        'SUCCEEDED',
        'PUBLISH',
        1,
        CURRENT_TIMESTAMP,
        prior.data_lance_version,
        prior.indexed_lance_version,
        prior.candidate_lance_uri,
        target.ingest_lance_uri,
        :'expected_uri',
        :'expected_version'::bigint,
        prior.artifact_manifest_uri,
        prior.artifact_digest,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    FROM current_target AS target
    JOIN prior ON prior.target_id = target.target_id
    JOIN restored ON restored.target_id = target.target_id
    RETURNING work_id
)
SELECT count(*) = 1 AS restored FROM audited \gset
\if :restored
COMMIT;
\else
ROLLBACK;
DO $$
BEGIN
    RAISE EXCEPTION 'Rollback refused because the retained publication or expected current catalog tuple did not match.';
END
$$;
\endif
SQL
