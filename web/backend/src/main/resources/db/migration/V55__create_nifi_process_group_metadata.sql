CREATE TABLE nifi_process_group_metadata (
    process_group_id   VARCHAR(100) PRIMARY KEY,
    process_group_name VARCHAR(200) NOT NULL,
    parent_group_id    VARCHAR(100),
    comments           TEXT,
    created_by         VARCHAR(255) NOT NULL,
    created_at         TIMESTAMP NOT NULL DEFAULT now(),
    updated_by         VARCHAR(255) NOT NULL,
    updated_at         TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_nifi_process_group_metadata_parent
    ON nifi_process_group_metadata (parent_group_id);

INSERT INTO nifi_process_group_metadata (
    process_group_id,
    process_group_name,
    parent_group_id,
    comments,
    created_by,
    created_at,
    updated_by,
    updated_at
)
SELECT
    nifi_pg_id,
    job_name,
    parent_pg_id,
    comments,
    'admin',
    COALESCE(created_at, first_seen_at, now()),
    'admin',
    COALESCE(updated_at, last_synced_at, now())
FROM etl_job
WHERE nifi_pg_id IS NOT NULL
ON CONFLICT (process_group_id) DO NOTHING;
