CREATE TABLE nifi_processor_edit_lock (
    processor_id       VARCHAR(64) PRIMARY KEY,
    processor_name     VARCHAR(255),
    owner_token        VARCHAR(64) NOT NULL,
    locked_by_user_id  VARCHAR(255) NOT NULL,
    locked_by_user_nm  VARCHAR(255) NOT NULL,
    acquired_at        TIMESTAMPTZ NOT NULL,
    expires_at         TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_nifi_processor_edit_lock_expires_at
    ON nifi_processor_edit_lock (expires_at);
