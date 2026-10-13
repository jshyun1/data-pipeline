ALTER TABLE nifi_process_group_metadata
    ADD COLUMN IF NOT EXISTS wizard_created boolean NOT NULL DEFAULT false;

ALTER TABLE nifi_process_group_metadata
    ADD COLUMN IF NOT EXISTS wizard_payload text;
