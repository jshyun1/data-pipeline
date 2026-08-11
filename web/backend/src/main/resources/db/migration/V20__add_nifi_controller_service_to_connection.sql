ALTER TABLE pipeline_connection
    ADD COLUMN nifi_controller_service_id VARCHAR(100),
    ADD COLUMN nifi_controller_service_name VARCHAR(150);
