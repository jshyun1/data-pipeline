CREATE TABLE nifi_canvas_status_label (
    group_id VARCHAR(100) PRIMARY KEY,
    parent_group_id VARCHAR(100) NOT NULL,
    label_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_nifi_canvas_status_label_label_id
    ON nifi_canvas_status_label (label_id);
