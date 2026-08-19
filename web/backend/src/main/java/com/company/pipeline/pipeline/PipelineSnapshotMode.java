package com.company.pipeline.pipeline;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;

public enum PipelineSnapshotMode {
    INITIAL("initial"),
    NO_DATA("no_data");

    private final String connectorValue;

    PipelineSnapshotMode(String connectorValue) { this.connectorValue = connectorValue; }

    public String connectorValue() { return connectorValue; }

    public static PipelineSnapshotMode from(String value) {
        if (value == null || value.isBlank()) return INITIAL;
        for (PipelineSnapshotMode mode : values()) {
            if (mode.name().equalsIgnoreCase(value) || mode.connectorValue.equalsIgnoreCase(value)) return mode;
        }
        throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                "지원하지 않는 스냅샷 모드입니다: " + value + " (INITIAL 또는 NO_DATA만 지원)");
    }
}
