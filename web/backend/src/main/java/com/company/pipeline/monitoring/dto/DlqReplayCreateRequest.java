package com.company.pipeline.monitoring.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record DlqReplayCreateRequest(Long pipelineId, Integer partition, Long offset,
        @NotBlank @Size(max=1000) String reason) {}
