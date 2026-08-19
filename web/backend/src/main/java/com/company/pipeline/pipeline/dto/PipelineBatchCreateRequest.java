package com.company.pipeline.pipeline.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import java.util.List;

public record PipelineBatchCreateRequest(
        @NotEmpty @Size(max = 50) List<@Valid PipelineCreateRequest> pipelines
) {}
