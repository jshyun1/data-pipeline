package com.company.pipeline.pipeline.dto;

import java.util.List;

public record PipelineBatchCreateResponse(int createdCount, List<PipelineResponse> pipelines) {}
