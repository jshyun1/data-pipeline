package com.company.pipeline.nifi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record NifiParameterContextListResponse(List<NifiParameterContextResponse> parameterContexts) {
}
