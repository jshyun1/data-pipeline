package com.company.pipeline.nifi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * GET /nifi-api/parameter-contexts/{id} 응답 중 파라미터 이름/값만.
 *
 * <p>sensitive=true인 파라미터는 NiFi가 값을 내려주지 않는다(항상 null). 미러에도
 * 값 없이 이름만 남긴다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NifiParameterContextResponse(String id, Component component) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Component(String id, String name, String description,
                            List<ParameterEntity> parameters) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ParameterEntity(Parameter parameter) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Parameter(String name, String value, Boolean sensitive, String description) {
    }
}
