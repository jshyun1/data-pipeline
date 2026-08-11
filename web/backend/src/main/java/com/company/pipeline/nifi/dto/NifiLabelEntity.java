package com.company.pipeline.nifi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;

/** GET/POST/PUT /nifi-api/labels 응답 중 캔버스 상태 라벨 동기화에 필요한 부분만. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NifiLabelEntity(String id, Revision revision, Component component) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Revision(String clientId, Long version) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Component(String id, String label, NifiFlowResponse.Position position,
                            Map<String, String> style) {
    }
}
