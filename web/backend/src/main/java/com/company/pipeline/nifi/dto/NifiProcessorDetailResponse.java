package com.company.pipeline.nifi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;

/**
 * GET /nifi-api/processors/{id} 응답 중 필요한 부분만.
 *
 * <p>status(recursive) 응답에는 프로세서 "설정"이 없어서, 적재 대상 테이블 같은 값은
 * 이 엔드포인트로 따로 읽어야 한다. 프로세서마다 한 번만 읽고 캐시한다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NifiProcessorDetailResponse(Component component) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Component(String id, String name, String type, Config config) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Config(Map<String, String> properties) {
    }

    /** PutDatabaseRecord의 적재 대상을 "schema.table"로. 알 수 없으면 null. */
    public String targetTable() {
        if (component == null || component.config() == null || component.config().properties() == null) {
            return null;
        }
        Map<String, String> properties = component.config().properties();
        String table = properties.get("put-db-record-table-name");
        if (table == null || table.isBlank()) {
            return null;
        }
        String schema = properties.get("put-db-record-schema-name");
        return schema == null || schema.isBlank() ? table : schema + "." + table;
    }
}
