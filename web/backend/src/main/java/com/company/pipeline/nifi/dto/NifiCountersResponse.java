package com.company.pipeline.nifi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/** GET /nifi-api/counters 응답. PutDatabaseRecord는 "INSERT updates performed" 카운터를
 * "Put-{프로세서 이름} ({프로세서 id})" 형태의 context로 등록한다. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NifiCountersResponse(Counters counters) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Counters(AggregateSnapshot aggregateSnapshot) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record AggregateSnapshot(List<Counter> counters) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Counter(String id, String context, String name, Long valueCount) {
    }
}
