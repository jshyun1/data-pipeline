package com.company.pipeline.infra.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * GET /api/v2/monitor/health 원본 응답. Airflow는 컴포넌트별로 heartbeat 키 이름이 다르다
 * (latest_scheduler_heartbeat / latest_triggerer_heartbeat / latest_dag_processor_heartbeat).
 *
 * <p>구성하지 않은 컴포넌트는 status가 null로 온다(이 배포에는 triggerer 컨테이너가 없어
 * 항상 null) - "죽었다"와 "안 쓴다"를 구분해야 하므로 null을 그대로 살려 둔다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AirflowHealthResponse(
        Metadatabase metadatabase,
        Scheduler scheduler,
        Triggerer triggerer,
        @JsonProperty("dag_processor") DagProcessor dagProcessor
) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Metadatabase(String status) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Scheduler(String status, @JsonProperty("latest_scheduler_heartbeat") String latestHeartbeat) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Triggerer(String status, @JsonProperty("latest_triggerer_heartbeat") String latestHeartbeat) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record DagProcessor(String status, @JsonProperty("latest_dag_processor_heartbeat") String latestHeartbeat) {
    }
}
