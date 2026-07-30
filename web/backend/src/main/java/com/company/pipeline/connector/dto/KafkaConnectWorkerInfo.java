package com.company.pipeline.connector.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** GET / 응답. 워커가 살아 있는지 확인하는 가장 가벼운 엔드포인트다. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record KafkaConnectWorkerInfo(
        String version,
        String commit,
        @JsonProperty("kafka_cluster_id") String kafkaClusterId
) {
}
