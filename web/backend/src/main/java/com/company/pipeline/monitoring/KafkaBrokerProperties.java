package com.company.pipeline.monitoring;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** KAFKA_BOOTSTRAP_SERVERS 환경변수로 설정 (docker-compose 네트워크 안에서는 kafka:9092). */
@ConfigurationProperties(prefix = "kafka")
public record KafkaBrokerProperties(String bootstrapServers) {
}
