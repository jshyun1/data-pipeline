package com.company.pipeline.connector;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** KAFKA_CONNECT_URL 환경변수로 설정 (docker-compose 네트워크 안에서는 http://kafka-connect:8083). */
@ConfigurationProperties(prefix = "kafka-connect")
public record KafkaConnectProperties(String baseUrl) {
}
