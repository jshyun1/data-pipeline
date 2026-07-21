package com.company.pipeline.connection;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * 실제 Postgres 컨테이너에 Flyway 마이그레이션 5개가 전부 적용되는지,
 * pipeline_connection에 실제로 저장/조회되는지를 검증한다.
 */
@Testcontainers
@SpringBootTest
class ConnectionRepositoryIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void cryptoProperties(DynamicPropertyRegistry registry) {
        registry.add("pipeline.crypto.secret", () -> "test-secret-value-not-for-prod");
        registry.add("pipeline.crypto.salt", () -> "deadbeef");
    }

    @Autowired
    private ConnectionRepository connectionRepository;

    @Test
    void save_and_findById_roundTrips() {
        PipelineConnection entity = new PipelineConnection(
                "oracle-source-poc", DbType.ORACLE, "oracle-db", 1521,
                null, "XEPDB1", null, "c##dbzuser", "cipher-text", null);

        PipelineConnection saved = connectionRepository.save(entity);

        assertThat(saved.getId()).isNotNull();
        PipelineConnection found = connectionRepository.findById(saved.getId()).orElseThrow();
        assertThat(found.getName()).isEqualTo("oracle-source-poc");
        assertThat(found.getStatus()).isEqualTo(ConnectionStatus.UNKNOWN);
        assertThat(found.getCreatedAt()).isNotNull();
    }
}
