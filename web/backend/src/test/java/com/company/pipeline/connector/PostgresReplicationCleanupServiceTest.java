package com.company.pipeline.connector;

import static org.mockito.Mockito.verifyNoInteractions;

import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.connection.PipelineConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * 실제 DB 접속 경로(DriverManager)는 이 환경에서 Testcontainers를 못 쓰기도 하고
 * (ConnectionRepositoryIT 참고) 굳이 흉내내기보단, 접속 자체를 시도하면 안 되는
 * 가드 조건들만 검증한다 - 접속을 시도했다면 passwordCryptoService.decrypt가 호출됐을
 * 것이므로 그게 안 불렸다는 것으로 "시도조차 안 했다"를 확인한다.
 */
@ExtendWith(MockitoExtension.class)
class PostgresReplicationCleanupServiceTest {

    @Mock
    private PasswordCryptoService passwordCryptoService;

    @Test
    void cleanup_nonPostgresSourceConnection_doesNothing() {
        PostgresReplicationCleanupService svc = new PostgresReplicationCleanupService(passwordCryptoService);
        PipelineConnector connector = new PipelineConnector(1L, "SOURCE", "source-1-oracle-appuser-customers",
                "io.debezium.connector.oracle.OracleConnector", "{}");
        PipelineConnection oracleSource = new PipelineConnection(
                "conn", DbType.ORACLE, "host", 1521, null, "XEPDB1", null, "c##user", "cipher", null);

        svc.cleanup(connector, oracleSource);

        verifyNoInteractions(passwordCryptoService);
    }

    @Test
    void cleanup_nonPostgresSourceConnectorClass_doesNothing() {
        PostgresReplicationCleanupService svc = new PostgresReplicationCleanupService(passwordCryptoService);
        // 커넥터 클래스가 Postgres 소스가 아니면(예: JDBC Sink) 소스 정리 대상이 아님.
        PipelineConnector connector = new PipelineConnector(1L, "SINK", "sink-1-postgresql-cdc_landing-customers",
                "io.debezium.connector.jdbc.JdbcSinkConnector", "{}");
        PipelineConnection postgresConnection = new PipelineConnection(
                "conn", DbType.POSTGRESQL, "host", 5432, "db", null, null, "user", "cipher", null);

        svc.cleanup(connector, postgresConnection);

        verifyNoInteractions(passwordCryptoService);
    }
}
