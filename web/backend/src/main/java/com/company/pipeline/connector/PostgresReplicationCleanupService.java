package com.company.pipeline.connector;

import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.connection.PipelineConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.springframework.stereotype.Component;

/**
 * Debezium Postgres 소스 커넥터를 등록하면 replication slot/publication이 소스 Postgres에
 * 자동으로 생기는데(DebeziumPostgresTemplate 참고), 이건 Kafka Connect 리소스가 아니라
 * 순수 Postgres 리소스라 Kafka Connect REST API로는 지울 방법이 없다 - 커넥터를 삭제해도
 * 그대로 남는다. 파이프라인 삭제 시 이 클래스가 소스 Postgres에 직접 접속해서 정리한다.
 *
 * 이 프로젝트의 "백엔드는 컨트롤 플레인만 맡고 데이터 플레인은 Kafka Connect가 처리한다"는
 * 원칙에서 벗어나는 유일한 예외다 - 여기 대응하는 Kafka Connect API 자체가 없어서 불가피함.
 * 비즈니스 데이터는 전혀 안 건드리고 관리용 DDL(DROP PUBLICATION, pg_drop_replication_slot)만
 * 실행한다. 실패해도(권한 부족, 소스 DB 응답 없음, 슬롯이 아직 활성 상태 등) 예외를 던지지
 * 않고 조용히 넘어간다 - 파이프라인 삭제 자체를 막을 이유가 없고, 남은 slot/publication은
 * 다음에 같은 이름의 파이프라인을 다시 만들 때도 어차피 이름이 안 겹치면 문제 없다.
 */
@Component
public class PostgresReplicationCleanupService {

    private final PasswordCryptoService passwordCryptoService;

    public PostgresReplicationCleanupService(PasswordCryptoService passwordCryptoService) {
        this.passwordCryptoService = passwordCryptoService;
    }

    public void cleanup(PipelineConnector connector, PipelineConnection sourceConnection) {
        if (sourceConnection.getDbType() != DbType.POSTGRESQL
                || !"io.debezium.connector.postgresql.PostgresConnector".equals(connector.getConnectorClass())) {
            return;
        }

        String name = ConnectorNaming.postgresSlotAndPublicationName(connector.getConnectorName());
        String url = "jdbc:postgresql://%s:%d/%s".formatted(
                sourceConnection.getHost(), sourceConnection.getPort(), sourceConnection.getDatabaseName());
        String password = passwordCryptoService.decrypt(sourceConnection.getEncryptedPassword());

        try (Connection conn = DriverManager.getConnection(url, sourceConnection.getUsername(), password)) {
            // 식별자는 바인드 파라미터로 못 넘기지만, name은 ConnectorNaming이 [a-z0-9_]만
            // 남기도록 정규화한 값이라 인젝션 여지가 없다.
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP PUBLICATION IF EXISTS \"" + name + "\"");
            }

            boolean slotExists;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    slotExists = rs.next();
                }
            }
            if (slotExists) {
                try (PreparedStatement ps = conn.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
                    ps.setString(1, name);
                    ps.execute();
                }
            }
        } catch (SQLException ex) {
            // 의도적으로 무시 - 클래스 상단 설명 참고.
        }
    }
}
