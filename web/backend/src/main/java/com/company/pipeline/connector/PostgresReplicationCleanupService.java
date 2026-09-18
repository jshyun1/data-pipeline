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
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * 실행한다. 실패해도(권한 부족, 소스 DB 응답 없음 등) 예외를 던지지 않는다 - 파이프라인 삭제 자체를
 * 막을 이유는 없다.
 *
 * <p><b>남은 slot 은 무해하지 않다.</b> 예전 주석은 «이름만 안 겹치면 문제 없다»고 했지만, 논리 복제 slot 은
 * 소비자가 없어도 자기 위치 이후의 WAL 을 지우지 못하게 붙잡는다 - 남겨 두면 소스 DB 디스크가 계속 찬다.
 * 그래서 실패를 조용히 넘기지 않고 경고 로그에 slot 이름과 수동 정리 SQL 을 남긴다.
 *
 * <p><b>커넥터를 지운 직후엔 slot 이 아직 «사용 중»이다(2026-09-17 실측).</b> Kafka Connect 가 DELETE 에 204 를
 * 준 뒤에도 소스 태스크의 복제 연결(walsender)이 1~2초 남아 slot 을 잡고 있어, 곧바로 지우면
 * {@code replication slot "…" is active for PID …} 로 실패했다(파이프라인 8번 slot 이 이렇게 남았다).
 * 풀릴 때까지 기다렸다 지우고, 그래도 안 풀리면 잡고 있는 연결을 끊고 지운다 - slot 이름이 이 파이프라인
 * 전용({@link ConnectorNaming#postgresSlotAndPublicationName})이고 커넥터는 이미 지웠으므로 그 연결은
 * 지운 커넥터가 남긴 것뿐이다.
 */
@Component
public class PostgresReplicationCleanupService {

    private static final Logger log = LoggerFactory.getLogger(PostgresReplicationCleanupService.class);
    /** slot 이 풀리길 기다리는 최대 시간. 실측은 1~2초 - 넉넉히 둔다(파이프라인 삭제 응답이 그만큼 늦을 수 있다). */
    private static final Duration RELEASE_WAIT = Duration.ofSeconds(15);
    /** 연결을 끊은 뒤 slot 이 풀리길 기다리는 시간. */
    private static final Duration TERMINATE_WAIT = Duration.ofSeconds(5);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);

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

            SlotState state = slotState(conn, name);
            if (!state.exists()) {
                return;
            }
            if (state.activePid() != null) {
                state = awaitRelease(conn, name, RELEASE_WAIT);
            }
            if (state.exists() && state.activePid() != null) {
                log.warn("복제 slot {} 이 {}초 뒤에도 사용 중(PID {}) - 지운 커넥터가 남긴 연결로 보고 끊습니다 ({}:{}/{})",
                        name, RELEASE_WAIT.toSeconds(), state.activePid(),
                        sourceConnection.getHost(), sourceConnection.getPort(), sourceConnection.getDatabaseName());
                try (PreparedStatement ps = conn.prepareStatement("SELECT pg_terminate_backend(?)")) {
                    ps.setInt(1, state.activePid());
                    ps.execute();
                }
                state = awaitRelease(conn, name, TERMINATE_WAIT);
            }
            if (state.exists()) {
                try (PreparedStatement ps = conn.prepareStatement("SELECT pg_drop_replication_slot(?)")) {
                    ps.setString(1, name);
                    ps.execute();
                }
            }
        } catch (SQLException ex) {
            log.warn("복제 slot/publication {} 정리 실패 ({}:{}/{}): {} - 남은 slot 은 소스 DB 의 WAL 을 붙잡습니다. "
                            + "소스 DB 에서 확인 후 지우세요: SELECT pg_drop_replication_slot('{}')",
                    name, sourceConnection.getHost(), sourceConnection.getPort(), sourceConnection.getDatabaseName(),
                    ex.getMessage(), name);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("복제 slot {} 정리를 기다리다 중단됨 - 소스 DB 에서 남았는지 확인하세요: SELECT pg_drop_replication_slot('{}')",
                    name, name);
        }
    }

    /** slot 이 없어지거나 풀릴 때까지(최대 wait) 기다린 뒤의 상태. */
    private SlotState awaitRelease(Connection conn, String name, Duration wait) throws SQLException, InterruptedException {
        long deadline = System.nanoTime() + wait.toNanos();
        SlotState state = slotState(conn, name);
        while (state.exists() && state.activePid() != null && System.nanoTime() < deadline) {
            Thread.sleep(POLL_INTERVAL.toMillis());
            state = slotState(conn, name);
        }
        return state;
    }

    private SlotState slotState(Connection conn, String name) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return new SlotState(false, null);
                }
                int pid = rs.getInt(1);
                return new SlotState(true, rs.wasNull() ? null : pid);
            }
        }
    }

    /** @param activePid slot 을 잡고 있는 연결. 풀려 있으면 null */
    private record SlotState(boolean exists, Integer activePid) {
    }
}
