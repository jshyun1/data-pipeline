package com.company.pipeline.connection;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.common.crypto.PasswordCryptoService;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.springframework.stereotype.Service;

/**
 * 파이프라인 생성 화면에서 소스/타겟 커넥션의 실제 스키마·테이블 목록을 조회한다.
 * 사용자가 스키마/테이블명을 자유 텍스트로 직접 입력하다 대소문자를 실제 카탈로그와
 * 다르게 입력해서 CDC 토픽이 어긋나는 문제(실제로 겪은 사례)를 원천적으로 없애기 위한
 * 것 - Fivetran/Airbyte 같은 도구들이 스키마/테이블을 드롭다운으로 고르게 하는 것과
 * 같은 패턴. 항상 라이브 커넥션으로 조회하고 캐시하지 않는다(POC 규모라 매번 조회해도
 * 충분히 빠르고, 캐시하면 스키마 변경이 반영 안 되는 문제가 생긴다).
 */
@Service
public class SchemaDiscoveryService {

    private static final Set<String> ORACLE_SYSTEM_SCHEMAS = Set.of(
            "SYS", "SYSTEM", "OUTLN", "XDB", "CTXSYS", "MDSYS", "WMSYS", "ORDSYS",
            "ORDDATA", "ORDPLUGINS", "LBACSYS", "APPQOSSYS", "GSMADMIN_INTERNAL",
            "DBSFWUSER", "DVSYS", "DVF", "AUDSYS", "REMOTE_SCHEDULER_AGENT",
            "GGSYS", "ANONYMOUS", "XS$NULL", "DIP");

    private static final Set<String> POSTGRES_SYSTEM_SCHEMAS = Set.of(
            "pg_catalog", "information_schema", "pg_toast");

    private final ConnectionRepository connectionRepository;
    private final PasswordCryptoService passwordCryptoService;

    public SchemaDiscoveryService(ConnectionRepository connectionRepository,
            PasswordCryptoService passwordCryptoService) {
        this.connectionRepository = connectionRepository;
        this.passwordCryptoService = passwordCryptoService;
    }

    public List<String> listSchemas(Long connectionId) {
        PipelineConnection connection = findOrThrow(connectionId);
        try (Connection jdbc = open(connection)) {
            List<String> schemas = new ArrayList<>();
            DatabaseMetaData meta = jdbc.getMetaData();
            try (ResultSet rs = meta.getSchemas()) {
                while (rs.next()) {
                    String schema = rs.getString("TABLE_SCHEM");
                    if (!isSystemSchema(connection.getDbType(), schema)) {
                        schemas.add(schema);
                    }
                }
            }
            schemas.sort(String::compareTo);
            return schemas;
        } catch (SQLException e) {
            throw new BusinessException(ErrorCode.SCHEMA_DISCOVERY_ERROR,
                    "스키마 목록을 조회할 수 없습니다(커넥션 정보를 확인하세요): " + e.getMessage());
        }
    }

    public List<String> listTables(Long connectionId, String schema) {
        PipelineConnection connection = findOrThrow(connectionId);
        try (Connection jdbc = open(connection)) {
            List<String> tables = new ArrayList<>();
            DatabaseMetaData meta = jdbc.getMetaData();
            try (ResultSet rs = meta.getTables(null, schema, "%", new String[] {"TABLE"})) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
            tables.sort(String::compareTo);
            return tables;
        } catch (SQLException e) {
            throw new BusinessException(ErrorCode.SCHEMA_DISCOVERY_ERROR,
                    "테이블 목록을 조회할 수 없습니다(커넥션 정보를 확인하세요): " + e.getMessage());
        }
    }

    /** 실제로 접속 가능한지 확인만 한다(성공/실패). ConnectionService가 이 결과로 status를 갱신한다. */
    public boolean testConnection(Long connectionId) {
        PipelineConnection connection = findOrThrow(connectionId);
        try (Connection jdbc = open(connection)) {
            return jdbc.isValid(5);
        } catch (SQLException e) {
            return false;
        }
    }

    /** 저장된 암호를 복호화해 제한된 메타데이터/검증 작업용 JDBC 연결을 연다. 호출자가 닫아야 한다. */
    public Connection openConnection(Long connectionId) throws SQLException {
        return open(findOrThrow(connectionId));
    }

    private boolean isSystemSchema(DbType dbType, String schema) {
        if (schema == null) {
            return true;
        }
        return switch (dbType) {
            case ORACLE -> ORACLE_SYSTEM_SCHEMAS.contains(schema.toUpperCase());
            case POSTGRESQL -> POSTGRES_SYSTEM_SCHEMAS.contains(schema.toLowerCase());
        };
    }

    Connection open(PipelineConnection connection) throws SQLException {
        String password = passwordCryptoService.decrypt(connection.getEncryptedPassword());
        return open(connection.getDbType(), connection.getHost(), connection.getPort(),
                connection.getDatabaseName(), connection.getServiceName(), connection.getUsername(), password);
    }

    Connection open(DbType dbType, String host, Integer port, String databaseName,
            String serviceName, String username, String password) throws SQLException {
        String url = switch (dbType) {
            case POSTGRESQL -> "jdbc:postgresql://%s:%d/%s".formatted(host, port, databaseName);
            case ORACLE -> "jdbc:oracle:thin:@%s:%d/%s".formatted(host, port, serviceName);
        };
        return DriverManager.getConnection(url, username, password);
    }

    private PipelineConnection findOrThrow(Long id) {
        return connectionRepository.findById(id)
                .orElseThrow(() -> new ConnectionNotFoundException(id));
    }
}
