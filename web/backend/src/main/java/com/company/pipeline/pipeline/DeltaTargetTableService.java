package com.company.pipeline.pipeline;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.connection.PipelineConnection;
import com.company.pipeline.connection.SchemaDiscoveryService;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 델타 파이프라인(DELTA_APPEND / DELTA_UPSERT)의 타깃 델타 테이블 준비.
 *
 * <p><b>DELTA_UPSERT</b>는 뼈대를 만들지 않는다. upsert 는 타깃에 소스 PK 와 같은 키가 있어야
 * 하는데(Postgres ON CONFLICT·MySQL ON DUPLICATE KEY 는 제약이 없으면 실패하거나 중복 삽입),
 * 소스 PK 컬럼을 앱이 타깃 DB 타입으로 옮겨 만드는 건 세 DB 간 타입 매핑 추측이 된다. 그래서
 * 테이블이 없으면 싱크(schema.evolution=basic, record_key)가 PK 포함해 만들게 두고(구분컬럼은
 * 뒤쪽에 붙음), 있으면 구분컬럼 존재 + <b>소스 PK 와 같은 PK/UNIQUE 존재</b>를 확인한다.
 * {@value PipelineLoadMode#DELTA_TS_COLUMN} 는 없으면 싱크가 첫 이벤트에서 ALTER ADD 한다.
 *
 * <p><b>DELTA_APPEND</b> 는 아래와 같다.
 *
 * <p>싱크(schema.evolution=basic)에 테이블 생성을 맡기면 구분컬럼이 <b>맨 뒤</b>에 붙는다
 * (ExtractNewRecordState 가 add.fields 를 레코드 끝에 추가하므로). 요구사항은 구분컬럼이
 * 맨 앞이라, 배포 시점에 <b>순번 컬럼 + 구분컬럼만 가진 뼈대</b>를 먼저 만든다. 소스 컬럼은
 * 첫 이벤트가 도착할 때 싱크가 자기 타입 매핑으로 ALTER ADD 한다 - 세 DB 간 타입 변환을
 * 여기서 흉내 내지 않기 위해서다(싱크가 만드는 타입과 어긋나면 적재가 깨진다).
 *
 * <p>순번 컬럼({@value #SEQUENCE_COLUMN})은 같은 행의 insert→update→delete 순서를 소비자가
 * 판단하는 근거다. 토픽이 단일 파티션이고 싱크 태스크가 하나라 삽입 순서 = 이벤트 순서다.
 *
 * <p>테이블이 이미 있으면(사용자가 직접 만든 경우) 구분컬럼이 있는지만 확인한다. 순번 컬럼은
 * 강제하지 않는다 - "구분컬럼 1 + 소스 컬럼 N" 모양을 정확히 맞추고 싶은 경우를 허용한다.
 *
 * <p>식별자는 인용하지 않는다. 싱크도 quote.identifiers=false 라 Oracle 은 대문자, Postgres 는
 * 소문자로 같은 규칙으로 접히므로, 여기서 만든 이름과 싱크가 찾는 이름이 일치한다.
 */
@Service
public class DeltaTargetTableService {

    private static final Logger log = LoggerFactory.getLogger(DeltaTargetTableService.class);

    public static final String SEQUENCE_COLUMN = "cdc_seq";

    /** 인용 없이 DDL 에 넣을 수 있는 이름만 허용(SQL 주입 방지 겸). */
    private static final Pattern IDENTIFIER = Pattern.compile("^[A-Za-z_][A-Za-z0-9_$]{0,127}$");

    private final SchemaDiscoveryService schemaDiscoveryService;

    public DeltaTargetTableService(SchemaDiscoveryService schemaDiscoveryService) {
        this.schemaDiscoveryService = schemaDiscoveryService;
    }

    public void ensureDeltaTable(PipelineDefinition pipeline, PipelineConnection target) {
        String schema = requireIdentifier("타깃 스키마", pipeline.getTargetSchema());
        String table = requireIdentifier("타깃 테이블", pipeline.getTargetTable());
        String opColumn = PipelineLoadMode.normalizeDeltaOpColumn(pipeline.getDeltaOpColumn());
        DbType dbType = target.getDbType();
        PipelineLoadMode loadMode = PipelineLoadMode.from(pipeline.getLoadMode());

        try (Connection connection = schemaDiscoveryService.openConnection(target.getId())) {
            Optional<Set<String>> existing = existingColumns(connection, dbType, schema, table);
            if (existing.isPresent()) {
                if (!existing.get().contains(opColumn.toLowerCase(Locale.ROOT))) {
                    throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                            "타깃 테이블 " + schema + "." + table + " 이(가) 이미 있는데 구분컬럼 '" + opColumn
                                    + "' 이 없습니다. 컬럼을 추가하거나 다른 테이블명을 쓰세요.");
                }
                if (loadMode == PipelineLoadMode.DELTA_UPSERT) {
                    verifyUpsertKey(connection, pipeline, dbType, schema, table);
                }
                log.info("델타 테이블 존재 확인 pipelineId={} mode={} table={}.{} opColumn={}",
                        pipeline.getId(), loadMode, schema, table, opColumn);
                return;
            }
            if (loadMode == PipelineLoadMode.DELTA_UPSERT) {
                log.info("델타(upsert) 테이블 없음 - 싱크가 PK 포함해 생성하도록 둠 pipelineId={} table={}.{}",
                        pipeline.getId(), schema, table);
                return;
            }
            String ddl = createTableDdl(dbType, schema, table, opColumn);
            try (Statement statement = connection.createStatement()) {
                statement.execute(ddl);
            }
            log.info("델타 테이블 뼈대 생성 pipelineId={} ddl={}", pipeline.getId(), ddl);
        } catch (SQLException exception) {
            throw new BusinessException(ErrorCode.SCHEMA_DISCOVERY_ERROR,
                    "델타 테이블 준비 실패(" + schema + "." + table + "): " + exception.getMessage());
        }
    }

    /**
     * DELTA_UPSERT: 타깃의 PK 또는 UNIQUE 인덱스 중 소스 PK 와 컬럼 집합이 같은 것이 있어야 한다.
     * 소스 PK 를 못 읽으면(권한 등) 검증을 건너뛰고 경고만 남긴다 - 싱크가 실패하면 그때 드러난다.
     */
    private void verifyUpsertKey(Connection connection, PipelineDefinition pipeline, DbType dbType,
            String schema, String table) throws SQLException {
        Set<String> sourceKey;
        try {
            sourceKey = schemaDiscoveryService.listColumns(pipeline.getSourceConnectionId(),
                            pipeline.getSourceSchema(), pipeline.getSourceTable()).stream()
                    .filter(column -> column.primaryKey())
                    .map(column -> column.name().toLowerCase(Locale.ROOT))
                    .collect(java.util.stream.Collectors.toSet());
        } catch (RuntimeException exception) {
            log.warn("소스 PK 조회 실패 - upsert 키 검증 생략 pipelineId={} cause={}", pipeline.getId(), exception.getMessage());
            return;
        }
        if (sourceKey.isEmpty()) {
            log.warn("소스 PK 없음 - upsert 키 검증 생략 pipelineId={}", pipeline.getId());
            return;
        }
        List<Set<String>> targetKeys = targetUniqueKeys(connection, dbType, schema, table);
        if (!keyMatches(sourceKey, targetKeys)) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "델타 최신상태(upsert) 적재는 타깃 테이블 " + schema + "." + table + " 에 소스 PK("
                            + String.join(", ", new java.util.TreeSet<>(sourceKey)) + ")와 같은 컬럼의 PK 또는 UNIQUE 제약이 있어야 합니다."
                            + (targetKeys.isEmpty() ? " 현재 키가 없습니다." : " 현재 키: " + targetKeys));
        }
    }

    /** 소스 PK 컬럼 집합(소문자)이 타깃 키 후보 중 하나와 정확히 같은지. */
    static boolean keyMatches(Set<String> sourceKey, List<Set<String>> targetKeys) {
        Set<String> wanted = sourceKey.stream().map(name -> name.toLowerCase(Locale.ROOT))
                .collect(java.util.stream.Collectors.toSet());
        return targetKeys.stream().anyMatch(key -> key.stream()
                .map(name -> name.toLowerCase(Locale.ROOT))
                .collect(java.util.stream.Collectors.toSet()).equals(wanted));
    }

    /** 타깃의 PK + UNIQUE 인덱스별 컬럼 집합(소문자). */
    private List<Set<String>> targetUniqueKeys(Connection connection, DbType dbType,
            String schema, String table) throws SQLException {
        String lookupSchema = lookupSchema(dbType, schema);
        String lookupTable = lookupTable(dbType, table);
        String catalog = dbType == DbType.MYSQL ? lookupSchema : null;
        String schemaPattern = dbType == DbType.MYSQL ? null : lookupSchema;
        java.util.Map<String, Set<String>> keys = new java.util.LinkedHashMap<>();
        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(catalog, schemaPattern, lookupTable)) {
            while (rs.next()) {
                keys.computeIfAbsent("PRIMARY KEY", k -> new HashSet<>())
                        .add(rs.getString("COLUMN_NAME").toLowerCase(Locale.ROOT));
            }
        }
        try (ResultSet rs = connection.getMetaData().getIndexInfo(catalog, schemaPattern, lookupTable, true, true)) {
            while (rs.next()) {
                String index = rs.getString("INDEX_NAME");
                String column = rs.getString("COLUMN_NAME");
                if (index == null || column == null) continue; // 통계 행(tableIndexStatistic)
                keys.computeIfAbsent(index, k -> new HashSet<>()).add(column.toLowerCase(Locale.ROOT));
            }
        }
        return new java.util.ArrayList<>(keys.values());
    }

    private static String lookupSchema(DbType dbType, String schema) {
        return switch (dbType) {
            case ORACLE -> schema.toUpperCase(Locale.ROOT);
            case POSTGRESQL -> schema.toLowerCase(Locale.ROOT);
            case MYSQL -> schema;
        };
    }

    private static String lookupTable(DbType dbType, String table) {
        return switch (dbType) {
            case ORACLE -> table.toUpperCase(Locale.ROOT);
            case POSTGRESQL -> table.toLowerCase(Locale.ROOT);
            case MYSQL -> table;
        };
    }

    /** 테이블이 없으면 empty, 있으면 소문자 컬럼명 집합. */
    private Optional<Set<String>> existingColumns(Connection connection, DbType dbType,
            String schema, String table) throws SQLException {
        String lookupSchema = lookupSchema(dbType, schema);
        String lookupTable = lookupTable(dbType, table);
        String catalog = dbType == DbType.MYSQL ? lookupSchema : null;
        String schemaPattern = dbType == DbType.MYSQL ? null : lookupSchema;
        Set<String> columns = new HashSet<>();
        try (ResultSet rs = connection.getMetaData().getColumns(catalog, schemaPattern, lookupTable, "%")) {
            while (rs.next()) columns.add(rs.getString("COLUMN_NAME").toLowerCase(Locale.ROOT));
        }
        return columns.isEmpty() ? Optional.empty() : Optional.of(columns);
    }

    static String createTableDdl(DbType dbType, String schema, String table, String opColumn) {
        String qualified = schema + "." + table;
        return switch (dbType) {
            case POSTGRESQL -> "CREATE TABLE " + qualified + " (" + SEQUENCE_COLUMN + " BIGSERIAL PRIMARY KEY, "
                    + opColumn + " VARCHAR(10) NOT NULL)";
            case ORACLE -> "CREATE TABLE " + qualified + " (" + SEQUENCE_COLUMN
                    + " NUMBER(19) GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
                    + opColumn + " VARCHAR2(10) NOT NULL)";
            case MYSQL -> "CREATE TABLE " + qualified + " (" + SEQUENCE_COLUMN
                    + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
                    + opColumn + " VARCHAR(10) NOT NULL)";
        };
    }

    private static String requireIdentifier(String label, String value) {
        if (value == null || !IDENTIFIER.matcher(value.trim()).matches()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    label + "명은 영문·숫자·_ 조합이어야 합니다: " + value);
        }
        return value.trim();
    }
}
