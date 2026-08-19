package com.company.pipeline.pipeline;

import com.company.pipeline.common.entity.BaseAuditEntity;
import com.company.pipeline.connection.DbType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "pipeline_definition")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PipelineDefinition extends BaseAuditEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 150)
    private String name;

    // 설계서 §7.2: TABLE_CDC, LOG_FILE. 로그 파이프라인은 2차 기능이라 지금은 TABLE_CDC만 씀.
    @Column(name = "pipeline_type", nullable = false, length = 50)
    private String pipelineType;

    // LOG_FILE 파이프라인은 소스가 DB 연결이 아니라 파일이라 이 값이 NULL이다.
    @Column(name = "source_connection_id")
    private Long sourceConnectionId;

    // 랜딩 DB는 파이프라인 유형과 무관하게 항상 필요 - NULL 금지는 서비스 레이어에서 검증.
    @Column(name = "target_connection_id")
    private Long targetConnectionId;

    @Enumerated(EnumType.STRING)
    @Column(name = "source_db_type", length = 30)
    private DbType sourceDbType;

    @Enumerated(EnumType.STRING)
    @Column(name = "target_db_type", length = 30)
    private DbType targetDbType;

    @Column(name = "source_schema", length = 100)
    private String sourceSchema;

    @Column(name = "source_table", length = 100)
    private String sourceTable;

    @Column(name = "target_schema", length = 100)
    private String targetSchema;

    @Column(name = "target_table", length = 100)
    private String targetTable;

    // Kafka 토픽의 topic.prefix 부분. 실제 토픽명은 {topicName}.{sourceSchema}.{sourceTable}
    // (Debezium이 자동 생성) - ConnectorNaming.topicName()이 이 규칙을 구현한다.
    @Column(name = "topic_name", length = 200)
    private String topicName;

    @Enumerated(EnumType.STRING)
    @Column(length = 30)
    private PipelineStatus status = PipelineStatus.CREATED;

    @Column(name = "snapshot_mode", length = 50)
    private String snapshotMode;

    @Column(name = "excluded_columns", columnDefinition = "TEXT")
    private String excludedColumns;

    @Column(name = "insert_enabled")
    private Boolean insertEnabled = true;

    @Column(name = "update_enabled")
    private Boolean updateEnabled = true;

    @Column(name = "delete_enabled")
    private Boolean deleteEnabled = true;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(name = "created_by", length = 100)
    private String createdBy;

    public PipelineDefinition(String name, String pipelineType,
            Long sourceConnectionId, Long targetConnectionId,
            DbType sourceDbType, DbType targetDbType,
            String sourceSchema, String sourceTable,
            String targetSchema, String targetTable,
            String topicName, Boolean deleteEnabled, String description, String createdBy) {
        this.name = name;
        this.pipelineType = pipelineType;
        this.sourceConnectionId = sourceConnectionId;
        this.targetConnectionId = targetConnectionId;
        this.sourceDbType = sourceDbType;
        this.targetDbType = targetDbType;
        this.sourceSchema = sourceSchema;
        this.sourceTable = sourceTable;
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.topicName = topicName;
        this.deleteEnabled = deleteEnabled != null ? deleteEnabled : true;
        this.description = description;
        this.createdBy = createdBy;
        this.status = PipelineStatus.CREATED;
    }
}
