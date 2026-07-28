package com.company.pipeline.monitoring;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** NiFi 적재 프로세서 실행 1회(카운터 증가분 감지 시점) 기록. V11 마이그레이션. */
@Entity
@Table(name = "nifi_execution_log")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class NifiExecutionLogEntry {

    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAILED = "FAILED";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "processor_id", length = 100, nullable = false)
    private String processorId;

    @Column(name = "processor_name", length = 200, nullable = false)
    private String processorName;

    @Column(name = "group_id", length = 100)
    private String groupId;

    @Column(name = "group_name", length = 200)
    private String groupName;

    @Column(name = "occurred_at", nullable = false)
    private LocalDateTime occurredAt;

    /** 적재 건수. 실패 행에는 셀 대상이 없으므로 null. */
    @Column(name = "inserted_count")
    private Long insertedCount;

    @Column(name = "status", length = 20, nullable = false)
    private String status;

    /** 실패 행의 bulletin 원문(어떤 예외였는지). 성공 행은 null. */
    @Column(name = "message", columnDefinition = "text")
    private String message;

    /** NiFi가 bulletin에 매기는 단조 증가 id. 중복 적재 방지 키. 성공 행은 null. */
    @Column(name = "bulletin_id")
    private Long bulletinId;

    /** bulletin 심각도(ERROR/WARNING). 성공 행은 null. */
    @Column(name = "level", length = 20)
    private String level;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    public NifiExecutionLogEntry(String processorId, String processorName, String groupId, String groupName,
            LocalDateTime occurredAt, Long insertedCount, String status) {
        this.processorId = processorId;
        this.processorName = processorName;
        this.groupId = groupId;
        this.groupName = groupName;
        this.occurredAt = occurredAt;
        this.insertedCount = insertedCount;
        this.status = status;
        this.createdAt = LocalDateTime.now();
    }

    /** NiFi bulletin에서 만든 실패 기록. */
    public static NifiExecutionLogEntry fromBulletin(String processorId, String processorName, String groupId,
            String groupName, LocalDateTime occurredAt, long bulletinId, String level, String message) {
        NifiExecutionLogEntry entry = new NifiExecutionLogEntry(
                processorId, processorName, groupId, groupName, occurredAt, null, STATUS_FAILED);
        entry.bulletinId = bulletinId;
        entry.level = level;
        entry.message = message;
        return entry;
    }
}
