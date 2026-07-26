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

    @Column(name = "inserted_count", nullable = false)
    private Long insertedCount;

    @Column(name = "status", length = 20, nullable = false)
    private String status;

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
}
