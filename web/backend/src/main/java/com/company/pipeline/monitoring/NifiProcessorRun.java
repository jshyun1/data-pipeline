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

/**
 * NiFi 적재 프로세서의 "실행 구간" 1회. V15 마이그레이션.
 *
 * <p>{@link NifiExecutionLogEntry}가 "이 주기에 카운터가 늘었다"는 시점 기록이라면,
 * 이쪽은 시작~종료가 있는 구간 기록이다. 6분 걸린 적재가 60초 주기 때문에 7행으로
 * 흩어져서 소요시간도 처리량도 알 수 없던 문제를 풀기 위해 만들었다.
 *
 * <p>NiFi가 실행 구간을 알려주지 않아서(Provenance 0건, 프로세서 Status History 빈
 * 응답 - 둘 다 이 환경에서 확인됨) {@link NifiProcessorRunTracker}가 폴링으로 직접
 * 관측해 만든다. 그래서 started_at/ended_at은 폴링 주기만큼 오차가 있는 추정값이다.
 */
@Entity
@Table(name = "nifi_processor_run")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class NifiProcessorRun {

    public static final String STATUS_RUNNING = "RUNNING";
    public static final String STATUS_SUCCESS = "SUCCESS";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "processor_id", length = 100, nullable = false)
    private String processorId;

    @Column(name = "processor_name", length = 200, nullable = false)
    private String processorName;

    /** PutDatabaseRecord / ExecuteGroovyScript 등. 소급 병합한 과거 구간은 null. */
    @Column(name = "processor_type", length = 100)
    private String processorType;

    @Column(name = "group_id", length = 100)
    private String groupId;

    @Column(name = "group_name", length = 200)
    private String groupName;

    /** 적재 대상 schema.table. 스크립트 기반 적재는 알 수 없어 null. */
    @Column(name = "target_table", length = 200)
    private String targetTable;

    @Column(name = "started_at", nullable = false)
    private LocalDateTime startedAt;

    /** 진행 중이면 null. 유휴가 확인된 뒤 마지막 활동 관측 시각으로 채운다. */
    @Column(name = "ended_at")
    private LocalDateTime endedAt;

    @Column(name = "inserted_count", nullable = false)
    private long insertedCount;

    @Column(name = "status", length = 20, nullable = false)
    private String status;

    /** 마지막으로 "돌고 있다"를 관측한 시각. 종료 판정 근거이자 ended_at이 될 값. */
    @Column(name = "last_seen_at", nullable = false)
    private LocalDateTime lastSeenAt;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    public NifiProcessorRun(String processorId, String processorName, String processorType,
            String groupId, String groupName, String targetTable, LocalDateTime startedAt) {
        this.processorId = processorId;
        this.processorName = processorName;
        this.processorType = processorType;
        this.groupId = groupId;
        this.groupName = groupName;
        this.targetTable = targetTable;
        this.startedAt = startedAt;
        this.lastSeenAt = startedAt;
        this.insertedCount = 0L;
        this.status = STATUS_RUNNING;
        this.createdAt = LocalDateTime.now();
    }

    /** 이번 주기에도 돌고 있는 것을 봤다. 늘어난 적재 건수를 더하고 관측 시각을 갱신한다. */
    public void observeActivity(LocalDateTime seenAt, long deltaCount) {
        this.lastSeenAt = seenAt;
        if (deltaCount > 0) {
            this.insertedCount += deltaCount;
        }
    }

    /**
     * 구간을 닫는다. 종료 시각은 "지금"이 아니라 마지막으로 활동을 본 시각이다 -
     * 유휴를 몇 주기 확인한 뒤에야 닫기 때문에, 지금으로 잡으면 그 확인 시간이
     * 통째로 소요시간에 얹혀 처리량이 실제보다 낮게 나온다.
     */
    public void close() {
        this.endedAt = this.lastSeenAt;
        this.status = STATUS_SUCCESS;
    }

    /** 이름/유형처럼 나중에야 알게 되는 값을 채운다(캔버스에서 이름이 바뀐 경우 포함). */
    public void describe(String processorName, String processorType, String groupName, String targetTable) {
        if (processorName != null) {
            this.processorName = processorName;
        }
        if (processorType != null) {
            this.processorType = processorType;
        }
        if (groupName != null) {
            this.groupName = groupName;
        }
        if (targetTable != null) {
            this.targetTable = targetTable;
        }
    }
}
