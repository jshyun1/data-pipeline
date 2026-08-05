package com.company.pipeline.jobcatalog;

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
 * 잡 실행 1회. V18 마이그레이션.
 *
 * <p>NiFi에 실행 이력 개념이 없어서 관측으로 만든다 - 잡 소속 프로세서 중 하나라도 활동을
 * 시작하면 열고, 모두 조용해지면 닫는다({@link com.company.pipeline.monitoring.NifiProcessorRunTracker}의
 * 15초 폴링을 잡 단위로 확장). 그래서 시작/종료는 폴링 주기만큼 오차가 있는 추정값이다.
 *
 * <p>{@code airflowDagRunId}는 아직 채우지 않는다 - 백엔드에 Airflow API 클라이언트가 없다
 * (헬스 체크만 무인증으로 호출 중). 붙이면 "이 실행을 누가 지시했나"가 완성된다.
 */
@Entity
@Table(name = "etl_job_run")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EtlJobRun {

    public static final String STATUS_RUNNING = "RUNNING";
    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAILED = "FAILED";
    public static final String TRIGGER_OBSERVED = "OBSERVED";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_id", nullable = false)
    private Long jobId;

    @Column(name = "airflow_dag_run_id", length = 250)
    private String airflowDagRunId;

    @Column(name = "trigger_source", length = 20, nullable = false)
    private String triggerSource = TRIGGER_OBSERVED;

    @Column(name = "started_at", nullable = false)
    private LocalDateTime startedAt;

    @Column(name = "ended_at")
    private LocalDateTime endedAt;

    @Column(name = "last_seen_at", nullable = false)
    private LocalDateTime lastSeenAt;

    @Column(name = "status", length = 20, nullable = false)
    private String status;

    @Column(name = "step_run_count", nullable = false)
    private int stepRunCount;

    @Column(name = "total_inserted", nullable = false)
    private long totalInserted;

    @Column(name = "failed_step_count", nullable = false)
    private int failedStepCount;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public EtlJobRun(Long jobId, LocalDateTime startedAt) {
        this.jobId = jobId;
        this.startedAt = startedAt;
        this.lastSeenAt = startedAt;
        this.status = STATUS_RUNNING;
        this.triggerSource = TRIGGER_OBSERVED;
        this.createdAt = LocalDateTime.now();
        this.updatedAt = this.createdAt;
    }

    /** 활동 관측 - 마지막 관측 시각을 밀고 적재 건수를 누적한다. */
    public void observeActivity(LocalDateTime at, long insertedDelta) {
        if (at.isAfter(this.lastSeenAt)) {
            this.lastSeenAt = at;
        }
        if (insertedDelta > 0) {
            this.totalInserted += insertedDelta;
        }
        this.updatedAt = LocalDateTime.now();
    }

    /** 스텝 구간이 새로 열릴 때마다 1 증가. */
    public void addStepRun() {
        this.stepRunCount++;
        this.updatedAt = LocalDateTime.now();
    }

    /** 마지막 활동 시각으로 닫는다. 실행 중 ERROR가 있었으면 FAILED. */
    public void close(int failedStepCount) {
        this.endedAt = this.lastSeenAt;
        this.failedStepCount = failedStepCount;
        this.status = failedStepCount > 0 ? STATUS_FAILED : STATUS_SUCCESS;
        this.updatedAt = LocalDateTime.now();
    }
}
