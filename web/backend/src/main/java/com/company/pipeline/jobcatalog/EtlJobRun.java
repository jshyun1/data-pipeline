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
    public static final String TRIGGER_WORKFLOW = "WORKFLOW";
    /** 완료를 어떻게 알았는가. CALLBACK=확정, OBSERVED=추정, TIMEOUT=포기. */
    public static final String SOURCE_CALLBACK = "CALLBACK";
    public static final String SOURCE_OBSERVED = "OBSERVED";

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

    @Column(name = "run_token", length = 64)
    private String runToken;

    @Column(name = "workflow_key", length = 80)
    private String workflowKey;

    @Column(name = "node_key", length = 80)
    private String nodeKey;

    @Column(name = "airflow_task_id", length = 250)
    private String airflowTaskId;

    @Column(name = "completion_source", length = 20)
    private String completionSource;

    @Column(name = "rows_processed")
    private Long rowsProcessed;

    @Column(name = "error_message")
    private String errorMessage;

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

    /**
     * Airflow가 이 실행을 지시했음을 표시한다(입양 포함).
     *
     * <p>관측기가 먼저 열어둔 run일 수도 있어서 새로 만들지 않고 표시만 덧붙인다 -
     * 그래야 한 실행이 두 행으로 갈라지지 않는다.
     */
    public void adoptByWorkflow(String runToken, String workflowKey, String nodeKey,
                                String dagRunId, String taskId) {
        this.runToken = runToken;
        this.workflowKey = workflowKey;
        this.nodeKey = nodeKey;
        this.airflowDagRunId = dagRunId;
        this.airflowTaskId = taskId;
        this.triggerSource = TRIGGER_WORKFLOW;
        this.updatedAt = LocalDateTime.now();
    }

    /** 완료 보고로 실행을 닫는다. 이미 닫혔으면 아무것도 하지 않는다(콜백 멱등). */
    public void completeBy(String source, String status, Long rows, String errorMessage,
                           LocalDateTime at) {
        // 이미 «판정»이 있으면 덮어쓰지 않는다(콜백 재전송 대비).
        //
        // 다만 유휴 정리(EtlJobRunService.closeIdleRuns)가 닫은 행은 결과를 아는 주체가 아니라
        // completion_source가 비어 있다. 그 위에는 실제 판정을 덮어써야 한다. 실제로 NiFi
        // 접속 실패로 끝난 실행을 유휴 정리가 먼저 SUCCESS로 닫아서, 뒤늦게 온 실패 보고가
        // 이 가드에 막혀 원장에 성공으로 남아 있었다(2026-08-31).
        if (this.endedAt != null && this.completionSource != null) {
            return;
        }
        this.endedAt = at;
        this.lastSeenAt = at;
        this.status = status;
        this.completionSource = source;
        this.rowsProcessed = rows;
        this.errorMessage = errorMessage;
        this.updatedAt = at;
    }
}
