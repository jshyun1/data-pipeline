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
 * NiFi 프로세스 그룹 하나 = 잡 하나. V17 마이그레이션.
 *
 * <p>원본은 NiFi 캔버스이고 이 엔티티는 {@link NifiJobMirrorService}가 5분마다 만드는
 * 사본이다. 여기 값을 바꿔도 캔버스에는 반영되지 않으며 다음 동기화에서 덮어써진다.
 *
 * <p>{@code nifiPgId}가 자연키다 - 캔버스에서 그룹 이름을 바꿔도 이력이 끊기지 않도록
 * 이름이 아니라 NiFi가 부여한 id로 잡을 식별한다(Airflow 동적 DAG가 dag_id에 그룹 id를
 * 쓰는 것과 같은 이유).
 */
@Entity
@Table(name = "etl_job")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EtlJob {

    public static final String ENGINE_NIFI = "NIFI";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "nifi_pg_id", length = 100, nullable = false)
    private String nifiPgId;

    @Column(name = "parent_pg_id", length = 100)
    private String parentPgId;

    @Column(name = "job_name", length = 200, nullable = false)
    private String jobName;

    @Column(name = "engine", length = 20, nullable = false)
    private String engine = ENGINE_NIFI;

    @Column(name = "comments")
    private String comments;

    @Column(name = "parameter_context_id", length = 100)
    private String parameterContextId;

    @Column(name = "parameter_context_name", length = 200)
    private String parameterContextName;

    @Column(name = "x_pos")
    private Double xPos;

    @Column(name = "y_pos")
    private Double yPos;

    @Column(name = "step_count", nullable = false)
    private int stepCount;

    @Column(name = "running_count", nullable = false)
    private int runningCount;

    @Column(name = "stopped_count", nullable = false)
    private int stoppedCount;

    @Column(name = "invalid_count", nullable = false)
    private int invalidCount;

    @Column(name = "airflow_dag_id", length = 200)
    private String airflowDagId;

    @Column(name = "first_seen_at", nullable = false)
    private LocalDateTime firstSeenAt;

    @Column(name = "last_synced_at", nullable = false)
    private LocalDateTime lastSyncedAt;

    @Column(name = "deleted_at")
    private LocalDateTime deletedAt;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public EtlJob(String nifiPgId, String jobName) {
        this.nifiPgId = nifiPgId;
        this.jobName = jobName;
        this.engine = ENGINE_NIFI;
        this.airflowDagId = airflowDagId(nifiPgId);
        LocalDateTime now = LocalDateTime.now();
        this.firstSeenAt = now;
        this.lastSyncedAt = now;
        this.createdAt = now;
        this.updatedAt = now;
    }

    /**
     * Airflow 동적 DAG(nifi_pipelines_dynamic.py)가 만드는 dag_id 규칙을 그대로 계산한다.
     * 그쪽이 그룹 id 앞 8자를 쓰므로 여기서도 같은 방식으로 맞춘다.
     */
    public static String airflowDagId(String nifiPgId) {
        if (nifiPgId == null || nifiPgId.length() < 8) {
            return null;
        }
        return "nifi_pipeline_" + nifiPgId.substring(0, 8) + "_control";
    }

    /** NiFi에서 읽어온 값으로 갱신. 변경이 있었으면 true를 돌려준다(스냅샷 판단에 쓰지 않고 로그용). */
    public boolean applySnapshot(String jobName, String parentPgId, String comments,
                                 String parameterContextId, String parameterContextName,
                                 Double xPos, Double yPos,
                                 int stepCount, int runningCount, int stoppedCount, int invalidCount) {
        boolean changed = !equalsNullable(this.jobName, jobName)
                || !equalsNullable(this.comments, comments)
                || this.stepCount != stepCount;
        this.jobName = jobName;
        this.parentPgId = parentPgId;
        this.comments = comments;
        this.parameterContextId = parameterContextId;
        this.parameterContextName = parameterContextName;
        this.xPos = xPos;
        this.yPos = yPos;
        this.stepCount = stepCount;
        this.runningCount = runningCount;
        this.stoppedCount = stoppedCount;
        this.invalidCount = invalidCount;
        this.airflowDagId = airflowDagId(this.nifiPgId);
        this.lastSyncedAt = LocalDateTime.now();
        this.updatedAt = this.lastSyncedAt;
        // 지웠던 잡이 다시 보이면 되살린다(캔버스 유실 후 복구된 경우).
        this.deletedAt = null;
        return changed;
    }

    public void markDeleted() {
        if (this.deletedAt == null) {
            this.deletedAt = LocalDateTime.now();
            this.updatedAt = this.deletedAt;
        }
    }

    private static boolean equalsNullable(String a, String b) {
        return a == null ? b == null : a.equals(b);
    }
}
