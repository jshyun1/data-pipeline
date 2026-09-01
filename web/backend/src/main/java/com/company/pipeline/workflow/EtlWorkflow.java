package com.company.pipeline.workflow;

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
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * 워크플로우 1개 = Airflow DAG 1개 = 스케줄 1개. V57 마이그레이션.
 *
 * <p>NiFi 캔버스에는 job(프로세스 그룹)만 그리고, "그 job들을 어떤 순서로 돌릴지"는 이쪽이
 * 유일한 원장이다. 예전에는 NiFi 출력포트 연결로 순서를 추론했는데 화면과 실제가 어긋나기
 * 쉬웠다.
 *
 * <p>{@code publishedSpec}이 채워진 것만 Airflow 팩토리가 읽는다 - 캔버스를 저장(draft)해도
 * 게시하기 전에는 DAG가 바뀌지 않는다. 반쯤 그린 그래프가 운영으로 새지 않게 하는 장치다.
 */
@Entity
@Table(name = "etl_workflow")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EtlWorkflow {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "workflow_key", length = 80, nullable = false)
    private String workflowKey;

    @Column(name = "name", length = 200, nullable = false)
    private String name;

    @Column(name = "description")
    private String description;

    @Column(name = "nifi_group_pg_id", length = 100)
    private String nifiGroupPgId;

    @Column(name = "schedule_cron", length = 120)
    private String scheduleCron;

    @Column(name = "timezone", length = 64, nullable = false)
    private String timezone;

    @Column(name = "catchup", nullable = false)
    private boolean catchup;

    @Column(name = "max_active_runs", nullable = false)
    private int maxActiveRuns;

    /** 캔버스·속성창에서 같이 고치는 메모. 그림만으로는 안 남는 맥락을 적어둔다. */
    @Column(name = "memo")
    private String memo;

    @Column(name = "suspend_on_error", nullable = false)
    private boolean suspendOnError;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "published_spec", columnDefinition = "jsonb")
    private String publishedSpec;

    @Column(name = "published_at")
    private LocalDateTime publishedAt;

    @Column(name = "published_by", length = 100)
    private String publishedBy;

    /** 이 워크플로우가 끝나면 갱신됐다고 알릴 Asset. 게시 시 시스템이 채운다. */
    @Column(name = "produces_asset_uri", length = 300)
    private String producesAssetUri;

    /** 선행 워크플로우 id 목록(JSON 배열). 비어 있으면 스케줄/수동으로만 실행된다. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "upstream_workflow_ids", columnDefinition = "jsonb")
    private String upstreamWorkflowIds;

    /** ALL=선행이 모두 끝나야, ANY=하나라도 끝나면. */
    @Column(name = "upstream_mode", length = 10, nullable = false)
    private String upstreamMode;

    @Column(name = "dag_id_override", length = 200)
    private String dagIdOverride;

    @Column(name = "deleted_at")
    private LocalDateTime deletedAt;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public EtlWorkflow(String workflowKey, String name) {
        LocalDateTime now = LocalDateTime.now();
        this.workflowKey = workflowKey;
        this.name = name;
        this.timezone = "Asia/Seoul";
        this.catchup = false;
        this.maxActiveRuns = 1;
        this.suspendOnError = true;
        this.upstreamMode = "ALL";
        this.createdAt = now;
        this.updatedAt = now;
    }

    /** 화면에서 고칠 수 있는 속성만 받는다. workflowKey는 dag_id의 축이라 바꾸지 않는다. */
    public void updateSettings(String name, String description, String nifiGroupPgId,
                               String scheduleCron, String timezone, Boolean catchup,
                               Integer maxActiveRuns, Boolean suspendOnError) {
        updateSettings(name, description, nifiGroupPgId, scheduleCron, timezone, catchup,
                maxActiveRuns, suspendOnError, null, null);
    }

    public void updateSettings(String name, String description, String nifiGroupPgId,
                               String scheduleCron, String timezone, Boolean catchup,
                               Integer maxActiveRuns, Boolean suspendOnError,
                               String upstreamWorkflowIds, String upstreamMode) {
        updateSettings(name, description, nifiGroupPgId, scheduleCron, timezone, catchup,
                maxActiveRuns, suspendOnError, upstreamWorkflowIds, upstreamMode, null);
    }

    public void updateSettings(String name, String description, String nifiGroupPgId,
                               String scheduleCron, String timezone, Boolean catchup,
                               Integer maxActiveRuns, Boolean suspendOnError,
                               String upstreamWorkflowIds, String upstreamMode, String memo) {
        if (memo != null) {
            this.memo = memo;
        }
        this.upstreamWorkflowIds = upstreamWorkflowIds;
        if (upstreamMode != null && !upstreamMode.isBlank()) {
            this.upstreamMode = upstreamMode;
        }
        if (name != null) {
            this.name = name;
        }
        this.description = description;
        this.nifiGroupPgId = nifiGroupPgId;
        this.scheduleCron = scheduleCron;
        if (timezone != null) {
            this.timezone = timezone;
        }
        if (catchup != null) {
            this.catchup = catchup;
        }
        if (maxActiveRuns != null) {
            this.maxActiveRuns = maxActiveRuns;
        }
        if (suspendOnError != null) {
            this.suspendOnError = suspendOnError;
        }
        this.updatedAt = LocalDateTime.now();
    }

    /** 게시할 때 이 워크플로우의 산출 Asset을 확정한다. key가 불변이라 URI도 불변이다. */
    public void ensureAssetUri() {
        if (this.producesAssetUri == null || this.producesAssetUri.isBlank()) {
            this.producesAssetUri = "cerebro://etl/" + this.workflowKey;
        }
    }

    public void markPublished(String spec, String by) {
        this.publishedSpec = spec;
        this.publishedAt = LocalDateTime.now();
        this.publishedBy = by;
        this.updatedAt = this.publishedAt;
    }

    /** 게시를 내린다. 팩토리가 다음 파싱 주기에 DAG를 거둬간다. */
    public void markUnpublished() {
        this.publishedSpec = null;
        this.publishedAt = null;
        this.publishedBy = null;
        this.updatedAt = LocalDateTime.now();
    }

    public void softDelete() {
        if (this.deletedAt == null) {
            this.deletedAt = LocalDateTime.now();
            this.updatedAt = this.deletedAt;
        }
    }

    public boolean isPublished() {
        return this.publishedAt != null;
    }

    /** dag_id는 불변이어야 실행 이력이 이어진다. 레거시 승계가 필요할 때만 override를 쓴다. */
    public String dagId() {
        return this.dagIdOverride != null && !this.dagIdOverride.isBlank()
                ? this.dagIdOverride
                : "etl_wf_" + this.workflowKey;
    }

    public void touch() {
        this.updatedAt = LocalDateTime.now();
    }
}
