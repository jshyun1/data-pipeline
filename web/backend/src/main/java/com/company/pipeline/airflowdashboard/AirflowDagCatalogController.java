package com.company.pipeline.airflowdashboard;

import com.company.pipeline.common.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.List;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/airflow/dag-catalog")
@com.company.pipeline.authz.RequirePermission(system = com.company.pipeline.authz.SystemCode.AIRFLOW)
public class AirflowDagCatalogController {

    private final AirflowDagCatalogRepository repository;
    private final AirflowDagMonitoringService monitoringService;
    private final AirflowDagCatalogSyncService syncService;
    private final AirflowDagCatalogDeletionService deletionService;
    private final AirflowDagRunClient dagRunClient;

    public AirflowDagCatalogController(
            AirflowDagCatalogRepository repository,
            AirflowDagMonitoringService monitoringService,
            AirflowDagCatalogSyncService syncService,
            AirflowDagCatalogDeletionService deletionService,
            AirflowDagRunClient dagRunClient) {
        this.repository = repository;
        this.monitoringService = monitoringService;
        this.syncService = syncService;
        this.deletionService = deletionService;
        this.dagRunClient = dagRunClient;
    }

    @GetMapping
    public ApiResponse<List<Response>> list() {
        return ApiResponse.success(repository
                .findByEnabledTrueOrderByBusinessGroupAscBusinessFolderAscSortOrderAscDisplayNameAsc()
                .stream().map(Response::from).toList());
    }

    @PostMapping("/sync")
    public ApiResponse<AirflowDagCatalogSyncService.SyncResult> synchronize() {
        return ApiResponse.success(syncService.synchronize());
    }

    @GetMapping("/{dagId}")
    public ApiResponse<Response> detail(@PathVariable String dagId) {
        return ApiResponse.success(repository.findByDagId(dagId)
                .map(Response::from)
                .orElseThrow(() -> new com.company.pipeline.common.BusinessException(
                        com.company.pipeline.common.ErrorCode.VALIDATION_ERROR,
                        "DAG를 찾을 수 없습니다: " + dagId)));
    }

    @DeleteMapping("/{dagId}")
    public ApiResponse<Void> delete(@PathVariable String dagId) {
        deletionService.delete(dagId);
        return ApiResponse.success(null);
    }

    /**
     * DAG 수동 실행. 브라우저가 Airflow 에 직접 쏘던 것을 pipeline-api 경유로 바꿔서, "누가 어떤 DAG 를
     * 실행했는지"가 감사 로그(AIRFLOW_CREATE + 대상 URI + 수행자)에 자동으로 남게 한다.
     */
    @PostMapping("/{dagId}/run")
    public ApiResponse<AirflowDagRunClient.DagRun> run(
            @PathVariable String dagId,
            @RequestBody(required = false) java.util.Map<String, Object> conf) {
        return ApiResponse.success(dagRunClient.triggerDag(dagId, conf));
    }

    /**
     * DAG 스케줄 저장(NiFi 생성 DAG 한정). schedule Variable 을 설정/삭제하고 auto_stop 을 켠다.
     * pipeline-api 경유라 "누가 스케줄을 바꿨는지"가 감사 로그(AIRFLOW_UPDATE)에 남는다.
     */
    @PutMapping("/{dagId}/schedule")
    public ApiResponse<Void> saveSchedule(@PathVariable String dagId, @RequestBody ScheduleRequest request) {
        // 워크플로우 DAG의 스케줄은 캔버스(etl_workflow.schedule_cron)가 단일 소스다.
        // 여기서 Variable을 덮어쓰면 게시할 때마다 되돌아가 두 값이 싸운다.
        if (dagId.startsWith("etl_wf_")) {
            throw new com.company.pipeline.common.BusinessException(
                    com.company.pipeline.common.ErrorCode.VALIDATION_ERROR,
                    "워크플로우 DAG의 스케줄은 워크플로우 > 설계 화면에서 변경해주세요.");
        }
        if (!dagId.matches("(?i)nifi_pipeline_[a-z0-9]{8}_control")) {
            throw new com.company.pipeline.common.BusinessException(
                    com.company.pipeline.common.ErrorCode.VALIDATION_ERROR,
                    "NiFi에서 생성된 DAG만 스케줄을 변경할 수 있습니다.");
        }
        String scheduleKey = dagId + "__schedule";
        if (request != null && request.cron() != null && !request.cron().isBlank()) {
            dagRunClient.upsertVariable(scheduleKey, request.cron().trim());
        } else {
            dagRunClient.deleteVariable(scheduleKey);
        }
        dagRunClient.upsertVariable(dagId + "__auto_stop_after_run", "true");
        return ApiResponse.success(null);
    }

    public record ScheduleRequest(String cron) {
    }

    @GetMapping("/{dagId}/monitoring")
    public ApiResponse<MonitoringResponse> monitoring(@PathVariable String dagId) {
        return ApiResponse.success(MonitoringResponse.from(monitoringService.getCatalog(dagId)));
    }

    @PatchMapping("/{dagId}/monitoring")
    public ApiResponse<MonitoringResponse> updateMonitoring(
            @PathVariable String dagId,
            @Valid @RequestBody MonitoringRequest request) {
        return ApiResponse.success(MonitoringResponse.from(monitoringService.updateSettings(
                dagId,
                request.monitoringEnabled(),
                request.consecutiveFailureThreshold(),
                request.staleDaysThreshold(),
                request.durationMultiplier(),
                request.slaMinutes())));
    }

    public record Response(
            String dagId,
            String businessGroup,
            String businessFolder,
            String displayName,
            String description,
            boolean monitoringEnabled,
            int consecutiveFailureThreshold,
            int staleDaysThreshold,
            BigDecimal durationMultiplier,
            Integer slaMinutes,
            /** 카탈로그에 처음 잡힌 시각. 실행 현황이 "오늘 새로 생긴 DAG"를 세는 근거다. */
            java.time.LocalDateTime createdAt) {

        static Response from(AirflowDagCatalog catalog) {
            return new Response(
                    catalog.getDagId(),
                    catalog.getBusinessGroup(),
                    catalog.getBusinessFolder(),
                    catalog.getDisplayName(),
                    catalog.getDescription(),
                    catalog.isMonitoringEnabled(),
                    catalog.getConsecutiveFailureThreshold(),
                    catalog.getStaleDaysThreshold(),
                    catalog.getDurationMultiplier(),
                    catalog.getSlaMinutes(),
                    catalog.getCreatedAt());
        }
    }

    public record MonitoringRequest(
            @NotNull Boolean monitoringEnabled,
            @NotNull @Min(1) Integer consecutiveFailureThreshold,
            @NotNull @Min(1) Integer staleDaysThreshold,
            @NotNull @DecimalMin("1.0") BigDecimal durationMultiplier,
            @Min(1) Integer slaMinutes) {
    }

    public record MonitoringResponse(
            String dagId,
            boolean monitoringEnabled,
            int consecutiveFailureThreshold,
            int staleDaysThreshold,
            BigDecimal durationMultiplier,
            Integer slaMinutes) {

        static MonitoringResponse from(AirflowDagCatalog catalog) {
            return new MonitoringResponse(
                    catalog.getDagId(),
                    catalog.isMonitoringEnabled(),
                    catalog.getConsecutiveFailureThreshold(),
                    catalog.getStaleDaysThreshold(),
                    catalog.getDurationMultiplier(),
                    catalog.getSlaMinutes());
        }
    }
}
