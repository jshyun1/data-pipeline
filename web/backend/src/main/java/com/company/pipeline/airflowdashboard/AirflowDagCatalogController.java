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

    public AirflowDagCatalogController(
            AirflowDagCatalogRepository repository,
            AirflowDagMonitoringService monitoringService,
            AirflowDagCatalogSyncService syncService,
            AirflowDagCatalogDeletionService deletionService) {
        this.repository = repository;
        this.monitoringService = monitoringService;
        this.syncService = syncService;
        this.deletionService = deletionService;
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
            Integer slaMinutes) {

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
                    catalog.getSlaMinutes());
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
