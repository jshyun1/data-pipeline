package com.company.pipeline.airflowdashboard;

import com.company.pipeline.common.ApiResponse;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/airflow/alerts")
public class AirflowDagAlertController {

    private final AirflowDagMonitoringService monitoringService;

    public AirflowDagAlertController(AirflowDagMonitoringService monitoringService) {
        this.monitoringService = monitoringService;
    }

    @GetMapping
    public ApiResponse<List<Response>> active() {
        return ApiResponse.success(monitoringService.activeAlerts().stream().map(Response::from).toList());
    }

    @GetMapping("/history")
    public ApiResponse<List<Response>> history() {
        return ApiResponse.success(monitoringService.alertHistory().stream().map(Response::from).toList());
    }

    @PatchMapping("/{id}/acknowledge")
    public ApiResponse<Response> acknowledge(@PathVariable Long id) {
        return ApiResponse.success(Response.from(monitoringService.acknowledge(id)));
    }

    public record Response(
            Long id,
            String dagId,
            AirflowDagAlert.RuleType ruleType,
            AirflowDagAlert.Severity severity,
            AirflowDagAlert.Status status,
            String message,
            LocalDateTime detectedAt,
            LocalDateTime lastDetectedAt,
            LocalDateTime acknowledgedAt,
            LocalDateTime resolvedAt) {

        static Response from(AirflowDagAlert alert) {
            return new Response(
                    alert.getId(),
                    alert.getDagId(),
                    alert.getRuleType(),
                    alert.getSeverity(),
                    alert.getStatus(),
                    alert.getMessage(),
                    alert.getDetectedAt(),
                    alert.getLastDetectedAt(),
                    alert.getAcknowledgedAt(),
                    alert.getResolvedAt());
        }
    }
}
