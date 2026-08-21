package com.company.pipeline.monitoring;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.monitoring.dto.CdcEventLogResponse;
import com.company.pipeline.monitoring.dto.CdcProcessingLogResponse;
import com.company.pipeline.monitoring.dto.DlqRecordDetailResponse;
import com.company.pipeline.monitoring.dto.DlqRecordResponse;
import com.company.pipeline.monitoring.dto.DlqReplayCreateRequest;
import com.company.pipeline.monitoring.dto.DlqReplayResponse;
import com.company.pipeline.user.AppUser;
import jakarta.validation.Valid;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.security.core.annotation.AuthenticationPrincipal;

@RestController
@RequestMapping("/api/cdc/logs")
@com.company.pipeline.authz.RequirePermission(system = com.company.pipeline.authz.SystemCode.KAFKA)
public class CdcLogController {

    private final CdcLogService cdcLogService;
    private final DlqReadService dlqReadService;
    private final DlqReplayService dlqReplayService;

    public CdcLogController(CdcLogService cdcLogService, DlqReadService dlqReadService,
            DlqReplayService dlqReplayService) {
        this.cdcLogService = cdcLogService;
        this.dlqReadService = dlqReadService;
        this.dlqReplayService = dlqReplayService;
    }

    @GetMapping("/processing")
    public ApiResponse<List<CdcProcessingLogResponse>> processingLogs(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to) {
        return ApiResponse.success(cdcLogService.processingLogs(from, to));
    }

    @GetMapping("/events")
    public ApiResponse<List<CdcEventLogResponse>> eventLogs(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to) {
        return ApiResponse.success(cdcLogService.eventLogs(from, to));
    }

    @GetMapping("/dlq")
    public ApiResponse<List<DlqRecordResponse>> dlqRecords(@RequestParam Long pipelineId,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {
        return ApiResponse.success(dlqReadService.list(pipelineId, from, to));
    }

    @GetMapping("/dlq/detail")
    public ApiResponse<DlqRecordDetailResponse> dlqDetail(@RequestParam Long pipelineId,
            @RequestParam int partition, @RequestParam long offset) {
        return ApiResponse.success(dlqReadService.detail(pipelineId, partition, offset));
    }

    @GetMapping("/dlq/replay-requests")
    public ApiResponse<List<DlqReplayResponse>> replayRequests(@AuthenticationPrincipal AppUser user) {
        return ApiResponse.success(dlqReplayService.history(user));
    }

    @PostMapping("/dlq/replay-requests")
    public ApiResponse<DlqReplayResponse> requestReplay(@Valid @RequestBody DlqReplayCreateRequest request,
            @AuthenticationPrincipal AppUser user) {
        return ApiResponse.success(dlqReplayService.request(request, user));
    }

    @PostMapping("/dlq/replay-requests/{id}/approve")
    public ApiResponse<DlqReplayResponse> approveReplay(@PathVariable Long id,
            @AuthenticationPrincipal AppUser user) {
        return ApiResponse.success(dlqReplayService.approve(id, user));
    }
}
