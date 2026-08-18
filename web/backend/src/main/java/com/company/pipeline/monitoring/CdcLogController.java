package com.company.pipeline.monitoring;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.monitoring.dto.CdcEventLogResponse;
import com.company.pipeline.monitoring.dto.CdcProcessingLogResponse;
import com.company.pipeline.monitoring.dto.DlqRecordDetailResponse;
import com.company.pipeline.monitoring.dto.DlqRecordResponse;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/cdc/logs")
public class CdcLogController {

    private final CdcLogService cdcLogService;
    private final DlqReadService dlqReadService;

    public CdcLogController(CdcLogService cdcLogService, DlqReadService dlqReadService) {
        this.cdcLogService = cdcLogService;
        this.dlqReadService = dlqReadService;
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
}
