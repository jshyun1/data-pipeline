package com.company.pipeline.monitoring;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.monitoring.dto.CdcEventLogResponse;
import com.company.pipeline.monitoring.dto.CdcProcessingLogResponse;
import java.time.LocalDate;
import java.util.List;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * CDC 로그 화면(처리 이력 · 오류/상태 이력).
 *
 * <p>실패 데이터(DLQ) 조회 · 재처리 요청/승인 API 는 2026-09-17 에 뺐다. 싱크 실패 정책을 «버리고 계속»(errors.tolerance=all
 * + DLQ 토픽)에서 «멈춤»(none)으로 바꿔 DLQ 에 쌓일 것이 없어졌기 때문이다 - 거부된 건은 오프셋이 그 자리에 멈춰, 고치고
 * 재시작하면 그 건부터 다시 들어온다. 결정 경위: docs/2026-09-09-cdc-error-policy-dlq.md
 */
@RestController
@RequestMapping("/api/cdc/logs")
@com.company.pipeline.authz.RequirePermission(system = com.company.pipeline.authz.SystemCode.KAFKA)
public class CdcLogController {

    private final CdcLogService cdcLogService;

    public CdcLogController(CdcLogService cdcLogService) {
        this.cdcLogService = cdcLogService;
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
}
