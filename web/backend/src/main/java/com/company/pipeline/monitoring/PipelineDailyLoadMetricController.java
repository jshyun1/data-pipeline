package com.company.pipeline.monitoring;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.monitoring.dto.DailyLoadIncrementRequest;
import com.company.pipeline.monitoring.dto.DailyLoadSummaryResponse;
import com.company.pipeline.monitoring.dto.HourlyLoadSummaryResponse;
import com.company.pipeline.monitoring.dto.RealtimePipelineMetricResponse;
import jakarta.validation.Valid;
import java.time.LocalDate;
import java.util.List;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 대시보드 "데이터 로드 현황" 섹션 전용. increment는 Kafka 스케줄러(내부 호출,
 * 이 컨트롤러 대신 서비스를 직접 씀)와 NiFi의 1분 주기 Airflow DAG(외부 HTTP 호출,
 * 다른 Airflow 제어 API들과 동일하게 permitAll)가 사용한다.
 */
@RestController
@RequestMapping("/api/metrics/daily-load")
public class PipelineDailyLoadMetricController {

    private final PipelineDailyLoadMetricService service;
    private final HourlyLoadMetricService hourlyLoadMetricService;
    private final RealtimePipelineMetricService realtimeMetricService;

    public PipelineDailyLoadMetricController(
            PipelineDailyLoadMetricService service,
            HourlyLoadMetricService hourlyLoadMetricService,
            RealtimePipelineMetricService realtimeMetricService) {
        this.service = service;
        this.hourlyLoadMetricService = hourlyLoadMetricService;
        this.realtimeMetricService = realtimeMetricService;
    }

    @PostMapping("/increment")
    public ApiResponse<Void> increment(@Valid @RequestBody DailyLoadIncrementRequest request) {
        service.incrementLoadedCount(
                request.pipelineSource(), request.pipelineKey(), request.taskKey(),
                request.pipelineLabel(), request.count());
        return ApiResponse.success(null);
    }

    @GetMapping("/summary")
    public ApiResponse<DailyLoadSummaryResponse> summary(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to,
            @RequestParam(required = false) String source) {
        return ApiResponse.success(service.getSummary(from, to, source));
    }

    /** 같은 조회 기간을 24시간 칸으로 접어 본 것. 일자별 차트와 짝으로 쓴다. */
    @GetMapping("/hourly")
    public ApiResponse<HourlyLoadSummaryResponse> hourly(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to,
            @RequestParam(required = false) String source) {
        return ApiResponse.success(hourlyLoadMetricService.getHourly(from, to, source));
    }

    @GetMapping("/realtime")
    public ApiResponse<List<RealtimePipelineMetricResponse>> realtime() {
        return ApiResponse.success(realtimeMetricService.getMetrics());
    }
}
