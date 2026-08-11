package com.company.pipeline.nifi;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.monitoring.NifiExecutionLogEntryRepository;
import com.company.pipeline.monitoring.NifiProcessorRunRepository;
import com.company.pipeline.nifi.dto.NifiExecutionLogResponse;
import com.company.pipeline.nifi.dto.NifiInitialDbToDbCreateRequest;
import com.company.pipeline.nifi.dto.NifiProcessGroupTreeResponse;
import com.company.pipeline.nifi.dto.NifiProcessGroupCreateRequest;
import com.company.pipeline.nifi.dto.NifiProcessGroupResponse;
import com.company.pipeline.nifi.dto.NifiProcessorRunResponse;
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

@RestController
@RequestMapping("/api/nifi")
public class NifiController {

    private final NifiClient nifiClient;
    private final NifiProcessGroupTreeService processGroupTreeService;
    private final NifiExecutionLogEntryRepository executionLogRepository;
    private final NifiProcessorRunRepository processorRunRepository;

    public NifiController(NifiClient nifiClient,
            NifiProcessGroupTreeService processGroupTreeService,
            NifiExecutionLogEntryRepository executionLogRepository,
            NifiProcessorRunRepository processorRunRepository) {
        this.nifiClient = nifiClient;
        this.processGroupTreeService = processGroupTreeService;
        this.executionLogRepository = executionLogRepository;
        this.processorRunRepository = processorRunRepository;
    }

    @PostMapping("/process-groups")
    public ApiResponse<NifiProcessGroupResponse> createProcessGroup(
            @Valid @RequestBody NifiProcessGroupCreateRequest request
    ) {
        return ApiResponse.success(nifiClient.createRootProcessGroup(request.name().trim()));
    }

    @PostMapping("/etl/initial-db-to-db")
    public ApiResponse<NifiProcessGroupResponse> createInitialDbToDbFlow(
            @Valid @RequestBody NifiInitialDbToDbCreateRequest request
    ) {
        return ApiResponse.success(nifiClient.createInitialDbToDbFlow(request));
    }

    @GetMapping("/process-group-tree")
    public ApiResponse<NifiProcessGroupTreeResponse> processGroupTree() {
        return ApiResponse.success(processGroupTreeService.getTree());
    }

    @GetMapping("/execution-logs")
    public ApiResponse<List<NifiExecutionLogResponse>> executionLogs(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to
    ) {
        var entries = executionLogRepository.findByOccurredAtBetweenOrderByOccurredAtDesc(
                from.atStartOfDay(), to.plusDays(1).atStartOfDay());
        return ApiResponse.success(entries.stream().map(NifiExecutionLogResponse::from).toList());
    }

    /**
     * ETL 로그 "처리 이력" - 적재 프로세서의 실행 구간(시작~종료~건수).
     *
     * <p>execution-logs가 "이 주기에 카운터가 늘었다"는 시점 기록이라 6분짜리 적재 하나가
     * 7행으로 흩어졌던 것을 구간 단위로 묶어 보여준다. 소요시간과 처리량은 여기서만 나온다.
     */
    @GetMapping("/processor-runs")
    public ApiResponse<List<NifiProcessorRunResponse>> processorRuns(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to
    ) {
        var runs = processorRunRepository.findByStartedAtBetweenOrderByStartedAtDesc(
                from.atStartOfDay(), to.plusDays(1).atStartOfDay());
        return ApiResponse.success(runs.stream().map(NifiProcessorRunResponse::from).toList());
    }
}
