package com.company.pipeline.nifi;

import com.company.pipeline.authz.RequirePermission;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobStep;
import com.company.pipeline.jobcatalog.EtlJobStepRepository;
import com.company.pipeline.monitoring.NifiExecutionLogEntry;
import com.company.pipeline.monitoring.NifiExecutionLogEntryRepository;
import com.company.pipeline.monitoring.NifiProcessorRunRepository;
import com.company.pipeline.nifi.dto.NifiExecutionLogResponse;
import com.company.pipeline.nifi.dto.NifiInitialDbToDbCreateRequest;
import com.company.pipeline.nifi.dto.NifiProcessorEditLockRequest;
import com.company.pipeline.nifi.dto.NifiProcessorEditLockResponse;
import com.company.pipeline.nifi.dto.NifiProcessGroupTreeResponse;
import com.company.pipeline.nifi.dto.NifiProcessGroupCreateRequest;
import com.company.pipeline.nifi.dto.NifiProcessGroupResponse;
import com.company.pipeline.nifi.dto.NifiProcessorDetailResponse;
import com.company.pipeline.nifi.dto.NifiProcessorRunResponse;
import com.company.pipeline.user.AppUser;
import jakarta.validation.Valid;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/nifi")
@RequirePermission(system = SystemCode.NIFI)
public class NifiController {

    private final NifiClient nifiClient;
    private final NifiProcessGroupTreeService processGroupTreeService;
    private final NifiExecutionLogEntryRepository executionLogRepository;
    private final NifiProcessorRunRepository processorRunRepository;
    private final EtlJobRepository etlJobRepository;
    private final EtlJobStepRepository etlJobStepRepository;
    private final NifiProcessorEditLockService processorEditLockService;

    public NifiController(NifiClient nifiClient,
            NifiProcessGroupTreeService processGroupTreeService,
            NifiExecutionLogEntryRepository executionLogRepository,
            NifiProcessorRunRepository processorRunRepository,
            EtlJobRepository etlJobRepository,
            EtlJobStepRepository etlJobStepRepository,
            NifiProcessorEditLockService processorEditLockService) {
        this.nifiClient = nifiClient;
        this.processGroupTreeService = processGroupTreeService;
        this.executionLogRepository = executionLogRepository;
        this.processorRunRepository = processorRunRepository;
        this.etlJobRepository = etlJobRepository;
        this.etlJobStepRepository = etlJobStepRepository;
        this.processorEditLockService = processorEditLockService;
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

    @GetMapping("/processors/{processorId}")
    public ApiResponse<NifiProcessorDetailResponse> processor(@PathVariable String processorId) {
        return ApiResponse.success(nifiClient.getProcessor(processorId));
    }

    @PostMapping("/processors/{processorId}/edit-lock")
    public ApiResponse<NifiProcessorEditLockResponse> acquireProcessorEditLock(
            @PathVariable String processorId,
            @Valid @RequestBody NifiProcessorEditLockRequest request,
            @AuthenticationPrincipal AppUser user) {
        return ApiResponse.success(processorEditLockService.acquire(processorId, request, user));
    }

    @PostMapping("/processors/{processorId}/edit-lock/heartbeat")
    public ApiResponse<NifiProcessorEditLockResponse> heartbeatProcessorEditLock(
            @PathVariable String processorId,
            @Valid @RequestBody NifiProcessorEditLockRequest request,
            @AuthenticationPrincipal AppUser user) {
        return ApiResponse.success(processorEditLockService.heartbeat(processorId, request, user));
    }

    @DeleteMapping("/processors/{processorId}/edit-lock")
    public ApiResponse<Void> releaseProcessorEditLock(
            @PathVariable String processorId,
            @Valid @RequestBody NifiProcessorEditLockRequest request,
            @AuthenticationPrincipal AppUser user) {
        processorEditLockService.release(processorId, request, user);
        return ApiResponse.success(null);
    }

    @GetMapping("/execution-logs")
    public ApiResponse<List<NifiExecutionLogResponse>> executionLogs(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to
    ) {
        var entries = executionLogRepository.findByOccurredAtBetweenOrderByOccurredAtDesc(
                from.atStartOfDay(), to.plusDays(1).atStartOfDay());
        var jobIds = entries.stream()
                .map(entry -> entry.getJobId())
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        var processorIds = entries.stream()
                .map(NifiExecutionLogEntry::getProcessorId)
                .filter(NifiController::hasText)
                .distinct()
                .toList();
        var groupIds = entries.stream()
                .map(NifiExecutionLogEntry::getGroupId)
                .filter(NifiController::hasText)
                .distinct()
                .toList();

        Map<String, EtlJobStep> stepsByProcessorId = etlJobStepRepository.findByNifiProcessorIdIn(processorIds).stream()
                .collect(Collectors.toMap(EtlJobStep::getNifiProcessorId, Function.identity(), (first, ignored) -> first));
        jobIds.addAll(stepsByProcessorId.values().stream()
                .map(EtlJobStep::getJobId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        Map<String, EtlJob> jobsByGroupId = etlJobRepository.findByNifiPgIdIn(groupIds).stream()
                .collect(Collectors.toMap(EtlJob::getNifiPgId, Function.identity(), (first, ignored) -> first));
        jobIds.addAll(jobsByGroupId.values().stream()
                .map(EtlJob::getId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        Map<Long, EtlJob> jobsById = etlJobRepository.findAllById(jobIds).stream()
                .collect(Collectors.toMap(EtlJob::getId, Function.identity(), (first, ignored) -> first));
        return ApiResponse.success(entries.stream()
                .map(entry -> NifiExecutionLogResponse.from(entry, resolveExecutionLogGroupName(
                        entry, jobsById, stepsByProcessorId, jobsByGroupId)))
                .toList());
    }

    private static String resolveExecutionLogGroupName(NifiExecutionLogEntry entry,
            Map<Long, EtlJob> jobsById,
            Map<String, EtlJobStep> stepsByProcessorId,
            Map<String, EtlJob> jobsByGroupId) {
        if (entry.getJobId() != null && jobsById.containsKey(entry.getJobId())) {
            return jobsById.get(entry.getJobId()).getJobName();
        }
        EtlJobStep step = stepsByProcessorId.get(entry.getProcessorId());
        if (step != null && jobsById.containsKey(step.getJobId())) {
            return jobsById.get(step.getJobId()).getJobName();
        }
        EtlJob groupJob = jobsByGroupId.get(entry.getGroupId());
        if (groupJob != null) {
            return groupJob.getJobName();
        }
        return hasText(entry.getGroupName()) ? entry.getGroupName() : null;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
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
