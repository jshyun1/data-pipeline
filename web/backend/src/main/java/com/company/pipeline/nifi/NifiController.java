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
import com.company.pipeline.nifi.dto.NifiFileLoadCreateRequest;
import com.company.pipeline.nifi.dto.NifiFileUploadResponse;
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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api/nifi")
@RequirePermission(system = SystemCode.NIFI)
public class NifiController {

    private static final Path FILE_LOAD_SERVER_ROOT = Path.of("/opt/etl_repo/file");
    private static final String FILE_LOAD_NIFI_ROOT = "/opt/nifi/file";

    private final NifiClient nifiClient;
    private final NifiProcessGroupTreeService processGroupTreeService;
    private final NifiExecutionLogEntryRepository executionLogRepository;
    private final NifiProcessorRunRepository processorRunRepository;
    private final EtlJobRepository etlJobRepository;
    private final EtlJobStepRepository etlJobStepRepository;
    private final NifiProcessorEditLockService processorEditLockService;
    private final NifiProcessGroupMetadataService processGroupMetadataService;

    public NifiController(NifiClient nifiClient,
            NifiProcessGroupTreeService processGroupTreeService,
            NifiExecutionLogEntryRepository executionLogRepository,
            NifiProcessorRunRepository processorRunRepository,
            EtlJobRepository etlJobRepository,
            EtlJobStepRepository etlJobStepRepository,
            NifiProcessorEditLockService processorEditLockService,
            NifiProcessGroupMetadataService processGroupMetadataService) {
        this.nifiClient = nifiClient;
        this.processGroupTreeService = processGroupTreeService;
        this.executionLogRepository = executionLogRepository;
        this.processorRunRepository = processorRunRepository;
        this.etlJobRepository = etlJobRepository;
        this.etlJobStepRepository = etlJobStepRepository;
        this.processorEditLockService = processorEditLockService;
        this.processGroupMetadataService = processGroupMetadataService;
    }

    @PostMapping("/process-groups")
    public ApiResponse<NifiProcessGroupResponse> createProcessGroup(
            @Valid @RequestBody NifiProcessGroupCreateRequest request,
            @AuthenticationPrincipal AppUser user
    ) {
        NifiProcessGroupResponse response = nifiClient.createRootProcessGroup(request.name().trim());
        processGroupMetadataService.recordCreated(response.id(), response.name(), response.parentGroupId(), null, user);
        processGroupTreeService.refreshAfterMutation();
        return ApiResponse.success(response);
    }

    @PostMapping("/etl/initial-db-to-db")
    public ApiResponse<NifiProcessGroupResponse> createInitialDbToDbFlow(
            @Valid @RequestBody NifiInitialDbToDbCreateRequest request,
            @AuthenticationPrincipal AppUser user
    ) {
        NifiProcessGroupResponse response = nifiClient.createInitialDbToDbFlow(request);
        processGroupMetadataService.recordCreated(response.id(), response.name(), response.parentGroupId(),
                request.comments(), user);
        processGroupTreeService.refreshAfterMutation();
        return ApiResponse.success(response);
    }

    @PostMapping("/etl/file-load")
    public ApiResponse<NifiProcessGroupResponse> createFileLoadFlow(
            @Valid @RequestBody NifiFileLoadCreateRequest request,
            @AuthenticationPrincipal AppUser user
    ) {
        NifiProcessGroupResponse response = nifiClient.createFileLoadFlow(request);
        processGroupMetadataService.recordCreated(response.id(), response.name(), response.parentGroupId(),
                request.comments(), user);
        processGroupTreeService.refreshAfterMutation();
        return ApiResponse.success(response);
    }

    @PostMapping(path = "/etl/file-load/files", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ApiResponse<NifiFileUploadResponse> uploadFileLoadFiles(
            @RequestParam String jobName,
            @RequestParam String fileExtension,
            @RequestParam("files") List<MultipartFile> files,
            @AuthenticationPrincipal AppUser user
    ) {
        if (files == null || files.isEmpty() || files.stream().allMatch(MultipartFile::isEmpty)) {
            throw new NifiClientException("업로드할 파일을 선택하세요.", null);
        }
        String normalizedExtension = fileExtension == null ? "" : fileExtension.trim().toLowerCase();
        if (!List.of("csv", "excel").contains(normalizedExtension)) {
            throw new NifiClientException("파일적재는 csv, excel 파일만 선택할 수 있습니다.", null);
        }
        String actor = user == null || !hasText(user.getUserId()) ? "anonymous" : user.getUserId();
        String folderName = safePathPart(actor) + "_" + safePathPart(jobName);
        if (!hasText(folderName.replace("_", ""))) {
            throw new NifiClientException("작업명을 입력하세요.", null);
        }
        Path targetDir = FILE_LOAD_SERVER_ROOT.resolve(folderName).normalize();
        if (!targetDir.startsWith(FILE_LOAD_SERVER_ROOT)) {
            throw new NifiClientException("파일 저장 경로가 올바르지 않습니다.", null);
        }

        try {
            Files.createDirectories(targetDir);
            List<String> storedFiles = new ArrayList<>();
            Path firstStoredPath = null;
            for (MultipartFile file : files) {
                if (file.isEmpty()) {
                    continue;
                }
                if (!matchesExtension(file.getOriginalFilename(), normalizedExtension)) {
                    throw new NifiClientException("선택한 파일확장자와 다른 파일이 포함되어 있습니다: "
                            + file.getOriginalFilename(), null);
                }
                String storedName = safeFileName(file.getOriginalFilename());
                Path destination = targetDir.resolve(storedName).normalize();
                if (!destination.startsWith(targetDir)) {
                    throw new NifiClientException("파일명이 올바르지 않습니다: " + file.getOriginalFilename(), null);
                }
                Files.copy(file.getInputStream(), destination, StandardCopyOption.REPLACE_EXISTING);
                storedFiles.add(storedName);
                if (firstStoredPath == null) {
                    firstStoredPath = destination;
                }
            }
            if (firstStoredPath == null) {
                throw new NifiClientException("저장된 파일이 없습니다.", null);
            }
            List<String> columns = "csv".equals(normalizedExtension)
                    ? csvColumns(firstStoredPath)
                    : excelColumns(firstStoredPath);
            return ApiResponse.success(new NifiFileUploadResponse(
                    folderName,
                    targetDir.toString(),
                    FILE_LOAD_NIFI_ROOT + "/" + folderName,
                    storedFiles,
                    columns
            ));
        } catch (IOException ex) {
            throw new NifiClientException("파일 저장 실패: " + ex.getMessage(), ex);
        }
    }

    @GetMapping("/process-group-tree")
    public ApiResponse<NifiProcessGroupTreeResponse> processGroupTree() {
        return ApiResponse.success(processGroupTreeService.getTree());
    }

    @PostMapping("/process-group-tree/refresh")
    public ApiResponse<NifiProcessGroupTreeResponse> refreshProcessGroupTree() {
        return ApiResponse.success(processGroupTreeService.refreshNow());
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

    private static String safePathPart(String value) {
        return (value == null ? "" : value.trim())
                .replaceAll("[^A-Za-z0-9가-힣._-]", "_")
                .replaceAll("_+", "_");
    }

    private static String safeFileName(String value) {
        String fileName = Path.of(value == null ? "upload" : value).getFileName().toString();
        String sanitized = safePathPart(fileName);
        return hasText(sanitized) ? sanitized : "upload";
    }

    private static boolean matchesExtension(String fileName, String fileExtension) {
        String lower = fileName == null ? "" : fileName.toLowerCase();
        if ("csv".equals(fileExtension)) {
            return lower.endsWith(".csv");
        }
        return lower.endsWith(".xlsx") || lower.endsWith(".xls");
    }

    private static List<String> csvColumns(Path path) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(path), StandardCharsets.UTF_8))) {
            String header = reader.readLine();
            return splitCsvHeader(header);
        }
    }

    private static List<String> excelColumns(Path path) throws IOException {
        try (var input = Files.newInputStream(path); var workbook = WorkbookFactory.create(input)) {
            Row row = workbook.getSheetAt(0).getRow(0);
            if (row == null) {
                return List.of();
            }
            DataFormatter formatter = new DataFormatter();
            List<String> columns = new ArrayList<>();
            for (int index = 0; index < row.getLastCellNum(); index++) {
                String value = formatter.formatCellValue(row.getCell(index)).trim();
                if (hasText(value)) {
                    columns.add(value);
                }
            }
            return columns;
        }
    }

    private static List<String> splitCsvHeader(String header) {
        if (!hasText(header)) {
            return List.of();
        }
        List<String> columns = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int index = 0; index < header.length(); index++) {
            char ch = header.charAt(index);
            if (ch == '"') {
                quoted = !quoted;
            } else if (ch == ',' && !quoted) {
                addColumn(columns, current);
            } else {
                current.append(ch);
            }
        }
        addColumn(columns, current);
        return columns;
    }

    private static void addColumn(List<String> columns, StringBuilder current) {
        String column = current.toString().trim();
        if (hasText(column)) {
            columns.add(column);
        }
        current.setLength(0);
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
