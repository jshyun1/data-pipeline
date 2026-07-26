package com.company.pipeline.nifi;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.monitoring.NifiExecutionLogEntryRepository;
import com.company.pipeline.nifi.dto.NifiExecutionLogResponse;
import com.company.pipeline.nifi.dto.NifiProcessGroupCreateRequest;
import com.company.pipeline.nifi.dto.NifiProcessGroupResponse;
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
    private final NifiExecutionLogEntryRepository executionLogRepository;

    public NifiController(NifiClient nifiClient, NifiExecutionLogEntryRepository executionLogRepository) {
        this.nifiClient = nifiClient;
        this.executionLogRepository = executionLogRepository;
    }

    @PostMapping("/process-groups")
    public ApiResponse<NifiProcessGroupResponse> createProcessGroup(
            @Valid @RequestBody NifiProcessGroupCreateRequest request
    ) {
        return ApiResponse.success(nifiClient.createRootProcessGroup(request.name().trim()));
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
}
