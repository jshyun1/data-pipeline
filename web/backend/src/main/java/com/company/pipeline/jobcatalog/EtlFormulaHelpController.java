package com.company.pipeline.jobcatalog;

import com.company.pipeline.authz.RequirePermission;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.jobcatalog.dto.CalciteSqlValidationRequest;
import com.company.pipeline.jobcatalog.dto.CalciteSqlValidationResponse;
import com.company.pipeline.jobcatalog.dto.EtlFormulaHelpResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/etl/formula-help")
@RequirePermission(system = SystemCode.NIFI)
public class EtlFormulaHelpController {

    private final EtlFormulaHelpRepository repository;
    private final ObjectMapper objectMapper;
    private final CalciteSqlValidationService validationService;

    public EtlFormulaHelpController(EtlFormulaHelpRepository repository, ObjectMapper objectMapper,
            CalciteSqlValidationService validationService) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.validationService = validationService;
    }

    @GetMapping
    public ApiResponse<List<EtlFormulaHelpResponse>> list() {
        return ApiResponse.success(repository.findByEnabledTrueOrderBySortOrderAscFunctionNameAsc().stream()
                .map(help -> EtlFormulaHelpResponse.from(help, objectMapper))
                .toList());
    }

    @PostMapping("/validate")
    public ApiResponse<CalciteSqlValidationResponse> validate(@Valid @RequestBody CalciteSqlValidationRequest request) {
        return ApiResponse.success(validationService.validate(request.sql()));
    }
}
