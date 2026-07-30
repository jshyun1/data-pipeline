package com.company.pipeline.infra;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.infra.dto.HostResourceResponse;
import com.company.pipeline.infra.dto.ProcessHealthResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** 대시보드 인프라 구역(서버 리소스 / 주요 프로세스 현황) 전용 읽기 API. */
@RestController
@RequestMapping("/api/infra")
public class InfraController {

    private final HostResourceService hostResourceService;
    private final ProcessHealthService processHealthService;

    public InfraController(HostResourceService hostResourceService, ProcessHealthService processHealthService) {
        this.hostResourceService = hostResourceService;
        this.processHealthService = processHealthService;
    }

    @GetMapping("/resources")
    public ApiResponse<HostResourceResponse> resources() {
        return ApiResponse.success(hostResourceService.collect());
    }

    @GetMapping("/processes")
    public ApiResponse<ProcessHealthResponse> processes() {
        return ApiResponse.success(processHealthService.collect());
    }
}
