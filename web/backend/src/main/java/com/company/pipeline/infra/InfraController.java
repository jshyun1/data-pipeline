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
    private final DiskBreakdownService diskBreakdownService;

    public InfraController(HostResourceService hostResourceService, ProcessHealthService processHealthService,
                           DiskBreakdownService diskBreakdownService) {
        this.hostResourceService = hostResourceService;
        this.processHealthService = processHealthService;
        this.diskBreakdownService = diskBreakdownService;
    }

    @GetMapping("/resources")
    public ApiResponse<HostResourceResponse> resources() {
        return ApiResponse.success(hostResourceService.collect());
    }

    @GetMapping("/processes")
    public ApiResponse<ProcessHealthResponse> processes() {
        return ApiResponse.success(processHealthService.collect());
    }

    /**
     * 디스크 용도별 사용량(원본 5-3 #12). 5분 주기로 백그라운드에서 계산된 캐시를 그대로 내린다.
     * 마운트를 안 걸어둔 환경에서는 빈 목록이 오고, 화면은 그 구역을 통째로 감춘다.
     */
    @GetMapping("/resources/breakdown")
    public ApiResponse<DiskBreakdownService.Snapshot> breakdown() {
        return ApiResponse.success(diskBreakdownService.current());
    }

    /**
     * 메모리 용도별 상세. 디스크와 달리 /proc/meminfo 한 번 읽는 비용이라 캐시 없이 즉시 계산한다.
     * 화면은 "상세 펼치기"를 눌렀을 때만 호출한다.
     */
    @GetMapping("/resources/memory-breakdown")
    public ApiResponse<java.util.List<java.util.Map<String, Object>>> memoryBreakdown() {
        return ApiResponse.success(hostResourceService.memoryBreakdown());
    }
}
