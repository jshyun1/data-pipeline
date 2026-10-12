package com.company.pipeline.pipeline;

import com.company.pipeline.authz.AccessBits;
import com.company.pipeline.authz.RequirePermission;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.pipeline.dto.PipelineResponse;
import com.company.pipeline.pipeline.dto.PipelineUpdateRequest;
import com.company.pipeline.user.AppUser;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 파이프라인 정의 수정 · «설정 변경 미반영» 조회(2026-10-12). 규칙은 {@link PipelineEditService} 에 있다.
 * 기존 {@link PipelineController} 와 같은 경로를 쓰되 따로 둔다 - 수정은 커넥터를 건드리지 않는 별도 흐름이다.
 */
@RestController
@RequestMapping("/api/pipelines")
@RequirePermission(system = SystemCode.KAFKA)
public class PipelineEditController {

    private final PipelineEditService editService;

    public PipelineEditController(PipelineEditService editService) {
        this.editService = editService;
    }

    /** 정의 값만 바꾼다. 등록된 커넥터는 그대로고, 바뀐 커넥터 설정은 다음 준비(배포) 때 들어간다. */
    @PutMapping("/{id}")
    @RequirePermission(system = SystemCode.KAFKA, bits = AccessBits.WRITE)
    public ApiResponse<PipelineResponse> update(@PathVariable Long id, @Valid @RequestBody PipelineUpdateRequest request,
                                                @AuthenticationPrincipal AppUser user) {
        return ApiResponse.success(editService.update(id, request, user == null ? "unknown" : user.getUserId()));
    }

    /** 파이프라인 id → 아직 커넥터에 안 들어간 항목(제외 컬럼 · 마스킹 컬럼 · DELETE 반영). 없는 파이프라인은 빠진다. */
    @GetMapping("/pending-config")
    public ApiResponse<Map<Long, List<String>>> pendingConfigAll() {
        return ApiResponse.success(editService.pendingConfigAll());
    }

    @GetMapping("/{id}/pending-config")
    public ApiResponse<List<String>> pendingConfig(@PathVariable Long id) {
        return ApiResponse.success(editService.pendingConfig(id));
    }
}
