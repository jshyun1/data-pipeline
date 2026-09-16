package com.company.pipeline.llm;

import com.company.pipeline.authz.RequirePermission;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.llm.dto.LlmStatusResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * GPT 기능 사용 가능 여부. 설정은 ADMIN 이지만 사용은 «그 화면을 볼 수 있는 사용자 누구나»라,
 * 이 조회는 로그인 사용자면 부를 수 있어야 한다(COMMON). 응답에 키·주소는 없다.
 */
@RestController
@RequirePermission(system = SystemCode.COMMON)
public class LlmStatusController {

    private final LlmProviderService service;

    public LlmStatusController(LlmProviderService service) {
        this.service = service;
    }

    @GetMapping("/api/llm/status")
    public ApiResponse<LlmStatusResponse> status() {
        return ApiResponse.success(service.status());
    }
}
