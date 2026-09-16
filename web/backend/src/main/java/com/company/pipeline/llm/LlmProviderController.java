package com.company.pipeline.llm;

import com.company.pipeline.authz.RequirePermission;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.llm.dto.LlmProviderRequest;
import com.company.pipeline.llm.dto.LlmProviderResponse;
import com.company.pipeline.llm.dto.LlmTestResult;
import com.company.pipeline.user.AppUser;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * GPT 설정 관리(ADMIN). 조회는 READ, 변경·테스트는 WRITE - 클래스 애노테이션이 HTTP 동사로 가른다.
 * 변경 요청은 {@code PermissionAspect} 가 감사 로그에 자동으로 남긴다(URI·메서드만 - 본문의 API 키는 기록되지 않는다).
 */
@RestController
@RequestMapping("/api/llm/providers")
@RequirePermission(system = SystemCode.ADMIN)
public class LlmProviderController {

    private final LlmProviderService service;

    public LlmProviderController(LlmProviderService service) {
        this.service = service;
    }

    @GetMapping
    public ApiResponse<List<LlmProviderResponse>> list() {
        return ApiResponse.success(service.list());
    }

    @GetMapping("/{id}")
    public ApiResponse<LlmProviderResponse> get(@PathVariable Long id) {
        return ApiResponse.success(service.get(id));
    }

    @PostMapping
    public ApiResponse<LlmProviderResponse> create(@Valid @RequestBody LlmProviderRequest request) {
        return ApiResponse.success(uniqueGuard(() -> service.create(request, actor())));
    }

    @PutMapping("/{id}")
    public ApiResponse<LlmProviderResponse> update(@PathVariable Long id, @Valid @RequestBody LlmProviderRequest request) {
        return ApiResponse.success(uniqueGuard(() -> service.update(id, request, actor())));
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        service.delete(id);
        return ApiResponse.success(null);
    }

    @PostMapping("/{id}/activate")
    public ApiResponse<LlmProviderResponse> activate(@PathVariable Long id) {
        try {
            return ApiResponse.success(service.activate(id, actor()));
        } catch (DataIntegrityViolationException ex) {
            throw new BusinessException(ErrorCode.LLM_PROVIDER_CONFLICT,
                    "다른 사용자가 먼저 선택을 변경했습니다. 목록을 새로고침한 뒤 다시 시도하십시오.");
        }
    }

    @PostMapping("/deactivate")
    public ApiResponse<Void> deactivate() {
        service.deactivate(actor());
        return ApiResponse.success(null);
    }

    /** 저장 전 입력값으로 테스트. 수정 화면은 {@code id} 를 넘겨 키 칸이 비었을 때 저장된 키를 쓰게 한다. */
    @PostMapping("/test")
    public ApiResponse<LlmTestResult> testDraft(@Valid @RequestBody LlmProviderRequest request,
                                                @RequestParam(required = false) Long id) {
        return ApiResponse.success(service.testDraft(request, id));
    }

    @PostMapping("/{id}/test")
    public ApiResponse<LlmProviderResponse> testSaved(@PathVariable Long id) {
        return ApiResponse.success(service.testSaved(id));
    }

    private <T> T uniqueGuard(java.util.function.Supplier<T> action) {
        try {
            return action.get();
        } catch (DataIntegrityViolationException ex) {
            // 사전 중복 검사를 통과했는데 동시 등록으로 이름 유니크 인덱스에 걸린 경우.
            throw new BusinessException(ErrorCode.LLM_PROVIDER_CONFLICT, "이미 같은 이름의 GPT 설정이 있습니다.");
        }
    }

    private static String actor() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof AppUser user) {
            return user.getUserId();
        }
        return "unknown";
    }
}
