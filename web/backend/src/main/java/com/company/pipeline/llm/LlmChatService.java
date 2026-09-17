package com.company.pipeline.llm;

import com.company.pipeline.authz.AccessBits;
import com.company.pipeline.authz.PermissionService;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.llm.client.LlmChatPrompt;
import com.company.pipeline.llm.client.LlmChatResult;
import com.company.pipeline.llm.client.LlmClientRegistry;
import com.company.pipeline.llm.dto.LlmChatRequest;
import com.company.pipeline.llm.dto.LlmContextPreviewResponse;
import com.company.pipeline.user.AppUser;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Service
public class LlmChatService {

    private final LlmProviderConfigRepository providerRepository;
    private final LlmUsageLogRepository usageLogRepository;
    private final PasswordCryptoService cryptoService;
    private final PermissionService permissionService;
    private final LlmContextService contextService;
    private final LlmClientRegistry clientRegistry;

    public LlmChatService(LlmProviderConfigRepository providerRepository, LlmUsageLogRepository usageLogRepository,
                          PasswordCryptoService cryptoService, PermissionService permissionService,
                          LlmContextService contextService, LlmClientRegistry clientRegistry) {
        this.providerRepository = providerRepository;
        this.usageLogRepository = usageLogRepository;
        this.cryptoService = cryptoService;
        this.permissionService = permissionService;
        this.contextService = contextService;
        this.clientRegistry = clientRegistry;
    }

    public SseEmitter stream(LlmChatRequest request) {
        String userId = currentUserId();
        requireTargetRead(userId, request.targetType());
        LlmProviderConfig config = activeConfig();
        LlmContext context = contextService.collect(request.targetType(), request.targetId(), config);
        LlmUsageLog usageLog = LlmUsageLog.create(userId, request.targetType(), request.targetId(), config, context.masked());
        usageLogRepository.save(usageLog);

        SseEmitter emitter = new SseEmitter(180_000L);
        long started = System.nanoTime();
        CompletableFuture.runAsync(() -> {
            try {
                String apiKey = config.getEncryptedApiKey() == null ? null : cryptoService.decrypt(config.getEncryptedApiKey());
                LlmChatPrompt prompt = new LlmChatPrompt(config.getProvider(), config.effectiveBaseUrl(),
                        config.getModelName(), apiKey, systemPrompt(context), request.safeMessages(), request.question().trim());
                LlmChatResult result = clientRegistry.get(config.getProvider()).stream(prompt, delta -> send(emitter, "delta", delta));
                int durationMs = elapsedMs(started);
                usageLog.setInputTokens(result.inputTokens());
                usageLog.setOutputTokens(result.outputTokens());
                usageLog.setDurationMs(durationMs);
                usageLog.setStatus(LlmUsageLog.STATUS_SUCCESS);
                usageLogRepository.save(usageLog);
                send(emitter, "done", Map.of(
                        "answer", result.answer(),
                        "providerName", config.getName(),
                        "modelName", config.getModelName(),
                        "masked", context.masked(),
                        "durationMs", durationMs));
                emitter.complete();
            } catch (BusinessException ex) {
                markFailed(usageLog, started, ex.getErrorCode().name());
                send(emitter, "error", ex.getMessage());
                emitter.complete();
            } catch (RuntimeException ex) {
                markFailed(usageLog, started, ErrorCode.LLM_CHAT_ERROR.name());
                send(emitter, "error", "GPT 요청 처리 중 오류가 발생했습니다: " + ex.getMessage());
                emitter.complete();
            }
        });
        return emitter;
    }

    public LlmContextPreviewResponse preview(LlmChatRequest request) {
        String userId = currentUserId();
        requireTargetRead(userId, request.targetType());
        LlmProviderConfig config = activeConfig();
        LlmContext context = contextService.collect(request.targetType(), request.targetId(), config);
        return new LlmContextPreviewResponse(context.title(), config.getName(), context.masked(), context.text());
    }

    private LlmProviderConfig activeConfig() {
        return providerRepository.findByActiveTrue()
                .orElseThrow(() -> new BusinessException(ErrorCode.LLM_PROVIDER_NOT_FOUND,
                        "선택된 GPT 설정이 없습니다. GPT 관리에서 사용할 항목을 선택하십시오."));
    }

    private void requireTargetRead(String userId, LlmTargetType targetType) {
        SystemCode system = switch (targetType) {
            case GENERAL, NIFI_GROUP -> SystemCode.NIFI;
            case ETL_JOB, WORKFLOW_NODE -> SystemCode.NIFI;
            case CDC_PIPELINE -> SystemCode.KAFKA;
            case WORKFLOW_TASK -> SystemCode.AIRFLOW;
        };
        if (!permissionService.check(userId, system, AccessBits.READ)) {
            throw new BusinessException(ErrorCode.FORBIDDEN);
        }
    }

    private static String systemPrompt(LlmContext context) {
        return """
                당신은 ETL 운영을 돕는 AI 어시스턴트입니다.
                아래 대상 정보를 근거로 답하십시오. 모르는 내용은 추측하지 말고 확인이 필요하다고 말하십시오.
                답변은 한국어로, 원인/확인할 것/조치 방법을 짧고 실무적으로 정리하십시오.

                [대상 정보]
                %s
                """.formatted(context.text());
    }

    private static int elapsedMs(long started) {
        return Math.toIntExact(Duration.ofNanos(System.nanoTime() - started).toMillis());
    }

    private void markFailed(LlmUsageLog usageLog, long started, String errorCode) {
        usageLog.setDurationMs(elapsedMs(started));
        usageLog.setStatus(LlmUsageLog.STATUS_FAILED);
        usageLog.setErrorCode(errorCode);
        usageLogRepository.save(usageLog);
    }

    private static void send(SseEmitter emitter, String event, Object data) {
        try {
            emitter.send(SseEmitter.event().name(event).data(data));
        } catch (IOException ex) {
            throw new BusinessException(ErrorCode.LLM_CHAT_ERROR, "GPT 스트림 전송 중 오류가 발생했습니다.", ex);
        }
    }

    private static String currentUserId() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof AppUser user) {
            return user.getUserId();
        }
        throw new BusinessException(ErrorCode.UNAUTHORIZED);
    }
}
