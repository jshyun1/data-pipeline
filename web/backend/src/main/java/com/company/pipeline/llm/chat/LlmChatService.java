package com.company.pipeline.llm.chat;

import com.company.pipeline.authz.AccessBits;
import com.company.pipeline.authz.PermissionService;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.llm.LlmProviderConfig;
import com.company.pipeline.llm.LlmProviderConfigRepository;
import com.company.pipeline.llm.agent.ConversationSummarizer;
import com.company.pipeline.llm.agent.ErrorAnalysisAgent;
import com.company.pipeline.llm.agent.JobAnalysisAgent;
import com.company.pipeline.llm.agent.LlmAgentType;
import com.company.pipeline.llm.client.LlmCallException;
import com.company.pipeline.llm.client.LlmChatClient;
import com.company.pipeline.llm.client.LlmChatResult;
import com.company.pipeline.llm.client.LlmMessage;
import com.company.pipeline.llm.context.LlmContext;
import com.company.pipeline.llm.context.LlmContextCollector;
import com.company.pipeline.llm.context.LlmContextCollector.CollectedTarget;
import com.company.pipeline.llm.context.LlmTargetType;
import com.company.pipeline.llm.context.SensitiveDataMasker;
import com.company.pipeline.llm.usage.LlmUsageLog;
import com.company.pipeline.llm.usage.LlmUsageLogRepository;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * GPT 호출(설계 5절). 순서: 선택된 LLM 확인 → 대상 종류별 권한 → 대상 정보 수집 → 자르기 → 마스킹 → (대화가 길면 줄이기) → 스트리밍 → 호출 기록.
 *
 * <p><b>입력 길이 나누기</b> - 설정의 «최대 입력 길이» 중 {@link #CONTEXT_SHARE} 를 대상 정보에, 나머지를 역할 지시문과 대화에 쓴다.
 * 대상 정보가 짧으면 남는 자리도 대화가 쓴다. 대화가 넘치면:
 * <ul>
 *   <li>오류 분석: 오래된 추가 문답부터 뺀다. 대상 정보와 첫 분석은 항상 남는다.</li>
 *   <li>속성창 질문: 오래된 문답을 LLM 으로 요약해 이어 간다(요약 호출 1회 추가). 요약이 실패하면 빼기로 물러난다.</li>
 * </ul>
 * 대화는 서버에 저장하지 않는다 - 요약도 {@code compacted} 이벤트로 화면에 넘기고, 화면이 다음 요청에 같이 보낸다.
 *
 * <p><b>트랜잭션을 걸지 않는다.</b> 답변은 수십 초~수 분 흐르는데, 그동안 DB 연결을 쥐고 있으면 안 된다. 수집기와 기록 저장이
 * 각자 짧게 끝난다.
 *
 * <p>권한: 설정은 ADMIN 이지만 사용은 «그 화면을 볼 수 있으면 누구나»(설계 7절). 화면에서 버튼을 숨기는 것만으로는 부족해
 * 요청마다 대상 종류로 다시 확인한다. 인가 스위치({@code authz.enforcement.enabled})가 꺼진 서버에서는 다른 API 처럼 로그인만 본다
 * (/api/llm/** 은 SecurityConfig 에서 로그인 필수).
 */
@Service
public class LlmChatService {

    private static final Logger log = LoggerFactory.getLogger(LlmChatService.class);
    /** 답 길이 상한. 오류 분석 네 칸 · 속성창 설명 모두 이 안에서 끝난다. */
    static final int MAX_OUTPUT_TOKENS = 4_096;
    /** 최대 입력 길이 중 대상 정보에 주는 몫. 나머지는 대화(첫 분석 · 문답 · 요약 · 질문)가 쓴다. */
    static final double CONTEXT_SHARE = 0.6;
    static final String AGENT_SUMMARY = "CONVERSATION_SUMMARY";
    /** 대상 정보 · 질문을 감싸는 머리말([분석 대상: …], [이전 대화 요약] 같은 제목 줄)과 안내 문구의 여유분. */
    static final int PROMPT_WRAPPER_CHARS = 200;

    private final LlmProviderConfigRepository configRepository;
    private final PasswordCryptoService cryptoService;
    private final PermissionService permissionService;
    private final LlmChatClient chatClient;
    private final LlmUsageLogRepository usageLogRepository;
    private final Map<LlmTargetType, LlmContextCollector> collectors = new EnumMap<>(LlmTargetType.class);

    @Value("${authz.enforcement.enabled:false}")
    private boolean enforcementEnabled;

    public LlmChatService(LlmProviderConfigRepository configRepository, PasswordCryptoService cryptoService,
                          PermissionService permissionService, LlmChatClient chatClient,
                          LlmUsageLogRepository usageLogRepository, List<LlmContextCollector> collectors) {
        this.configRepository = configRepository;
        this.cryptoService = cryptoService;
        this.permissionService = permissionService;
        this.chatClient = chatClient;
        this.usageLogRepository = usageLogRepository;
        collectors.forEach(collector -> this.collectors.put(collector.targetType(), collector));
    }

    /**
     * 검증 · 수집 · 마스킹까지 끝낸 호출 준비물. 스트림을 열기 전에 만들어, 여기서 난 오류는 일반 JSON 오류로 응답된다.
     * 대화 줄이기(요약은 LLM 호출이다)는 스트림을 연 뒤 {@link #stream} 에서 한다.
     */
    public record Prepared(LlmChatRequest request, String userId, LlmProviderConfig config, CollectedTarget target,
                           LlmContextPreview preview, String contextText, List<LlmMessage> history, String summary,
                           String question, int maxInputChars) {
    }

    /** 실제 호출 준비. 속성창 질문은 질문이 있어야 한다. */
    public Prepared prepareChat(LlmChatRequest request, String userId) {
        boolean question = request.question() != null && !request.question().isBlank();
        if (request.agentType() == LlmAgentType.JOB_ANALYSIS && !question) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "질문을 입력하십시오.");
        }
        return prepare(request, userId);
    }

    /** [보내는 정보 보기] 에도 쓴다 - 질문 없이 대상 정보만 모은다. */
    public Prepared prepare(LlmChatRequest request, String userId) {
        LlmProviderConfig config = configRepository.findByActiveTrue()
                .orElseThrow(() -> new BusinessException(ErrorCode.LLM_NOT_ENABLED));
        checkPermission(request.targetType(), userId);
        LlmContextCollector collector = collectors.get(request.targetType());
        if (collector == null) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "아직 지원하지 않는 분석 대상입니다: " + request.targetType());
        }

        CollectedTarget target = collector.collect(request.target(), request.errorText());
        int maxChars = config.getMaxInputChars() != null ? config.getMaxInputChars()
                : config.getProvider().getDefaultMaxInputChars();
        int contextBudget = (int) (maxChars * CONTEXT_SHARE);
        // 자른 뒤 마스킹한다. 마스킹 표시(<전화번호>)는 원문보다 짧거나 비슷해 제한을 넘기지 않는다.
        LlmContext.Assembled assembled = target.context().assemble(contextBudget);
        boolean masked = config.isMaskSensitive();
        String contextText = mask(masked, assembled.text());

        // 순서 · 개수를 그대로 둔다 - 요약 뒤 화면이 «앞에서 몇 개를 덜 보낼지»를 이 목록의 위치로 계산한다.
        List<LlmMessage> history = request.history() == null ? List.of() : request.history().stream()
                .map(turn -> new LlmMessage(turn.role(), mask(masked, turn.content())))
                .toList();

        LlmContextPreview preview = new LlmContextPreview(target.label(), config.getName(), config.getDeployment(),
                config.getModelName(), masked, assembled.truncated(), assembled.omittedTitles(),
                contextText.length(), contextBudget, contextText);
        return new Prepared(request, userId, config, target, preview, contextText, history,
                mask(masked, request.summary()), mask(masked, request.question()), maxChars);
    }

    /** 스트리밍 호출. 실패해도 예외를 던지지 않고 {@code listener.error} 로 알린다 - 이미 SSE 응답이 열려 있다. */
    public void stream(Prepared prepared, LlmChatListener listener) {
        LlmProviderConfig config = prepared.config();
        LlmContextPreview preview = prepared.preview();
        listener.meta(new LlmContextPreview(preview.targetLabel(), preview.providerName(), preview.deployment(),
                preview.modelName(), preview.masked(), preview.truncated(), preview.omittedSections(),
                preview.inputChars(), preview.maxInputChars(), null));

        LlmUsageLog usage = newUsageLog(prepared, prepared.request().agentType().name());
        long started = System.nanoTime();
        try {
            String apiKey = config.getEncryptedApiKey() == null ? null : cryptoService.decrypt(config.getEncryptedApiKey());
            List<LlmMessage> messages = prepared.request().agentType() == LlmAgentType.JOB_ANALYSIS
                    ? jobMessages(prepared, apiKey, listener)
                    : errorMessages(prepared, listener);
            usage.setInputChars(messages.stream().mapToInt(m -> m.content().length()).sum());
            LlmChatResult result = chatClient.stream(config.getProvider(), config.effectiveBaseUrl(), config.getModelName(),
                    apiKey, messages, MAX_OUTPUT_TOKENS, listener::delta);
            usage.setStatus("SUCCESS");
            usage.setInputTokens(result.inputTokens());
            usage.setOutputTokens(result.outputTokens());
            listener.done(result.inputTokens(), result.outputTokens(), result.durationMs());
        } catch (LlmCallException ex) {
            usage.setStatus("TIMEOUT".equals(ex.getCode()) ? "TIMEOUT" : "FAILED");
            usage.setErrorCode(ex.getCode());
            log.warn("GPT 호출 실패 user={} agent={} target={}:{} provider={} model={} code={} http={} message={}",
                    prepared.userId(), prepared.request().agentType(), prepared.request().targetType(),
                    prepared.target().targetId(), config.getProvider(), config.getModelName(), ex.getCode(),
                    ex.getHttpStatus(), ex.getMessage());
            safeError(listener, ex.getCode(), ex.getMessage());
        } catch (StreamClosedException ex) {
            // 화면이 창을 닫았거나 [중지] 를 눌렀다. 알릴 곳이 없다.
            usage.setStatus("CANCELLED");
        } catch (RuntimeException ex) {
            usage.setStatus("FAILED");
            usage.setErrorCode("INTERNAL");
            log.error("GPT 호출 중 내부 오류 user={} target={}:{}", prepared.userId(), prepared.request().targetType(),
                    prepared.target().targetId(), ex);
            safeError(listener, "INTERNAL", "분석 중 서버 오류가 발생했습니다.");
        } finally {
            saveUsage(usage, started);
        }
    }

    /** 오류 분석: 넘치면 오래된 추가 문답을 뺀다. */
    private List<LlmMessage> errorMessages(Prepared prepared, LlmChatListener listener) {
        LlmTargetType type = prepared.request().targetType();
        if (prepared.question() == null || prepared.question().isBlank()) {
            return ErrorAnalysisAgent.messages(type, prepared.contextText(), null, List.of(), 0, null);
        }
        ConversationFitter.Normalized normalized = ConversationFitter.normalize(prepared.history(), true);
        // 역할 지시문도 입력 길이에 든다 - 빼먹으면 한도가 작은 내부 모델에서 지시문만큼 넘친다(실측 2026-09-17: 6,000자 한도에 7,048자).
        int fixed = ErrorAnalysisAgent.SYSTEM_PROMPT.length() + prepared.contextText().length() + prepared.question().length()
                + PROMPT_WRAPPER_CHARS;
        ConversationFitter.Plan plan = ConversationFitter.fit(normalized, fixed, prepared.maxInputChars());
        if (!plan.dropped().isEmpty()) {
            listener.compacted("DROPPED", plan.droppedTurns(), plan.dropped().size(), null);
        }
        return ErrorAnalysisAgent.messages(type, prepared.contextText(), plan.firstAnswer(), plan.keptTurns(),
                plan.dropped().size(), prepared.question());
    }

    /** 속성창 질문: 넘치면 오래된 문답을 요약한다. 요약이 실패하면 빼기로 물러난다. */
    private List<LlmMessage> jobMessages(Prepared prepared, String apiKey, LlmChatListener listener) {
        LlmTargetType type = prepared.request().targetType();
        ConversationFitter.Normalized normalized = ConversationFitter.normalize(prepared.history(), false);
        int fixed = JobAnalysisAgent.SYSTEM_PROMPT.length() + prepared.contextText().length() + prepared.question().length()
                + PROMPT_WRAPPER_CHARS;
        String summary = prepared.summary();
        int summaryChars = summary == null ? 0 : summary.length();
        ConversationFitter.Plan plan = ConversationFitter.fit(normalized, fixed + summaryChars, prepared.maxInputChars());
        if (!plan.dropped().isEmpty()) {
            // 새 요약이 들어갈 자리를 비워 두고 다시 고른다.
            ConversationFitter.Plan reserved = ConversationFitter.fit(normalized,
                    fixed + ConversationSummarizer.MAX_SUMMARY_CHARS, prepared.maxInputChars());
            String newSummary = summarize(prepared, apiKey, summary, reserved.droppedTurnsAsMessages());
            if (newSummary != null) {
                listener.compacted("SUMMARIZED", reserved.droppedTurns(), reserved.dropped().size(), newSummary);
                summary = newSummary;
                plan = reserved;
            } else {
                listener.compacted("DROPPED", plan.droppedTurns(), plan.dropped().size(), null);
            }
        }
        return JobAnalysisAgent.messages(type, prepared.contextText(), summary, plan.keptTurns(), prepared.question());
    }

    /** @return 요약. 실패하면 null(호출 기록은 따로 남긴다) */
    private String summarize(Prepared prepared, String apiKey, String previousSummary, List<LlmMessage> dropped) {
        LlmProviderConfig config = prepared.config();
        LlmUsageLog usage = newUsageLog(prepared, AGENT_SUMMARY);
        List<LlmMessage> messages = ConversationSummarizer.messages(previousSummary, dropped);
        usage.setInputChars(messages.stream().mapToInt(m -> m.content().length()).sum());
        long started = System.nanoTime();
        StringBuilder out = new StringBuilder();
        try {
            LlmChatResult result = chatClient.stream(config.getProvider(), config.effectiveBaseUrl(), config.getModelName(),
                    apiKey, messages, ConversationSummarizer.MAX_OUTPUT_TOKENS, out::append);
            usage.setStatus("SUCCESS");
            usage.setInputTokens(result.inputTokens());
            usage.setOutputTokens(result.outputTokens());
            String text = out.toString().strip();
            if (text.isEmpty()) {
                return null;
            }
            return text.length() > ConversationSummarizer.MAX_SUMMARY_CHARS
                    ? text.substring(0, ConversationSummarizer.MAX_SUMMARY_CHARS) : text;
        } catch (LlmCallException ex) {
            usage.setStatus("TIMEOUT".equals(ex.getCode()) ? "TIMEOUT" : "FAILED");
            usage.setErrorCode(ex.getCode());
            log.warn("GPT 대화 요약 실패 - 오래된 문답을 빼고 이어 갑니다. user={} code={} message={}",
                    prepared.userId(), ex.getCode(), ex.getMessage());
            return null;
        } finally {
            saveUsage(usage, started);
        }
    }

    /** 화면 쪽 쓰기가 실패했음을 알리는 표시. 리스너 구현체가 던진다. */
    public static class StreamClosedException extends RuntimeException {
        public StreamClosedException(Throwable cause) {
            super("클라이언트 연결이 닫혔습니다.", cause);
        }
    }

    private void checkPermission(LlmTargetType targetType, String userId) {
        if (!enforcementEnabled) {
            return;
        }
        if (userId == null || !permissionService.check(userId, targetType.getRequiredSystem(), AccessBits.READ)) {
            throw new BusinessException(ErrorCode.FORBIDDEN, "이 대상을 조회할 권한이 없어 GPT 를 사용할 수 없습니다.");
        }
    }

    private static String mask(boolean masked, String text) {
        return masked ? SensitiveDataMasker.mask(text) : text;
    }

    private static LlmUsageLog newUsageLog(Prepared prepared, String agentType) {
        LlmUsageLog usage = new LlmUsageLog();
        usage.setUserId(prepared.userId() == null ? "unknown" : prepared.userId());
        usage.setAgentType(agentType);
        usage.setTargetType(prepared.request().targetType().name());
        String targetId = prepared.target().targetId();
        usage.setTargetId(targetId.length() > 300 ? targetId.substring(0, 300) : targetId);
        usage.setProviderConfigId(prepared.config().getId());
        usage.setProvider(prepared.config().getProvider().name());
        usage.setModelName(prepared.config().getModelName());
        usage.setDeployment(prepared.config().getDeployment().name());
        usage.setMasked(prepared.preview().masked());
        usage.setStatus("FAILED");
        return usage;
    }

    private void saveUsage(LlmUsageLog usage, long startedNanos) {
        usage.setDurationMs((int) Math.min(Integer.MAX_VALUE, (System.nanoTime() - startedNanos) / 1_000_000));
        try {
            usageLogRepository.save(usage);
        } catch (RuntimeException ex) {
            log.warn("GPT 호출 기록 저장 실패: {}", ex.getMessage());
        }
    }

    private static void safeError(LlmChatListener listener, String code, String message) {
        try {
            listener.error(code, message);
        } catch (RuntimeException ignored) {
            // 연결이 이미 끊겼다
        }
    }
}
