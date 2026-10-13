package com.company.pipeline.llm.tool;

import com.company.pipeline.llm.LlmProviderConfig;
import com.company.pipeline.llm.LlmProviderConfigRepository;
import com.company.pipeline.llm.chat.LlmChatListener;
import com.company.pipeline.llm.client.LlmCallException;
import com.company.pipeline.llm.client.LlmChatClient;
import com.company.pipeline.llm.client.LlmChatResult;
import com.company.pipeline.llm.client.LlmMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 도구 호출 반복(2026-10-02, «③ 도구 호출»). 모델이 도구를 부르면 실행해 결과를 돌려주고, 답이 나올 때까지 되풀이한다.
 *
 * <p><b>모델이 바뀌어도 깨지지 않게</b> - 도구를 못 받는 모델이면 «질문 속 대상 상세»(①)만으로 답하도록 물러난다:
 * <ol>
 *   <li>첫 차례에 도구를 실었더니 거절(4xx) → 그 설정을 UNSUPPORTED 로 적고, 도구 없이 같은 질문을 다시 보낸다.</li>
 *   <li>도구 호출을 정식 필드(tool_calls) 대신 글({@code <tool_call>{…}</tool_call>})로 내보내는 모델(서빙 쪽 파서가 꺼진 Qwen 등)은
 *       그 글을 화면에 흘리지 않고 붙잡아 도구 호출로 읽는다. 읽지 못하면 1과 같이 물러난다.</li>
 *   <li>반복 상한({@link #MAX_ROUNDS})에 닿으면 마지막 차례는 도구 없이 보내 지금까지 모은 것으로 답을 마무리하게 한다.</li>
 * </ol>
 * 도구를 받아 줬는데 쓰지 않고 바로 답하면 그대로 끝난다 - ①의 상세가 이미 붙어 있다.
 */
@Component
public class LlmToolLoop {

    private static final Logger log = LoggerFactory.getLogger(LlmToolLoop.class);
    /** 도구를 실어 보내는 최대 차례 수. 그다음 한 번은 도구 없이 답을 받는다. */
    static final int MAX_ROUNDS = 4;
    private static final Pattern TEXT_TOOL_CALL = Pattern.compile("<tool_call>\\s*(\\{.*?})\\s*</tool_call>", Pattern.DOTALL);

    /** 시스템 지시에 덧붙이는 도구 사용 안내. */
    static final String TOOL_GUIDE = """

            [도구]
            - 질문이 이 대상 밖의 job · CDC 파이프라인 · 워크플로우를 묻거나, 위 정보에 없는 구성(소스 · 타깃 · SQL · 커넥터 · 실행 이력)을 물으면
              도구로 조회한 뒤 답한다. id 를 모르면 search_targets 로 먼저 찾는다.
            - 위 정보로 충분하면 도구를 부르지 않는다. 조회 결과에 없는 내용은 추측하지 말고 «확인되지 않는다»고 말한다.
            - 도구를 쓸 수 있으므로 답할 수 있는 범위에 «이름으로 찾은 다른 ETL job · CDC 파이프라인 · 워크플로우의 구성 · 단계 · 최근 실행 · 상태»가 더해진다.
              [답변 범위] 문단은 도구로 조회해 봐도 확인되지 않을 때만 쓰고, 문단의 범위에 이 조회도 함께 적는다(2026-10-13).""";

    private final LlmChatClient chatClient;
    private final LlmToolbox toolbox;
    private final LlmProviderConfigRepository configRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public LlmToolLoop(LlmChatClient chatClient, LlmToolbox toolbox, LlmProviderConfigRepository configRepository) {
        this.chatClient = chatClient;
        this.toolbox = toolbox;
        this.configRepository = configRepository;
    }

    /**
     * @param messages 도구 없이 보낼 때와 같은 메시지(시스템 지시 · 대상 정보 · 대화 · 질문)
     * @return 마지막 답의 결과. 토큰 수는 모든 차례의 합이다
     */
    public LlmChatResult run(LlmProviderConfig config, String apiKey, List<LlmMessage> messages, int maxOutputTokens,
                             LlmChatListener listener, LlmToolbox.Caller caller) {
        List<LlmMessage> conversation = withGuide(messages);
        Totals totals = new Totals();
        for (int round = 0; ; round++) {
            boolean lastRound = round >= MAX_ROUNDS;
            GuardedSink sink = new GuardedSink(listener::delta);
            LlmChatResult result;
            try {
                result = chatClient.stream(config.getProvider(), config.effectiveBaseUrl(), config.getModelName(), apiKey,
                        conversation, maxOutputTokens, sink, lastRound ? null : toolbox.specs());
            } catch (LlmCallException ex) {
                if (round == 0 && isRejection(ex)) {
                    log.info("GPT 도구 호출 거절 - 도구 없이 다시 묻습니다. config={} model={} code={} http={}",
                            config.getId(), config.getModelName(), ex.getCode(), ex.getHttpStatus());
                    recordSupport(config, LlmProviderConfig.TOOL_UNSUPPORTED);
                    listener.toolMode(false);
                    return chatClient.stream(config.getProvider(), config.effectiveBaseUrl(), config.getModelName(), apiKey,
                            messages, maxOutputTokens, listener::delta);
                }
                throw ex;
            }
            totals.add(result);

            List<LlmMessage.ToolCall> calls = result.toolCalls();
            String visibleText = result.text();
            if (calls.isEmpty() && sink.suppressed()) {
                calls = parseTextToolCalls(result.text());
                visibleText = TEXT_TOOL_CALL.matcher(result.text()).replaceAll("").strip();
                if (calls.isEmpty()) {
                    // 도구 호출처럼 보이는 글을 냈는데 읽을 수 없다 - 이 모델은 도구를 제대로 다루지 못한다.
                    log.info("GPT 도구 호출 형식을 읽지 못해 도구 없이 다시 묻습니다. config={} model={}",
                            config.getId(), config.getModelName());
                    recordSupport(config, LlmProviderConfig.TOOL_UNSUPPORTED);
                    listener.toolMode(false);
                    LlmChatResult retry = chatClient.stream(config.getProvider(), config.effectiveBaseUrl(),
                            config.getModelName(), apiKey, messages, maxOutputTokens, listener::delta);
                    totals.add(retry);
                    return totals.result(retry);
                }
            }
            if (calls.isEmpty() || lastRound) {
                sink.finish();
                return totals.result(result);
            }
            if (!result.toolCalls().isEmpty() && !LlmProviderConfig.TOOL_SUPPORTED.equals(config.getToolSupport())) {
                recordSupport(config, LlmProviderConfig.TOOL_SUPPORTED);
            }
            // 도구를 부른 차례에 같이 흘린 글(«조회해 보겠습니다» 같은)은 화면에 이미 나갔다 - 대화에도 남긴다.
            sink.finish();
            conversation.add(LlmMessage.assistantToolCalls(visibleText, calls));
            for (LlmMessage.ToolCall call : calls) {
                LlmToolbox.Outcome outcome = toolbox.execute(call.name(), call.arguments(), caller);
                listener.tool(call.name(), outcome.label());
                conversation.add(LlmMessage.toolResult(call.id(), outcome.text()));
            }
        }
    }

    /** 첫 시스템 지시에 도구 안내를 덧붙인다(시스템 메시지를 둘로 나누면 받지 않는 모델 템플릿이 있다). */
    private static List<LlmMessage> withGuide(List<LlmMessage> messages) {
        List<LlmMessage> out = new ArrayList<>(messages);
        if (!out.isEmpty() && "system".equals(out.get(0).role())) {
            out.set(0, LlmMessage.system(out.get(0).content() + TOOL_GUIDE));
        } else {
            out.add(0, LlmMessage.system(TOOL_GUIDE.strip()));
        }
        return out;
    }

    /** 도구를 실은 요청 자체를 거절했나(형식 · 필드를 모르는 서버). 인증 · 한도 · 서버 오류는 도구 탓이 아니다. */
    private static boolean isRejection(LlmCallException ex) {
        Integer http = ex.getHttpStatus();
        return http != null && (http == 400 || http == 404 || http == 422) || "BAD_REQUEST".equals(ex.getCode());
    }

    List<LlmMessage.ToolCall> parseTextToolCalls(String text) {
        List<LlmMessage.ToolCall> calls = new ArrayList<>();
        Matcher matcher = TEXT_TOOL_CALL.matcher(text == null ? "" : text);
        int index = 0;
        while (matcher.find()) {
            try {
                JsonNode node = objectMapper.readTree(matcher.group(1));
                String name = node.path("name").asText("");
                JsonNode args = node.path("arguments");
                if (!name.isBlank()) {
                    calls.add(new LlmMessage.ToolCall("text_call_" + index++, name,
                            args.isTextual() ? args.asText() : objectMapper.writeValueAsString(args)));
                }
            } catch (Exception ex) {
                return List.of();
            }
        }
        return calls;
    }

    private void recordSupport(LlmProviderConfig config, String support) {
        config.recordToolSupport(support);
        try {
            configRepository.findById(config.getId()).ifPresent(saved -> {
                saved.recordToolSupport(support);
                configRepository.save(saved);
            });
        } catch (RuntimeException ex) {
            log.warn("도구 호출 지원 여부 저장 실패: {}", ex.getMessage());
        }
    }

    /**
     * 도구를 실은 차례의 글을 화면에 흘려보내는 창구. 모델이 도구 호출을 글({@code <tool_call>})로 내보내면 그 글이 화면에
     * 찍히지 않게 붙잡는다 - 시작 몇 글자를 모아 보고, 도구 호출 글이면 끝까지 붙잡아 둔다.
     */
    static final class GuardedSink implements Consumer<String> {
        private static final int PEEK = 16;
        private final Consumer<String> target;
        private final StringBuilder pending = new StringBuilder();
        private boolean decided;
        private boolean suppressed;

        GuardedSink(Consumer<String> target) {
            this.target = target;
        }

        @Override
        public void accept(String text) {
            if (decided) {
                if (!suppressed) {
                    target.accept(text);
                }
                return;
            }
            pending.append(text);
            String head = pending.toString().stripLeading();
            if (head.startsWith("<tool_call")) {
                decided = true;
                suppressed = true;
            } else if (head.length() >= PEEK || (!head.isEmpty() && !"<tool_call".startsWith(head.substring(0, Math.min(head.length(), 10))))) {
                decided = true;
                target.accept(pending.toString());
                pending.setLength(0);
            }
        }

        /** 끝까지 모아만 둔 짧은 답(16자 미만)을 내보낸다. */
        void finish() {
            if (!suppressed && !pending.isEmpty()) {
                target.accept(pending.toString());
                pending.setLength(0);
            }
            decided = true;
        }

        boolean suppressed() {
            return suppressed;
        }
    }

    private static final class Totals {
        Integer input;
        Integer output;
        long durationMs;

        void add(LlmChatResult result) {
            input = sum(input, result.inputTokens());
            output = sum(output, result.outputTokens());
            durationMs += result.durationMs();
        }

        LlmChatResult result(LlmChatResult last) {
            return new LlmChatResult(input, output, durationMs, last.text(), last.toolCalls());
        }

        private static Integer sum(Integer a, Integer b) {
            // 삼항 연산 하나로 쓰면 a + b(int) 때문에 null 이 풀려 NPE 가 난다 - 제공자가 토큰 수를 안 주는 경우.
            if (a == null) {
                return b;
            }
            if (b == null) {
                return a;
            }
            return a + b;
        }
    }
}
