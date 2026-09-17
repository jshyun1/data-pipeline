package com.company.pipeline.llm.client;

import com.company.pipeline.llm.LlmProvider;
import com.company.pipeline.llm.dto.LlmTestResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.springframework.stereotype.Component;

/**
 * LLM 스트리밍 호출. 답이 글자 단위로 흘러나오는 대로 {@code onDelta} 에 넘긴다(설계 8절).
 *
 * <p>요청 형식은 {@link LlmConnectionTester} 와 같은 분기다. 제공자별 차이:
 * <ul>
 *   <li>OpenAI · OpenAI 호환: {@code /chat/completions}, SSE {@code data: {choices[0].delta.content}} … {@code [DONE]}</li>
 *   <li>Anthropic: {@code /v1/messages}, system 은 별도 필드, {@code content_block_delta} 이벤트</li>
 *   <li>Gemini: {@code :streamGenerateContent?alt=sse}, system 은 systemInstruction, 역할은 user · model</li>
 * </ul>
 *
 * <p><b>스트리밍을 무시하는 서버</b>도 받는다. 일부 OpenAI 호환 프록시는 {@code stream:true} 를 무시하고 JSON 한 덩어리로 답한다 -
 * 응답이 SSE 가 아니면 본문 전체를 읽어 한 번에 넘긴다.
 *
 * <p>시간 제한: 첫 응답(헤더)까지 {@link #FIRST_RESPONSE_TIMEOUT}, 전체 {@link #TOTAL_TIMEOUT}. Java HttpClient 는 읽는 중에
 * 멈춘 스트림을 스스로 끊지 못해, 전체 제한은 감시 타이머가 입력 스트림을 닫는 방식으로 건다.
 */
@Component
public class LlmChatClient {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    /** 내부 모델은 첫 호출에 모델을 메모리에 올리느라 오래 걸린다. */
    static final Duration FIRST_RESPONSE_TIMEOUT = Duration.ofSeconds(120);
    static final Duration TOTAL_TIMEOUT = Duration.ofSeconds(300);
    private static final String ANTHROPIC_VERSION = "2023-06-01";

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "llm-stream-watchdog");
        thread.setDaemon(true);
        return thread;
    });

    public LlmChatClient() {
        // HTTP/1.1 고정 - 이유는 LlmConnectionTester 생성자 주석과 같다(uvicorn 계열 내부 서버의 h2c 업그레이드 문제).
        this(HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(CONNECT_TIMEOUT).build());
    }

    LlmChatClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    /**
     * 호출하고 끝날 때까지 이 스레드에서 흘려보낸다. {@code onDelta} 가 예외를 던지면(화면 연결이 끊김 등) 즉시 멈춘다.
     *
     * @throws LlmCallException 연결·인증·시간 초과 등으로 실패했을 때
     */
    public LlmChatResult stream(LlmProvider provider, String baseUrl, String model, String apiKey,
                                List<LlmMessage> messages, int maxOutputTokens, Consumer<String> onDelta) {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new LlmCallException("INVALID_CONFIG", "API 주소가 필요합니다.", null);
        }
        HttpRequest request;
        try {
            request = buildRequest(provider, trimSlash(baseUrl.trim()), model, apiKey, messages, maxOutputTokens);
        } catch (IllegalArgumentException | IOException ex) {
            throw new LlmCallException("INVALID_CONFIG", "요청을 만들 수 없습니다: " + ex.getMessage(), null);
        }

        long started = System.nanoTime();
        HttpResponse<InputStream> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
        } catch (HttpConnectTimeoutException ex) {
            throw new LlmCallException("UNREACHABLE",
                    "서버에서 LLM 주소로 연결할 수 없습니다(시간 초과). 폐쇄망·방화벽·프록시 설정을 확인하십시오.", null);
        } catch (HttpTimeoutException ex) {
            throw new LlmCallException("TIMEOUT",
                    "LLM 이 " + FIRST_RESPONSE_TIMEOUT.toSeconds() + "초 안에 응답하지 않았습니다. 잠시 뒤 다시 시도하십시오.", null);
        } catch (ConnectException ex) {
            throw new LlmCallException("UNREACHABLE", "서버에서 LLM 주소로 연결할 수 없습니다. 주소·방화벽을 확인하십시오.", null);
        } catch (IOException ex) {
            throw new LlmCallException("UNREACHABLE", "LLM 과 통신 중 오류가 발생했습니다: " + ex.getMessage(), null);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new LlmCallException("CANCELLED", "호출이 중단되었습니다.", null);
        }

        AtomicBoolean timedOut = new AtomicBoolean(false);
        try (InputStream body = response.body()) {
            ScheduledFuture<?> guard = watchdog.schedule(() -> {
                timedOut.set(true);
                closeQuietly(body);
            }, TOTAL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            try {
                if (response.statusCode() / 100 != 2) {
                    String text = new String(body.readAllBytes(), StandardCharsets.UTF_8);
                    LlmTestResult classified = LlmConnectionTester.classify(response.statusCode(), text, elapsedMs(started));
                    throw new LlmCallException(classified.status(), classified.message(), response.statusCode());
                }
                String contentType = response.headers().firstValue("content-type").orElse("");
                Usage usage = contentType.contains("text/event-stream")
                        ? readSse(provider, body, onDelta)
                        : readWhole(provider, body, onDelta);
                return new LlmChatResult(usage.input, usage.output, elapsedMs(started));
            } finally {
                guard.cancel(false);
            }
        } catch (IOException ex) {
            if (timedOut.get()) {
                throw new LlmCallException("TIMEOUT",
                        "답변이 " + TOTAL_TIMEOUT.toSeconds() + "초 안에 끝나지 않아 중단했습니다.", null);
            }
            throw new LlmCallException("UNREACHABLE", "답변을 받는 중 연결이 끊겼습니다: " + ex.getMessage(), null);
        }
    }

    // ------------------------------------------------------------------ 요청

    HttpRequest buildRequest(LlmProvider provider, String base, String model, String apiKey,
                             List<LlmMessage> messages, int maxOutputTokens) throws IOException {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .timeout(FIRST_RESPONSE_TIMEOUT)
                .header("Content-Type", "application/json")
                .header("Accept", "text/event-stream, application/json");
        String system = messages.stream().filter(m -> "system".equals(m.role()))
                .map(LlmMessage::content).reduce((a, b) -> a + "\n\n" + b).orElse(null);
        List<LlmMessage> conversation = messages.stream().filter(m -> !"system".equals(m.role())).toList();
        Map<String, Object> payload = new LinkedHashMap<>();
        switch (provider) {
            case ANTHROPIC -> {
                builder.uri(URI.create(base + "/v1/messages")).header("anthropic-version", ANTHROPIC_VERSION);
                if (hasText(apiKey)) {
                    builder.header("x-api-key", apiKey);
                }
                payload.put("model", model);
                payload.put("max_tokens", maxOutputTokens);
                if (system != null) {
                    payload.put("system", system);
                }
                payload.put("messages", conversation.stream()
                        .map(m -> Map.of("role", m.role(), "content", m.content())).toList());
                payload.put("stream", true);
            }
            case GEMINI -> {
                builder.uri(URI.create(base + "/v1beta/models/" + URLEncoder.encode(model, StandardCharsets.UTF_8)
                        + ":streamGenerateContent?alt=sse"));
                if (hasText(apiKey)) {
                    builder.header("x-goog-api-key", apiKey);
                }
                if (system != null) {
                    payload.put("systemInstruction", Map.of("parts", List.of(Map.of("text", system))));
                }
                payload.put("contents", conversation.stream()
                        .map(m -> Map.of("role", "assistant".equals(m.role()) ? "model" : "user",
                                "parts", List.of(Map.of("text", m.content()))))
                        .toList());
                payload.put("generationConfig", Map.of("maxOutputTokens", maxOutputTokens));
            }
            case OPENAI, OPENAI_COMPATIBLE -> {
                builder.uri(URI.create(base + "/chat/completions"));
                if (hasText(apiKey)) {
                    builder.header("Authorization", "Bearer " + apiKey);
                }
                payload.put("model", model);
                payload.put("messages", messages.stream()
                        .map(m -> Map.of("role", m.role(), "content", m.content())).toList());
                payload.put("stream", true);
                if (provider == LlmProvider.OPENAI) {
                    // 최신 OpenAI 모델은 max_tokens · temperature 지정을 거절한다(연결 테스트 주석 참고).
                    payload.put("max_completion_tokens", maxOutputTokens);
                    payload.put("stream_options", Map.of("include_usage", true));
                } else {
                    // 호환 서버는 max_tokens 만 아는 경우가 많고, stream_options 는 모르는 필드라 거절하는 서버가 있다.
                    payload.put("max_tokens", maxOutputTokens);
                    payload.put("temperature", 0.2);
                }
            }
            default -> throw new IllegalArgumentException("지원하지 않는 제공자: " + provider);
        }
        String body = objectMapper.writeValueAsString(payload);
        return builder.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build();
    }

    // ------------------------------------------------------------------ 응답

    static final class Usage {
        Integer input;
        Integer output;
    }

    /** SSE 를 줄 단위로 읽는다. 이벤트는 빈 줄로 끝나고, data 줄이 여러 개면 이어 붙인다. */
    Usage readSse(LlmProvider provider, InputStream body, Consumer<String> onDelta) throws IOException {
        Usage usage = new Usage();
        BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8));
        List<String> data = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isEmpty()) {
                if (!data.isEmpty() && handleEvent(provider, String.join("\n", data), usage, onDelta)) {
                    return usage;
                }
                data.clear();
            } else if (line.startsWith("data:")) {
                data.add(line.substring(5).stripLeading());
            }
        }
        if (!data.isEmpty()) {
            handleEvent(provider, String.join("\n", data), usage, onDelta);
        }
        return usage;
    }

    /** @return 스트림 끝 표시를 만났으면 true */
    private boolean handleEvent(LlmProvider provider, String data, Usage usage, Consumer<String> onDelta) throws IOException {
        if ("[DONE]".equals(data)) {
            return true;
        }
        JsonNode node;
        try {
            node = objectMapper.readTree(data);
        } catch (IOException ex) {
            return false;   // keep-alive 주석 등 JSON 이 아닌 줄은 건너뛴다
        }
        switch (provider) {
            case ANTHROPIC -> {
                String type = node.path("type").asText();
                if ("error".equals(type)) {
                    throw new LlmCallException("PROVIDER_ERROR",
                            "제공자가 답변 중 오류를 보냈습니다: " + node.path("error").path("message").asText(), null);
                }
                if ("message_start".equals(type)) {
                    usage.input = intOrNull(node.path("message").path("usage").path("input_tokens"));
                } else if ("content_block_delta".equals(type)) {
                    emit(onDelta, node.path("delta").path("text").asText(""));
                } else if ("message_delta".equals(type)) {
                    usage.output = intOrNull(node.path("usage").path("output_tokens"));
                } else if ("message_stop".equals(type)) {
                    return true;
                }
            }
            case GEMINI -> {
                for (JsonNode part : node.path("candidates").path(0).path("content").path("parts")) {
                    emit(onDelta, part.path("text").asText(""));
                }
                readGeminiUsage(node, usage);
            }
            default -> {
                if (node.has("error")) {
                    throw new LlmCallException("PROVIDER_ERROR",
                            "제공자가 답변 중 오류를 보냈습니다: " + node.path("error").path("message").asText(node.path("error").asText()), null);
                }
                emit(onDelta, node.path("choices").path(0).path("delta").path("content").asText(""));
                readOpenAiUsage(node, usage);
            }
        }
        return false;
    }

    /** 스트리밍을 무시하고 JSON 한 덩어리로 답한 경우. */
    Usage readWhole(LlmProvider provider, InputStream body, Consumer<String> onDelta) throws IOException {
        Usage usage = new Usage();
        JsonNode node = objectMapper.readTree(body.readAllBytes());
        switch (provider) {
            case ANTHROPIC -> {
                for (JsonNode block : node.path("content")) {
                    emit(onDelta, block.path("text").asText(""));
                }
                usage.input = intOrNull(node.path("usage").path("input_tokens"));
                usage.output = intOrNull(node.path("usage").path("output_tokens"));
            }
            case GEMINI -> {
                JsonNode chunks = node.isArray() ? node : objectMapper.createArrayNode().add(node);
                for (JsonNode chunk : chunks) {
                    for (JsonNode part : chunk.path("candidates").path(0).path("content").path("parts")) {
                        emit(onDelta, part.path("text").asText(""));
                    }
                    readGeminiUsage(chunk, usage);
                }
            }
            default -> {
                emit(onDelta, node.path("choices").path(0).path("message").path("content").asText(""));
                readOpenAiUsage(node, usage);
            }
        }
        return usage;
    }

    private static void emit(Consumer<String> onDelta, String text) {
        if (!text.isEmpty()) {
            onDelta.accept(text);
        }
    }

    private static void readOpenAiUsage(JsonNode node, Usage usage) {
        JsonNode u = node.path("usage");
        if (u.isObject()) {
            usage.input = intOrNull(u.path("prompt_tokens"));
            usage.output = intOrNull(u.path("completion_tokens"));
        }
    }

    private static void readGeminiUsage(JsonNode node, Usage usage) {
        JsonNode u = node.path("usageMetadata");
        if (u.isObject()) {
            usage.input = intOrNull(u.path("promptTokenCount"));
            usage.output = intOrNull(u.path("candidatesTokenCount"));
        }
    }

    private static Integer intOrNull(JsonNode node) {
        return node.isNumber() ? node.asInt() : null;
    }

    private static void closeQuietly(InputStream stream) {
        try {
            stream.close();
        } catch (IOException ignored) {
            // 감시 타이머가 닫는 중 - 읽던 쪽이 IOException 으로 빠져나온다
        }
    }

    private static long elapsedMs(long startedNanos) {
        return Duration.ofNanos(System.nanoTime() - startedNanos).toMillis();
    }

    private static String trimSlash(String url) {
        String out = url;
        while (out.endsWith("/")) {
            out = out.substring(0, out.length() - 1);
        }
        return out;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
