package com.company.pipeline.llm.client;

import com.company.pipeline.llm.LlmProvider;
import com.company.pipeline.llm.dto.LlmTestResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 짧은 요청 1회로 «이 설정으로 실제 답이 오는가»를 확인하고, 실패는 원인별로 나눠 안내한다.
 *
 * <p>테스트 요청은 제공자마다 형식이 다르다. 2단계(스트리밍 호출)에서 같은 분기를 {@code LlmClient} 로 옮긴다.
 * 여기서는 응답 내용을 보지 않고 «2xx 가 왔는가»와 오류 코드만 본다 - 모델마다 답 모양이 달라도 판정이 흔들리지 않게.
 *
 * <p>시간 제한은 넉넉히 둔다(요청 30초). 내부 서빙 모델은 첫 호출 때 모델을 메모리에 올리느라 수십 초가 걸리기도 한다.
 */
@Component
public class LlmConnectionTester {

    private static final Logger log = LoggerFactory.getLogger(LlmConnectionTester.class);
    private static final String PROBE = "Reply with the single word OK.";
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final int DETAIL_LIMIT = 300;

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public LlmConnectionTester() {
        // HTTP/1.1 고정: 기본값(HTTP_2)은 http:// 주소에 h2c 업그레이드 헤더를 먼저 붙이는데, vLLM(uvicorn) 같은
        // 내부 서빙 서버가 이를 제대로 못 받아 실패하는 경우가 있다. LLM 호출에 HTTP/2 이득도 없다.
        this(HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(CONNECT_TIMEOUT).build());
    }

    LlmConnectionTester(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    /**
     * 테스트하고 결과를 서버 로그에 남긴다. 저장 전 테스트는 DB 에 결과가 남지 않아, 로그가 없으면
     * «화면에 에러가 떴다»는 제보를 서버에서 확인할 방법이 없었다(2026-09-14). 키는 절대 기록하지 않는다 -
     * 제공자·접속 호스트·모델명·결과·응답 요약만.
     */
    public LlmTestResult test(LlmProvider provider, String baseUrl, String modelName, String apiKey) {
        LlmTestResult result = doTest(provider, baseUrl, modelName, apiKey);
        String host = hostOf(baseUrl);
        if (result.success()) {
            log.info("LLM 연결 테스트 성공 provider={} host={} model={} {}ms", provider, host, modelName, result.durationMs());
        } else {
            log.warn("LLM 연결 테스트 실패 provider={} host={} model={} status={} http={} {}ms message={} detail={}",
                    provider, host, modelName, result.status(), result.httpStatus(), result.durationMs(),
                    result.message(), result.detail());
        }
        return result;
    }

    /**
     * 도구 호출(function calling) 을 받아 주는지 확인한다(2026-10-02). OpenAI · OpenAI 호환만 확인한다 - 다른 제공자는 null.
     *
     * <p>도구 하나를 실어 «이 도구를 불러라»고 요청한다. 응답에 {@code tool_calls} 가 오면 SUPPORTED, 요청을 거절(4xx)하거나
     * 도구를 부르지 않고 글로만 답하면 UNSUPPORTED. 연결 오류 · 시간 초과처럼 판정할 수 없으면 null(감지 결과를 바꾸지 않는다).
     */
    public String probeTools(LlmProvider provider, String baseUrl, String modelName, String apiKey) {
        if ((provider != LlmProvider.OPENAI && provider != LlmProvider.OPENAI_COMPATIBLE) || baseUrl == null || baseUrl.isBlank()) {
            return null;
        }
        try {
            Map<String, Object> tool = Map.of("type", "function", "function", Map.of(
                    "name", "lookup_item",
                    "description", "Look up an item by id",
                    "parameters", Map.of("type", "object",
                            "properties", Map.of("id", Map.of("type", "integer")),
                            "required", List.of("id"))));
            Map<String, Object> payload = new java.util.LinkedHashMap<>();
            payload.put("model", modelName);
            payload.put("messages", List.of(Map.of("role", "user", "content", "Call the tool lookup_item with id 1.")));
            payload.put("tools", List.of(tool));
            payload.put("tool_choice", "auto");
            if (provider == LlmProvider.OPENAI_COMPATIBLE) {
                payload.put("max_tokens", 64);
                payload.put("temperature", 0);
            }
            HttpRequest.Builder builder = HttpRequest.newBuilder().timeout(REQUEST_TIMEOUT)
                    .uri(URI.create(trimSlash(baseUrl.trim()) + "/chat/completions"))
                    .header("Content-Type", "application/json");
            if (hasText(apiKey)) {
                builder.header("Authorization", "Bearer " + apiKey);
            }
            HttpResponse<String> response = httpClient.send(builder
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload), StandardCharsets.UTF_8))
                    .build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            String result;
            if (response.statusCode() / 100 == 4) {
                result = "UNSUPPORTED";
            } else if (response.statusCode() / 100 != 2) {
                return null;
            } else {
                JsonNode message = objectMapper.readTree(response.body()).path("choices").path(0).path("message");
                result = message.path("tool_calls").isArray() && message.path("tool_calls").size() > 0
                        ? "SUPPORTED" : "UNSUPPORTED";
            }
            log.info("LLM 도구 호출 확인 provider={} host={} model={} → {}", provider, hostOf(baseUrl), modelName, result);
            return result;
        } catch (IOException | IllegalArgumentException ex) {
            log.warn("LLM 도구 호출 확인 실패(판정 보류) provider={} host={} model={}: {}", provider, hostOf(baseUrl), modelName, ex.getMessage());
            return null;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    private static String hostOf(String baseUrl) {
        try {
            URI uri = URI.create(baseUrl == null ? "" : baseUrl.trim());
            return uri.getHost() == null ? baseUrl : uri.getScheme() + "://" + uri.getHost() + (uri.getPort() > 0 ? ":" + uri.getPort() : "");
        } catch (IllegalArgumentException ex) {
            return "(주소 형식 오류)";
        }
    }

    private LlmTestResult doTest(LlmProvider provider, String baseUrl, String modelName, String apiKey) {
        if (baseUrl == null || baseUrl.isBlank()) {
            return result("INVALID_CONFIG", false, "API 주소가 필요합니다.", null, 0, null);
        }
        HttpRequest request;
        try {
            request = buildRequest(provider, trimSlash(baseUrl.trim()), modelName, apiKey);
        } catch (IllegalArgumentException ex) {
            return result("INVALID_CONFIG", false, "API 주소 형식이 올바르지 않습니다: " + ex.getMessage(), null, 0, null);
        } catch (IOException ex) {
            return result("INVALID_CONFIG", false, "요청을 만들 수 없습니다: " + ex.getMessage(), null, 0, null);
        }

        long started = System.nanoTime();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            return classify(response.statusCode(), response.body(), elapsedMs(started));
        } catch (HttpConnectTimeoutException ex) {
            return result("UNREACHABLE", false,
                    "서버에서 해당 주소로 연결할 수 없습니다(시간 초과). 폐쇄망·방화벽·프록시 설정을 확인하십시오.",
                    null, elapsedMs(started), null);
        } catch (HttpTimeoutException ex) {
            return result("TIMEOUT", false,
                    "응답이 " + REQUEST_TIMEOUT.toSeconds() + "초 안에 오지 않았습니다. 내부 모델이면 처음 불러오는 중일 수 있으니 잠시 뒤 다시 시도하십시오.",
                    null, elapsedMs(started), null);
        } catch (ConnectException ex) {
            return result("UNREACHABLE", false,
                    "서버에서 해당 주소로 연결할 수 없습니다. 주소·포트, 폐쇄망·방화벽·프록시 설정을 확인하십시오.",
                    null, elapsedMs(started), truncate(ex.getMessage()));
        } catch (IOException ex) {
            return result("UNREACHABLE", false,
                    "통신 중 오류가 발생했습니다. 주소와 네트워크 경로를 확인하십시오.",
                    null, elapsedMs(started), truncate(ex.getMessage()));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return result("TIMEOUT", false, "테스트가 중단되었습니다.", null, elapsedMs(started), null);
        }
    }

    private HttpRequest buildRequest(LlmProvider provider, String base, String model, String apiKey) throws IOException {
        HttpRequest.Builder builder = HttpRequest.newBuilder().timeout(REQUEST_TIMEOUT)
                .header("Content-Type", "application/json");
        String body;
        switch (provider) {
            case ANTHROPIC -> {
                builder.uri(URI.create(base + "/v1/messages"))
                        .header("anthropic-version", "2023-06-01");
                if (hasText(apiKey)) {
                    builder.header("x-api-key", apiKey);
                }
                body = objectMapper.writeValueAsString(Map.of(
                        "model", model,
                        "max_tokens", 16,
                        "messages", List.of(Map.of("role", "user", "content", PROBE))));
            }
            case GEMINI -> {
                builder.uri(URI.create(base + "/v1beta/models/" + model + ":generateContent"));
                if (hasText(apiKey)) {
                    builder.header("x-goog-api-key", apiKey);
                }
                body = objectMapper.writeValueAsString(Map.of(
                        "contents", List.of(Map.of("parts", List.of(Map.of("text", PROBE))))));
            }
            case OPENAI, OPENAI_COMPATIBLE -> {
                builder.uri(URI.create(base + "/chat/completions"));
                if (hasText(apiKey)) {
                    builder.header("Authorization", "Bearer " + apiKey);
                }
                Map<String, Object> payload = new java.util.LinkedHashMap<>();
                payload.put("model", model);
                payload.put("messages", List.of(Map.of("role", "user", "content", PROBE)));
                // OpenAI 최신 모델은 max_tokens 를 거절한다(max_completion_tokens 로 바뀜). 호환 서버들은
                // 반대로 max_tokens 만 아는 경우가 많다. 그래서 OpenAI 에는 길이 제한을 넣지 않는다 - 짧은 질문이라 괜찮다.
                if (provider == LlmProvider.OPENAI_COMPATIBLE) {
                    payload.put("max_tokens", 16);
                }
                body = objectMapper.writeValueAsString(payload);
            }
            default -> throw new IllegalArgumentException("지원하지 않는 제공자: " + provider);
        }
        return builder.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build();
    }

    static LlmTestResult classify(int status, String body, long durationMs) {
        String detail = truncate(body);
        if (status / 100 == 2) {
            return result("SUCCESS", true, "연결되었습니다. 응답 시간 " + durationMs + "ms", status, durationMs, null);
        }
        return switch (status) {
            case 401 -> result("AUTH_FAILED", false, "API 키가 올바르지 않습니다.", status, durationMs, detail);
            case 403 -> result("FORBIDDEN", false, "이 키로는 해당 모델을 쓸 권한이 없습니다.", status, durationMs, detail);
            case 404 -> result("NOT_FOUND", false, "모델명 또는 API 주소를 확인하십시오.", status, durationMs, detail);
            case 429 -> result("RATE_LIMITED", false, "요청 한도 또는 사용량 한도를 넘었습니다. 요금제·한도를 확인하십시오.", status, durationMs, detail);
            default -> status / 100 == 4
                    ? result("BAD_REQUEST", false, "제공자가 요청을 거절했습니다(HTTP " + status + "). 모델명과 설정을 확인하십시오.", status, durationMs, detail)
                    : result("PROVIDER_ERROR", false, "제공자 서버 오류입니다(HTTP " + status + "). 잠시 뒤 다시 시도하십시오.", status, durationMs, detail);
        };
    }

    private static LlmTestResult result(String status, boolean success, String message, Integer http, long ms, String detail) {
        return new LlmTestResult(status, success, message, http, ms, detail);
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

    private static String truncate(String value) {
        if (value == null) {
            return null;
        }
        String flat = value.replaceAll("\\s+", " ").trim();
        return flat.length() > DETAIL_LIMIT ? flat.substring(0, DETAIL_LIMIT) + "…" : flat;
    }
}
