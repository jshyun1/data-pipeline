package com.company.pipeline.llm.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.company.pipeline.llm.LlmProvider;
import com.company.pipeline.llm.dto.LlmTestResult;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LlmConnectionTesterTest {

    private MockWebServer server;
    private final LlmConnectionTester tester = new LlmConnectionTester();

    @BeforeEach
    void start() throws Exception {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void stop() throws Exception {
        server.shutdown();
    }

    private String base() {
        String url = server.url("/v1/").toString();
        return url;
    }

    @Test
    void openAiCompatible_success_sendsChatCompletionsWithBearer() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{\"choices\":[]}"));

        LlmTestResult result = tester.test(LlmProvider.OPENAI_COMPATIBLE, base(), "llama", "k-123");

        assertThat(result.success()).isTrue();
        RecordedRequest req = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(req.getPath()).isEqualTo("/v1/chat/completions");
        assertThat(req.getHeader("Authorization")).isEqualTo("Bearer k-123");
        assertThat(req.getBody().readUtf8()).contains("\"model\":\"llama\"").contains("max_tokens");
    }

    @Test
    void openAiCompatible_withoutKey_sendsNoAuthorizationHeader() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));

        tester.test(LlmProvider.OPENAI_COMPATIBLE, base(), "llama", null);

        assertThat(server.takeRequest(1, TimeUnit.SECONDS).getHeader("Authorization")).isNull();
    }

    @Test
    void anthropic_usesMessagesEndpointAndApiKeyHeader() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));
        String root = server.url("/").toString();

        tester.test(LlmProvider.ANTHROPIC, root, "claude", "ak");

        RecordedRequest req = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(req.getPath()).isEqualTo("/v1/messages");
        assertThat(req.getHeader("x-api-key")).isEqualTo("ak");
        assertThat(req.getHeader("anthropic-version")).isNotBlank();
    }

    @Test
    void gemini_usesGenerateContentPath() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));
        String root = server.url("/").toString();

        tester.test(LlmProvider.GEMINI, root, "gemini-x", "gk");

        RecordedRequest req = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(req.getPath()).isEqualTo("/v1beta/models/gemini-x:generateContent");
        assertThat(req.getHeader("x-goog-api-key")).isEqualTo("gk");
    }

    @Test
    void classifiesProviderErrors() {
        assertThat(LlmConnectionTester.classify(401, "{}", 5).status()).isEqualTo("AUTH_FAILED");
        assertThat(LlmConnectionTester.classify(403, "{}", 5).status()).isEqualTo("FORBIDDEN");
        assertThat(LlmConnectionTester.classify(404, "{}", 5).status()).isEqualTo("NOT_FOUND");
        assertThat(LlmConnectionTester.classify(429, "{}", 5).status()).isEqualTo("RATE_LIMITED");
        assertThat(LlmConnectionTester.classify(400, "{}", 5).status()).isEqualTo("BAD_REQUEST");
        assertThat(LlmConnectionTester.classify(503, "{}", 5).status()).isEqualTo("PROVIDER_ERROR");
    }

    @Test
    void unreachableHost_isReportedAsUnreachable() throws Exception {
        String dead = server.url("/v1").toString();
        server.shutdown();

        LlmTestResult result = tester.test(LlmProvider.OPENAI_COMPATIBLE, dead, "llama", null);

        assertThat(result.success()).isFalse();
        assertThat(result.status()).isEqualTo("UNREACHABLE");
    }

    @Test
    void missingBaseUrl_isInvalidConfig() {
        assertThat(tester.test(LlmProvider.OPENAI_COMPATIBLE, "  ", "llama", null).status()).isEqualTo("INVALID_CONFIG");
    }
}
