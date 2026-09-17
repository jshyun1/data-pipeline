package com.company.pipeline.llm.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.company.pipeline.llm.LlmProvider;
import java.util.List;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LlmChatClientTest {

    private MockWebServer server;
    private final LlmChatClient client = new LlmChatClient();
    private final List<LlmMessage> messages = List.of(
            LlmMessage.system("역할"), LlmMessage.user("분석해 줘"), LlmMessage.assistant("첫 답"), LlmMessage.user("추가 질문"));

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
        return server.url("/v1").toString();
    }

    private static MockResponse sse(String body) {
        return new MockResponse().setResponseCode(200).setHeader("Content-Type", "text/event-stream").setBody(body);
    }

    @Test
    void openAiCompatible_streamsDeltasInOrder_andSendsMaxTokensWithBearer() throws Exception {
        server.enqueue(sse("""
                data: {"choices":[{"delta":{"role":"assistant"}}]}

                data: {"choices":[{"delta":{"content":"원인 "}}]}

                : keep-alive

                data: {"choices":[{"delta":{"content":"추정"}}]}

                data: {"choices":[],"usage":{"prompt_tokens":120,"completion_tokens":7}}

                data: [DONE]

                """));
        StringBuilder out = new StringBuilder();

        LlmChatResult result = client.stream(LlmProvider.OPENAI_COMPATIBLE, base(), "gemma4", "k-1", messages, 256, out::append);

        assertThat(out).hasToString("원인 추정");
        assertThat(result.inputTokens()).isEqualTo(120);
        assertThat(result.outputTokens()).isEqualTo(7);
        RecordedRequest request = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(request.getPath()).isEqualTo("/v1/chat/completions");
        assertThat(request.getHeader("Authorization")).isEqualTo("Bearer k-1");
        String body = request.getBody().readUtf8();
        assertThat(body).contains("\"stream\":true", "\"max_tokens\":256", "\"role\":\"system\"")
                .doesNotContain("stream_options");
    }

    @Test
    void serverIgnoringStream_jsonBodyIsDeliveredAtOnce() {
        server.enqueue(new MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json")
                .setBody("{\"choices\":[{\"message\":{\"content\":\"한 번에 온 답\"}}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3}}"));
        StringBuilder out = new StringBuilder();

        LlmChatResult result = client.stream(LlmProvider.OPENAI_COMPATIBLE, base(), "gemma4", null, messages, 256, out::append);

        assertThat(out).hasToString("한 번에 온 답");
        assertThat(result.outputTokens()).isEqualTo(3);
    }

    @Test
    void anthropic_systemGoesToSeparateField_andEventsAreParsed() throws Exception {
        server.enqueue(sse("""
                event: message_start
                data: {"type":"message_start","message":{"usage":{"input_tokens":50}}}

                event: content_block_delta
                data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"안녕"}}

                event: message_delta
                data: {"type":"message_delta","usage":{"output_tokens":2}}

                event: message_stop
                data: {"type":"message_stop"}

                """));
        StringBuilder out = new StringBuilder();

        LlmChatResult result = client.stream(LlmProvider.ANTHROPIC, server.url("/").toString(), "claude", "ak", messages, 100, out::append);

        assertThat(out).hasToString("안녕");
        assertThat(result.inputTokens()).isEqualTo(50);
        assertThat(result.outputTokens()).isEqualTo(2);
        RecordedRequest request = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(request.getPath()).isEqualTo("/v1/messages");
        assertThat(request.getHeader("x-api-key")).isEqualTo("ak");
        String body = request.getBody().readUtf8();
        assertThat(body).contains("\"system\":\"역할\"").doesNotContain("\"role\":\"system\"");
    }

    @Test
    void gemini_usesModelRole_andSseAlt() throws Exception {
        server.enqueue(sse("""
                data: {"candidates":[{"content":{"parts":[{"text":"원인"}]}}]}

                data: {"candidates":[{"content":{"parts":[{"text":" 추정"}]}}],"usageMetadata":{"promptTokenCount":9,"candidatesTokenCount":4}}

                """));
        StringBuilder out = new StringBuilder();

        LlmChatResult result = client.stream(LlmProvider.GEMINI, server.url("/").toString(), "gemini-2.5", "gk", messages, 100, out::append);

        assertThat(out).hasToString("원인 추정");
        assertThat(result.outputTokens()).isEqualTo(4);
        RecordedRequest request = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(request.getPath()).isEqualTo("/v1beta/models/gemini-2.5:streamGenerateContent?alt=sse");
        String body = request.getBody().readUtf8();
        assertThat(body).contains("\"role\":\"model\"", "systemInstruction");
    }

    @Test
    void httpError_isClassifiedLikeConnectionTest() {
        server.enqueue(new MockResponse().setResponseCode(401).setBody("{\"error\":\"bad key\"}"));

        assertThatThrownBy(() -> client.stream(LlmProvider.OPENAI_COMPATIBLE, base(), "gemma4", "wrong", messages, 10, t -> { }))
                .isInstanceOf(LlmCallException.class)
                .satisfies(ex -> {
                    assertThat(((LlmCallException) ex).getCode()).isEqualTo("AUTH_FAILED");
                    assertThat(((LlmCallException) ex).getHttpStatus()).isEqualTo(401);
                });
    }

    @Test
    void listenerFailure_stopsStreamingAndPropagates() {
        server.enqueue(sse("data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n"));
        IllegalStateException closed = new IllegalStateException("화면 연결 끊김");

        assertThatThrownBy(() -> client.stream(LlmProvider.OPENAI_COMPATIBLE, base(), "gemma4", null, messages, 10, t -> {
            throw closed;
        })).isSameAs(closed);
    }
}
