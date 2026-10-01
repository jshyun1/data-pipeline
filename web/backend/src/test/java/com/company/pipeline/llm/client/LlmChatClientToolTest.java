package com.company.pipeline.llm.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.company.pipeline.llm.LlmProvider;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 도구 호출(function calling) 요청 · 응답 형식(2026-10-02). 스트림 모양은 KT AI Nexus vLLM(Qwen3) 실측 그대로다. */
class LlmChatClientToolTest {

    private MockWebServer server;
    private final LlmChatClient client = new LlmChatClient();
    private final List<Map<String, Object>> tools = List.of(Map.of("type", "function",
            "function", Map.of("name", "get_cdc_pipeline", "parameters", Map.of("type", "object"))));

    @BeforeEach
    void start() throws Exception {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void stop() throws Exception {
        server.shutdown();
    }

    @Test
    void streamedToolCallChunks_areJoinedIntoOneCall_andToolsAreSent() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setHeader("Content-Type", "text/event-stream").setBody("""
                data: {"choices":[{"delta":{"role":"assistant","content":null}}]}

                data: {"choices":[{"delta":{"tool_calls":[{"id":"chatcmpl-tool-1","type":"function","index":0,"function":{"name":"get_cdc_pipeline"}}]}}]}

                data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\\"pipeline_id\\": "}}]}}]}

                data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"3}"}}]},"finish_reason":"tool_calls"}]}

                data: [DONE]

                """));
        StringBuilder out = new StringBuilder();

        LlmChatResult result = client.stream(LlmProvider.OPENAI_COMPATIBLE, server.url("/v1").toString(), "qwen3.8-27b",
                "k", List.of(LlmMessage.system("역할"), LlmMessage.user("파이프라인 3 구성")), 256, out::append, tools);

        assertThat(out).isEmpty();
        assertThat(result.hasToolCalls()).isTrue();
        assertThat(result.toolCalls()).singleElement().satisfies(call -> {
            assertThat(call.id()).isEqualTo("chatcmpl-tool-1");
            assertThat(call.name()).isEqualTo("get_cdc_pipeline");
            assertThat(call.arguments()).isEqualTo("{\"pipeline_id\": 3}");
        });
        RecordedRequest request = server.takeRequest(1, TimeUnit.SECONDS);
        assertThat(request.getBody().readUtf8()).contains("\"tools\":[", "\"tool_choice\":\"auto\"");
    }

    @Test
    void toolTurnsAreSerialisedInOpenAiShape_andNoToolsMeansNoToolsField() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json")
                .setBody("{\"choices\":[{\"message\":{\"content\":\"구성은 …\"}}]}"));
        List<LlmMessage> messages = List.of(
                LlmMessage.user("질문"),
                LlmMessage.assistantToolCalls("", List.of(new LlmMessage.ToolCall("c1", "get_etl_job", "{\"job_id\":28}"))),
                LlmMessage.toolResult("c1", "job 상세"));

        LlmChatResult result = client.stream(LlmProvider.OPENAI_COMPATIBLE, server.url("/v1").toString(), "m", null,
                messages, 128, text -> { }, null);

        assertThat(result.text()).isEqualTo("구성은 …");
        assertThat(result.hasToolCalls()).isFalse();
        String body = server.takeRequest(1, TimeUnit.SECONDS).getBody().readUtf8();
        assertThat(body).contains("\"tool_calls\":[{", "\"id\":\"c1\"", "\"type\":\"function\"", "\"name\":\"get_etl_job\"",
                "\"role\":\"tool\"", "\"tool_call_id\":\"c1\"").doesNotContain("\"tools\"");
    }
}
