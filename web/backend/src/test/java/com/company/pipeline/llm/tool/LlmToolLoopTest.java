package com.company.pipeline.llm.tool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.llm.LlmProvider;
import com.company.pipeline.llm.LlmProviderConfig;
import com.company.pipeline.llm.LlmProviderConfigRepository;
import com.company.pipeline.llm.chat.LlmChatListener;
import com.company.pipeline.llm.chat.LlmContextPreview;
import com.company.pipeline.llm.client.LlmChatClient;
import com.company.pipeline.llm.client.LlmChatResult;
import com.company.pipeline.llm.client.LlmMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 도구 호출 반복과 «도구를 못 받는 모델이면 물러나기»(2026-10-02). */
class LlmToolLoopTest {

    private MockWebServer server;
    private final LlmToolbox toolbox = mock(LlmToolbox.class);
    private final LlmProviderConfigRepository repository = mock(LlmProviderConfigRepository.class);
    private LlmToolLoop loop;
    private LlmProviderConfig config;
    private final Recorder listener = new Recorder();
    private final LlmToolbox.Caller caller = new LlmToolbox.Caller("u1", false, 2000);
    private final List<LlmMessage> messages = List.of(LlmMessage.system("역할"), LlmMessage.user("tb_imp018m 구성?"));

    @BeforeEach
    void start() throws Exception {
        server = new MockWebServer();
        server.start();
        loop = new LlmToolLoop(new LlmChatClient(), toolbox, repository);
        config = LlmProviderConfig.create("Qwen3", LlmProvider.OPENAI_COMPATIBLE, "tester");
        config.setId(5L);
        config.setBaseUrl(server.url("/v1").toString());
        config.setModelName("qwen3.8-27b");
        when(repository.findById(5L)).thenReturn(Optional.of(config));
        when(toolbox.specs()).thenReturn(List.of(Map.of("type", "function", "function", Map.of("name", "get_cdc_pipeline"))));
        when(toolbox.execute(eq("get_cdc_pipeline"), any(), any()))
                .thenReturn(new LlmToolbox.Outcome("CDC 파이프라인 «AAA-tb_imp018m» 조회", "소스 CSB.TB_IMP018M → lw_imp018m"));
    }

    @AfterEach
    void stop() throws Exception {
        server.shutdown();
    }

    private static MockResponse json(String body) {
        return new MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody(body);
    }

    @Test
    void toolCall_isExecuted_resultSentBack_andOnlyTheFinalAnswerIsStreamed() throws Exception {
        server.enqueue(json("""
                {"choices":[{"message":{"content":null,"tool_calls":[{"id":"t1","type":"function","function":{"name":"get_cdc_pipeline","arguments":"{\\"pipeline_id\\":2}"}}]}}],"usage":{"prompt_tokens":10,"completion_tokens":3}}"""));
        server.enqueue(json("""
                {"choices":[{"message":{"content":"Oracle CSB.TB_IMP018M 에서 lw_imp018m 으로 적재합니다."}}],"usage":{"prompt_tokens":40,"completion_tokens":9}}"""));

        LlmChatResult result = loop.run(config, "k", messages, 512, listener, caller);

        assertThat(listener.text).hasToString("Oracle CSB.TB_IMP018M 에서 lw_imp018m 으로 적재합니다.");
        assertThat(listener.tools).containsExactly("CDC 파이프라인 «AAA-tb_imp018m» 조회");
        assertThat(result.inputTokens()).isEqualTo(50);
        assertThat(config.getToolSupport()).isEqualTo(LlmProviderConfig.TOOL_SUPPORTED);
        server.takeRequest();
        String second = server.takeRequest().getBody().readUtf8();
        assertThat(second).contains("\"role\":\"tool\"", "\"tool_call_id\":\"t1\"", "lw_imp018m", "[도구]");
    }

    @Test
    void modelRejectingTools_fallsBackWithoutTools_andIsMarkedUnsupported() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(400).setBody("{\"error\":{\"message\":\"tools not supported\"}}"));
        server.enqueue(json("{\"choices\":[{\"message\":{\"content\":\"질문 속 상세로 답합니다.\"}}]}"));

        loop.run(config, "k", messages, 512, listener, caller);

        assertThat(listener.text).hasToString("질문 속 상세로 답합니다.");
        assertThat(listener.toolModes).containsExactly(false);
        assertThat(config.getToolSupport()).isEqualTo(LlmProviderConfig.TOOL_UNSUPPORTED);
        verify(repository).save(config);
        server.takeRequest();
        assertThat(server.takeRequest().getBody().readUtf8()).doesNotContain("\"tools\"", "[도구]");
    }

    @Test
    void toolCallWrittenAsText_isNotShown_andIsExecuted() throws Exception {
        server.enqueue(json("""
                {"choices":[{"message":{"content":"<tool_call>\\n{\\"name\\": \\"get_cdc_pipeline\\", \\"arguments\\": {\\"pipeline_id\\": 2}}\\n</tool_call>"}}]}"""));
        server.enqueue(json("{\"choices\":[{\"message\":{\"content\":\"답\"}}]}"));

        loop.run(config, "k", messages, 512, listener, caller);

        assertThat(listener.text).hasToString("답");
        verify(toolbox).execute(eq("get_cdc_pipeline"), eq("{\"pipeline_id\":2}"), any());
    }

    @Test
    void textCallParser_readsQwenStyleTags() {
        assertThat(loop.parseTextToolCalls("<tool_call>{\"name\":\"search_targets\",\"arguments\":{\"keyword\":\"dz\"}}</tool_call>"))
                .singleElement().satisfies(call -> {
                    assertThat(call.name()).isEqualTo("search_targets");
                    assertThat(call.arguments()).isEqualTo("{\"keyword\":\"dz\"}");
                });
        assertThat(loop.parseTextToolCalls("<tool_call>{깨진 json}</tool_call>")).isEmpty();
    }

    private static final class Recorder implements LlmChatListener {
        final StringBuilder text = new StringBuilder();
        final List<String> tools = new ArrayList<>();
        final List<Boolean> toolModes = new ArrayList<>();

        @Override public void meta(LlmContextPreview preview) { }
        @Override public void compacted(String mode, int droppedTurns, int droppedPairs, String summary) { }
        @Override public void delta(String t) { text.append(t); }
        @Override public void done(Integer inputTokens, Integer outputTokens, long durationMs) { }
        @Override public void error(String code, String message) { }
        @Override public void toolMode(boolean enabled) { toolModes.add(enabled); }
        @Override public void tool(String name, String label) { tools.add(label); }
    }
}
