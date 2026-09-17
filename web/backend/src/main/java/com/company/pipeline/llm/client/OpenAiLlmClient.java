package com.company.pipeline.llm.client;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.llm.LlmProvider;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.springframework.stereotype.Component;

@Component
public class OpenAiLlmClient extends AbstractLlmClient {

    public OpenAiLlmClient(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public boolean supports(LlmProvider provider) {
        return provider == LlmProvider.OPENAI;
    }

    @Override
    public LlmChatResult stream(LlmChatPrompt prompt, Consumer<String> onDelta) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("model", prompt.modelName());
        payload.put("messages", openAiMessages(prompt.systemPrompt(), prompt.history(), prompt.question()));
        payload.put("temperature", 0.2);
        payload.put("stream", true);
        payload.put("stream_options", Map.of("include_usage", true));
        return streamChatCompletions(prompt, payload, onDelta);
    }

    LlmChatResult streamChatCompletions(LlmChatPrompt prompt, Map<String, Object> payload, Consumer<String> onDelta) {
        StringBuilder answer = new StringBuilder();
        Integer inputTokens = null;
        Integer outputTokens = null;
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .timeout(REQUEST_TIMEOUT)
                    .uri(URI.create(trimSlash(prompt.baseUrl()) + "/chat/completions"))
                    .header("Content-Type", "application/json");
            if (hasText(prompt.apiKey())) {
                builder.header("Authorization", "Bearer " + prompt.apiKey());
            }
            HttpRequest request = builder.POST(HttpRequest.BodyPublishers.ofString(
                    objectMapper.writeValueAsString(payload), StandardCharsets.UTF_8)).build();
            HttpResponse<Stream<String>> response = httpClient.send(request, HttpResponse.BodyHandlers.ofLines());
            if (response.statusCode() / 100 != 2) {
                String body = String.join(" ", response.body().toList());
                rejectIfFailed(response.statusCode(), body);
            }
            try (Stream<String> lines = response.body()) {
                var iterator = lines.iterator();
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    if (!line.startsWith("data:")) {
                        continue;
                    }
                    String data = line.substring("data:".length()).trim();
                    if ("[DONE]".equals(data) || data.isBlank()) {
                        continue;
                    }
                    JsonNode root = objectMapper.readTree(data);
                    JsonNode usage = root.path("usage");
                    if (!usage.isMissingNode() && !usage.isNull()) {
                        inputTokens = intOrNull(usage, "prompt_tokens");
                        outputTokens = intOrNull(usage, "completion_tokens");
                    }
                    JsonNode choices = root.path("choices");
                    if (choices.isArray() && choices.size() > 0) {
                        String delta = choices.get(0).path("delta").path("content").asText("");
                        if (hasText(delta)) {
                            answer.append(delta);
                            onDelta.accept(delta);
                        }
                    }
                }
            }
            if (!hasText(answer.toString())) {
                throw new BusinessException(ErrorCode.LLM_CHAT_ERROR, "LLM 응답 파싱 실패: GPT 응답에서 답변을 찾을 수 없습니다.");
            }
            return new LlmChatResult(answer.toString().strip(), inputTokens, outputTokens);
        } catch (ConnectException ex) {
            throw new BusinessException(ErrorCode.LLM_CHAT_ERROR, "GPT 서버에 연결할 수 없습니다. 주소와 네트워크 경로를 확인하십시오.", ex);
        } catch (IOException ex) {
            throw new BusinessException(ErrorCode.LLM_CHAT_ERROR, "GPT 통신 중 오류가 발생했습니다: " + ex.getMessage(), ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new BusinessException(ErrorCode.LLM_CHAT_ERROR, "GPT 요청이 중단되었습니다.", ex);
        }
    }
}
