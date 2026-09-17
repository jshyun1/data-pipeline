package com.company.pipeline.llm.client;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.llm.LlmProvider;
import com.company.pipeline.llm.dto.LlmChatMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.springframework.stereotype.Component;

@Component
public class AnthropicLlmClient extends AbstractLlmClient {

    public AnthropicLlmClient(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public boolean supports(LlmProvider provider) {
        return provider == LlmProvider.ANTHROPIC;
    }

    @Override
    public LlmChatResult stream(LlmChatPrompt prompt, Consumer<String> onDelta) {
        StringBuilder answer = new StringBuilder();
        Integer outputTokens = null;
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .timeout(REQUEST_TIMEOUT)
                    .uri(URI.create(trimSlash(prompt.baseUrl()) + "/v1/messages"))
                    .header("Content-Type", "application/json")
                    .header("anthropic-version", "2023-06-01");
            if (hasText(prompt.apiKey())) {
                builder.header("x-api-key", prompt.apiKey());
            }
            String body = objectMapper.writeValueAsString(Map.of(
                    "model", prompt.modelName(),
                    "max_tokens", 1200,
                    "stream", true,
                    "system", prompt.systemPrompt(),
                    "messages", messages(prompt.history(), prompt.question())));
            HttpResponse<Stream<String>> response = httpClient.send(builder.POST(
                    HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)).build(),
                    HttpResponse.BodyHandlers.ofLines());
            if (response.statusCode() / 100 != 2) {
                rejectIfFailed(response.statusCode(), String.join(" ", response.body().toList()));
            }
            try (Stream<String> lines = response.body()) {
                var iterator = lines.iterator();
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    if (!line.startsWith("data:")) {
                        continue;
                    }
                    String data = line.substring("data:".length()).trim();
                    if (data.isBlank() || "[DONE]".equals(data)) {
                        continue;
                    }
                    JsonNode root = objectMapper.readTree(data);
                    String type = root.path("type").asText();
                    if ("content_block_delta".equals(type)) {
                        String delta = root.path("delta").path("text").asText("");
                        if (hasText(delta)) {
                            answer.append(delta);
                            onDelta.accept(delta);
                        }
                    } else if ("message_delta".equals(type)) {
                        JsonNode usage = root.path("usage");
                        if (!usage.isMissingNode()) {
                            outputTokens = intOrNull(usage, "output_tokens");
                        }
                    }
                }
            }
            if (!hasText(answer.toString())) {
                throw new BusinessException(ErrorCode.LLM_CHAT_ERROR, "LLM 응답 파싱 실패: GPT 응답에서 답변을 찾을 수 없습니다.");
            }
            return new LlmChatResult(answer.toString().strip(), null, outputTokens);
        } catch (ConnectException ex) {
            throw new BusinessException(ErrorCode.LLM_CHAT_ERROR, "GPT 서버에 연결할 수 없습니다. 주소와 네트워크 경로를 확인하십시오.", ex);
        } catch (IOException ex) {
            throw new BusinessException(ErrorCode.LLM_CHAT_ERROR, "GPT 통신 중 오류가 발생했습니다: " + ex.getMessage(), ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new BusinessException(ErrorCode.LLM_CHAT_ERROR, "GPT 요청이 중단되었습니다.", ex);
        }
    }

    private static List<Map<String, Object>> messages(List<LlmChatMessage> history, String question) {
        List<Map<String, Object>> messages = new ArrayList<>();
        for (LlmChatMessage message : history) {
            if (allowedRole(message.role())) {
                messages.add(Map.of("role", message.role(), "content", message.content()));
            }
        }
        messages.add(Map.of("role", "user", "content", question));
        return messages;
    }
}
