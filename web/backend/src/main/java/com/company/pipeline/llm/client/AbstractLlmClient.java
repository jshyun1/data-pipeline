package com.company.pipeline.llm.client;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.llm.dto.LlmChatMessage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

abstract class AbstractLlmClient implements LlmClient {

    static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(180);
    private static final int RESPONSE_DETAIL_LIMIT = 500;

    final HttpClient httpClient;
    final ObjectMapper objectMapper;

    AbstractLlmClient(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(CONNECT_TIMEOUT)
                .build();
    }

    static String trimSlash(String url) {
        String out = url == null ? "" : url.trim();
        while (out.endsWith("/")) {
            out = out.substring(0, out.length() - 1);
        }
        return out;
    }

    static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    static boolean allowedRole(String role) {
        return "user".equals(role) || "assistant".equals(role);
    }

    static String truncate(String value) {
        if (value == null) {
            return "";
        }
        String flat = value.replaceAll("\\s+", " ").trim();
        return flat.length() > RESPONSE_DETAIL_LIMIT ? flat.substring(0, RESPONSE_DETAIL_LIMIT) + "…" : flat;
    }

    static void rejectIfFailed(int status, String body) {
        if (status / 100 != 2) {
            String detail = truncate(body);
            String message = switch (status) {
                case 401 -> "GPT API 키가 올바르지 않습니다.";
                case 403 -> "GPT API 키에 해당 모델을 사용할 권한이 없습니다.";
                case 404 -> "GPT 모델명 또는 API 주소가 올바르지 않습니다.";
                case 429 -> "GPT 요청 한도 또는 사용량 한도를 초과했습니다.";
                default -> "GPT 제공자가 요청을 거절했습니다(HTTP " + status + ").";
            };
            throw new BusinessException(ErrorCode.LLM_CHAT_ERROR, message + (detail.isBlank() ? "" : " 상세: " + detail));
        }
    }

    static List<Map<String, String>> openAiMessages(String systemPrompt, List<LlmChatMessage> history, String question) {
        List<Map<String, String>> messages = new ArrayList<>();
        messages.add(Map.of("role", "system", "content", systemPrompt));
        for (LlmChatMessage message : history) {
            if (allowedRole(message.role())) {
                messages.add(Map.of("role", message.role(), "content", message.content()));
            }
        }
        messages.add(Map.of("role", "user", "content", question));
        return messages;
    }

    static Integer intOrNull(JsonNode node, String field) {
        return node.has(field) && node.path(field).canConvertToInt() ? node.path(field).asInt() : null;
    }
}
