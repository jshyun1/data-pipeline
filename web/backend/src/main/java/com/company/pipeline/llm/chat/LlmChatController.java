package com.company.pipeline.llm.chat;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.user.AppUser;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * GPT 분석 API(설계 5 · 8절).
 *
 * <p>답변은 SSE({@code text/event-stream})로 흘려보낸다. 이벤트: {@code meta}(전송 대상 · 마스킹 · 생략 여부) → ({@code compacted}: 대화를 줄였을 때)
 * → {@code delta}(글자) …
 * → {@code done}(토큰 수) 또는 {@code error}. data 는 모두 JSON 한 줄이다 - 답에 줄바꿈이 있어도 SSE 형식이 깨지지 않게.
 *
 * <p><b>SseEmitter(비동기) 대신 요청 스레드에서 직접 쓴다.</b> 비동기 디스패치는 JWT 필터(OncePerRequestFilter)를 다시 타지 않아
 * 완료 시점에 인가 필터가 익명으로 보고 거절하는 문제가 있고, 이 화면은 관리 도구라 동시 분석 수가 적다.
 * 검증 · 권한 · 수집은 스트림을 열기 전에 끝내므로, 거기서 난 오류는 다른 API 와 같은 JSON 오류로 응답된다.
 *
 * <p>권한 애노테이션을 달지 않는다 - 대상 종류마다 필요한 권한이 달라 {@link LlmChatService} 가 요청 본문을 보고 확인한다.
 */
@RestController
public class LlmChatController {

    private final LlmChatService service;
    private final ObjectMapper objectMapper;

    public LlmChatController(LlmChatService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    /** [보내는 정보 보기] - 호출하지 않고 전송될 대상 정보만 돌려준다. */
    @PostMapping("/api/llm/chat/preview")
    public ApiResponse<LlmContextPreview> preview(@Valid @RequestBody LlmChatRequest request) {
        return ApiResponse.success(service.prepare(request, currentUserId()).preview());
    }

    @PostMapping("/api/llm/chat")
    public void chat(@Valid @RequestBody LlmChatRequest request, HttpServletResponse response) throws IOException {
        LlmChatService.Prepared prepared = service.prepareChat(request, currentUserId());

        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("text/event-stream");
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        response.setHeader("Cache-Control", "no-cache");
        // nginx 가 이 경로를 버퍼링하지 않게(nginx.conf 의 /api/llm/ 설정과 이중으로 건다).
        response.setHeader("X-Accel-Buffering", "no");
        OutputStream out = response.getOutputStream();

        service.stream(prepared, new LlmChatListener() {
            @Override
            public void meta(LlmContextPreview preview) {
                send(out, "meta", preview);
            }

            @Override
            public void compacted(String mode, int droppedTurns, int droppedPairs, String summary) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("mode", mode);
                data.put("droppedTurns", droppedTurns);
                data.put("droppedPairs", droppedPairs);
                data.put("summary", summary);
                send(out, "compacted", data);
            }

            @Override
            public void delta(String text) {
                send(out, "delta", Map.of("text", text));
            }

            @Override
            public void done(Integer inputTokens, Integer outputTokens, long durationMs) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("inputTokens", inputTokens);
                data.put("outputTokens", outputTokens);
                data.put("durationMs", durationMs);
                send(out, "done", data);
            }

            @Override
            public void error(String code, String message) {
                send(out, "error", Map.of("code", code, "message", message));
            }
        });
    }

    private void send(OutputStream out, String event, Object data) {
        try {
            String frame = "event: " + event + "\ndata: " + objectMapper.writeValueAsString(data) + "\n\n";
            out.write(frame.getBytes(StandardCharsets.UTF_8));
            out.flush();
        } catch (IOException ex) {
            throw new LlmChatService.StreamClosedException(ex);
        }
    }

    private static String currentUserId() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return auth != null && auth.getPrincipal() instanceof AppUser user ? user.getUserId() : null;
    }
}
