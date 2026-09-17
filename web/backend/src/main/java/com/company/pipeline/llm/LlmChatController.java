package com.company.pipeline.llm;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.llm.dto.LlmChatRequest;
import com.company.pipeline.llm.dto.LlmContextPreviewResponse;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/llm/chat")
public class LlmChatController {

    private final LlmChatService service;

    public LlmChatController(LlmChatService service) {
        this.service = service;
    }

    @PostMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter chat(@Valid @RequestBody LlmChatRequest request) {
        return service.stream(request);
    }

    @PostMapping("/preview")
    public ApiResponse<LlmContextPreviewResponse> preview(@Valid @RequestBody LlmChatRequest request) {
        return ApiResponse.success(service.preview(request));
    }
}
