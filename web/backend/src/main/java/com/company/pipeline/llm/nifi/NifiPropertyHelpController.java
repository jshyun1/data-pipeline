package com.company.pipeline.llm.nifi;

import com.company.pipeline.common.ApiResponse;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class NifiPropertyHelpController {

    private final NifiPropertyHelpService service;

    public NifiPropertyHelpController(NifiPropertyHelpService service) {
        this.service = service;
    }

    @PostMapping("/api/llm/nifi/property-help")
    public ApiResponse<NifiPropertyHelpResponse> interpret(@Valid @RequestBody NifiPropertyHelpRequest request) {
        return ApiResponse.success(service.interpret(request));
    }
}
