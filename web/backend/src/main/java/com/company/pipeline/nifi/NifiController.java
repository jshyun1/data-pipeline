package com.company.pipeline.nifi;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.nifi.dto.NifiProcessGroupCreateRequest;
import com.company.pipeline.nifi.dto.NifiProcessGroupResponse;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/nifi")
public class NifiController {

    private final NifiClient nifiClient;

    public NifiController(NifiClient nifiClient) {
        this.nifiClient = nifiClient;
    }

    @PostMapping("/process-groups")
    public ApiResponse<NifiProcessGroupResponse> createProcessGroup(
            @Valid @RequestBody NifiProcessGroupCreateRequest request
    ) {
        return ApiResponse.success(nifiClient.createRootProcessGroup(request.name().trim()));
    }
}
