package com.company.pipeline.pipeline;

import com.company.pipeline.authz.AccessBits;
import com.company.pipeline.authz.RequirePermission;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.pipeline.dto.PipelineGroupRequest;
import com.company.pipeline.pipeline.dto.PipelineGroupResponse;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** CDC 관리 화면 트리의 그룹(폴더). 조회는 읽기, 만들고 고치고 지우는 것은 쓰기 권한이다. */
@RestController
@RequestMapping("/api/pipeline-groups")
@RequirePermission(system = SystemCode.KAFKA)
public class PipelineGroupController {

    private final PipelineGroupService groupService;

    public PipelineGroupController(PipelineGroupService groupService) {
        this.groupService = groupService;
    }

    @GetMapping
    public ApiResponse<List<PipelineGroupResponse>> list() {
        return ApiResponse.success(groupService.list());
    }

    @PostMapping
    @RequirePermission(system = SystemCode.KAFKA, bits = AccessBits.WRITE)
    public ApiResponse<PipelineGroupResponse> create(@Valid @RequestBody PipelineGroupRequest request) {
        return ApiResponse.success(groupService.create(request));
    }

    @PutMapping("/{id}")
    @RequirePermission(system = SystemCode.KAFKA, bits = AccessBits.WRITE)
    public ApiResponse<PipelineGroupResponse> update(@PathVariable Long id,
                                                     @Valid @RequestBody PipelineGroupRequest request) {
        return ApiResponse.success(groupService.update(id, request));
    }

    @DeleteMapping("/{id}")
    @RequirePermission(system = SystemCode.KAFKA, bits = AccessBits.WRITE)
    public ApiResponse<Void> delete(@PathVariable Long id) {
        groupService.delete(id);
        return ApiResponse.success(null);
    }
}
