package com.company.pipeline.pipeline.dto;

import com.company.pipeline.pipeline.PipelineGroup;

/** 그룹 한 칸. 트리는 화면이 parentId 로 세운다(계층이 얕아 서버가 미리 접을 이유가 없다). */
public record PipelineGroupResponse(Long id, Long parentId, String name) {

    public static PipelineGroupResponse from(PipelineGroup entity) {
        return new PipelineGroupResponse(entity.getId(), entity.getParentId(), entity.getName());
    }
}
