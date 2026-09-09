package com.company.pipeline.pipeline.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * 그룹 생성·수정 요청.
 *
 * <p>parentId 가 null 이면 «전체 파이프라인 바로 아래»다. 수정에서도 같은 뜻이라,
 * 이름만 바꾸려는 화면은 현재 parentId 를 그대로 실어 보내야 그룹이 최상단으로
 * 올라가지 않는다.
 */
public record PipelineGroupRequest(
        Long parentId,
        @NotBlank(message = "그룹 이름을 입력해주세요.")
        @Size(max = 100, message = "그룹 이름은 100자를 넘을 수 없습니다.")
        String name
) {
}
