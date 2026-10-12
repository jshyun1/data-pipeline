package com.company.pipeline.pipeline.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.List;

/**
 * 파이프라인 정의 수정(2026-10-12). 저장하면 정의(DB) 값만 바뀌고 등록된 커넥터는 그대로다 - 다음 준비(배포) 때 반영된다.
 *
 * <p>고칠 수 있는 값: 이름 · 설명 · 그룹(실행 영향 없음), 제외 · 마스킹 컬럼 · DELETE 반영(커넥터 설정만 바뀌고 이름 · 오프셋은 그대로).
 *
 * <p>잠긴 값(연결 · 소스/타깃 스키마 · 테이블 · Topic Prefix · 적재 방식 · 구분컬럼 · 스냅샷 모드)은 화면이 지금 값을 그대로 보낸다.
 * 보냈으면 지금 값과 같은지만 확인하고, 다르면 거절한다(화면 우회로 바꾸지 못하게). 안 보내면(null) 확인하지 않는다.
 *
 * @param excludedColumns null 이면 그대로, 빈 목록이면 모두 해제
 * @param maskedColumns   null 이면 그대로, 빈 목록이면 모두 해제
 * @param deleteEnabled   null 이면 그대로
 */
public record PipelineUpdateRequest(
        @NotBlank @Size(max = 150) String name,
        String description,
        Long groupId,
        List<String> excludedColumns,
        List<String> maskedColumns,
        Boolean deleteEnabled,
        Long sourceConnectionId,
        Long targetConnectionId,
        String sourceSchema,
        String sourceTable,
        String targetSchema,
        String targetTable,
        String topicPrefix,
        String loadMode,
        String deltaOpColumn,
        String snapshotMode
) {
}
