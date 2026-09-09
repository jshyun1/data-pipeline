package com.company.pipeline.connection.dto;

import com.company.pipeline.connection.DbType;

/**
 * CDC 소스로 쓸 «개별 테이블»의 준비 상태. 연결 단위 {@link CdcPrerequisiteResponse} 와 달리
 * 스키마·테이블마다 달라지는 조건(Oracle 보충 로깅, Postgres REPLICA IDENTITY)만 담는다.
 */
public record CdcTableReadinessResponse(
        String schema,
        String table,
        DbType dbType,
        /** PASS | WARN | FAIL | UNKNOWN */
        String status,
        String actualValue,
        String guidance
) {
}
