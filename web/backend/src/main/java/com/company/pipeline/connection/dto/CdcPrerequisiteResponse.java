package com.company.pipeline.connection.dto;

import com.company.pipeline.connection.DbType;
import java.time.LocalDateTime;
import java.util.List;

public record CdcPrerequisiteResponse(
        Long connectionId,
        DbType dbType,
        LocalDateTime checkedAt,
        String overallStatus,
        List<CdcPrerequisiteCheckResponse> checks
) {
}
