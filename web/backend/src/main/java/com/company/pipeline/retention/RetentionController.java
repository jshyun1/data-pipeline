package com.company.pipeline.retention;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 보존 정책 관리 (U6). 보존일/배치크기/활성화를 조회·수정한다. 하한(min_retention_days)은
 * DB CHECK 제약이 지키므로, 그보다 짧게 주면 400 으로 거절된다(고객이 계약 하한을 못 뚫는다).
 */
@RestController
@RequestMapping("/api/admin/retention")
@com.company.pipeline.authz.RequirePermission(system = com.company.pipeline.authz.SystemCode.ADMIN)
public class RetentionController {

    private final JdbcTemplate jdbc;
    private final RetentionService retentionService;

    public RetentionController(DataSource dataSource, RetentionService retentionService) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.retentionService = retentionService;
    }

    public record UpdatePolicyRequest(Integer retentionDays, Integer batchRows, Boolean enabled) {}

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        return ApiResponse.success(jdbc.queryForList("""
                SELECT table_name, time_column, retention_days, min_retention_days, batch_rows,
                       purge_mode, enabled, last_run_at, last_deleted_rows, last_duration_ms,
                       backlog_rows, last_error
                FROM retention_policy ORDER BY table_name"""));
    }

    @PutMapping("/{table}")
    public ApiResponse<Void> update(@PathVariable String table, @RequestBody UpdatePolicyRequest req) {
        try {
            int updated = jdbc.update("""
                    UPDATE retention_policy SET
                        retention_days = COALESCE(?, retention_days),
                        batch_rows = COALESCE(?, batch_rows),
                        enabled = COALESCE(?, enabled),
                        updated_at = now(), updated_by = 'admin'
                    WHERE table_name = ?
                    """, req.retentionDays(), req.batchRows(), req.enabled(), table);
            if (updated == 0) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR, "보존 정책이 없습니다: " + table);
            }
        } catch (DataAccessException ex) {
            // CHECK 제약 위반(보존일 < 하한, 배치크기 범위 밖 등).
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "보존 정책 값이 허용 범위를 벗어났습니다(하한/배치크기 확인).");
        }
        return ApiResponse.success(null);
    }

    /** 수동 트리거(검증/운영자용). 즉시 한 바퀴 정리한다. */
    @PostMapping("/run")
    public ApiResponse<Void> run() {
        retentionService.runOnce();
        return ApiResponse.success(null);
    }
}
