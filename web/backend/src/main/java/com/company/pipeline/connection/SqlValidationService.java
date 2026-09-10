package com.company.pipeline.connection;

import com.company.pipeline.connection.dto.SqlValidationResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.springframework.stereotype.Service;

@Service
public class SqlValidationService {

    private static final Pattern PARAMETER_TOKEN = Pattern.compile("#\\{[^}\\n]+}");

    private final SchemaDiscoveryService schemaDiscoveryService;

    public SqlValidationService(SchemaDiscoveryService schemaDiscoveryService) {
        this.schemaDiscoveryService = schemaDiscoveryService;
    }

    public SqlValidationResponse validateSelect(Long connectionId, String sql) {
        long started = System.nanoTime();
        LocalDateTime testedAt = LocalDateTime.now();
        String normalized = normalize(sql);
        if (normalized.isBlank()) {
            return response(false, testedAt, started, "SQL문을 입력하세요.");
        }
        if (!isSelectLike(normalized)) {
            return response(false, testedAt, started, "적재로직 SQL문은 SELECT 또는 WITH 문만 검증할 수 있습니다.");
        }

        String probeSql = "SELECT * FROM (\n%s\n) sql_validation_probe WHERE 1 = 0".formatted(normalized);
        try (Connection jdbc = schemaDiscoveryService.openConnection(connectionId);
             PreparedStatement statement = jdbc.prepareStatement(probeSql)) {
            statement.setQueryTimeout(10);
            statement.setMaxRows(1);
            statement.executeQuery();
            return response(true, testedAt, started, "유효성 체크에 성공했습니다.");
        } catch (Exception ex) {
            return response(false, testedAt, started, safeMessage(ex));
        }
    }

    private String normalize(String sql) {
        String normalized = PARAMETER_TOKEN.matcher(sql == null ? "" : sql).replaceAll("0").trim();
        while (normalized.endsWith(";")) {
            normalized = normalized.substring(0, normalized.length() - 1).trim();
        }
        return normalized;
    }

    private boolean isSelectLike(String sql) {
        String lower = sql.stripLeading().toLowerCase();
        return lower.startsWith("select ") || lower.startsWith("select\n")
                || lower.startsWith("with ") || lower.startsWith("with\n");
    }

    private SqlValidationResponse response(boolean success, LocalDateTime testedAt, long started, String message) {
        return new SqlValidationResponse(success, testedAt,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started), message);
    }

    private String safeMessage(Exception ex) {
        String message = ex.getMessage();
        if (message == null || message.isBlank()) {
            return ex.getClass().getSimpleName();
        }
        return message.replaceAll("(?i)(password|passwd|pwd)=[^&\\s]+", "$1=***");
    }
}
