package com.company.pipeline.jobcatalog;

import com.company.pipeline.jobcatalog.dto.CalciteSqlValidationResponse;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.springframework.stereotype.Service;

@Service
public class CalciteSqlValidationService {

    public CalciteSqlValidationResponse validate(String sql) {
        long started = System.nanoTime();
        LocalDateTime testedAt = LocalDateTime.now();
        String normalized = normalize(sql);
        if (normalized.isBlank()) {
            return response(false, testedAt, started, "Calcite SQL을 입력하세요.");
        }
        try {
            SqlParser.create(normalized).parseQuery();
            return response(true, testedAt, started, "Calcite 문법 검증에 성공했습니다.");
        } catch (SqlParseException ex) {
            return response(false, testedAt, started, ex.getMessage());
        } catch (Exception ex) {
            return response(false, testedAt, started,
                    ex.getMessage() == null || ex.getMessage().isBlank()
                            ? ex.getClass().getSimpleName()
                            : ex.getMessage());
        }
    }

    private String normalize(String sql) {
        String normalized = sql == null ? "" : sql.trim();
        while (normalized.endsWith(";")) {
            normalized = normalized.substring(0, normalized.length() - 1).trim();
        }
        return normalized;
    }

    private CalciteSqlValidationResponse response(boolean success, LocalDateTime testedAt,
            long started, String message) {
        return new CalciteSqlValidationResponse(success, testedAt,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started), message);
    }
}
