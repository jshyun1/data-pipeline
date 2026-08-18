package com.company.pipeline.pipeline;

import com.company.pipeline.connection.DbType;
import com.company.pipeline.connection.SchemaDiscoveryService;
import com.company.pipeline.pipeline.dto.PipelineConsistencyCheckResponse;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.springframework.stereotype.Service;

/** 운영 테이블을 스캔하지 않고 DB 통계의 추정 행 수만 비교한다. */
@Service
public class PipelineConsistencyService {
    private final PipelineDefinitionRepository pipelineRepository;
    private final PipelineConsistencyCheckRepository checkRepository;
    private final SchemaDiscoveryService schemaDiscoveryService;

    public PipelineConsistencyService(PipelineDefinitionRepository pipelineRepository,
            PipelineConsistencyCheckRepository checkRepository, SchemaDiscoveryService schemaDiscoveryService) {
        this.pipelineRepository = pipelineRepository;
        this.checkRepository = checkRepository;
        this.schemaDiscoveryService = schemaDiscoveryService;
    }

    public PipelineConsistencyCheckResponse check(Long pipelineId) {
        PipelineDefinition pipeline = pipelineRepository.findById(pipelineId)
                .orElseThrow(() -> new PipelineNotFoundException(pipelineId));
        if (pipeline.getSourceConnectionId() == null) {
            PipelineConsistencyCheck saved = checkRepository.save(new PipelineConsistencyCheck(
                    pipelineId, null, null, "UNSUPPORTED", "로그파일 파이프라인은 소스 DB 통계 검증을 지원하지 않습니다."));
            return PipelineConsistencyCheckResponse.from(saved);
        }
        Long source = estimate(pipeline.getSourceConnectionId(), pipeline.getSourceDbType(), pipeline.getSourceSchema(), pipeline.getSourceTable());
        Long target = estimate(pipeline.getTargetConnectionId(), pipeline.getTargetDbType(), pipeline.getTargetSchema(), pipeline.getTargetTable());
        String result = source == null || target == null ? "UNKNOWN" : source.equals(target) ? "MATCH" : "MISMATCH";
        String message = result.equals("UNKNOWN")
                ? "DB 통계가 없거나 갱신되지 않았습니다. ANALYZE/통계 수집 후 다시 확인하세요."
                : "DB 옵티마이저 통계의 추정 행 수 비교 결과입니다. 정확한 COUNT(*) 결과가 아닙니다.";
        return PipelineConsistencyCheckResponse.from(checkRepository.save(
                new PipelineConsistencyCheck(pipelineId, source, target, result, message)));
    }

    public List<PipelineConsistencyCheckResponse> history(Long pipelineId) {
        if (!pipelineRepository.existsById(pipelineId)) throw new PipelineNotFoundException(pipelineId);
        return checkRepository.findTop20ByPipelineIdOrderByCheckedAtDesc(pipelineId).stream()
                .map(PipelineConsistencyCheckResponse::from).toList();
    }

    private Long estimate(Long connectionId, DbType dbType, String schema, String table) {
        String sql = dbType == DbType.POSTGRESQL
                ? "SELECT CAST(c.reltuples AS BIGINT) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=? AND c.relname=?"
                : "SELECT num_rows FROM all_tables WHERE owner=? AND table_name=?";
        try (Connection connection = schemaDiscoveryService.openConnection(connectionId);
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setQueryTimeout(5);
            statement.setString(1, dbType == DbType.ORACLE ? schema.toUpperCase() : schema);
            statement.setString(2, dbType == DbType.ORACLE ? table.toUpperCase() : table);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) return null;
                long value = resultSet.getLong(1);
                return resultSet.wasNull() ? null : Math.max(value, 0L);
            }
        } catch (SQLException exception) {
            return null;
        }
    }
}
