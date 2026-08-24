package com.company.pipeline.connector;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.connector.dto.LogSinkConnectorRequest;
import com.company.pipeline.connector.dto.RenderedConnectorConfig;
import com.company.pipeline.connector.dto.SinkConnectorRequest;
import com.company.pipeline.connector.dto.SourceConnectorRequest;
import org.springframework.stereotype.Component;

/** 소스/타겟 DB 유형에 맞는 템플릿을 골라 Connector 설정을 만드는 진입점. */
@Component
public class ConnectorConfigRenderer {

    private final DebeziumOracleTemplate oracleSourceTemplate;
    private final DebeziumPostgresTemplate postgresSourceTemplate;
    private final DebeziumMysqlTemplate mysqlSourceTemplate;
    private final JdbcSinkTemplate jdbcSinkTemplate;

    public ConnectorConfigRenderer(DebeziumOracleTemplate oracleSourceTemplate,
            DebeziumPostgresTemplate postgresSourceTemplate,
            DebeziumMysqlTemplate mysqlSourceTemplate,
            JdbcSinkTemplate jdbcSinkTemplate) {
        this.oracleSourceTemplate = oracleSourceTemplate;
        this.postgresSourceTemplate = postgresSourceTemplate;
        this.mysqlSourceTemplate = mysqlSourceTemplate;
        this.jdbcSinkTemplate = jdbcSinkTemplate;
    }

    public RenderedConnectorConfig renderSource(SourceConnectorRequest request) {
        if (request.sourceDbType() == DbType.ORACLE) {
            return oracleSourceTemplate.render(request);
        }
        if (request.sourceDbType() == DbType.POSTGRESQL) {
            return postgresSourceTemplate.render(request);
        }
        if (request.sourceDbType() == DbType.MYSQL) {
            return mysqlSourceTemplate.render(request);
        }
        throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                "지원하지 않는 소스 DB 유형입니다: " + request.sourceDbType());
    }

    public RenderedConnectorConfig renderSink(SinkConnectorRequest request) {
        return jdbcSinkTemplate.render(request);
    }

    public RenderedConnectorConfig renderLogSink(LogSinkConnectorRequest request) {
        return jdbcSinkTemplate.renderForLogPipeline(request);
    }
}
