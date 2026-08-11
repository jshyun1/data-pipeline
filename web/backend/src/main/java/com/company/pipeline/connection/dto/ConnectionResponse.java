package com.company.pipeline.connection.dto;

import com.company.pipeline.connection.ConnectionStatus;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.connection.PipelineConnection;
import java.time.LocalDateTime;

// 비밀번호/암호문 필드는 의도적으로 포함하지 않음.
public record ConnectionResponse(
        Long id,
        String name,
        DbType dbType,
        String host,
        Integer port,
        String databaseName,
        String serviceName,
        String schemaName,
        String username,
        String nifiControllerServiceId,
        String nifiControllerServiceName,
        ConnectionStatus status,
        LocalDateTime lastTestedAt,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
    public static ConnectionResponse from(PipelineConnection entity) {
        return new ConnectionResponse(
                entity.getId(),
                entity.getName(),
                entity.getDbType(),
                entity.getHost(),
                entity.getPort(),
                entity.getDatabaseName(),
                entity.getServiceName(),
                entity.getSchemaName(),
                entity.getUsername(),
                entity.getNifiControllerServiceId(),
                entity.getNifiControllerServiceName(),
                entity.getStatus(),
                entity.getLastTestedAt(),
                entity.getCreatedAt(),
                entity.getUpdatedAt()
        );
    }
}
