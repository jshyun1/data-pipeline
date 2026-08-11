package com.company.pipeline.connection;

import com.company.pipeline.common.entity.BaseAuditEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "pipeline_connection")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PipelineConnection extends BaseAuditEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 100)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(name = "db_type", nullable = false, length = 30)
    private DbType dbType;

    @Column(nullable = false, length = 200)
    private String host;

    @Column(nullable = false)
    private Integer port;

    @Column(name = "database_name", length = 100)
    private String databaseName;

    @Column(name = "service_name", length = 100)
    private String serviceName;

    @Column(name = "schema_name", length = 100)
    private String schemaName;

    @Column(nullable = false, length = 100)
    private String username;

    @Column(name = "encrypted_password", nullable = false, columnDefinition = "TEXT")
    private String encryptedPassword;

    @Column(name = "jdbc_url", columnDefinition = "TEXT")
    private String jdbcUrl;

    @Column(name = "nifi_controller_service_id", length = 100)
    private String nifiControllerServiceId;

    @Column(name = "nifi_controller_service_name", length = 150)
    private String nifiControllerServiceName;

    @Enumerated(EnumType.STRING)
    @Column(length = 30)
    private ConnectionStatus status = ConnectionStatus.UNKNOWN;

    @Column(name = "last_tested_at")
    private LocalDateTime lastTestedAt;

    public PipelineConnection(String name, DbType dbType, String host, Integer port,
            String databaseName, String serviceName, String schemaName,
            String username, String encryptedPassword, String jdbcUrl) {
        this.name = name;
        this.dbType = dbType;
        this.host = host;
        this.port = port;
        this.databaseName = databaseName;
        this.serviceName = serviceName;
        this.schemaName = schemaName;
        this.username = username;
        this.encryptedPassword = encryptedPassword;
        this.jdbcUrl = jdbcUrl;
        this.status = ConnectionStatus.UNKNOWN;
    }
}
