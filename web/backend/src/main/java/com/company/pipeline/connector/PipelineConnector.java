package com.company.pipeline.connector;

import com.company.pipeline.common.entity.BaseAuditEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
@Table(name = "pipeline_connector")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PipelineConnector extends BaseAuditEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "pipeline_id", nullable = false)
    private Long pipelineId;

    @Column(name = "connector_role", nullable = false, length = 30)
    private String connectorRole;

    @Column(name = "connector_name", nullable = false, length = 200)
    private String connectorName;

    @Column(name = "connector_class", nullable = false, length = 300)
    private String connectorClass;

    @Column(name = "connector_config_json", nullable = false, columnDefinition = "TEXT")
    private String connectorConfigJson;

    @Column(name = "connect_cluster_url", length = 300)
    private String connectClusterUrl = "http://kafka-connect:8083";

    @Column(length = 30)
    private String status = "CREATED";

    @Column(name = "last_status_json", columnDefinition = "TEXT")
    private String lastStatusJson;

    @Column(name = "deployed_at")
    private LocalDateTime deployedAt;

    public PipelineConnector(Long pipelineId, String connectorRole, String connectorName,
            String connectorClass, String connectorConfigJson) {
        this.pipelineId = pipelineId;
        this.connectorRole = connectorRole;
        this.connectorName = connectorName;
        this.connectorClass = connectorClass;
        this.connectorConfigJson = connectorConfigJson;
        this.status = "CREATED";
    }
}
