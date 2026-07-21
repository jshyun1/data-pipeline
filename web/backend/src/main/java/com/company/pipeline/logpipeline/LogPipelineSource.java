package com.company.pipeline.logpipeline;

import com.company.pipeline.common.entity.BaseAuditEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "log_pipeline_source")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class LogPipelineSource extends BaseAuditEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "pipeline_id", nullable = false)
    private Long pipelineId;

    @Column(name = "agent_host", length = 200)
    private String agentHost;

    @Column(name = "file_path", nullable = false, columnDefinition = "TEXT")
    private String filePath;

    @Column(name = "file_pattern", length = 200)
    private String filePattern;

    // BEGINNING, END
    @Column(name = "read_from", length = 30)
    private String readFrom = "END";

    // PLAIN, JSON, REGEX, DELIMITER - 이번 증분은 PLAIN만 지원(서비스 레이어에서 검증)
    @Column(name = "parse_type", length = 30)
    private String parseType = "PLAIN";

    @Column(length = 30)
    private String encoding = "UTF-8";

    @Column(name = "multiline_enabled")
    private Boolean multilineEnabled = false;

    @Column(name = "topic_name", nullable = false, length = 200)
    private String topicName;

    public LogPipelineSource(Long pipelineId, String agentHost, String filePath, String filePattern,
            String readFrom, String parseType, String encoding, Boolean multilineEnabled, String topicName) {
        this.pipelineId = pipelineId;
        this.agentHost = agentHost;
        this.filePath = filePath;
        this.filePattern = filePattern;
        this.readFrom = readFrom != null ? readFrom : "END";
        this.parseType = parseType != null ? parseType : "PLAIN";
        this.encoding = encoding != null ? encoding : "UTF-8";
        this.multilineEnabled = multilineEnabled != null ? multilineEnabled : false;
        this.topicName = topicName;
    }
}
