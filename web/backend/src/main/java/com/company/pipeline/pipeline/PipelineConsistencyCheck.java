package com.company.pipeline.pipeline;

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

@Entity
@Table(name = "pipeline_consistency_check")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PipelineConsistencyCheck {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(name = "pipeline_id", nullable = false)
    private Long pipelineId;
    @Column(name = "check_mode", nullable = false, length = 30)
    private String checkMode;
    @Column(name = "source_count")
    private Long sourceCount;
    @Column(name = "target_count")
    private Long targetCount;
    @Column(nullable = false, length = 30)
    private String result;
    @Column(columnDefinition = "TEXT")
    private String message;
    @Column(name = "checked_at", nullable = false)
    private LocalDateTime checkedAt;

    public PipelineConsistencyCheck(Long pipelineId, Long sourceCount, Long targetCount, String result, String message) {
        this.pipelineId = pipelineId;
        this.checkMode = "STATISTICS_ESTIMATE";
        this.sourceCount = sourceCount;
        this.targetCount = targetCount;
        this.result = result;
        this.message = message;
        this.checkedAt = LocalDateTime.now();
    }
}
