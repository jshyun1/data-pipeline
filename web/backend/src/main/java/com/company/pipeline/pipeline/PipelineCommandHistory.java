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
import lombok.Setter;

/** 엔티티 쉘만 존재. PipelineDeployService가 생기는 증분에서 실제로 채워진다. */
@Entity
@Table(name = "pipeline_command_history")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PipelineCommandHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "pipeline_id", nullable = false)
    private Long pipelineId;

    @Column(nullable = false, length = 50)
    private String command;

    @Column(length = 30)
    private String result;

    @Column(columnDefinition = "TEXT")
    private String message;

    @Column(name = "requested_by", length = 100)
    private String requestedBy;

    @Column(name = "requested_at")
    private LocalDateTime requestedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;
}
