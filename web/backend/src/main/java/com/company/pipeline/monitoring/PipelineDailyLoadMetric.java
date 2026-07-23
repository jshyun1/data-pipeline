package com.company.pipeline.monitoring;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDate;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** 일자/파이프라인/태스크별 실제 적재 건수 롤업. V9 마이그레이션. */
@Entity
@Table(name = "pipeline_daily_load_metric")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class PipelineDailyLoadMetric {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "pipeline_source", nullable = false, length = 20)
    private String pipelineSource;

    @Column(name = "pipeline_key", nullable = false, length = 200)
    private String pipelineKey;

    @Column(name = "task_key", nullable = false, length = 200)
    private String taskKey;

    @Column(name = "pipeline_label", length = 200)
    private String pipelineLabel;

    @Column(name = "load_date", nullable = false)
    private LocalDate loadDate;

    @Column(name = "loaded_count", nullable = false)
    private Long loadedCount = 0L;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
}
