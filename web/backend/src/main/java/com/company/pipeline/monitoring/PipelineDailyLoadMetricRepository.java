package com.company.pipeline.monitoring;

import com.company.pipeline.monitoring.dto.DailyCountProjection;
import com.company.pipeline.monitoring.dto.KeyedCountProjection;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface PipelineDailyLoadMetricRepository extends JpaRepository<PipelineDailyLoadMetric, Long> {

    Optional<PipelineDailyLoadMetric> findTopByPipelineSourceAndPipelineKeyOrderByUpdatedAtDesc(
            String pipelineSource, String pipelineKey);

    /**
     * 오늘 날짜 row가 없으면 만들고, 있으면 loaded_count에 더한다(원자적 UPSERT).
     * (pipeline_source, pipeline_key, task_key, load_date) 유니크 제약을 그대로 이용.
     */
    @Modifying
    @Transactional
    @Query(
            value = """
            INSERT INTO pipeline_daily_load_metric
                (pipeline_source, pipeline_key, task_key, pipeline_label, load_date, loaded_count, updated_at)
            VALUES (:source, :key, :task, :label, :date, :count, now())
            ON CONFLICT (pipeline_source, pipeline_key, task_key, load_date)
            DO UPDATE SET
                loaded_count = pipeline_daily_load_metric.loaded_count + EXCLUDED.loaded_count,
                pipeline_label = EXCLUDED.pipeline_label,
                updated_at = now()
            """,
            nativeQuery = true)
    void upsertIncrement(
            @Param("source") String source,
            @Param("key") String key,
            @Param("task") String task,
            @Param("label") String label,
            @Param("date") LocalDate date,
            @Param("count") long count);

    @Query(
            value = "SELECT load_date AS date, SUM(loaded_count) AS count FROM pipeline_daily_load_metric "
                    + "WHERE load_date BETWEEN :from AND :to "
                    + "AND (CAST(:source AS VARCHAR) IS NULL OR pipeline_source = :source) "
                    + "GROUP BY load_date ORDER BY load_date",
            nativeQuery = true)
    List<DailyCountProjection> findDailyTotals(
            @Param("from") LocalDate from, @Param("to") LocalDate to, @Param("source") String source);

    @Query(
            value = "SELECT pipeline_key AS key, MAX(pipeline_label) AS label, SUM(loaded_count) AS count "
                    + "FROM pipeline_daily_load_metric WHERE load_date BETWEEN :from AND :to "
                    + "AND (CAST(:source AS VARCHAR) IS NULL OR pipeline_source = :source) "
                    + "GROUP BY pipeline_key ORDER BY SUM(loaded_count) DESC LIMIT :limit",
            nativeQuery = true)
    List<KeyedCountProjection> findTopPipelines(
            @Param("from") LocalDate from, @Param("to") LocalDate to,
            @Param("source") String source, @Param("limit") int limit);

    @Query(
            value = "SELECT pipeline_key || '.' || task_key AS key, "
                    + "MAX(pipeline_label) || '.' || task_key AS label, SUM(loaded_count) AS count "
                    + "FROM pipeline_daily_load_metric WHERE load_date BETWEEN :from AND :to "
                    + "AND (CAST(:source AS VARCHAR) IS NULL OR pipeline_source = :source) "
                    + "GROUP BY pipeline_key, task_key ORDER BY SUM(loaded_count) DESC LIMIT :limit",
            nativeQuery = true)
    List<KeyedCountProjection> findTopTasks(
            @Param("from") LocalDate from, @Param("to") LocalDate to,
            @Param("source") String source, @Param("limit") int limit);
}
