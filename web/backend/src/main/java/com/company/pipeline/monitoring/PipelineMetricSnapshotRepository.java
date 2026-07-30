package com.company.pipeline.monitoring;

import com.company.pipeline.monitoring.dto.HourlyCountProjection;
import java.util.List;
import java.util.Optional;
import java.time.LocalDateTime;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PipelineMetricSnapshotRepository extends JpaRepository<PipelineMetricSnapshot, Long> {

    Optional<PipelineMetricSnapshot> findTopByPipelineIdOrderByCollectedAtDesc(Long pipelineId);

    List<PipelineMetricSnapshot> findTop2ByPipelineIdOrderByCollectedAtDesc(Long pipelineId);

    Optional<PipelineMetricSnapshot> findTopByPipelineIdAndCollectedAtBeforeOrderByCollectedAtDesc(
            Long pipelineId, LocalDateTime collectedAt);

    List<PipelineMetricSnapshot> findByPipelineIdInAndCollectedAtBetweenOrderByCollectedAtAsc(
            List<Long> pipelineIds, LocalDateTime from, LocalDateTime to);

    /**
     * 시간대별 CDC 처리 건수. CDC는 "적재 1회"라는 이벤트가 없어서 일자별 집계와 똑같이
     * committed offset의 증가분을 건수로 본다(KafkaPipelineMetricScheduler가 일자별 롤업에
     * 넣는 값과 같은 정의).
     *
     * <p>커넥터를 다시 만들면 offset이 되감기면서 증가분이 음수가 되므로 0으로 눌러
     * 실제로 처리한 적 없는 건수가 끼지 않게 한다.
     */
    @Query(
            value = """
            WITH deltas AS (
                SELECT CAST(collected_at AS DATE) AS date,
                       CAST(EXTRACT(HOUR FROM collected_at) AS INTEGER) AS hour,
                       GREATEST(committed_offset
                           - LAG(committed_offset) OVER (PARTITION BY pipeline_id ORDER BY collected_at), 0) AS delta
                FROM pipeline_metric_snapshot
                WHERE collected_at >= :from AND collected_at < :to AND committed_offset IS NOT NULL
            )
            SELECT date, hour, SUM(delta) AS count
            FROM deltas
            WHERE delta IS NOT NULL
            GROUP BY date, hour
            ORDER BY date, hour
            """,
            nativeQuery = true)
    List<HourlyCountProjection> findHourlyCommittedDeltas(
            @Param("from") LocalDateTime from, @Param("to") LocalDateTime to);
}
