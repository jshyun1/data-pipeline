package com.company.pipeline.monitoring;

import com.company.pipeline.monitoring.dto.HourlyCountProjection;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface NifiExecutionLogEntryRepository extends JpaRepository<NifiExecutionLogEntry, Long> {

    List<NifiExecutionLogEntry> findByOccurredAtBetweenOrderByOccurredAtDesc(LocalDateTime from, LocalDateTime to);

    /**
     * 시간대별 적재 건수. 일자별 집계(pipeline_daily_load_metric)와 같은 카운터 증가분에서
     * 만들어진 행들이므로(NifiPipelineMetricScheduler가 증가분 1건마다 여기에 SUCCESS 행을
     * 남기고 같은 값으로 일자별 롤업을 올린다) 두 차트의 기간 합계가 서로 어긋나지 않는다.
     */
    @Query(
            value = """
            SELECT CAST(occurred_at AS DATE) AS date,
                   CAST(EXTRACT(HOUR FROM occurred_at) AS INTEGER) AS hour,
                   SUM(inserted_count) AS count
            FROM nifi_execution_log
            WHERE occurred_at >= :from AND occurred_at < :to
              AND status = 'SUCCESS' AND inserted_count IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 1, 2
            """,
            nativeQuery = true)
    List<HourlyCountProjection> findHourlyInsertedTotals(
            @Param("from") LocalDateTime from, @Param("to") LocalDateTime to);

    /**
     * 최근 {@code since} 이후에 같은 bulletin id를 이미 넣었는지.
     *
     * <p>bulletin id는 NiFi 프로세스 안에서만 단조 증가하고 재시작하면 1부터 다시
     * 시작한다. 그래서 id만으로 중복 판정을 하면 재시작 이후의 새 실패가 "이미 본
     * 것"으로 걸러진다 - 실제로 이것 때문에 DZ 테이블이 통째로 비워진 사고가 ETL
     * 로그에 한 줄도 안 남았다(2026-07-29).
     *
     * <p>시간 범위를 같이 보면 두 경우가 다 맞는다. 같은 NiFi 세션에서 30초마다
     * 다시 긁어온 같은 bulletin은 범위 안에 있으니 걸러지고, 재시작 뒤 재사용된
     * id는 예전 행이 범위 밖이라 새 행으로 남는다. pipeline-api가 재기동돼도
     * DB를 보므로 직전 몇 분치가 중복되지 않는다.
     */
    boolean existsByBulletinIdAndOccurredAtAfter(Long bulletinId, LocalDateTime since);
}
