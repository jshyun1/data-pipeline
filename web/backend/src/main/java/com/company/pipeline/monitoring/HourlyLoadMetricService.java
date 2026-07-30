package com.company.pipeline.monitoring;

import com.company.pipeline.monitoring.dto.HourlyCountProjection;
import com.company.pipeline.monitoring.dto.HourlyLoadSummaryResponse;
import com.company.pipeline.monitoring.dto.HourlyLoadSummaryResponse.HourlyPoint;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

/**
 * 대시보드 "일별·시간대별 적재 건수" 막대차트의 데이터 소스(날짜 × 시간대).
 *
 * <p>일별 롤업 테이블(pipeline_daily_load_metric)은 날짜 단위라 시간대를 알 수 없어서,
 * 시각이 남아 있는 원본 관측 테이블에서 직접 집계한다 - NiFi는 적재 카운터 증가분 로그
 * (nifi_execution_log), CDC는 20초 주기 offset 스냅샷(pipeline_metric_snapshot).
 */
@Service
@Transactional(readOnly = true)
public class HourlyLoadMetricService {

    public static final String SOURCE_NIFI = "NIFI";
    public static final String SOURCE_KAFKA = "KAFKA";

    private static final int HOURS_PER_DAY = 24;

    private final NifiExecutionLogEntryRepository executionLogRepository;
    private final PipelineMetricSnapshotRepository snapshotRepository;

    public HourlyLoadMetricService(
            NifiExecutionLogEntryRepository executionLogRepository,
            PipelineMetricSnapshotRepository snapshotRepository) {
        this.executionLogRepository = executionLogRepository;
        this.snapshotRepository = snapshotRepository;
    }

    /** source가 비어 있으면 NiFi + CDC를 같은 (날짜, 시간)끼리 합산한다. */
    public HourlyLoadSummaryResponse getHourly(LocalDate from, LocalDate to, String source) {
        LocalDateTime start = from.atStartOfDay();
        // to는 "그 날까지 포함"이라 하루를 더한 자정 직전까지가 조회 구간이다.
        LocalDateTime end = to.plusDays(1).atStartOfDay();

        List<HourlyCountProjection> rows = new ArrayList<>();
        String normalized = StringUtils.hasText(source) ? source.toUpperCase(Locale.ROOT) : null;
        if (normalized == null || SOURCE_NIFI.equals(normalized)) {
            rows.addAll(executionLogRepository.findHourlyInsertedTotals(start, end));
        }
        if (normalized == null || SOURCE_KAFKA.equals(normalized)) {
            rows.addAll(snapshotRepository.findHourlyCommittedDeltas(start, end));
        }

        return new HourlyLoadSummaryResponse(toPoints(rows));
    }

    private List<HourlyPoint> toPoints(List<HourlyCountProjection> rows) {
        List<HourlyPoint> points = new ArrayList<>(rows.size());
        for (HourlyCountProjection row : rows) {
            LocalDate date = row.getDate();
            Integer hour = row.getHour();
            Long count = row.getCount();
            if (date == null || hour == null || count == null || hour < 0 || hour >= HOURS_PER_DAY) {
                continue;
            }
            points.add(new HourlyPoint(date, hour, count));
        }
        points.sort(Comparator.comparing(HourlyPoint::date).thenComparingInt(HourlyPoint::hour));
        return points;
    }
}
