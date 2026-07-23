package com.company.pipeline.monitoring;

import com.company.pipeline.monitoring.dto.DailyLoadSummaryResponse;
import com.company.pipeline.monitoring.dto.DailyLoadSummaryResponse.DailyPoint;
import com.company.pipeline.monitoring.dto.DailyLoadSummaryResponse.KeyedPoint;
import java.time.LocalDate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 * 대시보드 "일자별 데이터 로드 건수" / "Top N" 차트의 실제 데이터 소스.
 * Kafka(Spring @Scheduled)와 NiFi(Airflow 1분 스케줄 DAG) 양쪽이 새로 적재된 건수를
 * 감지할 때마다 여기로 누적시키고, 대시보드는 이 롤업 테이블만 읽는다(Airflow의
 * 원시 asset_event를 매번 다시 집계하는 것보다 훨씬 적은 row로 조회 가능).
 */
@Service
public class PipelineDailyLoadMetricService {

    private static final int TOP_PIPELINES_LIMIT = 5;
    private static final int TOP_TASKS_LIMIT = 10;

    private final PipelineDailyLoadMetricRepository repository;

    public PipelineDailyLoadMetricService(PipelineDailyLoadMetricRepository repository) {
        this.repository = repository;
    }

    public void incrementLoadedCount(
            String pipelineSource, String pipelineKey, String taskKey, String pipelineLabel, long count) {
        if (count <= 0) {
            return;
        }
        String label = StringUtils.hasText(pipelineLabel) ? pipelineLabel : pipelineKey;
        repository.upsertIncrement(pipelineSource, pipelineKey, taskKey, label, LocalDate.now(), count);
    }

    public DailyLoadSummaryResponse getSummary(LocalDate from, LocalDate to) {
        var daily = repository.findDailyTotals(from, to).stream().map(DailyPoint::from).toList();
        var topPipelines = repository.findTopPipelines(from, to, TOP_PIPELINES_LIMIT).stream()
                .map(KeyedPoint::from).toList();
        var topTasks = repository.findTopTasks(from, to, TOP_TASKS_LIMIT).stream()
                .map(KeyedPoint::from).toList();
        return new DailyLoadSummaryResponse(daily, topPipelines, topTasks);
    }
}
