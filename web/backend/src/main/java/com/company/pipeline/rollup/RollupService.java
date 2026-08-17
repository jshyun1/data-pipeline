package com.company.pipeline.rollup;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * 적재 롤업 기록 (U7, 설계서 4-5절). 누적형 KPI·차트·베이스라인의 단일 소스.
 *
 * <p>핵심: 수집부의 {@code if(delta>0)} 가드를 제거하고 delta==0 에도 recordObservation 을 호출해
 * observation_count 를 올린다. 그래야 "적재 0건(관측됨)"과 "미관측(수집 죽음)"을 구분할 수 있다
 * (사고 1의 데이터모델 뿌리). 버킷 경계는 install_info.display_zone 로컬 정시로 자르고 저장은
 * 그 경계의 절대시각(TIMESTAMPTZ)이다.
 *
 * <p>1 관측 = MIN5/HOUR/DAY 3개 UPSERT. (설계는 주기당 1회 다중 VALUES flush 를 권하나, 여기서는
 * 정확성 우선의 직접 UPSERT 로 두고 배치 flush 는 후속 최적화로 남긴다.)
 */
@Service
public class RollupService {

    private static final Logger log = LoggerFactory.getLogger(RollupService.class);

    private static final String UPSERT_SQL = """
            INSERT INTO pipeline_load_rollup
                (granularity, bucket_start, pipeline_source, pipeline_key, task_key, pipeline_label,
                 etl_job_id, loaded_count, observation_count, error_count,
                 first_observed_at, last_observed_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, now(), now(), now())
            ON CONFLICT (granularity, bucket_start, pipeline_source, pipeline_key, task_key) DO UPDATE SET
                loaded_count      = pipeline_load_rollup.loaded_count + EXCLUDED.loaded_count,
                observation_count = pipeline_load_rollup.observation_count + 1,
                error_count       = pipeline_load_rollup.error_count + EXCLUDED.error_count,
                pipeline_label    = COALESCE(EXCLUDED.pipeline_label, pipeline_load_rollup.pipeline_label),
                etl_job_id        = COALESCE(EXCLUDED.etl_job_id, pipeline_load_rollup.etl_job_id),
                last_observed_at  = now(),
                updated_at        = now()
            """;

    private final JdbcTemplate jdbc;
    private volatile ZoneId zone;

    public RollupService(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    private ZoneId zone() {
        if (zone == null) {
            try {
                String z = jdbc.queryForObject("SELECT display_zone FROM install_info WHERE id = 1", String.class);
                zone = ZoneId.of(z != null ? z : "Asia/Seoul");
            } catch (Exception ex) {
                zone = ZoneId.of("Asia/Seoul");
            }
        }
        return zone;
    }

    /** 한 번의 관측. deltaLoaded 는 이번 주기 증가분(0 가능, 음수는 0으로 클램프). errorDelta 는 오류 증가분. */
    public void recordObservation(String source, String pipelineKey, String taskKey, String label,
                                  Long etlJobId, long deltaLoaded, long errorDelta) {
        long delta = Math.max(0, deltaLoaded);
        long err = Math.max(0, errorDelta);
        ZonedDateTime now = ZonedDateTime.now(zone());
        upsert("MIN5", min5Bucket(now), source, pipelineKey, taskKey, label, etlJobId, delta, err);
        upsert("HOUR", now.truncatedTo(ChronoUnit.HOURS).toOffsetDateTime(), source, pipelineKey, taskKey, label, etlJobId, delta, err);
        upsert("DAY", now.toLocalDate().atStartOfDay(zone()).toOffsetDateTime(), source, pipelineKey, taskKey, label, etlJobId, delta, err);
    }

    public void recordObservation(String source, String pipelineKey, String taskKey, String label,
                                  Long etlJobId, long deltaLoaded) {
        recordObservation(source, pipelineKey, taskKey, label, etlJobId, deltaLoaded, 0);
    }

    private OffsetDateTime min5Bucket(ZonedDateTime now) {
        int m = (now.getMinute() / 5) * 5;
        return now.truncatedTo(ChronoUnit.HOURS).plusMinutes(m).toOffsetDateTime();
    }

    private void upsert(String gran, OffsetDateTime bucket, String source, String key, String task,
                        String label, Long etlJobId, long delta, long err) {
        try {
            jdbc.update(UPSERT_SQL, gran, bucket, source, key, task, label, etlJobId, delta, err);
        } catch (Exception ex) {
            // 롤업 기록 실패가 수집 자체를 막으면 안 된다.
            log.warn("적재 롤업 UPSERT 실패({}, {}/{}): {}", gran, source, key, ex.getMessage());
        }
    }
}
