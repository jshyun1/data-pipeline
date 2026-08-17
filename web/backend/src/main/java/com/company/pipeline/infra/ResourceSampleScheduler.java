package com.company.pipeline.infra;

import com.company.pipeline.heartbeat.HeartbeatService;
import com.company.pipeline.infra.dto.HostResourceResponse;
import com.company.pipeline.infra.dto.HostResourceResponse.DiskUsage;
import java.time.OffsetDateTime;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * 서버 리소스 시계열 수집 (U36, 설계서 4-8절/6-6절).
 *
 * <p>"서버 리소스 이력이 저장되는 곳이 지금까지 0곳이었다" - HostResourceService.collect() 는 화면을
 * 보는 사람이 있을 때만 순간값을 반환했다. 그래서 사고 2 당시 76.8% 시점의 과거값이 DB 에 한 톨도
 * 없었다. 여기서 60초마다 관측을 infra_resource_sample 에 적재해 재기동을 넘어 이력을 남긴다.
 *
 * <p>이번 증분 범위: 호스트(MEMORY/CPU/LOAD1) + 파일시스템(DISK). 컨테이너 cgroup·PSI·적응형 주기·
 * 롤업 다운샘플은 후속. sampled_at 은 분 단위 절삭(시리즈당 1분 1행), 충돌은 DO NOTHING(시계 역행 시
 * 기존 관측 보존).
 */
@Service
public class ResourceSampleScheduler {

    private static final Logger log = LoggerFactory.getLogger(ResourceSampleScheduler.class);

    private static final String SAMPLE_SQL = """
            INSERT INTO infra_resource_sample
                (scope, target_key, metric, sampled_at, used_bytes, total_bytes, used_percent, value_num, quality)
            VALUES (?, ?, ?, date_trunc('minute', ?::timestamptz), ?, ?, ?, ?, 'OK')
            ON CONFLICT (scope, target_key, metric, sampled_at) DO NOTHING
            """;

    private static final String SERIES_SQL = """
            INSERT INTO infra_resource_series
                (scope, target_key, metric, label, priority_class, first_observed_at, last_sampled_at, updated_at)
            VALUES (?, ?, ?, ?, ?, now(), now(), now())
            ON CONFLICT (scope, target_key, metric) DO UPDATE SET
                last_sampled_at = now(),
                first_observed_at = COALESCE(infra_resource_series.first_observed_at, now()),
                updated_at = now()
            """;

    private final HostResourceService hostResourceService;
    private final HeartbeatService heartbeat;
    private final JdbcTemplate jdbc;

    public ResourceSampleScheduler(HostResourceService hostResourceService, HeartbeatService heartbeat,
                                   DataSource dataSource) {
        this.hostResourceService = hostResourceService;
        this.heartbeat = heartbeat;
        this.jdbc = new JdbcTemplate(dataSource);
    }

    @Scheduled(fixedRate = 60_000, initialDelay = 30_000)
    public void sample() {
        HostResourceResponse r;
        try {
            r = hostResourceService.collect();
        } catch (Exception ex) {
            // procfs 미존재 등 - 주기를 완주 못 했으므로 beat 하지 않는다.
            log.warn("리소스 수집 실패(다음 주기 재시도): {}", ex.getMessage());
            return;
        }
        OffsetDateTime now = OffsetDateTime.now();
        try {
            if (r.memory() != null) {
                series("HOST", "host", "MEMORY", "메모리", "P0");
                sample("HOST", "host", "MEMORY", now, r.memory().usedBytes(), r.memory().totalBytes(),
                        r.memory().usedPercent(), null);
            }
            if (r.cpu() != null) {
                series("HOST", "host", "CPU", "CPU", "P0");
                sample("HOST", "host", "CPU", now, null, null, r.cpu().usedPercent(), (double) r.cpu().cores());
            }
            if (r.system() != null) {
                series("HOST", "host", "LOAD1", "Load(1m)", "P0");
                sample("HOST", "host", "LOAD1", now, null, null, null, r.system().load1());
            }
            if (r.disks() != null) {
                for (DiskUsage d : r.disks()) {
                    series("FILESYSTEM", d.mount(), "DISK", "디스크 " + d.mount(), "P1");
                    sample("FILESYSTEM", d.mount(), "DISK", now, d.usedBytes(), d.totalBytes(),
                            d.usedPercent(), null);
                }
            }
            // 주기 정상 완주.
            heartbeat.beat("infra-host");
        } catch (Exception ex) {
            log.warn("리소스 시계열 적재 실패: {}", ex.getMessage());
            heartbeat.beatFailed("infra-host", ex.getMessage());
        }
    }

    private void series(String scope, String targetKey, String metric, String label, String priority) {
        jdbc.update(SERIES_SQL, scope, targetKey, metric, label, priority);
    }

    private void sample(String scope, String targetKey, String metric, OffsetDateTime at,
                        Long usedBytes, Long totalBytes, Double usedPercent, Double valueNum) {
        jdbc.update(SAMPLE_SQL, scope, targetKey, metric, at, usedBytes, totalBytes, usedPercent, valueNum);
    }
}
