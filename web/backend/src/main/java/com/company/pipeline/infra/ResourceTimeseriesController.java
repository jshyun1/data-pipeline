package com.company.pipeline.infra;

import com.company.pipeline.common.ApiResponse;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 리소스 추이(스파크라인) — 원본 문서 5-3 "#11 리소스 스파크라인".
 *
 * <p>ResourceSampleScheduler 가 1분마다 적재하는 infra_resource_sample 을 그대로 읽는다.
 * 화면은 "상승 중인지 하강 중인지"만 판단하면 되므로 다운샘플 없이 원본 표본을 내린다.
 *
 * <p>결손 구간은 행이 아예 없다. 프론트가 선을 끊을 수 있도록 시각을 함께 내려서
 * 등간격 배열로 착각하지 않게 한다.
 */
@RestController
@RequestMapping("/api/infra/resources")
public class ResourceTimeseriesController {

    /** 스파크라인 1개당 상한. 1분 간격이므로 60분이면 60점이다. */
    private static final int MAX_POINTS = 240;

    private final JdbcTemplate jdbc;

    public ResourceTimeseriesController(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    public record Point(String at, Double usedPercent, Long usedBytes, Long totalBytes) {}

    @GetMapping("/timeseries")
    public ApiResponse<Map<String, List<Point>>> timeseries(
            @RequestParam(defaultValue = "60") int minutes) {
        int window = Math.max(5, Math.min(minutes, 1440));
        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT metric, target_key,
                       to_char(sampled_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS at,
                       used_percent, used_bytes, total_bytes
                FROM infra_resource_sample
                WHERE sampled_at >= now() - make_interval(mins => ?)
                  AND quality = 'OK'
                ORDER BY metric, target_key, sampled_at
                """, window);

        Map<String, List<Point>> out = new LinkedHashMap<>();
        for (Map<String, Object> r : rows) {
            // DISK 는 마운트별로 여러 계열이 오므로 키에 마운트를 붙인다(CPU/MEMORY 는 host 단일).
            String metric = (String) r.get("metric");
            String key = "DISK".equals(metric) ? metric + ":" + r.get("target_key") : metric;
            List<Point> series = out.computeIfAbsent(key, k -> new ArrayList<>());
            if (series.size() >= MAX_POINTS) {
                continue;
            }
            series.add(new Point(
                    (String) r.get("at"),
                    r.get("used_percent") == null ? null : ((Number) r.get("used_percent")).doubleValue(),
                    r.get("used_bytes") == null ? null : ((Number) r.get("used_bytes")).longValue(),
                    r.get("total_bytes") == null ? null : ((Number) r.get("total_bytes")).longValue()));
        }
        return ApiResponse.success(out);
    }
}
