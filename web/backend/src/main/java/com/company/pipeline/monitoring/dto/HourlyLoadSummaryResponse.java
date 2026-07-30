package com.company.pipeline.monitoring.dto;

import java.time.LocalDate;
import java.util.List;

/**
 * 날짜 × 시간대(0~23시) 적재 건수. 대시보드는 이걸 3시간 구간으로 묶어서 "날짜별로
 * 하루 중 언제 얼마나 들어왔는지"를 한 차트에 그린다(x축 날짜, 범례 시간대).
 *
 * <p>건수가 0인 조합은 내려보내지 않는다 - 화면이 날짜 축과 구간을 알고 있으므로
 * 빈 칸은 화면에서 채우는 편이 응답 크기 면에서 낫다.
 */
public record HourlyLoadSummaryResponse(List<HourlyPoint> hourly) {

    public record HourlyPoint(LocalDate date, int hour, long count) {
    }
}
