package com.company.pipeline.heartbeat;

import java.util.List;

/**
 * 기대 수집기 목록의 단일 원천 (U5, 설계서 4-3/6-5). DB 행이 아니라 코드가 원천이라야
 * "한 번도 안 뜬 수집기"도 interval x 5 경과 후 DOWN 으로 판정할 수 있다(회색 미탐 방지).
 *
 * <p>리소스 수집기(infra-host/container/filesystem/rollup)는 U36 에서 이 목록에 키만 추가한다.
 * 알림 엔진 컴포넌트(alert-eval 등)는 U9/U11 에서 추가한다.
 */
public final class HeartbeatComponentRegistry {

    private HeartbeatComponentRegistry() {
    }

    /** metricSource: NIFI / KAFKA / AIRFLOW / INFRA / ENGINE. */
    public record Component(String key, String label, String metricSource, int expectedIntervalSeconds) {
    }

    public static final List<Component> COMPONENTS = List.of(
            new Component("kafka-metrics", "Kafka 지표 수집", "KAFKA", 20),
            new Component("nifi-counter", "NiFi 카운터 수집", "NIFI", 60),
            new Component("nifi-processor", "NiFi 처리 추적", "NIFI", 15),
            // U36 리소스 수집기(호스트). 컨테이너/파일시스템/롤업은 후속에서 키 추가.
            new Component("infra-host", "리소스 — 호스트", "INFRA", 60));
}
