package com.company.pipeline.infra.dto;

import java.time.LocalDateTime;
import java.util.List;

/** 파이프라인 축(CDC/NiFi/Airflow)별로 "떠 있어야 하는" 프로세스들의 현황. */
public record ProcessHealthResponse(LocalDateTime collectedAt, List<ProcessGroup> groups) {

    public record ProcessGroup(String key, String label, ProcessStatus status, List<ProcessItem> processes) {

        public static ProcessGroup of(String key, String label, List<ProcessItem> processes) {
            ProcessStatus status = processes.stream()
                    .map(ProcessItem::status)
                    .reduce(ProcessStatus.UP, ProcessStatus::worst);
            return new ProcessGroup(key, label, status, processes);
        }
    }

    /**
     * detail은 화면에 그대로 보여줄 한 줄 설명(버전, heartbeat 시각, 실패 개수 등).
     *
     * <p>원본 문서 5-4 반영분 3가지가 여기 붙는다:
     * <ul>
     *   <li>{@code configured=false} → 화면이 "미사용(회색)"으로 렌더하고 이상 카운트에서 뺀다.
     *       지금까지 UNKNOWN 하나가 "상위가 죽어서 못 물어봤다"와 "애초에 안 쓴다"를 섞고 있었다.
     *       전자는 조사 대상이고 후자는 영원히 정상이라, 섞이면 회색을 아무도 안 보게 된다.</li>
     *   <li>{@code lastHeartbeatAt} → 툴팁의 "마지막 성공" 줄.</li>
     *   <li>{@code staleAfterSeconds}/{@code downAfterSeconds} → 툴팁의 "판정 기준" 줄.
     *       화면에 "180초"를 하드코딩하면 설정이 바뀌는 순간 화면이 거짓말을 한다.</li>
     * </ul>
     *
     * <p>{@code ProcessStatus} enum 에 값을 추가하지 않는다 — exhaustive switch 가 걸린
     * {@code severity()}/{@code worst()} 판정과 그룹 대표 색이 연쇄로 바뀐다.
     */
    public record ProcessItem(
            String name,
            ProcessStatus status,
            String detail,
            boolean configured,
            LocalDateTime lastHeartbeatAt,
            Integer staleAfterSeconds,
            Integer downAfterSeconds) {

        /** 임계값을 노출할 게 없는 단순 항목(브로커 생존 등). 기존 호출부는 전부 이 형태다. */
        public ProcessItem(String name, ProcessStatus status, String detail) {
            this(name, status, detail, true, null, null, null);
        }

        /** 이 설치에 구성되지 않은 구성요소. 장애가 아니라 "감시 대상 아님"이다. */
        public static ProcessItem unconfigured(String name, String detail) {
            return new ProcessItem(name, ProcessStatus.UNKNOWN, detail, false, null, null, null);
        }

        /** heartbeat 기반 항목. 툴팁이 판정 기준을 그대로 읽어갈 수 있게 임계값을 함께 싣는다. */
        public static ProcessItem withThresholds(String name, ProcessStatus status, String detail,
                                                 LocalDateTime lastHeartbeatAt,
                                                 Integer staleAfterSeconds, Integer downAfterSeconds) {
            return new ProcessItem(name, status, detail, true, lastHeartbeatAt, staleAfterSeconds, downAfterSeconds);
        }
    }
}
