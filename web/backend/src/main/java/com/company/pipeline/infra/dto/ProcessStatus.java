package com.company.pipeline.infra.dto;

/**
 * 프로세스 생존 상태.
 *
 * <p>UNKNOWN은 "확인할 수 없었다"는 뜻이다(상위 프로세스가 죽어서 물어볼 수 없거나,
 * 애초에 구성하지 않은 컴포넌트). DOWN과 섞지 않아야 사람이 어디를 봐야 하는지 안다.
 */
public enum ProcessStatus {
    UP,
    STOPPED,
    DEGRADED,
    DOWN,
    UNKNOWN;

    /**
     * 그룹 대표 상태를 뽑을 때 쓰는 심각도. 큰 쪽이 더 나쁘다.
     *
     * <p>STOPPED(사람이 일부러 멈춘 상태)는 장애가 아니므로 그룹을 물들이지 않는다 -
     * 의도적으로 중지한 Sink 때문에 CDC 전체가 빨갛게 보이면 빨간색을 믿지 않게 된다.
     * 중지 사실은 해당 줄과 상단 요약 카드의 "일시정지" 수치로 드러난다.
     */
    private int severity() {
        return switch (this) {
            case UP, STOPPED -> 0;
            case UNKNOWN -> 1;
            case DEGRADED -> 2;
            case DOWN -> 3;
        };
    }

    public ProcessStatus worst(ProcessStatus other) {
        return other.severity() > severity() ? other : this;
    }
}
