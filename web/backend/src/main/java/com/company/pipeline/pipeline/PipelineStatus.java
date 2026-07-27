package com.company.pipeline.pipeline;

/**
 * 설계서 §10 전체 상태 다이어그램 중 이번 증분까지 실제로 쓰는 상태.
 * STOPPING/DELETING 같은 전이 중(in-flight) 상태는 만들지 않는다 - 우리 구현은
 * Kafka Connect 호출이 동기식이라 별도 저장할 "진행 중" 구간이 사실상 없다.
 */
public enum PipelineStatus {
    CREATED,
    DEPLOYING,
    /** Source/Sink Connector가 모두 STOPPED로 준비되어 Airflow의 최초 start를 기다리는 상태. */
    READY,
    DEPLOYED,
    PAUSED,
    STOPPED,
    FAILED
}
