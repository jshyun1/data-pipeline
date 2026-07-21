package com.company.pipeline.connection;

/**
 * 설계서(docs/kafka-webservice-design.md) §7.1에는 DB 기본값 'UNKNOWN'만 명시돼 있고
 * 전체 상태값 목록은 없어 이번 증분에서 새로 정의함. ConnectionTestService(다음 증분)가
 * TESTING -> SUCCESS/FAILED로 전이시킬 예정.
 */
public enum ConnectionStatus {
    UNKNOWN,
    TESTING,
    SUCCESS,
    FAILED
}
