package com.company.pipeline.connector;

/**
 * Kafka Connect 제어 호출이 응답을 기다리다 끊긴 경우.
 *
 * <p><b>타임아웃은 "실패"가 아니라 "결과를 모름"이다.</b> 커넥터 등록/갱신·정지·재개는
 * 클러스터 리밸런스를 유발해 응답이 늦는데, 그 사이 Kafka Connect 는 요청을 이미 반영해
 * 두는 경우가 많다. 실제로 2026-08-26 에 {@code PUT /connectors/{name}/config} 가
 * 타임아웃됐지만 설정은 정상 반영됐고, 그런데도 파이프라인만 FAILED 로 기록돼 이후
 * start 가 막혔다.
 *
 * <p>그래서 이 예외를 일반 호출 실패와 구분한다. 호출부는 이걸 받으면 실패로 단정하지 말고
 * <b>커넥터의 실제 상태를 다시 조회해</b> 판정해야 한다(재시도가 아니라 확인이다 - 변경
 * 요청을 다시 보내면 리밸런스가 겹쳐 상황이 나빠진다).
 */
public class KafkaConnectTimeoutException extends KafkaConnectClientException {

    public KafkaConnectTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
