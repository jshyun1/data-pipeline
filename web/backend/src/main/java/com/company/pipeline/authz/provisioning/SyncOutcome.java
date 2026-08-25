package com.company.pipeline.authz.provisioning;

/**
 * NiFi/Airflow 개인계정 동기화 결과.
 *
 * <p>동기화는 best-effort 라 실패해도 역할 배정 자체는 성공한다. 그런데 실패를 조용히 삼키면
 * 사용자에게는 "역할을 줬는데 콘솔이 안 열린다"만 남고, 원인은 감사 로그를 뒤져야 나온다
 * (실제로 Airflow 이메일 중복 409 를 그렇게 찾았다 - 2026-08-25). 그래서 호출부가 사유를
 * 응답에 실어 보낼 수 있도록 결과를 돌려준다.
 *
 * @param attempted 기능이 켜져 있고 입력이 유효해 실제로 시도했는지. false 면 성공/실패가 아니다
 * @param ok        시도했고 성공했는지
 * @param message   실패 사유(성공이면 null)
 */
public record SyncOutcome(boolean attempted, boolean ok, String message) {

    /** 기능 off 또는 입력이 비어 시도하지 않음. 실패가 아니다. */
    public static SyncOutcome skipped() {
        return new SyncOutcome(false, false, null);
    }

    public static SyncOutcome success() {
        return new SyncOutcome(true, true, null);
    }

    public static SyncOutcome failure(String message) {
        return new SyncOutcome(true, false, message);
    }

    /** 일괄 동기화 집계용 - 기존 boolean 계약과 같다. */
    public boolean succeeded() {
        return attempted && ok;
    }

    /** 시도했는데 실패한 경우에만 true. 화면에 경고를 띄울 조건이다. */
    public boolean failed() {
        return attempted && !ok;
    }
}
