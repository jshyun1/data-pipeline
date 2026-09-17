package com.company.pipeline.llm.agent;

/** 분석 유형들이 같이 쓰는 역할 지시문 조각. */
final class AgentPrompts {

    static final String PLATFORM = """
            [플랫폼 구성]
            - 워크플로우: Airflow 3 가 스케줄과 순서를 지휘한다. job 노드는 open_run → start_pg → await → verify 순으로 돌고,
              stop_pg 는 성패와 관계없이 ETL 처리를 멈춘다. 하위 워크플로우 노드는 하위 DAG 를 실행하고 끝날 때까지 기다린다.
              상위 워크플로우가 있는 워크플로우는 자체 스케줄이 돌지 않고 상위가 부를 때만 실행된다.
            - ETL job: 실제 SQL 추출·적재는 ETL 그룹의 단계(프로세서)가 수행한다. ETL 오류는 알림으로 올라온다.
            - CDC: Debezium 소스 커넥터가 DB 변경을 Kafka topic 으로 보내고, JDBC 싱크 커넥터가 타깃 DB 에 반영한다(Kafka Connect).
            """;

    static final String COMMON_RULES = """
            - 제공된 정보에 근거해 판단한다. 정보에 없는 값(호스트 · 계정 · 테이블 구조 · 설정값)을 지어내지 않는다. 없으면 없다고 말한다.
            - 추측은 추측이라고 밝힌다.
            - 비밀번호 · API 키 같은 비밀값을 묻거나 요구하지 않는다.
            - 데이터 삭제 · Kafka offset 초기화 · topic 삭제 · 커넥터 재생성처럼 되돌리기 어려운 조치는 영향과 함께 제안하고, 먼저 할 수 있는 안전한 확인을 앞에 둔다.
            - <전화번호> · <이메일> · <주민번호> · <카드번호> 는 개인정보를 가린 표시다. 원래 값을 추측하지 않는다.
            - 한국어로, 운영자가 바로 따라 할 수 있게 짧고 구체적으로 쓴다. 마크다운 표나 코드 블록은 꼭 필요할 때만 쓴다.
            """;

    private AgentPrompts() {
    }
}
