package com.company.pipeline.llm.context;

import com.company.pipeline.authz.SystemCode;

/**
 * GPT 에 묻는 대상의 종류. 호출 위치마다 필요한 조회 권한이 달라, 서버가 요청마다 이 값으로 권한을 다시 확인한다(설계 7절).
 *
 * <p>ETL 쪽(ETL_JOB)은 ETL 담당 작업자가 수집기를 붙인다. 종류는 미리 두되 수집기가 없으면 «아직 지원하지 않는 대상»으로 거절한다.
 */
public enum LlmTargetType {
    /** ETL 관리 화면의 범용 운영 어시스턴트 */
    ETL_ASSISTANT(SystemCode.NIFI),
    /** ETL 관리 속성창 · ETL 로그 */
    ETL_JOB(SystemCode.NIFI),
    /** CDC 관리 속성창 · CDC 로그 */
    CDC_PIPELINE(SystemCode.KAFKA),
    /** 워크플로우 설계 화면 속성창(노드를 고르지 않았을 때 = 워크플로우 전체) */
    WORKFLOW(SystemCode.NIFI),
    /** 워크플로우 생성 화면 속성창(아직 저장하지 않은 초안 - 화면이 작성 중 설계를 보낸다) */
    WORKFLOW_DRAFT(SystemCode.NIFI),
    /** 워크플로우 캔버스 노드 속성창 */
    WORKFLOW_NODE(SystemCode.NIFI),
    /** 워크플로우 로그 실패 로그 창 */
    WORKFLOW_TASK(SystemCode.AIRFLOW);

    private final SystemCode requiredSystem;

    LlmTargetType(SystemCode requiredSystem) {
        this.requiredSystem = requiredSystem;
    }

    public SystemCode getRequiredSystem() {
        return requiredSystem;
    }
}
