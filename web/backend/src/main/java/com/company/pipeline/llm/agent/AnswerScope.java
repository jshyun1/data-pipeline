package com.company.pipeline.llm.agent;

import com.company.pipeline.llm.context.LlmTargetType;
import java.util.EnumMap;
import java.util.Map;

/**
 * 속성창 AI 어시스턴트가 «어디까지 답할 수 있는지»(2026-10-13 요청).
 *
 * <p>질문에 정확히 답할 수 없을 때(범위 밖이거나, 받은 정보 · 도구 조회로도 확인되지 않을 때) 답 첫머리에
 * «{@value #MARKER} 이 AI 어시스턴트는 … 까지 답할 수 있습니다» 한 문단으로 범위를 먼저 밝히게 한다. 화면은 이 머리말로 시작하는 줄을
 * 안내 상자로 그린다(GptConversationView). 범위는 대상 종류마다 수집기가 실제로 보내는 정보 기준이다 - 수집기를 바꾸면 여기도 맞춘다.
 *
 * <p>도구 조회로 더 볼 수 있는 것은 여기 넣지 않는다. 도구를 실을 때만 붙는 안내({@code LlmToolLoop.TOOL_GUIDE})에 있다 -
 * 모델이 도구를 거절해 도구 없이 다시 물을 때 범위가 사실과 어긋나지 않게.
 */
public final class AnswerScope {

    /** 답 첫머리 안내 문단의 머리말. 화면이 이 글자로 안내 상자를 알아본다. */
    public static final String MARKER = "[답변 범위]";

    /** 답에 쓰는 이름 · 한 줄 범위와 지시문에 적는 상세(수집기 섹션 기준). */
    record Scope(String subject, String summary, String details) {
    }

    private static final Map<LlmTargetType, Scope> SCOPES = new EnumMap<>(LlmTargetType.class);

    static {
        SCOPES.put(LlmTargetType.ETL_ASSISTANT, new Scope("선택한 ETL job · ETL 그룹",
                "job 의 구성(적재 유형 · 소스 → 타깃 · 스케줄 · 작성자) · 단계 SQL · 처리 흐름 · 연결된 워크플로우 · 최근 실행,"
                        + " 그룹이면 하위 job 목록과 질문에 맞는 job 의 상세",
                "선택한 job 의 기본 정보(적재 유형 · 소스 → 타깃 · 스케줄 · 생성일자 · 작성자) · 단계(SQL · 대상 테이블) · 처리 흐름 ·"
                        + " 연결된 Airflow DAG · 최근 실행 20건 요약, 그룹을 고르면 하위 그룹 · job 목록(최대 80개)과 질문에 맞는 job 상세(최대 5개),"
                        + " 화면에 표시된 오류 문구"));
        SCOPES.put(LlmTargetType.ETL_JOB, new Scope("이 ETL job",
                "구성 · 단계(SQL · 대상 테이블) · 최근 실행 · 화면에 보인 오류 문구",
                "ETL job 정보(이름 · 그룹 · Airflow DAG · 단계 수 · 상태 카운트) · 단계(SQL · 대상 테이블) · 최근 실행(최신순) ·"
                        + " 화면에 표시된 오류 문구"));
        SCOPES.put(LlmTargetType.CDC_PIPELINE, new Scope("이 CDC 파이프라인",
                "구성(소스 → 타깃 · Topic · 적재 방식 · 컬럼 설정) · 커넥터 상태와 오류 · 오프셋 · 지연 · 처리량 · 최근 제어 이력",
                "구성(소스 → 타깃 · Kafka Topic · 적재 방식 · 반영 이벤트 · 초기 적재 방식 · 제외/마스킹 컬럼 · 등록자) · 실측 상태와 판정 사유 ·"
                        + " 배포된 커넥터 · 소스/싱크 커넥터와 태스크 상태 · 커넥터 태스크 오류 추적 · 오프셋 · 소비 지연(lag) · 처리량 ·"
                        + " 마지막 처리 시각 · 최근 제어 명령 8건, 화면에 표시된 오류 문구"));
        SCOPES.put(LlmTargetType.WORKFLOW, new Scope("이 워크플로우",
                "노드 구성 · 실행 순서 · 스케줄 · 상하위 워크플로우 · 게시 · job 단계 · 최근 실행",
                "워크플로우 정보(설명 · 실행 방식 · 스케줄 · 상위 워크플로우 · 게시 · 실패 시 중단 · 메모 · 등록자 · Airflow DAG) ·"
                        + " 노드 구성(실행 순서 · 선행 연결 · 실행 조건) · job 노드의 단계(SQL · 대상 테이블) · 최근 실행 5건"));
        SCOPES.put(LlmTargetType.WORKFLOW_NODE, new Scope("이 워크플로우 노드",
                "노드 유형 · 선행/후행 노드 · 실행 조건 · 재시도 · 연결된 job 의 단계(또는 하위 워크플로우) · 속한 워크플로우의 스케줄 · 게시",
                "노드 유형 · 선행/후행 노드 · 실행 조건(trigger rule) · 재시도 · 노드가 실행하는 job 의 단계(또는 하위 워크플로우) ·"
                        + " 속한 워크플로우와 그 상위 · 스케줄 · 게시"));
        SCOPES.put(LlmTargetType.WORKFLOW_DRAFT, new Scope("작성 중인 새 워크플로우",
                "화면에서 그리고 있는 설계(노드 · 연결 · 실행 조건 · 스케줄)와 게시 검증 결과, 노드에 넣은 job 의 단계",
                "화면에서 그리고 있는 저장 전 설계(이름 · 설명 · 실행 방식 · 스케줄 · 실패 시 중단 · 메모) · 노드 구성과 연결 ·"
                        + " 구조 요약(시작 · 마지막 · 연결 안 된 노드) · 게시 검증 결과(오류 · 경고) · 노드에 넣은 job 의 단계"));
        SCOPES.put(LlmTargetType.WORKFLOW_TASK, new Scope("이 실패한 작업",
                "실패한 단계 · 화면에 보인 실패 로그 · 노드 설정 · 연결된 job 의 단계 · 워크플로우 구성",
                "실패한 작업(워크플로우 · 실행 id · task id · 실패한 단계) · 화면에 표시된 실패 로그 · 노드 유형 · 선행 노드 ·"
                        + " 실행 조건 · 재시도 · 노드가 실행하는 job 의 단계 · 워크플로우의 노드 구성 · 스케줄"));
        SCOPES.put(LlmTargetType.WORKFLOW_RUN, new Scope("선택한 실행 차수",
                "상태 · 시작/종료 · 소요 · 적재 합계 · 노드별 결과(하위 워크플로우 포함) · 실패 단계 로그 끝부분 · 직전 실행 · 지금 워크플로우 설계",
                "선택한 실행 차수(실행 id · 차수 · 실행 유형 · 상태 · 대기열 등록 · 시작/종료 · 소요 · 적재 합계) ·"
                        + " 노드별 결과(실행 순서, 하위 워크플로우 3단계까지) · 실패한 단계의 로그 끝부분(최대 3개 · 25줄) ·"
                        + " 같은 워크플로우의 직전 실행 5건 · 지금 기준 워크플로우 설계"));
    }

    private AnswerScope() {
    }

    /**
     * 질문이 있을 때 역할 지시문에 덧붙이는 범위 안내. 질문 없는 첫 요약에는 붙이지 않는다(형식이 정해져 있다).
     */
    public static String guide(LlmTargetType targetType) {
        Scope scope = scope(targetType);
        String unsaved = targetType == LlmTargetType.WORKFLOW_DRAFT ? "" : " · 화면에서 아직 저장하지 않은 변경";
        return """

                [답변 범위]
                - 네가 받은 정보(= 답할 수 있는 범위): %s.
                - 질문에 이름이 나온 다른 ETL job · CDC 파이프라인 · 워크플로우의 구성은 서버가 찾아 «질문에 나온 대상의 상세»로 함께 붙인다.
                - 받지 않은 것: 테이블에 실제로 들어 있는 데이터와 DB 스키마 전체 · 서버 · 컨테이너 자원 · 로그 원문(보낸 끝부분 제외) ·
                  계정 · 비밀번호 · 키 · 보낸 범위보다 오래된 이력%s. 시작 · 정지 · 재실행 같은 조치는 네가 직접 하지 못하고 방법만 안내한다.
                - 질문이 이 범위 밖이거나 받은 정보로 정확히 답할 수 없으면, 답 맨 앞을 아래 한 문단으로 시작한다(머리말 «%s» 그대로, 한 줄로):
                  %s 이 AI 어시스턴트는 %s에 대해 %s까지 답할 수 있습니다. 이번 질문의 «…» 부분은 받은 정보로 확인할 수 없습니다.
                  그다음 확인된 것만 답하고, 운영자가 직접 확인할 곳(화면 · 로그 · DB)을 알려 준다. 확인되지 않은 부분을 추측으로 채우지 않는다.
                - 범위 안에서 정확히 답할 수 있는 질문에는 이 문단을 쓰지 않는다.
                """.formatted(scope.details(), unsaved, MARKER, MARKER, scope.subject(), scope.summary());
    }

    static Scope scope(LlmTargetType targetType) {
        Scope scope = SCOPES.get(targetType);
        if (scope == null) {
            throw new IllegalStateException("답변 범위가 정의되지 않은 대상: " + targetType);
        }
        return scope;
    }
}
