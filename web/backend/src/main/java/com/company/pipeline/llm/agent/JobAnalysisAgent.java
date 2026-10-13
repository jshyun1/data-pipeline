package com.company.pipeline.llm.agent;

import com.company.pipeline.llm.client.LlmMessage;
import com.company.pipeline.llm.context.LlmTargetType;
import java.util.ArrayList;
import java.util.List;

/**
 * 속성창 GPT 탭(설계 5-2) - 선택한 대상(CDC 파이프라인 · 워크플로우 노드 · ETL job)에 대한 자유 질문.
 *
 * <p>첫 사용자 메시지 맨 앞에 대상 정보를 붙인다(요청마다 새로 모은 것). 대화가 길어지면 {@code ConversationFitter} 가 오래된 문답을
 * 요약하고, 그 요약도 대상 정보 바로 뒤에 붙인다 - 역할이 user · assistant 번갈아 이어지도록(Anthropic · Gemini 는 순서를 강제한다).
 */
public final class JobAnalysisAgent {

    public static final String SYSTEM_PROMPT = """
            너는 데이터 파이프라인 운영 플랫폼 «Cerebro ETL» 의 운영 도우미다. 운영자가 속성창에서 고른 대상(CDC 파이프라인 · 워크플로우 노드 ·
            ETL job)에 대해 질문한다. 구성 설명, SQL · 적재 방식 해석, 실행 순서와 영향, 개선점, 점검 방법 등을 답한다.

            """ + AgentPrompts.PLATFORM + """

            [규칙]
            """ + AgentPrompts.COMMON_RULES + """
            - 대상 정보는 질문 시점에 저장된 설계와 상태다. 화면에서 아직 저장하지 않은 변경은 들어 있지 않다.
            - 질문에 맞는 길이로 답한다. 정해진 형식은 없고, 절차는 번호 목록으로 쓴다.
            - [이전 대화 요약] 이 있으면 앞 대화의 결론으로 보고 이어서 답한다.
            """;

    /** 저장된 대상 전제 규칙. 초안 대상에서는 {@link #DRAFT_RULE} 로 바꾼다. */
    static final String SAVED_RULE = "- 대상 정보는 질문 시점에 저장된 설계와 상태다. 화면에서 아직 저장하지 않은 변경은 들어 있지 않다.";
    static final String DRAFT_RULE = "- 대상 정보는 운영자가 화면에서 그리고 있는, 아직 저장하지 않은 새 워크플로우 설계다(질문을 보낸 시점의 화면 그대로).";

    /**
     * 워크플로우 생성 화면(WORKFLOW_DRAFT, 2026-09-18) - 같은 자유 질문 흐름(요약 포함)에 «저장 · 게시 전 설계 검토» 관점을 더한다.
     * 따로 에이전트를 두지 않은 이유: 묻는 방식(자유 질문 · 긴 대화 요약)이 속성창과 같고, 달라지는 것은 대상의 전제와 짚을 점뿐이다.
     */
    static final String DRAFT_GUIDE = """

            [작성 중인 새 워크플로우]
            - 저장 · 게시 전에 설계를 함께 검토하는 역할이다. «검증 결과» 구역은 서버의 게시 검증을 이 초안에 돌린 결과다.
            - 설계를 봐 달라고 하면 이 순서로 짚는다: (1) 검증 오류 - 게시가 막히는 이유와 캔버스에서 고치는 방법
              (2) 실행 순서 - 선행 연결, 함께 돌 수 있는 노드, 연결 안 된 노드 (3) 실행 조건(trigger rule) · 연결 조건이 의도와 맞는지
              (4) 스케줄을 말로 풀기와 동시 실행 · catchup · 실패 시 중단의 영향 (5) job 단계로 본 적재 대상 겹침 · 순서 의존.
            - 노드 배치 · 연결 · 속성 입력은 운영자가 화면에서 한다. 이름이 비어 있으면 저장할 수 없다는 점을 알려 준다.
            """;

    /**
     * 탭을 열자마자 나가는 <b>첫 요약 요청</b>(2026-09-27). 운영자가 아무것도 묻기 전에, 고른 대상이 무엇이고
     * 지금 어떤 상태인지를 한 번 정리해 준다 - 로그 화면의 [GPT 해석] 이 누르자마자 답을 주는 것과 같은 자리다.
     *
     * <p>질문 문구를 화면이 아니라 서버가 들고 있는 이유: 화면마다 달라지면 답의 모양도 달라진다.
     * 형식은 고정하되, <b>정보에 없는 항목은 «확인되지 않음»</b> 으로 적게 해서 지어내지 않게 한다.
     *
     * <p>2026-10-12 요청: «지금 상태» → «현재 상태», «눈여겨볼 점» 구분은 뺐다(워크플로우 · ETL · CDC 속성창 모두 이 요청을 쓴다).
     * 이상 징후는 «현재 상태»에서 근거와 함께 짚는다.
     */
    public static final String SUMMARY_REQUEST = """
            위 대상을 처음 보는 운영자에게 설명하듯 요약해 줘. 제공된 정보에 있는 것만 쓰고, 없는 항목은 «확인되지 않음» 이라고 적는다.
            아래 형식의 구분만 쓰고, 형식에 없는 구분(눈여겨볼 점 · 참고 등)은 덧붙이지 않는다.

            [형식]
            한 줄 요약
            (무엇을 하는 대상인지 한 문장)

            구성
            - 대상마다 중요한 것부터: 워크플로우면 노드 구성 · 실행 순서 · 실행 방식 · 스케줄 · 상위 워크플로우,
              CDC 파이프라인이면 소스 → 타깃 · 적재 방식 · Topic · 커넥터,
              ETL job 이면 적재 유형 · 소스(DB · 테이블) → 적재 테이블 · 스케줄 · 처리 흐름 · 생성일자와 작성자,
              ETL 그룹이면 하위 그룹 · job 수와 주요 job(최대 5개)마다 소스 → 타깃 · 연결 DAG · 상태 · 프로세서 수.

            현재 상태
            - 실행/커넥터 상태, 최근 실행이나 최근 제어, 오프셋 · 지연처럼 «현재 어떤지»를 말해 주는 값.
              ETL job 이면 최근 5일 실행 횟수(성공 · 실패) · 연속 성공 · 최근 실행 소요 시간.
            - 이상 징후가 보이면 그 근거와 함께 짚는다.

            등록 · 변경
            - 등록자 · 등록 시각 · 최종 수정 · 게시 정보 중 있는 것.
            """;

    /**
     * 워크플로우 로그에서 고른 «실행 차수» 한 건의 첫 요약(2026-10-11 요청 - 워크플로우 설명이 아니라 그 회차의 결과를 말한다).
     * 머리말 이름은 화면이 굵은 제목(■)으로 그리는 목록(GptConversationView KNOWN_SECTIONS)과 맞춘다.
     *
     * <p>2026-10-12 요청: «한 줄 요약 · 구성 · 실행 결과» 세 구분으로 줄였고, «실행 결과»는 전체 결과만 적는다
     * (노드별 결과 · 직전 실행 비교는 빼달라는 요청). 실패한 차수만 멈춘 곳 · 원인을 한 줄 붙인다 - 그게 전체 결과의 일부다.
     * 노드별 결과 · 비교는 수집 정보에 그대로 있어 추가 질문에는 답한다.
     */
    public static final String RUN_SUMMARY_REQUEST = """
            위 «선택한 실행 차수» 한 건을 운영자에게 보고하듯 요약해 줘. 제공된 정보에 있는 것만 쓰고, 없는 항목은 «확인되지 않음» 이라고 적는다.
            아래 세 구분만 쓰고, 형식에 없는 구분(job별 결과 · 이전 실행과 비교 · 눈여겨볼 점 등)은 만들지 않는다.

            [형식]
            한 줄 요약
            (이 차수가 언제 · 어떻게 시작돼 어떤 결과로 끝났는지 한 문장)

            구성
            - 이 차수에서 돈 노드를 실행 순서대로: job · 하위 워크플로우(그 안의 job 까지) · CDC 노드, 선행 관계.
            - 실행 유형(수동 · 스케줄 · 상위 워크플로우 호출)과 몇 번째 실행(차수)인지.

            실행 결과
            - 전체 결과만: 상태 · 시작/종료 · 소요 · 적재 합계(하위 워크플로우 포함).
            - 노드별 결과 · 직전 실행 비교는 쓰지 않는다. 실패한 차수만 멈춘 노드 · 단계와 원인(로그 근거)을 한 줄로 덧붙인다.
            """;

    private JobAnalysisAgent() {
    }

    /** 질문 없는 첫 요청에 쓸 요약 요청. 실행 차수만 형식이 다르다. */
    public static String summaryRequest(LlmTargetType targetType) {
        return targetType == LlmTargetType.WORKFLOW_RUN ? RUN_SUMMARY_REQUEST : SUMMARY_REQUEST;
    }

    /**
     * 실제로 보내는 역할 지시문. 질문이 있으면 «답변 범위» 안내를 덧붙인다(2026-10-13 요청) - 정확히 답할 수 없는 질문에는
     * 답 첫머리에서 «이 AI 가 무엇을 어디까지 답할 수 있는지»를 먼저 밝히게 한다. 질문 없는 첫 요약은 형식이 정해져 있어 붙이지 않는다.
     * 입력 길이 계산에도 같은 것을 쓴다.
     */
    public static String instructions(LlmTargetType targetType, boolean hasQuestion) {
        return hasQuestion ? systemPrompt(targetType) + AnswerScope.guide(targetType) : systemPrompt(targetType);
    }

    /** 대상 종류에 맞는 역할 지시문(범위 안내 전). */
    public static String systemPrompt(LlmTargetType targetType) {
        if (targetType == LlmTargetType.WORKFLOW_DRAFT) {
            return SYSTEM_PROMPT.replace(SAVED_RULE, DRAFT_RULE) + DRAFT_GUIDE;
        }
        return SYSTEM_PROMPT;
    }

    /**
     * @param summary   앞 대화 요약(없으면 null)
     * @param keptTurns 남긴 문답(user 로 시작해 user · assistant 번갈아)
     * @param question  이번 질문
     */
    public static List<LlmMessage> messages(LlmTargetType targetType, String contextText, String summary,
                                            List<LlmMessage> keptTurns, String question) {
        StringBuilder head = new StringBuilder("[대상: ").append(ErrorAnalysisAgent.targetLabel(targetType)).append("]\n\n")
                .append(contextText);
        if (summary != null && !summary.isBlank()) {
            head.append("\n\n[이전 대화 요약]\n").append(summary.strip());
        }
        head.append("\n\n[질문]\n");

        // 질문이 없으면 «탭을 연 첫 요청»이다 - 서버가 가진 요약 요청으로 바꾼다.
        boolean hasQuestion = question != null && !question.isBlank();
        String ask = hasQuestion ? question.strip() : summaryRequest(targetType);
        List<LlmMessage> out = new ArrayList<>();
        out.add(LlmMessage.system(instructions(targetType, hasQuestion)));
        List<LlmMessage> conversation = new ArrayList<>(keptTurns);
        conversation.add(LlmMessage.user(ask));
        LlmMessage first = conversation.get(0);
        out.add(LlmMessage.user(head + first.content()));
        out.addAll(conversation.subList(1, conversation.size()));
        return out;
    }
}
