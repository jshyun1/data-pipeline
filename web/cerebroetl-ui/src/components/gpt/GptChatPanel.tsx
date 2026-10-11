import { useEffect } from "react";
import { Button } from "antd";
import type { LlmTargetType } from "../../api/llm";
import { downloadGptMarkdown, TARGET_TYPE_LABEL } from "./gptMarkdown";
import { useGptConversation } from "./useGptConversation";
import { useLlmStatus } from "./useGptEnabled";
import { GptAskBox, GptCompactedNote, GptFailure, GptPreviewModal, GptTurns } from "./GptConversationView";

export interface GptChatPanelProps {
  targetType: LlmTargetType;
  /** CDC_PIPELINE: pipelineId / WORKFLOW_NODE: workflowId · nodeKey · jobId | subWorkflowId */
  target: Record<string, string>;
  /** 대상 이름(«이 파이프라인», «이 노드»). 윗줄 안내 문구를 뺀 뒤로는 화면에 쓰지 않는다(호출부 호환). */
  targetNoun?: string;
  /** 대상 정보 추가 안내. 윗줄 안내 문구를 뺀 뒤로는 화면에 쓰지 않는다(호출부 호환). */
  notice?: string;
  /** WORKFLOW_DRAFT: 작성 중 설계(JSON)를 만드는 함수. 질문 · 전송 정보 볼 때마다 부른다. */
  draft?: () => string;
}

/**
 * 탭을 열 때 화면에 남길 첫 질문 한 줄(2026-10-02 요청). 서버에는 질문 없이 보내 정해진 요약 요청을 쓰되,
 * 대화에는 «무엇을 물어서 나온 답인지»가 보이게 한다.
 */
const OPENING_ASK: Partial<Record<LlmTargetType, string>> = {
  ETL_ASSISTANT: "ETL 정보 요약해줘",
  CDC_PIPELINE: "CDC 정보 요약해줘",
  WORKFLOW: "워크플로우 정보 요약해줘",
  WORKFLOW_DRAFT: "워크플로우 정보 요약해줘",
  WORKFLOW_NODE: "워크플로우 정보 요약해줘",
  WORKFLOW_RUN: "선택한 실행 차수 요약해줘",
};

/**
 * 속성창 GPT 탭(설계 5-2). CDC 파이프라인 상세 창 · 워크플로우 캔버스 노드 속성창 · ETL 관리 속성창(2026-10-10)이 같이 쓴다.
 *
 * <p>대화는 저장하지 않는다 - 창을 닫거나 다른 대상을 고르면 사라진다(부모가 대상별 key 로 새로 그린다).
 * 대화가 길어지면 서버가 앞 문답을 요약해 이어 가고, 요약된 부분은 흐리게 남겨 둔다.
 */
export function GptChatPanel({ targetType, target, draft }: GptChatPanelProps) {
  const status = useLlmStatus().data;
  const chat = useGptConversation({ agentType: "JOB_ANALYSIS", targetType, target, draft });

  useEffect(() => {
    // 탭을 열면 묻기 전에 대상 요약부터 내어 준다(2026-09-27 요청) - 로그 화면의 [GPT 해석] 과 같은 자리다.
    // 질문 없이 보내면 서버가 «요약 요청»으로 바꿔 답한다(JobAnalysisAgent.SUMMARY_REQUEST).
    // 화면에는 그 자리에 «… 요약해줘» 한 줄을 남긴다(2026-10-02 요청).
    // 대상이 바뀌면 부모가 key 로 이 컴포넌트를 새로 만들므로 여기서 대상 변화를 보지 않는다.
    void chat.sendOpening(OPENING_ASK[targetType] ?? "대상 정보 요약해줘");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  const providerName = chat.meta?.providerName ?? status?.providerName;
  const targetLabel = chat.meta?.targetLabel ?? TARGET_TYPE_LABEL[targetType];

  const saveMarkdown = () => downloadGptMarkdown({
    title: `${targetLabel} AI 어시스턴트`,
    targetType,
    providerName,
    deployment: chat.meta?.deployment ?? status?.deployment ?? undefined,
    masked: chat.meta?.masked ?? status?.maskSensitive,
    turns: chat.turns,
    failure: chat.failure,
    fileSuffix: "_AI_어시스턴트",
  });

  // 모양은 ETL 관리 화면 속성창의 «AI 어시스턴트» 탭(nifi-ai-*)과 같다(2026-09-17 요청): 위 요약 카드(GPT 이름 · 안내 ·
  // 전송될 정보 보기) → 대화 칸(남은 높이를 다 씀) → 둥근 보내기 버튼이 붙은 입력칸. 보내는 정보 · 대화 규칙은 그대로다.
  return (
    <section className="nifi-ai-panel gpt-chat" aria-label="AI 어시스턴트">
      {/* 윗줄은 왼쪽 GPT 이름 · 오른쪽 «프롬프트 편집» 버튼만(2026-09-18 요청, 이름은 2026-10-05 변경).
          대상 안내 · 배치/마스킹 · 저장 안 된 변경 안내 문구는 뺐다 - 보내는 내용은 그 창이 그대로 보여 주고 고칠 수도 있다. */}
      <div className="nifi-ai-summary gpt-chat-head">
        <strong>
          {/* «· 도구 조회 사용/미사용» 표시는 뺐다(2026-10-06 요청). 바로 앞 답의 사용 여부라서 탭을 열 때의 요약
              (도구 없이 답함)에는 설정이 «사용»이어도 «미사용»으로 보여 헷갈렸다. 실제 조회는 답 위 «🔎 … 조회» 줄이 보여 준다. */}
          {providerName ?? "선택된 GPT"}
        </strong>
        <span className="gpt-chat-head-actions">
          <Button size="small" onClick={() => void chat.openPreview()}>프롬프트 편집</Button>
          <Button size="small" onClick={saveMarkdown} disabled={chat.turns.length === 0}>MD 저장하기</Button>
        </span>
      </div>

      <GptFailure message={chat.failure} />
      <div className="nifi-ai-conversation gpt-chat-conversation">
        {chat.turns.length === 0 && !chat.failure && (
          <div className="nifi-ai-empty">
            대상 요약을 준비하고 있습니다.
            <br />요약이 나온 뒤 이어서 물어보세요. 예) 실행 순서와 영향 · 개선할 점 · 점검 방법
          </div>
        )}
        <GptTurns turns={chat.turns} summarizedUpto={chat.summarizedUpto} waitingText="답변을 준비하고 있습니다." />
      </div>
      <GptCompactedNote compacted={chat.compacted} />

      <GptAskBox variant="composer" busy={chat.busy} placeholder="무엇이든 질문하세요" onSend={chat.send} />
      {(chat.busy || chat.turns.length > 0) && (
        <div className="gpt-chat-actions">
          {chat.busy
            ? <Button size="small" onClick={chat.stop}>중지</Button>
            : <Button size="small" onClick={chat.reset}>새 대화</Button>}
        </div>
      )}
      <GptPreviewModal preview={chat.preview} onClose={chat.closePreview}
                       onApply={chat.applyContextOverride} overridden={Boolean(chat.contextOverride)} />
    </section>
  );
}
