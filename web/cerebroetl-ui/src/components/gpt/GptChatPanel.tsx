import { Button } from "antd";
import type { LlmTargetType } from "../../api/llm";
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
 * 속성창 GPT 탭(설계 5-2). CDC 파이프라인 상세 창 · 워크플로우 캔버스 노드 속성창이 같이 쓴다.
 *
 * <p>대화는 저장하지 않는다 - 창을 닫거나 다른 대상을 고르면 사라진다(부모가 대상별 key 로 새로 그린다).
 * 대화가 길어지면 서버가 앞 문답을 요약해 이어 가고, 요약된 부분은 흐리게 남겨 둔다.
 */
export function GptChatPanel({ targetType, target, draft }: GptChatPanelProps) {
  const status = useLlmStatus().data;
  const chat = useGptConversation({ agentType: "JOB_ANALYSIS", targetType, target, draft });
  const providerName = chat.meta?.providerName ?? status?.providerName;

  // 모양은 ETL 관리 화면 속성창의 «AI 어시스턴트» 탭(nifi-ai-*)과 같다(2026-09-17 요청): 위 요약 카드(GPT 이름 · 안내 ·
  // 전송될 정보 보기) → 대화 칸(남은 높이를 다 씀) → 둥근 보내기 버튼이 붙은 입력칸. 보내는 정보 · 대화 규칙은 그대로다.
  return (
    <section className="nifi-ai-panel gpt-chat" aria-label="AI 어시스턴트">
      {/* 윗줄은 왼쪽 GPT 이름 · 오른쪽 «전송 정보» 버튼만(2026-09-18 요청). 대상 안내 · 배치/마스킹 · 저장 안 된 변경 안내 문구는 뺐다 -
          보내는 내용은 «전송 정보» 창이 그대로 보여 준다. */}
      <div className="nifi-ai-summary gpt-chat-head">
        <strong>{providerName ?? "선택된 GPT"}</strong>
        <Button size="small" onClick={() => void chat.openPreview()}>전송 정보</Button>
      </div>

      <GptFailure message={chat.failure} />
      <div className="nifi-ai-conversation gpt-chat-conversation">
        {chat.turns.length === 0 && !chat.failure && (
          <div className="nifi-ai-empty">
            질문을 보내면 이 영역에 대화가 표시됩니다.
            <br />예) 무엇을 하는지 · 실행 순서와 영향 · 개선할 점 · 점검 방법
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
      <GptPreviewModal preview={chat.preview} onClose={chat.closePreview} />
    </section>
  );
}
