import { Button } from "antd";
import { DEPLOYMENT_LABEL, type LlmTargetType } from "../../api/llm";
import { useGptConversation } from "./useGptConversation";
import { useLlmStatus } from "./useGptEnabled";
import { GptAskBox, GptCompactedNote, GptFailure, GptPreviewModal, GptTurns } from "./GptConversationView";

export interface GptChatPanelProps {
  targetType: LlmTargetType;
  /** CDC_PIPELINE: pipelineId / WORKFLOW_NODE: workflowId · nodeKey · jobId | subWorkflowId */
  target: Record<string, string>;
  /** 안내 문구에 쓰는 대상 이름(«이 파이프라인», «이 노드»). */
  targetNoun: string;
  /** 대상 정보에 대한 추가 안내(예: 캔버스에서 저장하지 않은 변경이 있음). */
  notice?: string;
}

/**
 * 속성창 GPT 탭(설계 5-2). CDC 파이프라인 상세 창 · 워크플로우 캔버스 노드 속성창이 같이 쓴다.
 *
 * <p>대화는 저장하지 않는다 - 창을 닫거나 다른 대상을 고르면 사라진다(부모가 대상별 key 로 새로 그린다).
 * 대화가 길어지면 서버가 앞 문답을 요약해 이어 가고, 요약된 부분은 흐리게 남겨 둔다.
 */
export function GptChatPanel({ targetType, target, targetNoun, notice }: GptChatPanelProps) {
  const status = useLlmStatus().data;
  const chat = useGptConversation({ agentType: "JOB_ANALYSIS", targetType, target });
  const providerName = chat.meta?.providerName ?? status?.providerName;
  const deployment = chat.meta?.deployment ?? status?.deployment ?? undefined;
  const masked = chat.meta?.masked ?? status?.maskSensitive;

  return (
    <div className="gpt-chat">
      <div className="gpt-chat-intro">
        <div>
          {targetNoun}의 정보가 질문과 함께 전달됩니다
          <Button type="link" size="small" onClick={() => void chat.openPreview()}>보기</Button>
        </div>
        <div className="gpt-analysis-meta">
          전송 대상: {providerName ?? "-"}{deployment ? ` (${DEPLOYMENT_LABEL[deployment]})` : ""}
          {" · "}개인정보 마스킹 {masked ? "켜짐" : "꺼짐"}
        </div>
        {notice && <div className="gpt-analysis-note">{notice}</div>}
      </div>

      <div className="gpt-chat-body">
        {chat.turns.length === 0 && !chat.failure && (
          <div className="gpt-chat-empty">
            {targetNoun}에 대해 궁금한 것을 물어보세요.
            <br />예) 무엇을 하는지 · 실행 순서와 영향 · 개선할 점 · 점검 방법
          </div>
        )}
        <GptTurns turns={chat.turns} summarizedUpto={chat.summarizedUpto} waitingText="답변을 준비하고 있습니다." />
        <GptFailure message={chat.failure} />
      </div>
      <GptCompactedNote compacted={chat.compacted} />

      <div className="gpt-chat-footer">
        <GptAskBox busy={chat.busy} placeholder="질문을 입력하세요 · Enter 보내기" onSend={chat.send} />
        <div className="gpt-chat-actions">
          {chat.busy
            ? <Button size="small" onClick={chat.stop}>중지</Button>
            : chat.turns.length > 0 && <Button size="small" onClick={chat.reset}>새 대화</Button>}
        </div>
      </div>
      <GptPreviewModal preview={chat.preview} onClose={chat.closePreview} />
    </div>
  );
}
