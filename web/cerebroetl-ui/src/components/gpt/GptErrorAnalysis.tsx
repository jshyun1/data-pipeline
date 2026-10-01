import { useEffect } from "react";
import { Button } from "antd";
import { DEPLOYMENT_LABEL, type LlmTargetType } from "../../api/llm";
import { useGptConversation } from "./useGptConversation";
import { useLlmStatus } from "./useGptEnabled";
import {
  GptAskBox,
  GptCompactedNote,
  GptFailure,
  GptPreviewModal,
  GptTurns,
  ERROR_ANALYSIS_HEADING_LABELS,
  ERROR_ANALYSIS_HEADINGS,
  type GptAnswerHeading,
  type GptAnswerHeadingLabels,
} from "./GptConversationView";

export interface GptErrorAnalysisProps {
  targetType: LlmTargetType;
  /** WORKFLOW_TASK: dagId · runId · taskId / CDC_PIPELINE: pipelineId */
  target: Record<string, string>;
  /** 화면에 보이는 오류 문구. 대상 정보는 서버가 모은다. */
  errorText: string;
  onClose: () => void;
  headings?: GptAnswerHeading[];
  headingLabels?: GptAnswerHeadingLabels;
}

const TARGET_TYPE_LABEL: Record<LlmTargetType, string> = {
  ETL_ASSISTANT: "ETL 관리 어시스턴트",
  ETL_JOB: "ETL job",
  CDC_PIPELINE: "CDC 파이프라인",
  WORKFLOW: "워크플로우",
  WORKFLOW_DRAFT: "작성 중인 워크플로우",
  WORKFLOW_NODE: "워크플로우 노드",
  WORKFLOW_TASK: "워크플로우 작업",
};

function safeMarkdown(text?: string | null) {
  return (text ?? "").replace(/```/g, "``\\`");
}

function fileSafeName(text: string) {
  const cleaned = text.trim().replace(/[\\/:*?"<>|]+/g, "_").replace(/\s+/g, "_");
  return (cleaned || "gpt-error-analysis").slice(0, 80);
}

function targetFallbackLabel(targetType: LlmTargetType, target: Record<string, string>) {
  return target.name ?? target.selectedName ?? target.jobName ?? target.taskId ?? target.jobId ?? target.pipelineId
    ?? target.workflowId ?? TARGET_TYPE_LABEL[targetType];
}

/**
 * 로그 화면의 GPT 오류 분석 창(설계 5-3). 열리면 바로 분석을 시작하고, 답 아래에서 추가 질문을 이어 갈 수 있다.
 *
 * <p>대화는 저장하지 않는다 - 창을 닫으면 사라진다. 추가 질문마다 오류 내용 · 첫 분석 · 앞 문답을 같이 보낸다.
 * 길어지면 서버가 오래된 추가 문답부터 빼고 안내한다(대상 정보와 첫 분석은 항상 남는다).
 */
export function GptErrorAnalysis({ targetType, target, errorText, onClose, headings, headingLabels }: GptErrorAnalysisProps) {
  const status = useLlmStatus().data;
  const chat = useGptConversation({ agentType: "ERROR_ANALYSIS", targetType, target, errorText });

  useEffect(() => {
    // 열릴 때 한 번 분석한다. 대상이 바뀌면 부모가 창을 새로 연다.
    void chat.send();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const providerName = chat.meta?.providerName ?? status?.providerName;
  const deployment = chat.meta?.deployment ?? status?.deployment ?? undefined;
  const masked = chat.meta?.masked ?? status?.maskSensitive;
  const targetLabel = chat.meta?.targetLabel ?? targetFallbackLabel(targetType, target);

  const saveMarkdown = () => {
    const lines = [
      `# ${targetLabel} 실패 로그`,
      "",
      "## GPT 해석",
      "",
      `- 대상: ${TARGET_TYPE_LABEL[targetType]}`,
      `- 모델: ${providerName ?? "-"}${deployment ? ` (${DEPLOYMENT_LABEL[deployment]})` : ""}`,
      `- 개인정보 마스킹: ${masked ? "켜짐" : "꺼짐"}`,
      `- 저장 시각: ${new Date().toLocaleString()}`,
      "",
      "## 대화",
      "",
      ...chat.turns.flatMap((turn, index) => [
        `### ${turn.role === "user" ? "질문" : index === 0 ? "답변" : "답변"}${turn.stopped ? " (중지됨)" : ""}`,
        "",
        safeMarkdown(turn.content || (turn.streaming ? "답변 중입니다." : "")),
        "",
      ]),
      ...(chat.failure ? ["## 오류", "", safeMarkdown(chat.failure), ""] : []),
      "## 원문 로그",
      "",
      "```text",
      safeMarkdown(errorText),
      "```",
      "",
    ];
    const blob = new Blob([lines.join("\n")], { type: "text/markdown;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${fileSafeName(targetLabel)}_실패_로그.md`;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="gpt-analysis">
      <div className="gpt-analysis-head">
        <span className="gpt-analysis-title">GPT 해석</span>
        <span className="gpt-analysis-meta">
          {providerName ?? "-"}{deployment ? ` (${DEPLOYMENT_LABEL[deployment]})` : ""}
          {" · "}개인정보 마스킹 {masked ? "켜짐" : "꺼짐"}
        </span>
        <span className="gpt-analysis-actions">
          <Button type="link" size="small" onClick={() => void chat.openPreview()}>프롬프트</Button>
          <Button size="small" onClick={saveMarkdown}>MD 저장하기</Button>
          {chat.busy
            ? <Button size="small" onClick={chat.stop}>중지</Button>
            : <Button size="small" onClick={onClose}>닫기</Button>}
        </span>
      </div>
      {chat.meta?.truncated && (
        <div className="gpt-analysis-note">
          대상 정보 길이 제한({chat.meta.maxInputChars.toLocaleString()}자)에 맞춰 일부를 줄여 보냈습니다
          {chat.meta.omittedSections.length > 0 ? ` - 뺀 항목: ${chat.meta.omittedSections.join(", ")}` : ""}
        </div>
      )}
      <GptCompactedNote compacted={chat.compacted} />

      <div className="gpt-analysis-body">
        <GptTurns
          turns={chat.turns}
          waitingText="분석 중입니다. 내부 모델은 첫 응답까지 수십 초가 걸릴 수 있습니다."
          headings={headings ?? ERROR_ANALYSIS_HEADINGS}
          headingLabels={headingLabels ?? ERROR_ANALYSIS_HEADING_LABELS}
        />
        <GptFailure message={chat.failure} onRetry={!chat.answered && !chat.busy ? () => void chat.retryFirst() : undefined} />
      </div>

      {chat.answered && (
        <GptAskBox busy={chat.busy} placeholder="추가 질문 (Enter 보내기 · Shift+Enter 줄바꿈)" onSend={chat.send} />
      )}
      <GptPreviewModal preview={chat.preview} onClose={chat.closePreview} />
    </div>
  );
}
