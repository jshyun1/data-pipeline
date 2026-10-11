import { DEPLOYMENT_LABEL, type LlmDeployment, type LlmTargetType } from "../../api/llm";
import type { GptTurn } from "./useGptConversation";

/** 대상 종류를 사람이 읽는 이름으로. MD 머리말에 적는다. */
export const TARGET_TYPE_LABEL: Record<LlmTargetType, string> = {
  ETL_ASSISTANT: "ETL 관리 어시스턴트",
  ETL_JOB: "ETL job",
  CDC_PIPELINE: "CDC 파이프라인",
  WORKFLOW: "워크플로우",
  WORKFLOW_DRAFT: "작성 중인 워크플로우",
  WORKFLOW_NODE: "워크플로우 노드",
  WORKFLOW_TASK: "워크플로우 작업",
  WORKFLOW_RUN: "워크플로우 실행 차수",
};

/** 본문 안의 ``` 가 코드블록을 깨지 않게 막는다. */
export function safeMarkdown(text?: string | null) {
  return (text ?? "").replace(/```/g, "``\\`");
}

export function fileSafeName(text: string, fallback: string) {
  const cleaned = text.trim().replace(/[\\/:*?"<>|]+/g, "_").replace(/\s+/g, "_");
  return (cleaned || fallback).slice(0, 80);
}

/**
 * 대화를 마크다운 파일로 내려받는다(2026-10-02 요청으로 속성창 AI 어시스턴트도 같이 쓴다).
 * 로그 화면 [GPT 해석] 의 «MD 저장하기» 와 같은 내용 · 같은 형식이다.
 */
export function downloadGptMarkdown({
  title, targetType, providerName, deployment, masked, turns, failure, sections, fileSuffix,
}: {
  title: string;
  targetType: LlmTargetType;
  providerName?: string | null;
  deployment?: LlmDeployment | null;
  masked?: boolean | null;
  turns: GptTurn[];
  failure?: string;
  /** 대화 뒤에 붙일 것(원문 로그 등). 제목과 본문을 그대로 적는다. */
  sections?: Array<{ heading: string; body: string; code?: boolean }>;
  /** 파일 이름 끝에 붙는 말(«_실패_로그» 등). */
  fileSuffix: string;
}) {
  const lines = [
    `# ${title}`,
    "",
    `- 대상: ${TARGET_TYPE_LABEL[targetType]}`,
    `- 모델: ${providerName ?? "-"}${deployment ? ` (${DEPLOYMENT_LABEL[deployment]})` : ""}`,
    `- 개인정보 마스킹: ${masked ? "켜짐" : "꺼짐"}`,
    `- 저장 시각: ${new Date().toLocaleString()}`,
    "",
    "## 대화",
    "",
    ...turns.flatMap((turn) => [
      `### ${turn.role === "user" ? "질문" : "답변"}${turn.stopped ? " (중지됨)" : ""}`,
      "",
      safeMarkdown(turn.content || (turn.streaming ? "답변 중입니다." : "")),
      "",
    ]),
    ...(failure ? ["## 오류", "", safeMarkdown(failure), ""] : []),
    ...(sections ?? []).flatMap(({ heading, body, code }) => [
      `## ${heading}`,
      "",
      ...(code ? ["```text", safeMarkdown(body), "```"] : [safeMarkdown(body)]),
      "",
    ]),
  ];
  const blob = new Blob([lines.join("\n")], { type: "text/markdown;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `${fileSafeName(title, "gpt")}${fileSuffix}.md`;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}
