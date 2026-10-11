import { useEffect, useRef, useState } from "react";
import { Alert, Button, Input, Modal, Spin } from "antd";
import type { TextAreaRef } from "antd/es/input/TextArea";
import { ArrowUpOutlined } from "@ant-design/icons";
import { DEPLOYMENT_LABEL, type LlmCompacted } from "../../api/llm";
import type { GptPreviewState, GptTurn } from "./useGptConversation";

/**
 * 첫 답변 형식의 세 제목(서버 ErrorAnalysisAgent 와 같다). 모델이 «## 원인» · «**원인**» 처럼 꾸며도 제목으로 본다.
 *
 * <p>옛 제목(원인 추정 · 확인할 것 · 확신도)도 남겨 둔다 - 모델이 예전 형식으로 답하거나 화면에 남아 있던
 * 지난 답을 다시 그릴 때 제목이 본문으로 섞이지 않게 한다(2026-09-28 문구 변경).
 */
const HEADINGS = ["원인", "확인사항", "조치 방법", "원인 추정", "확인할 것", "확신도"];
const HEADING_PATTERN = new RegExp(`^\\s*(?:#+\\s*)?(?:\\*\\*)?(${HEADINGS.join("|")})(?:\\*\\*)?\\s*:?\\s*$`);
export type GptAnswerHeading = typeof HEADINGS[number];
/** 옛 형식 제목 → 지금 제목. 모델이 예전 이름으로 답해도 같은 칸에 담는다. */
const HEADING_ALIAS: Partial<Record<GptAnswerHeading, GptAnswerHeading>> = { "원인 추정": "원인", "확인사항": "확인할 것" };
/** 오류 분석 첫 답의 표시 순서와 이름(2026-10-02 요청 - 워크플로우 · ETL · CDC 로그 창 공통). */
export const ERROR_ANALYSIS_HEADINGS: GptAnswerHeading[] = ["원인", "조치 방법", "확인할 것"];
export const ERROR_ANALYSIS_HEADING_LABELS: GptAnswerHeadingLabels = { "조치 방법": "조치방법", "확인할 것": "확인할것" };
export type GptAnswerHeadingLabels = Partial<Record<GptAnswerHeading, string>>;

/**
 * 서버 지시문이 정해 둔 구분 이름들. 모델이 번호 없이 이 이름만 적어도 구분으로 본다
 * (속성창 요약: 한 줄 요약 · 구성 · 지금 상태 · 등록 · 변경 · 눈여겨볼 점 / 로그 분석: 원인 · 조치 방법 · 확인할 것).
 * «등록 · 변경» · «눈여겨볼 점»은 2026-10-10 에 더했다(JobAnalysisAgent.SUMMARY_REQUEST 의 마지막 두 구분) - 빠져 있어 ■ 없이 본문처럼 보였다.
 * 띄어쓰기는 빼고 견준다 - 모델이 «등록·변경» · «눈여겨 볼 점»처럼 적어도 같은 구분이다.
 */
const KNOWN_SECTIONS = [
  ...HEADINGS, "한 줄 요약", "구성", "지금 상태", "등록 · 변경", "등록 및 변경", "눈여겨볼 점",
  "요약", "기본정보", "스케줄", "실행 방식", "참고", "다음 단계",
  // 워크플로우 실행 차수 요약(서버 JobAnalysisAgent.RUN_SUMMARY_REQUEST, 2026-10-11).
  "실행 결과", "job별 결과", "이전 실행과 비교",
];
const sectionKey = (title: string) => title.replace(/\s+/g, "");
const KNOWN_SECTION_KEYS = new Set(KNOWN_SECTIONS.map(sectionKey));

/**
 * 한 줄이 «구분 제목»인지 본다(2026-10-05 요청). 제목이면 번호 · 꾸밈 기호를 뺀 이름을 돌려준다.
 *
 * <p>«1. 원인» 처럼 번호가 붙은 제목은 번호를 떼고, «1. PostgreSQL 에 접속해 …» 같은 절차 문장은
 * 제목으로 보지 않는다 - 제목은 짧고 문장부호로 끝나지 않는다.
 */
function sectionTitleOf(raw: string): string | null {
  const line = raw.trim();
  if (!line || line.startsWith("-") || line.startsWith("·")) {
    return null;
  }
  const plain = (text: string) => text.replace(/\*\*/g, "").replace(/\s*:$/, "").trim();
  const md = /^#{1,6}\s*(.+)$/.exec(line);
  if (md) {
    return plain(md[1]);
  }
  const bold = /^\*\*(.+?)\*\*\s*:?$/.exec(line);
  if (bold) {
    return plain(bold[1]);
  }
  const numbered = /^\(?\d+\)?\s*[.)]?\s+(.+)$/.exec(line);
  const body = numbered ? plain(numbered[1]) : plain(line);
  if (KNOWN_SECTION_KEYS.has(sectionKey(body))) {
    return body;
  }
  if (numbered) {
    // 번호가 붙은 줄은 «구성» 안의 목록(«1. COM001M (job)»)인 경우가 대부분이다 - 정해진 구분 이름일 때만
    // 제목으로 본다(2026-10-05 확인). 예전에는 짧기만 하면 제목으로 봐서 목록 첫 줄에만 ■ 가 붙었다.
    return null;
  }
  // 번호도 기호도 없는 짧은 단독 줄(«구성:» 같은 것). 문장 · 코드 이름이 든 줄은 제외한다.
  return body.length <= 14 && !/[.!?]$/.test(body) && !/[,·`()]/.test(body) && /:$/.test(line) ? body : null;
}

export function renderAssistantText(text: string) {
  return text
    .replace(/\\\((?:\\rightarrow|\\to|->)\\\)/g, "→")
    .replace(/\$(?:\\rightarrow|\\to|->)\$/g, "→")
    .replace(/\\(?:rightarrow|to)\b/g, "→");
}

function AnswerText({ text, headings, headingLabels }: {
  text: string;
  headings?: GptAnswerHeading[];
  headingLabels?: GptAnswerHeadingLabels;
}) {
  // 모델이 제목 앞뒤에 빈 줄을 넣으면 제목 여백과 겹쳐 간격이 벌어진다 - 연속 빈 줄은 하나로, 제목 앞뒤 빈 줄은 뺀다.
  const lines = text.split("\n").filter((line, index, all) => {
    if (line.trim()) return true;
    const prev = all[index - 1];
    const next = all[index + 1];
    return prev !== undefined && prev.trim() !== "" && !HEADING_PATTERN.test(prev)
      && next !== undefined && next.trim() !== "" && !HEADING_PATTERN.test(next);
  });
  // 제목을 골라 순서대로 보여 주는 것은 «제목이 있는 답»(첫 분석)에만 한다. 추가 질문의 답은 형식 없이 오는데,
  // 예전에는 여기서도 제목에 걸리는 줄만 골라서 답이 통째로 빈칸이 됐다(ETL 로그 «추가 질문 무응답», 2026-10-02).
  const hasHeading = lines.some((line) => HEADING_PATTERN.test(line));
  if (headings && hasHeading) {
    const sectionLines = new Map<GptAnswerHeading, string[]>();
    let currentHeading: GptAnswerHeading | null = null;
    for (const line of lines) {
      const raw = HEADING_PATTERN.exec(line)?.[1] as GptAnswerHeading | undefined;
      // 옛 제목도 같은 칸으로 모은다(원인 추정 → 원인, 확인사항 → 확인할 것).
      const heading = raw ? (HEADING_ALIAS[raw] ?? raw) : undefined;
      if (heading) {
        currentHeading = heading;
        if (!sectionLines.has(heading)) {
          sectionLines.set(heading, []);
        }
        continue;
      }
      if (currentHeading && sectionLines.has(currentHeading)) {
        sectionLines.get(currentHeading)?.push(line);
      }
    }
    return (
      <div className="gpt-analysis-answer">
        {headings.filter((heading) => sectionLines.has(heading)).map((heading) => {
          const body = sectionLines.get(heading) ?? [];
          // 제목 앞에는 ■ 를 붙이고(번호 대신, 2026-10-05 요청) 본문은 한 칸 들여 쓴다.
          // 본문 안의 번호 목록은 «(1)» 로 바꾼다 - 제목과 항목이 같은 «1.» 모양이면 헷갈린다(2026-10-02).
          return (
            <div key={heading}>
              <div className="gpt-analysis-heading">{headingLabels?.[heading] ?? heading}</div>
              {body.length > 0
                ? body.map((line, index) => {
                  const text = line.replace(/\*\*/g, "").replace(/^#+\s*/, "");
                  const item = /^\s*(\d+)[.)]\s+(.*)$/.exec(text);
                  return item ? (
                    <div key={`${heading}-${index}`} className="gpt-analysis-line gpt-analysis-indent gpt-analysis-item">
                      ({item[1]}) {renderAssistantText(item[2])}
                    </div>
                  ) : (
                    <div key={`${heading}-${index}`} className="gpt-analysis-line gpt-analysis-indent">
                      {renderAssistantText(text) || " "}
                    </div>
                  );
                })
                : <div className="gpt-analysis-line gpt-analysis-indent"> </div>}
            </div>
          );
        })}
      </div>
    );
  }
  // 형식이 정해지지 않은 답(속성창 요약 · 추가 질문)도 같은 모양으로 그린다:
  // 구분 제목은 ■ 를 달고, 그 아래 줄은 한 칸 들여 쓴다(2026-10-05 요청).
  let inSection = false;
  return (
    <div className="gpt-analysis-answer">
      {lines.map((line, index) => {
        const title = sectionTitleOf(line);
        if (title) {
          inSection = true;
          return <div key={index} className="gpt-analysis-heading">{title}</div>;
        }
        // 마크다운 강조·제목 기호는 글자로 보이면 거슬려 걷어 낸다. 목록 번호는 그대로 둔다.
        const text = renderAssistantText(line.replace(/\*\*/g, "").replace(/^#+\s*/, "")) || " ";
        return (
          <div key={index} className={inSection ? "gpt-analysis-line gpt-analysis-indent" : "gpt-analysis-line"}>
            {text}
          </div>
        );
      })}
    </div>
  );
}

/** 대화 목록. 요약에 들어간 앞부분은 흐리게 두고 경계에 안내 줄을 긋는다. */
export function GptTurns({ turns, summarizedUpto = 0, waitingText, headings, headingLabels }: {
  turns: GptTurn[];
  summarizedUpto?: number;
  waitingText: string;
  headings?: GptAnswerHeading[];
  headingLabels?: GptAnswerHeadingLabels;
}) {
  return (
    <>
      {turns.map((turn, index) => (
        <div key={index} className={index < summarizedUpto ? "gpt-analysis-summarized" : undefined}>
          {turn.role === "user" ? (
            <div className="gpt-analysis-question">{turn.content}</div>
          ) : (
            <div className="gpt-analysis-reply">
              {(turn.toolSteps?.length ?? 0) > 0 && (
                <div className="gpt-analysis-tools">
                  {turn.toolSteps!.map((step, stepIndex) => <div key={stepIndex}>🔎 {step}</div>)}
                </div>
              )}
              {turn.content ? <AnswerText text={turn.content} headings={headings} headingLabels={headingLabels} /> : null}
              {turn.streaming && (
                <div className="gpt-analysis-waiting">
                  <Spin size="small" /> {turn.content ? "답변 중" : waitingText}
                </div>
              )}
              {turn.stopped && <div className="gpt-analysis-stopped">중지했습니다.</div>}
            </div>
          )}
          {index === summarizedUpto - 1 && (
            <div className="gpt-analysis-divider">여기까지의 대화는 요약해서 이어 갑니다</div>
          )}
        </div>
      ))}
    </>
  );
}

/** 대화가 길어 서버가 앞부분을 줄였을 때의 안내. */
export function GptCompactedNote({ compacted }: { compacted?: LlmCompacted }) {
  if (!compacted) return null;
  return (
    <div className="gpt-analysis-note">
      {compacted.mode === "SUMMARIZED"
        ? `대화가 길어 앞 문답 ${compacted.droppedPairs}개를 요약해서 이어 갑니다.`
        : `대화가 길어 가장 오래된 추가 문답 ${compacted.droppedPairs}개를 빼고 보냈습니다.`}
    </div>
  );
}

/**
 * 질문 입력칸. Enter 보내기 · Shift+Enter 줄바꿈 · 한글 조합 중 Enter 는 무시.
 *
 * <p>{@code variant="composer"}: ETL 관리 화면 AI 어시스턴트와 같은 모양(큰 입력칸 안 오른쪽 아래 둥근 보내기 버튼).
 * 보내기 규칙은 기본 모양과 같다.
 */
export function GptAskBox({ busy, placeholder, onSend, variant = "inline" }: {
  busy: boolean;
  placeholder: string;
  /** @returns 시작 전에 거절되면 그 질문 - 입력칸으로 되돌린다 */
  onSend: (question: string) => Promise<string | undefined>;
  variant?: "inline" | "composer";
}) {
  const [question, setQuestion] = useState("");
  /*
   * 창 · 탭을 열면 커서를 질문칸에 둔다(2026-10-05 요청).
   * 답이 흐르는 동안에는 칸이 꺼져(disabled) 커서가 빠지므로, 답이 끝나 다시 쓸 수 있게 될 때 넣는다.
   * 다른 곳(검색칸 · 다른 입력칸)에 커서가 있으면 그대로 둔다 - 쓰던 자리를 뺏지 않는다.
   */
  const boxRef = useRef<HTMLTextAreaElement>(null);
  const antdBoxRef = useRef<TextAreaRef>(null);
  useEffect(() => {
    if (busy) {
      return;
    }
    // 탭 · 단추를 눌러 들어오면 그 단추에 커서가 남는다 - 그런 자리는 넘겨받고,
    // 다른 입력칸에 글을 쓰고 있을 때만 그대로 둔다.
    const active = document.activeElement as HTMLElement | null;
    const typingElsewhere = Boolean(active)
      && (active!.tagName === "INPUT" || active!.tagName === "TEXTAREA" || active!.isContentEditable);
    if (typingElsewhere) {
      return;
    }
    (boxRef.current ?? antdBoxRef.current)?.focus();
  }, [busy]);

  const send = async () => {
    const text = question.trim();
    if (!text || busy) return;
    setQuestion("");
    const rejected = await onSend(text);
    if (rejected) setQuestion(rejected);
  };
  if (variant === "composer") {
    return (
      <div className="nifi-ai-composer">
        <textarea
          ref={boxRef}
          rows={3}
          value={question}
          maxLength={4000}
          placeholder={placeholder}
          aria-label="AI 어시스턴트 질문"
          disabled={busy}
          onChange={(event) => setQuestion(event.currentTarget.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
              event.preventDefault();
              void send();
            }
          }}
        />
        <Button
          className="nifi-ai-send-button"
          shape="circle"
          icon={<ArrowUpOutlined />}
          loading={busy}
          disabled={!busy && !question.trim()}
          aria-label="보내기"
          title="보내기"
          onClick={() => void send()}
        />
      </div>
    );
  }
  return (
    <div className="gpt-analysis-ask">
      <Input.TextArea
        ref={antdBoxRef}
        value={question}
        onChange={(event) => setQuestion(event.target.value)}
        onPressEnter={(event) => {
          if (!event.shiftKey && !event.nativeEvent.isComposing) {
            event.preventDefault();
            void send();
          }
        }}
        autoSize={{ minRows: 1, maxRows: 4 }}
        maxLength={4000}
        placeholder={placeholder}
        disabled={busy}
      />
      <Button type="primary" onClick={() => void send()} disabled={busy || !question.trim()}>보내기</Button>
    </div>
  );
}

export function GptFailure({ message, onRetry }: { message?: string; onRetry?: () => void }) {
  if (!message) return null;
  return (
    <Alert type="error" showIcon message={message}
           action={onRetry ? <Button size="small" onClick={onRetry}>다시 시도</Button> : undefined} />
  );
}

/**
 * [프롬프트] 창 - 마스킹·자르기가 적용된 대상 정보. 2026-10-02 요청으로 <b>고칠 수 있다</b>:
 * 고쳐서 «적용»하면 이 창의 다음 질문부터 서버가 모은 것 대신 이 내용을 보낸다(되돌리면 원래대로).
 */
export function GptPreviewModal({ preview, onClose, onApply, overridden = false }: {
  preview: GptPreviewState;
  onClose: () => void;
  /** 없으면 예전처럼 읽기 전용으로만 보여 준다. */
  onApply?: (text?: string) => void;
  /** 지금 고친 내용을 쓰고 있는지 - 창 안내에 적는다. */
  overridden?: boolean;
}) {
  const [draft, setDraft] = useState<string>("");
  // 창을 열 때마다 지금 보낼 내용으로 입력칸을 맞춘다.
  useEffect(() => {
    if (preview.open && preview.data) {
      setDraft(preview.data.text ?? "");
    }
  }, [preview.open, preview.data]);

  return (
    <Modal open={preview.open} title="프롬프트" onCancel={onClose} width={860} rootClassName="cerebro-modal"
           footer={onApply ? (
             <>
               {overridden && <Button onClick={() => { onApply(undefined); onClose(); }}>원래대로</Button>}
               <Button type="primary" onClick={() => { onApply(draft); onClose(); }}>적용</Button>
               <Button onClick={onClose}>닫기</Button>
             </>
           ) : null}>
      {preview.loading ? (
        <div className="failure-log-empty"><Spin size="small" /> 프롬프트 정보를 모으는 중입니다.</div>
      ) : preview.error ? (
        <Alert type="error" showIcon message={preview.error} />
      ) : preview.data ? (
        <>
          <div className="gpt-analysis-note">
            {preview.data.providerName} ({DEPLOYMENT_LABEL[preview.data.deployment]}) · {preview.data.modelName}
            {" · "}개인정보 마스킹 {preview.data.masked ? "적용" : "미적용"}
            {" · "}{preview.data.inputChars.toLocaleString()}자 / 대상 정보 제한 {preview.data.maxInputChars.toLocaleString()}자
            {preview.data.truncated ? " · 일부 생략" : ""}
          </div>
          {onApply ? (
            <Input.TextArea className="gpt-analysis-preview-edit" value={draft} autoSize={{ minRows: 12, maxRows: 24 }}
                            aria-label="프롬프트 내용" onChange={(event) => setDraft(event.target.value)} />
          ) : (
            <pre className="failure-log-lines gpt-analysis-preview">{preview.data.text}</pre>
          )}
          <div className="failure-log-empty">
            이 프롬프트 정보와 함께 역할 지시문과 이번 창의 대화가 전송됩니다. 연결 계정·비밀번호·API 키는 수집하지 않습니다.
            {onApply ? " 고친 내용은 «적용»을 누르면 이 창의 다음 질문부터 쓰입니다." : ""}
            {overridden ? " (지금은 고친 내용을 쓰고 있습니다)" : ""}
          </div>
        </>
      ) : null}
    </Modal>
  );
}
