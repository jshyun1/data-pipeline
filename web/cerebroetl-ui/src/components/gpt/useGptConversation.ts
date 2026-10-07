import { useCallback, useEffect, useRef, useState } from "react";
import {
  previewLlmContext,
  streamLlmChat,
  type LlmAgentType,
  type LlmChatRequest,
  type LlmChatTurn,
  type LlmCompacted,
  type LlmContextPreview,
  type LlmTargetType,
} from "../../api/llm";

export interface GptTurn extends LlmChatTurn {
  /** 답이 흘러나오는 중 */
  streaming?: boolean;
  /** [중지]로 끊은 답 */
  stopped?: boolean;
  /** 이 답을 쓰기 전에 도구로 조회한 대상(2026-10-02) - «🔎 CDC 파이프라인 «이름» 조회» 줄로 보여 준다. 대화로 보내지 않는다. */
  toolSteps?: string[];
}

export interface GptPreviewState {
  open: boolean;
  loading: boolean;
  data?: LlmContextPreview;
  error?: string;
}

export interface GptConversationOptions {
  agentType: LlmAgentType;
  targetType: LlmTargetType;
  target: Record<string, string>;
  /** 화면에 보이는 오류 문구(오류 분석). 속성창 질문이면 없다. */
  errorText?: string;
  /** WORKFLOW_DRAFT: 작성 중 설계를 요청 때마다 만든다(보낼 때의 최신 설계가 가게). */
  draft?: () => string;
}

const errorText = (error: unknown) => (error instanceof Error ? error.message : String(error));

/**
 * GPT 대화 한 창의 상태(설계 5-2 · 5-3). 오류 분석 창과 속성창 GPT 탭이 같이 쓴다.
 *
 * <p>대화는 화면만 들고 있다(서버는 저장하지 않는다). 요청마다 이번 창의 대화를 같이 보내고, 서버가 대상 정보를 새로 모아 붙인다.
 * 대화가 길어지면 서버가 앞부분을 줄이고 {@code compacted} 로 알린다:
 * <ul>
 *   <li>DROPPED(오류 분석) - 오래된 추가 문답을 뺐다. 화면은 안내만 하고 대화는 그대로 들고 있는다(다음에도 서버가 같은 규칙으로 뺀다).</li>
 *   <li>SUMMARIZED(속성창) - 앞부분을 요약했다. 화면은 요약을 받아 두고, 요약에 들어간 문답은 다음부터 보내지 않는다(흐리게 남겨 보여 준다).</li>
 * </ul>
 */
export function useGptConversation({ agentType, targetType, target, errorText: screenError, draft }: GptConversationOptions) {
  const draftRef = useRef(draft);
  draftRef.current = draft;
  const [turns, setTurns] = useState<GptTurn[]>([]);
  const [summary, setSummary] = useState<string | null>(null);
  /** turns 중 앞에서 몇 개가 요약에 들어갔는지. 이 앞은 보내지 않는다. */
  const [summarizedUpto, setSummarizedUpto] = useState(0);
  const [compacted, setCompacted] = useState<LlmCompacted>();
  const [meta, setMeta] = useState<LlmContextPreview>();
  const [failure, setFailure] = useState<string>();
  const [busy, setBusy] = useState(false);
  const [preview, setPreview] = useState<GptPreviewState>({ open: false, loading: false });
  /**
   * [프롬프트] 창에서 고친 대상 정보(2026-10-02 요청). 있으면 이 창의 다음 요청부터 서버가 모은 것 대신 이것을 보낸다.
   * 되돌리면 다시 서버가 모은 것으로 돌아간다.
   */
  const [contextOverride, setContextOverride] = useState<string>();
  const abortRef = useRef<AbortController | null>(null);

  const baseRequest = useCallback((): LlmChatRequest => ({
    agentType, targetType, target, errorText: screenError, draft: draftRef.current?.(),
    contextOverride: contextOverride ?? null,
  }),
    // target 은 호출하는 쪽이 매 렌더 새 객체로 넘긴다 - 내용으로 비교한다.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [agentType, targetType, JSON.stringify(target), screenError, contextOverride]);

  /**
   * @param history 지금까지의 대화(화면에 보이는 그대로)
   * @param ask     이번 질문. 오류 분석 · 대상 요약의 첫 요청이면 없다
   * @param shownAsk 화면에만 적는 질문(2026-10-02 요청). 서버에는 질문 없이 보내 정해진 요약 · 분석 요청을 그대로 쓰되,
   *                 대화에는 «무엇을 물었는지»가 한 줄 남는다. 다음 요청부터는 이 줄도 대화에 포함해 보낸다
   */
  const run = async (history: GptTurn[], ask?: string, shownAsk?: string) => {
    const controller = new AbortController();
    abortRef.current = controller;
    setBusy(true);
    setFailure(undefined);
    const asked = ask ?? shownAsk;
    setTurns([...history, ...(asked ? [{ role: "user" as const, content: asked }] : []), { role: "assistant", content: "", streaming: true }]);
    const updateLast = (update: (turn: GptTurn) => GptTurn) =>
      setTurns((current) => current.map((turn, index) => (index === current.length - 1 ? update(turn) : turn)));

    // 보낼 대화: 요약에 들어간 앞부분과 빈 답은 뺀다. 서버가 «앞에서 몇 개를 줄였다»고 하면 이 목록의 위치로 되짚는다.
    const from = agentType === "JOB_ANALYSIS" ? summarizedUpto : 0;
    const sent = history.map((turn, index) => ({ turn, index })).slice(from).filter(({ turn }) => turn.content.trim());
    try {
      await streamLlmChat(
        {
          ...baseRequest(),
          history: sent.map(({ turn }) => ({ role: turn.role, content: turn.content })),
          summary: agentType === "JOB_ANALYSIS" ? summary : null,
          question: ask ?? null,
        },
        {
          onMeta: setMeta,
          onCompacted: (event) => {
            setCompacted(event);
            if (event.mode === "SUMMARIZED" && event.summary) {
              setSummary(event.summary);
              setSummarizedUpto(event.droppedTurns < sent.length ? sent[event.droppedTurns].index : history.length);
            }
          },
          onDelta: (text) => updateLast((turn) => ({ ...turn, content: turn.content + text })),
          onTool: (tool) => updateLast((turn) => ({ ...turn, toolSteps: [...(turn.toolSteps ?? []), tool.label] })),
          onError: (error) => setFailure(error.message),
        },
        controller.signal,
      );
    } catch (error) {
      // 창을 닫는 중이거나 새 요청이 이미 시작됐으면(개발 모드의 이중 마운트 등) 이 요청의 뒷정리는 화면에 반영하지 않는다.
      if (abortRef.current !== controller) return;
      if (controller.signal.aborted) {
        updateLast((turn) => ({ ...turn, stopped: true }));
      } else {
        // 시작 전에 거절됐다(권한·대상 없음·선택 해제 등). 빈 답 자리를 치우고, 질문은 입력칸으로 되돌린다.
        setTurns(history);
        setFailure(errorText(error));
        abortRef.current = null;
        setBusy(false);
        return ask;
      }
    }
    if (abortRef.current !== controller) return;
    updateLast((turn) => (turn.role === "assistant" ? { ...turn, streaming: false } : turn));
    setTurns((current) => current.filter((turn) => turn.role === "user" || turn.content || turn.stopped));
    setBusy(false);
    abortRef.current = null;
    return undefined;
  };

  /** 창을 닫거나 대상이 바뀌면 흐르던 요청을 끊는다. */
  useEffect(() => () => {
    const controller = abortRef.current;
    abortRef.current = null;
    controller?.abort();
  }, []);

  const stop = () => abortRef.current?.abort();

  /** 새 대화 - 속성창 GPT 탭의 [새 대화]. */
  const reset = () => {
    stop();
    abortRef.current = null;
    setTurns([]);
    setSummary(null);
    setSummarizedUpto(0);
    setCompacted(undefined);
    setFailure(undefined);
    setBusy(false);
  };

  const openPreview = async () => {
    setPreview({ open: true, loading: true });
    try {
      setPreview({ open: true, loading: false, data: await previewLlmContext(baseRequest()) });
    } catch (error) {
      setPreview({ open: true, loading: false, error: errorText(error) });
    }
  };

  return {
    turns,
    summarizedUpto,
    compacted,
    meta,
    failure,
    busy,
    preview,
    /** @returns 시작 전에 거절되면 그 질문(입력칸으로 되돌리라고) */
    send: (ask?: string) => run(turns, ask),
    /** 탭·창을 열 때의 첫 요청. 질문은 서버가 정한 것을 쓰고, 화면에는 shownAsk 한 줄을 남긴다. */
    sendOpening: (shownAsk: string) => run([], undefined, shownAsk),
    retryFirst: () => run([]),
    stop,
    reset,
    openPreview,
    closePreview: () => setPreview({ open: false, loading: false }),
    contextOverride,
    /** [프롬프트] 창의 고친 내용을 이 창에 적용한다. 빈 값이면 서버가 모은 것으로 되돌린다. */
    applyContextOverride: (text?: string) => setContextOverride(text && text.trim() ? text : undefined),
    answered: turns.some((turn) => turn.role === "assistant" && turn.content && !turn.streaming),
  };
}
