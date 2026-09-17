import { useQuery } from "@tanstack/react-query";
import { getLlmStatus } from "../../api/llm";

/** 선택된 LLM 상태. 여러 화면·여러 줄이 같은 캐시를 쓴다. */
export function useLlmStatus() {
  return useQuery({ queryKey: ["llm-status"], queryFn: getLlmStatus, staleTime: 60_000, retry: false });
}

/**
 * GPT 기능이 켜져 있는지(선택된 LLM 이 있는지). 꺼져 있으면 [GPT 분석] 버튼을 <b>그리지 않는다</b> -
 * 회색 비활성이 아니라 원래 없던 화면이 된다(설계 5-1).
 */
export function useGptEnabled(): boolean {
  return Boolean(useLlmStatus().data?.enabled);
}
