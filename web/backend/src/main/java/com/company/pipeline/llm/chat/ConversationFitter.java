package com.company.pipeline.llm.chat;

import com.company.pipeline.llm.client.LlmMessage;
import java.util.ArrayList;
import java.util.List;

/**
 * 대화가 입력 길이 제한을 넘지 않게 오래된 문답을 골라낸다.
 *
 * <p>대상 정보 · (오류 분석의) 첫 분석 · 이번 질문 · 요약은 항상 남기고, 그 뒤의 문답을 <b>최신부터</b> 채워 넣어 자리가 없으면
 * 오래된 쪽을 뺀다. 문답은 «질문 + 답» 한 쌍으로만 빼서 user · assistant 순서가 깨지지 않게 한다.
 *
 * <p>화면이 보낸 대화에는 답을 못 받은 질문(시작 전에 거절 · 답 없이 끊김)이 섞일 수 있어, 답이 붙은 질문만 쌍으로 인정한다.
 */
public final class ConversationFitter {

    private ConversationFitter() {
    }

    /** 질문과 답 한 쌍. {@code startIndex} 는 화면이 보낸 대화 목록에서 질문의 위치다. */
    public record Pair(LlmMessage user, LlmMessage assistant, int startIndex) {
        int chars() {
            return user.content().length() + assistant.content().length();
        }
    }

    /**
     * @param firstAnswer 오류 분석의 첫 분석 답(대화 맨 앞의 assistant). 속성창 대화면 null
     * @param rawSize     화면이 보낸 대화 목록 길이
     */
    public record Normalized(String firstAnswer, List<Pair> pairs, int rawSize) {
    }

    /**
     * @param droppedTurns 화면이 보낸 대화 목록 앞에서부터 몇 개가 빠졌는지(요약으로 넘긴 뒤 화면이 그만큼 덜 보내면 된다)
     */
    public record Plan(String firstAnswer, List<Pair> kept, List<Pair> dropped, int droppedTurns) {

        public List<LlmMessage> keptTurns() {
            List<LlmMessage> out = new ArrayList<>();
            kept.forEach(pair -> {
                out.add(pair.user());
                out.add(pair.assistant());
            });
            return out;
        }

        public List<LlmMessage> droppedTurnsAsMessages() {
            List<LlmMessage> out = new ArrayList<>();
            dropped.forEach(pair -> {
                out.add(pair.user());
                out.add(pair.assistant());
            });
            return out;
        }
    }

    /** @param leadingAnswer 오류 분석이면 true - 맨 앞 assistant 들을 첫 분석으로 본다 */
    public static Normalized normalize(List<LlmMessage> history, boolean leadingAnswer) {
        List<LlmMessage> turns = history == null ? List.of() : history;
        int i = 0;
        StringBuilder first = new StringBuilder();
        if (leadingAnswer) {
            while (i < turns.size() && "assistant".equals(turns.get(i).role())) {
                if (!first.isEmpty()) {
                    first.append("\n\n");
                }
                first.append(turns.get(i).content());
                i++;
            }
        }
        List<Pair> pairs = new ArrayList<>();
        while (i < turns.size()) {
            LlmMessage turn = turns.get(i);
            if ("user".equals(turn.role()) && i + 1 < turns.size() && "assistant".equals(turns.get(i + 1).role())) {
                pairs.add(new Pair(turn, turns.get(i + 1), i));
                i += 2;
            } else {
                i++;   // 답 없는 질문 · 질문 없는 답은 건너뛴다
            }
        }
        return new Normalized(first.isEmpty() ? null : first.toString(), pairs, turns.size());
    }

    /**
     * @param fixedChars 항상 보내는 것(대상 정보 · 이번 질문 · 요약 자리)의 글자 수
     * @param maxChars   입력 길이 제한
     */
    public static Plan fit(Normalized normalized, int fixedChars, int maxChars) {
        int available = maxChars - fixedChars - (normalized.firstAnswer() == null ? 0 : normalized.firstAnswer().length());
        List<Pair> pairs = normalized.pairs();
        int keepFrom = pairs.size();
        int used = 0;
        for (int index = pairs.size() - 1; index >= 0; index--) {
            int cost = pairs.get(index).chars();
            if (used + cost > available) {
                break;
            }
            used += cost;
            keepFrom = index;
        }
        List<Pair> kept = List.copyOf(pairs.subList(keepFrom, pairs.size()));
        List<Pair> dropped = List.copyOf(pairs.subList(0, keepFrom));
        int droppedTurns = dropped.isEmpty() ? 0
                : kept.isEmpty() ? normalized.rawSize() : kept.get(0).startIndex();
        return new Plan(normalized.firstAnswer(), kept, dropped, droppedTurns);
    }
}
