package com.company.pipeline.llm.context;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 수집기가 모은 «대상 정보». 제목 붙은 구역들로 이루어지고, 구역마다 자를 때의 우선순위가 있다.
 *
 * <p>입력 길이 제한({@code max_input_chars})을 넘으면 우선순위가 높은 구역부터 채우고, 남은 자리에 못 들어가는 구역은
 * 뒤를 잘라 넣거나 뺀다(설계 5-2: 오류 원문 > 단계 SQL > 실행 이력). 출력은 우선순위가 아니라 추가한 순서다 - 읽는 순서가 자연스럽게.
 */
public final class LlmContext {

    /** 오류 원문처럼 반드시 보내야 하는 것. */
    public static final int PRIORITY_ERROR = 0;
    /** 대상의 정체(이름 · 경로 · 구성). 짧다. */
    public static final int PRIORITY_IDENTITY = 1;
    /** SQL · 커넥터 오류 추적처럼 원인 파악에 직접 쓰이는 것. 길다. */
    public static final int PRIORITY_DETAIL = 2;
    /** 최근 실행 · 명령 이력. */
    public static final int PRIORITY_HISTORY = 3;

    /** 잘린 구역 끝에 붙이는 표시. 너무 조금 남으면 자르지 않고 뺀다. */
    private static final String CUT_MARK = "\n…(길이 제한으로 뒷부분 생략)";
    private static final int MIN_USEFUL_CHARS = 200;

    public record Section(String title, String body, int priority, int order) {
    }

    /** 조립 결과. {@code truncated} 면 화면에 «일부 생략» 을 알린다. */
    public record Assembled(String text, boolean truncated, List<String> omittedTitles) {
    }

    private final List<Section> sections = new ArrayList<>();

    public LlmContext add(String title, String body, int priority) {
        if (body != null && !body.isBlank()) {
            sections.add(new Section(title, body.strip(), priority, sections.size()));
        }
        return this;
    }

    public List<Section> sections() {
        return List.copyOf(sections);
    }

    public Assembled assemble(int maxChars) {
        List<Section> byPriority = new ArrayList<>(sections);
        byPriority.sort(Comparator.comparingInt(Section::priority).thenComparingInt(Section::order));
        int remaining = Math.max(maxChars, MIN_USEFUL_CHARS);
        boolean truncated = false;
        List<String> omitted = new ArrayList<>();
        List<Section> kept = new ArrayList<>();
        for (Section section : byPriority) {
            int cost = header(section).length() + section.body().length() + 2;
            if (cost <= remaining) {
                kept.add(section);
                remaining -= cost;
                continue;
            }
            truncated = true;
            int room = remaining - header(section).length() - CUT_MARK.length() - 2;
            if (room >= MIN_USEFUL_CHARS) {
                kept.add(new Section(section.title(), section.body().substring(0, room) + CUT_MARK,
                        section.priority(), section.order()));
                remaining = 0;
            } else {
                omitted.add(section.title());
            }
        }
        kept.sort(Comparator.comparingInt(Section::order));
        StringBuilder out = new StringBuilder();
        for (Section section : kept) {
            if (!out.isEmpty()) {
                out.append("\n\n");
            }
            out.append(header(section)).append(section.body());
        }
        return new Assembled(out.toString(), truncated, omitted);
    }

    private static String header(Section section) {
        return "## " + section.title() + "\n";
    }
}
