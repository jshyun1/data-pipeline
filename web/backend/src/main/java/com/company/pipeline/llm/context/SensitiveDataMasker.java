package com.company.pipeline.llm.context;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 개인정보 패턴 마스킹(설계 6절). 값을 지우지 않고 <b>종류를 남긴다</b> - «이 칸에 전화번호가 있었다»는 게 분석에 필요하다.
 *
 * <p>형태가 정해진 값만 잡는다. 이름 · 주소 · 자유 문장은 못 가린다(«완전 차단»이 아니라 «위험 줄이기»).
 * 오탐을 줄이려고 숫자열은 형식까지 확인한다: 주민번호는 앞 6자리가 날짜여야 하고, 카드번호는 Luhn 검사를 통과해야 한다.
 * 그렇지 않으면 Kafka offset · epoch 밀리초 같은 긴 숫자가 전부 가려져 분석이 망가진다.
 */
public final class SensitiveDataMasker {

    public static final String EMAIL = "<이메일>";
    public static final String RRN = "<주민번호>";
    public static final String CARD = "<카드번호>";
    public static final String PHONE = "<전화번호>";

    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}");
    /** 주민 · 외국인등록번호. 뒷자리 첫 숫자 1~8. */
    private static final Pattern RRN_PATTERN =
            Pattern.compile("(?<!\\d)(\\d{2})(\\d{2})(\\d{2})[- ]?([1-8]\\d{6})(?!\\d)");
    private static final Pattern CARD_PATTERN =
            Pattern.compile("(?<!\\d)(\\d{4})[- ]?(\\d{4})[- ]?(\\d{4})[- ]?(\\d{4})(?!\\d)");
    /** 휴대전화 010 등, 지역번호 02 · 031~064, 대표번호 15xx/16xx/18xx 는 제외(업무 번호). */
    private static final Pattern PHONE_PATTERN = Pattern.compile(
            "(?<!\\d)(?:01[016789]|02|0[3-6][1-5])[-. ]?\\d{3,4}[-. ]?\\d{4}(?!\\d)");

    private SensitiveDataMasker() {
    }

    public static String mask(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        String out = EMAIL_PATTERN.matcher(text).replaceAll(EMAIL);
        out = replaceIf(RRN_PATTERN, out, RRN, SensitiveDataMasker::looksLikeRrn);
        out = replaceIf(CARD_PATTERN, out, CARD, m -> luhn(m.group(1) + m.group(2) + m.group(3) + m.group(4)));
        out = PHONE_PATTERN.matcher(out).replaceAll(PHONE);
        return out;
    }

    private interface Check {
        boolean test(Matcher matcher);
    }

    private static String replaceIf(Pattern pattern, String text, String replacement, Check check) {
        Matcher matcher = pattern.matcher(text);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            matcher.appendReplacement(out, Matcher.quoteReplacement(check.test(matcher) ? replacement : matcher.group()));
        }
        matcher.appendTail(out);
        return out.toString();
    }

    private static boolean looksLikeRrn(Matcher matcher) {
        int month = Integer.parseInt(matcher.group(2));
        int day = Integer.parseInt(matcher.group(3));
        return month >= 1 && month <= 12 && day >= 1 && day <= 31;
    }

    static boolean luhn(String digits) {
        int sum = 0;
        boolean doubled = false;
        for (int i = digits.length() - 1; i >= 0; i--) {
            int d = digits.charAt(i) - '0';
            if (doubled) {
                d *= 2;
                if (d > 9) {
                    d -= 9;
                }
            }
            sum += d;
            doubled = !doubled;
        }
        return sum % 10 == 0;
    }
}
