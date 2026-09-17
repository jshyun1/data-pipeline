package com.company.pipeline.llm.context;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/** 수집기들이 같이 쓰는 글 다듬기. */
final class ContextText {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    /** 화면이 보낸 오류 문구의 상한. 이보다 긴 건 대부분 반복 로그라 앞뒤만 둔다. */
    static final int MAX_ERROR_TEXT = 20_000;

    private ContextText() {
    }

    static String time(LocalDateTime value) {
        return value == null ? "-" : value.format(TIME);
    }

    static String orDash(Object value) {
        return value == null || value.toString().isBlank() ? "-" : value.toString();
    }

    static String cut(String value, int max) {
        if (value == null) {
            return null;
        }
        String flat = value.strip();
        return flat.length() <= max ? flat : flat.substring(0, max) + "…";
    }

    /** 긴 오류는 앞(최초 원인)과 뒤(마지막 상태)를 남기고 가운데를 뺀다. */
    static String errorText(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String text = value.strip();
        if (text.length() <= MAX_ERROR_TEXT) {
            return text;
        }
        int half = MAX_ERROR_TEXT / 2;
        return text.substring(0, half) + "\n…(중간 " + (text.length() - MAX_ERROR_TEXT) + "자 생략)…\n"
                + text.substring(text.length() - half);
    }

    static String required(Map<String, String> target, String key) {
        String value = target == null ? null : target.get(key);
        if (value == null || value.isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "분석 대상 정보(" + key + ")가 없습니다.");
        }
        return value.trim();
    }
}
