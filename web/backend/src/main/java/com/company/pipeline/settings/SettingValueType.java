package com.company.pipeline.settings;

/**
 * 설정 값의 타입 (설계서 4-2). app_setting.value_type CHECK 제약과 정확히 일치해야 한다.
 * 범위 검증은 SettingKey 카탈로그가 하고 DB는 타입만 잡는다.
 */
public enum SettingValueType {
    INT,
    DECIMAL,
    BOOLEAN,
    STRING,
    DURATION_SECONDS,
    BYTES,
    JSON
}
