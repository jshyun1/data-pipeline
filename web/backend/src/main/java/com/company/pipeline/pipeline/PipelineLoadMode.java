package com.company.pipeline.pipeline;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import java.util.regex.Pattern;

/**
 * CDC 파이프라인의 타깃 적재 방식.
 *
 * <ul>
 *   <li>{@link #UPSERT}: 기존 동작. JDBC 싱크가 PK 기준 upsert/delete 로 타깃을 소스와 같은 모습으로 유지.</li>
 *   <li>{@link #DELTA_APPEND}: 변경 이벤트 한 건을 타깃 델타 테이블에 한 행씩 append 한다.
 *       맨 앞 구분컬럼({@code deltaOpColumn})에 Debezium op 코드(c=insert, u=update, d=delete,
 *       r=스냅샷 읽기)가 들어가고 나머지는 소스 컬럼 그대로다. CDC 기능이 없는 외부 솔루션이
 *       이 테이블을 주기적으로 읽어 자기 방식으로 반영하는 용도. 순서는 함께 만드는
 *       {@code cdc_seq} 순번 컬럼으로 판단한다.</li>
 *   <li>{@link #DELTA_UPSERT}: 같은 델타 테이블이지만 <b>PK당 한 행</b>만 남긴다(마지막 상태 +
 *       마지막 작업 종류). insert→update 면 u 한 행, insert→delete 면 d 한 행(삭제 직전 값).
 *       외부 솔루션이 "주기적으로 최신 상태만 동기화"할 때 쓴다: c/u/r 은 merge, d 는 있으면 삭제.
 *       순번 대신 덮어쓸 때마다 갱신되는 {@value #DELTA_TS_COLUMN}(이벤트 시각, epoch ms)로
 *       "가져간 만큼 삭제"의 기준을 잡는다. 타깃에 소스 PK 와 같은 PK/UNIQUE 가 있어야 한다.</li>
 * </ul>
 */
public enum PipelineLoadMode {
    UPSERT,
    DELTA_APPEND,
    DELTA_UPSERT;

    /** 델타 구분컬럼 기본 이름. */
    public static final String DEFAULT_DELTA_OP_COLUMN = "cdc_op";

    /** DELTA_UPSERT 가 SMT 로 함께 넣는 이벤트 시각 컬럼(ts_ms, epoch ms). */
    public static final String DELTA_TS_COLUMN = "cdc_ts";

    /** 델타 계열(구분컬럼을 쓰는) 방식인지. */
    public boolean isDelta() {
        return this != UPSERT;
    }

    /** 세 DB(Oracle 30자 제한 포함)에서 인용 없이 쓸 수 있는 식별자만 허용한다. */
    private static final Pattern IDENTIFIER = Pattern.compile("^[A-Za-z][A-Za-z0-9_]{0,29}$");

    public static PipelineLoadMode from(String value) {
        if (value == null || value.isBlank()) return UPSERT;
        for (PipelineLoadMode mode : values()) {
            if (mode.name().equalsIgnoreCase(value)) return mode;
        }
        throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                "지원하지 않는 적재 방식입니다: " + value + " (UPSERT, DELTA_APPEND, DELTA_UPSERT만 지원)");
    }

    /** 델타 구분컬럼명을 정규화(공백 제거, 미입력 시 기본값)하고 식별자 규칙을 검사한다. */
    public static String normalizeDeltaOpColumn(String value) {
        String column = value == null || value.isBlank() ? DEFAULT_DELTA_OP_COLUMN : value.trim();
        if (!IDENTIFIER.matcher(column).matches()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "구분컬럼명은 영문으로 시작하는 영문·숫자·_ 조합 30자 이하여야 합니다: " + column);
        }
        if (column.equalsIgnoreCase(DeltaTargetTableService.SEQUENCE_COLUMN)
                || column.equalsIgnoreCase(DELTA_TS_COLUMN)) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "구분컬럼명 '" + column + "' 은 순번/시각 컬럼으로 예약되어 있습니다.");
        }
        return column;
    }
}
