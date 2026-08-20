package com.company.pipeline.authz;

/**
 * 접근 권한 비트마스크(설계서 §4.3, nd_suite와 동일 규칙). 저장은 3비트지만 2단계 UI에서
 * 만들어지는 값은 0(권한 없음)/1(읽기)/7(쓰기) 세 가지다. 실행을 나중에 분리해도 스키마
 * 변경 없이 화면만 고치면 된다.
 */
public final class AccessBits {

    /** 보기. */
    public static final int READ = 1;
    /** 변경. 2단계 UI에서는 "쓰기" 체크박스에 EXECUTE와 함께 묶인다. */
    public static final int WRITE = 2;
    /** 실행. 2단계 UI에서는 "쓰기" 체크박스에 묶인다. */
    public static final int EXECUTE = 4;
    /** 보기+변경+실행. 2단계 "쓰기"가 저장하는 값. */
    public static final int ALL = READ | WRITE | EXECUTE;   // 7

    private AccessBits() {
    }

    /**
     * 부여된 권한이 요구 권한을 포함하는지. 판정은 한 줄이다(설계서 §2.2/§4.3):
     * {@code (granted & required) == required}.
     */
    public static boolean allows(int granted, int required) {
        return (granted & required) == required;
    }
}
