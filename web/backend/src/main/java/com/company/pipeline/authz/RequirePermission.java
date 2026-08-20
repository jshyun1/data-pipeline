package com.company.pipeline.authz;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 이 엔드포인트를 호출하려면 주어진 시스템에 대해 요구 비트 권한이 필요함을 선언한다(설계서 §7.1).
 * 실제 강제는 {@link PermissionAspect} 가 하며, {@code authz.enforcement.enabled=true} 일 때만
 * 동작한다(기본 off - 그때까지는 순수 선언이라 어떤 동작도 바꾸지 않는다).
 *
 * <p>URL 패턴이 아니라 애노테이션으로 권한을 표현한다 - 라우팅이 바뀌어도 조용히 깨지지 않도록.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface RequirePermission {

    SystemCode system();

    /** 요구 비트. 2단계에서는 READ(1) 또는 WRITE(7). */
    int bits() default AccessBits.READ;
}
