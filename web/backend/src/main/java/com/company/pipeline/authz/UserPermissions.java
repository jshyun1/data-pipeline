package com.company.pipeline.authz;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 한 사용자의 최종 권한(설계서 §4.5). 역할 여러 개의 시스템 비트를 합집합(OR)한 결과 +
 * admin 단락 + 노출 메뉴 트리. JWT에 담지 않고 요청마다 {@link PermissionService}가 캐시에서
 * 돌려준다(권한 회수가 즉시 반영되도록).
 */
public record UserPermissions(
        String userId,
        boolean admin,
        Set<String> roles,
        Map<SystemCode, Integer> systemBits,
        List<MenuNode> menus) {

    public int bitsFor(SystemCode system) {
        return systemBits.getOrDefault(system, 0);
    }

    /** admin 이면 무조건 허용, 아니면 (granted &amp; required) == required. */
    public boolean can(SystemCode system, int requiredBits) {
        if (admin) {
            return true;
        }
        return AccessBits.allows(bitsFor(system), requiredBits);
    }
}
