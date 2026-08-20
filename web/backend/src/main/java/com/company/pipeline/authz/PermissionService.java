package com.company.pipeline.authz;

import com.company.pipeline.user.AppUser;
import com.company.pipeline.user.AppUserRepository;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 사용자의 최종 권한을 계산하고(설계서 §4.5) 5분 TTL 인프로세스 캐시로 돌려준다. Caffeine을
 * 별도 의존성으로 들이지 않고 ConcurrentHashMap + 만료시각으로 가볍게 구현한다(값이 작고
 * 조회 빈도가 낮다). 권한 변경 시 해당 사용자 캐시를 {@link #invalidate(String)}로 즉시 비운다.
 *
 * <p>이 클래스는 아직 인가를 강제하지 않는다 - 조회 API(AuthzController)만 쓴다. 실제 강제
 * (@RequirePermission 애스펙트 + SecurityConfig)는 이후 단계(P4)에서 붙인다.
 */
@Service
public class PermissionService {

    private static final long CACHE_TTL_MS = 5 * 60 * 1000L;

    private final AppUserRepository appUserRepository;
    private final AppUserRoleRepository appUserRoleRepository;
    private final AppRoleSystemPermissionRepository systemPermissionRepository;
    private final AppMenuRepository appMenuRepository;
    private final AppRoleMenuOverrideRepository menuOverrideRepository;

    private final Map<String, Cached> cache = new ConcurrentHashMap<>();

    private record Cached(UserPermissions perms, long expiresAt) {
    }

    public PermissionService(AppUserRepository appUserRepository,
                             AppUserRoleRepository appUserRoleRepository,
                             AppRoleSystemPermissionRepository systemPermissionRepository,
                             AppMenuRepository appMenuRepository,
                             AppRoleMenuOverrideRepository menuOverrideRepository) {
        this.appUserRepository = appUserRepository;
        this.appUserRoleRepository = appUserRoleRepository;
        this.systemPermissionRepository = systemPermissionRepository;
        this.appMenuRepository = appMenuRepository;
        this.menuOverrideRepository = menuOverrideRepository;
    }

    @Transactional(readOnly = true)
    public UserPermissions resolve(String userId) {
        long now = System.currentTimeMillis();
        Cached cached = cache.get(userId);
        if (cached != null && cached.expiresAt() > now) {
            return cached.perms();
        }
        UserPermissions perms = load(userId);
        cache.put(userId, new Cached(perms, now + CACHE_TTL_MS));
        return perms;
    }

    public boolean check(String userId, SystemCode system, int requiredBits) {
        return resolve(userId).can(system, requiredBits);
    }

    public void invalidate(String userId) {
        cache.remove(userId);
    }

    public void invalidateAll() {
        cache.clear();
    }

    private UserPermissions load(String userId) {
        AppUser user = appUserRepository.findById(userId).orElse(null);
        boolean admin = user != null && user.isAdmin();

        Set<String> roleIds = new LinkedHashSet<>();
        for (AppUserRole ur : appUserRoleRepository.findByUserId(userId)) {
            roleIds.add(ur.getRoleId());
        }

        Map<SystemCode, Integer> bits = new EnumMap<>(SystemCode.class);
        for (SystemCode sc : SystemCode.values()) {
            bits.put(sc, 0);
        }
        if (!roleIds.isEmpty()) {
            for (AppRoleSystemPermission p : systemPermissionRepository.findByRoleIdIn(roleIds)) {
                SystemCode sc = parseSystem(p.getSystemCode());
                if (sc != null) {
                    bits.merge(sc, p.getAccessBits(), (a, b) -> a | b);
                }
            }
        }
        // admin_yn='Y' → 전 시스템 7 (설계서 §4.5 부트스트랩·백도어)
        if (admin) {
            for (SystemCode sc : SystemCode.values()) {
                bits.put(sc, AccessBits.ALL);
            }
        }

        List<MenuNode> menus = buildMenuTree(bits, admin, roleIds);
        return new UserPermissions(userId, admin, roleIds, bits, menus);
    }

    private List<MenuNode> buildMenuTree(Map<SystemCode, Integer> bits, boolean admin, Set<String> roleIds) {
        List<AppMenu> all = appMenuRepository.findByUseYnOrderBySortOrdAsc("Y");

        // 역할별 재정의: 한 역할이라도 visible=true 로 지정하면 우선 노출(설계서 §4.5).
        Map<String, Boolean> override = new HashMap<>();
        if (!roleIds.isEmpty()) {
            for (AppRoleMenuOverride o : menuOverrideRepository.findByRoleIdIn(roleIds)) {
                override.merge(o.getMenuId(), o.isVisible(), (a, b) -> a || b);
            }
        }

        Map<String, Boolean> selfVisible = new HashMap<>();
        Map<String, List<AppMenu>> childrenOf = new HashMap<>();
        List<AppMenu> roots = new ArrayList<>();
        for (AppMenu m : all) {
            Boolean ov = override.get(m.getMenuId());
            boolean vis;
            if (ov != null) {
                vis = ov;
            } else {
                SystemCode sc = parseSystem(m.getSystemCode());
                int granted = sc == null ? 0 : bits.getOrDefault(sc, 0);
                vis = admin || AccessBits.allows(granted, m.getRequiredBits());
            }
            selfVisible.put(m.getMenuId(), vis);
            if (m.getParentId() == null) {
                roots.add(m);
            } else {
                childrenOf.computeIfAbsent(m.getParentId(), k -> new ArrayList<>()).add(m);
            }
        }

        List<MenuNode> tree = new ArrayList<>();
        for (AppMenu root : roots) {
            MenuNode node = toNode(root, childrenOf, selfVisible);
            if (node != null) {
                tree.add(node);
            }
        }
        return tree;
    }

    /** 리프는 selfVisible이면 포함, 그룹은 selfVisible이고 노출 자식이 하나라도 있으면 포함. */
    private MenuNode toNode(AppMenu menu, Map<String, List<AppMenu>> childrenOf, Map<String, Boolean> selfVisible) {
        if (!Boolean.TRUE.equals(selfVisible.get(menu.getMenuId()))) {
            return null;
        }
        List<MenuNode> children = new ArrayList<>();
        for (AppMenu child : childrenOf.getOrDefault(menu.getMenuId(), List.of())) {
            MenuNode cn = toNode(child, childrenOf, selfVisible);
            if (cn != null) {
                children.add(cn);
            }
        }
        if (menu.isGroup() && children.isEmpty()) {
            return null;   // 노출할 자식이 없는 그룹 노드는 감춘다
        }
        return new MenuNode(menu.getMenuId(), menu.getParentId(), menu.getMenuNm(),
                menu.getMenuUrl(), menu.getIcon(), menu.getSystemCode(), menu.getSortOrd(), children);
    }

    private SystemCode parseSystem(String code) {
        if (code == null) {
            return null;
        }
        try {
            return SystemCode.valueOf(code);
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }
}
