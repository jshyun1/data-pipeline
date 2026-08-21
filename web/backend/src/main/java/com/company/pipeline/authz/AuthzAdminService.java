package com.company.pipeline.authz;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.AppUser;
import com.company.pipeline.user.AppUserRepository;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 역할·권한·배정·메뉴 재정의·감사 관리(설계서 §7.1). 모든 쓰기는 감사 로그를 남기고
 * 변경 영향 사용자의 권한 캐시를 무효화한다(역할/시스템권한 변경은 광범위하므로 전체 무효화).
 *
 * <p>이 서비스는 인가를 강제하지 않는다 - 강제는 P4(@RequirePermission)에서 붙는다. 현 단계는
 * 관리 화면이 동작하도록 CRUD 만 제공한다.
 */
@Service
public class AuthzAdminService {

    private final AppRoleRepository roleRepository;
    private final AppRoleSystemPermissionRepository systemPermissionRepository;
    private final AppUserRoleRepository userRoleRepository;
    private final AppMenuRepository menuRepository;
    private final AppRoleMenuOverrideRepository menuOverrideRepository;
    private final AppUserRepository userRepository;
    private final PermissionAuditLogRepository auditLogRepository;
    private final PermissionService permissionService;
    private final PermissionAuditService auditService;
    private final com.company.pipeline.authz.provisioning.NifiTenantSyncService nifiTenantSyncService;
    private final com.company.pipeline.authz.provisioning.AirflowUserSyncService airflowUserSyncService;

    public AuthzAdminService(AppRoleRepository roleRepository,
                             AppRoleSystemPermissionRepository systemPermissionRepository,
                             AppUserRoleRepository userRoleRepository,
                             AppMenuRepository menuRepository,
                             AppRoleMenuOverrideRepository menuOverrideRepository,
                             AppUserRepository userRepository,
                             PermissionAuditLogRepository auditLogRepository,
                             PermissionService permissionService,
                             PermissionAuditService auditService,
                             com.company.pipeline.authz.provisioning.NifiTenantSyncService nifiTenantSyncService,
                             com.company.pipeline.authz.provisioning.AirflowUserSyncService airflowUserSyncService) {
        this.roleRepository = roleRepository;
        this.systemPermissionRepository = systemPermissionRepository;
        this.userRoleRepository = userRoleRepository;
        this.menuRepository = menuRepository;
        this.menuOverrideRepository = menuOverrideRepository;
        this.userRepository = userRepository;
        this.auditLogRepository = auditLogRepository;
        this.permissionService = permissionService;
        this.auditService = auditService;
        this.nifiTenantSyncService = nifiTenantSyncService;
        this.airflowUserSyncService = airflowUserSyncService;
    }

    // ----- view records ---------------------------------------------------

    public record RoleView(String roleId, String roleNm, String roleDesc, boolean builtIn,
                           String useYn, long userCount, Map<String, Integer> systemBits) {
    }

    public record RoleDetail(String roleId, String roleNm, String roleDesc, boolean builtIn,
                             Map<String, Integer> systemBits, Map<String, Boolean> menuOverrides) {
    }

    public record MenuView(String menuId, String parentId, String menuNm, String menuUrl,
                           String systemCode, int requiredBits, int sortOrd) {
    }

    public record AssignmentView(String userId, String userNm, String email,
                                 List<String> roleIds, List<String> roleNames) {
    }

    public record AuditView(Long id, String occurredAt, String actorId, String action,
                            String targetType, String targetId, String beforeValue,
                            String afterValue, String detail, String clientIp) {
    }

    // ----- roles ----------------------------------------------------------

    @Transactional(readOnly = true)
    public List<RoleView> listRoles() {
        List<RoleView> out = new ArrayList<>();
        for (AppRole role : roleRepository.findAllByOrderByRoleIdAsc()) {
            out.add(new RoleView(role.getRoleId(), role.getRoleNm(), role.getRoleDesc(),
                    role.isBuiltIn(), role.getUseYn(),
                    userRoleRepository.countByRoleId(role.getRoleId()),
                    bitsOf(role.getRoleId())));
        }
        return out;
    }

    @Transactional(readOnly = true)
    public RoleDetail getRole(String roleId) {
        AppRole role = roleRepository.findById(roleId).orElseThrow(this::roleNotFound);
        Map<String, Boolean> overrides = new LinkedHashMap<>();
        for (AppRoleMenuOverride o : menuOverrideRepository.findByRoleId(roleId)) {
            overrides.put(o.getMenuId(), o.isVisible());
        }
        return new RoleDetail(role.getRoleId(), role.getRoleNm(), role.getRoleDesc(),
                role.isBuiltIn(), bitsOf(roleId), overrides);
    }

    @Transactional
    public void createRole(String roleId, String roleNm, String roleDesc, String actor) {
        if (roleId == null || roleId.isBlank() || roleNm == null || roleNm.isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "역할 ID와 이름은 필수입니다.");
        }
        if (roleRepository.existsById(roleId)) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "이미 존재하는 역할 ID입니다: " + roleId);
        }
        roleRepository.save(new AppRole(roleId, roleNm, roleDesc, actor));
        auditService.record(actor, "CREATE_ROLE", "ROLE", roleId, roleNm);
    }

    @Transactional
    public void updateRole(String roleId, String roleNm, String roleDesc, String actor) {
        AppRole role = roleRepository.findById(roleId).orElseThrow(this::roleNotFound);
        if (role.isBuiltIn() && roleNm != null && !roleNm.equals(role.getRoleNm())) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "기본 역할은 이름을 변경할 수 없습니다.");
        }
        String before = role.getRoleNm();
        if (roleNm != null && !roleNm.isBlank()) {
            role.setRoleNm(roleNm);
        }
        role.setRoleDesc(roleDesc);
        role.setUpdatedBy(actor);
        roleRepository.save(role);
        auditService.record(actor, "UPDATE_ROLE", "ROLE", roleId, before, role.getRoleNm(), null, null);
    }

    @Transactional
    public void deleteRole(String roleId, String actor) {
        AppRole role = roleRepository.findById(roleId).orElseThrow(this::roleNotFound);
        if (role.isBuiltIn()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "기본 역할은 삭제할 수 없습니다.");
        }
        roleRepository.delete(role);   // FK ON DELETE CASCADE 로 권한/배정/메뉴재정의도 함께 삭제
        permissionService.invalidateAll();
        auditService.record(actor, "DELETE_ROLE", "ROLE", roleId, role.getRoleNm());
    }

    @Transactional
    public void setRolePermissions(String roleId, Map<String, Integer> systemBits, String actor) {
        AppRole role = roleRepository.findById(roleId).orElseThrow(this::roleNotFound);
        String before = bitsOf(roleId).toString();
        systemPermissionRepository.deleteAll(systemPermissionRepository.findByRoleId(roleId));
        List<AppRoleSystemPermission> rows = new ArrayList<>();
        for (SystemCode sc : SystemCode.values()) {
            int bits = systemBits.getOrDefault(sc.name(), 0);
            if (bits < 0 || bits > AccessBits.ALL) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR, "권한 비트값이 올바르지 않습니다: " + bits);
            }
            rows.add(new AppRoleSystemPermission(roleId, sc.name(), bits, actor));
        }
        systemPermissionRepository.saveAll(rows);
        permissionService.invalidateAll();
        auditService.record(actor, "GRANT_SYSTEM", "ROLE", roleId, before, bitsOf(roleId).toString(), null, null);
    }

    @Transactional
    public void setRoleMenuOverrides(String roleId, Map<String, Boolean> overrides, String actor) {
        roleRepository.findById(roleId).orElseThrow(this::roleNotFound);
        menuOverrideRepository.deleteAll(menuOverrideRepository.findByRoleId(roleId));
        List<AppRoleMenuOverride> rows = new ArrayList<>();
        if (overrides != null) {
            overrides.forEach((menuId, visible) -> rows.add(new AppRoleMenuOverride(roleId, menuId, visible)));
        }
        menuOverrideRepository.saveAll(rows);
        permissionService.invalidateAll();
        auditService.record(actor, "SET_MENU", "ROLE", roleId, "메뉴 재정의 " + rows.size() + "건");
    }

    // ----- assignments ----------------------------------------------------

    @Transactional(readOnly = true)
    public List<AssignmentView> listAssignments() {
        Map<String, String> roleNames = new LinkedHashMap<>();
        roleRepository.findAllByOrderByRoleIdAsc().forEach(r -> roleNames.put(r.getRoleId(), r.getRoleNm()));

        Map<String, List<String>> rolesByUser = new LinkedHashMap<>();
        for (AppUserRole ur : userRoleRepository.findAll()) {
            rolesByUser.computeIfAbsent(ur.getUserId(), k -> new ArrayList<>()).add(ur.getRoleId());
        }

        List<AssignmentView> out = new ArrayList<>();
        for (AppUser user : userRepository.findAll()) {
            List<String> roleIds = rolesByUser.getOrDefault(user.getUserId(), List.of());
            List<String> names = roleIds.stream().map(id -> roleNames.getOrDefault(id, id)).toList();
            out.add(new AssignmentView(user.getUserId(), user.getUserNm(), user.getEmail(), roleIds, names));
        }
        return out;
    }

    @Transactional
    public void setUserRoles(String userId, List<String> roleIds, String actor) {
        AppUser targetUser = userRepository.findById(userId).orElseThrow(() ->
                new BusinessException(ErrorCode.VALIDATION_ERROR, "사용자를 찾을 수 없습니다: " + userId));
        List<String> before = userRoleRepository.findByUserId(userId).stream()
                .map(AppUserRole::getRoleId).toList();
        userRoleRepository.deleteAll(userRoleRepository.findByUserId(userId));
        if (roleIds != null) {
            for (String roleId : roleIds) {
                if (!roleRepository.existsById(roleId)) {
                    throw new BusinessException(ErrorCode.VALIDATION_ERROR, "존재하지 않는 역할입니다: " + roleId);
                }
                userRoleRepository.save(new AppUserRole(userId, roleId, actor));
            }
        }
        permissionService.invalidate(userId);
        // 역할이 바뀌면 NiFi/Airflow 개인계정도 맞춘다(P5b, 기본 off·best-effort).
        nifiTenantSyncService.syncUser(userId);
        airflowUserSyncService.syncUser(userId, targetUser.getUserNm(), targetUser.getEmail());
        auditService.record(actor, "ASSIGN_ROLE", "USER", userId,
                String.join(",", before), roleIds == null ? "" : String.join(",", roleIds), null, null);
    }

    // ----- menus / audit --------------------------------------------------

    @Transactional(readOnly = true)
    public List<MenuView> listMenus() {
        return menuRepository.findByUseYnOrderBySortOrdAsc("Y").stream()
                .map(m -> new MenuView(m.getMenuId(), m.getParentId(), m.getMenuNm(), m.getMenuUrl(),
                        m.getSystemCode(), m.getRequiredBits(), m.getSortOrd()))
                .toList();
    }

    @Transactional(readOnly = true)
    public List<AuditView> listAudit(int limit) {
        int size = Math.max(1, Math.min(limit, 1000));
        return auditLogRepository.findAll(PageRequest.of(0, size, Sort.by(Sort.Direction.DESC, "occurredAt")))
                .getContent().stream()
                .map(a -> new AuditView(a.getId(),
                        a.getOccurredAt() == null ? null : a.getOccurredAt().toString(),
                        a.getActorId(), a.getAction(), a.getTargetType(), a.getTargetId(),
                        a.getBeforeValue(), a.getAfterValue(), a.getDetail(), a.getClientIp()))
                .toList();
    }

    // ----- helpers --------------------------------------------------------

    private Map<String, Integer> bitsOf(String roleId) {
        Map<String, Integer> bits = new LinkedHashMap<>();
        for (SystemCode sc : SystemCode.values()) {
            bits.put(sc.name(), 0);
        }
        for (AppRoleSystemPermission p : systemPermissionRepository.findByRoleId(roleId)) {
            bits.put(p.getSystemCode(), p.getAccessBits());
        }
        return bits;
    }

    private BusinessException roleNotFound() {
        return new BusinessException(ErrorCode.VALIDATION_ERROR, "역할을 찾을 수 없습니다.");
    }
}
