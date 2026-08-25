package com.company.pipeline.authz.provisioning;

import com.company.pipeline.authz.AccessBits;
import com.company.pipeline.authz.PermissionAuditService;
import com.company.pipeline.authz.PermissionService;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.user.AppUser;
import com.company.pipeline.user.AppUserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Cerebro 계정을 NiFi 개인 사용자(테넌트)+정책으로 <b>조정</b>한다(설계서 §7.7, P5b). 역할/상태에
 * 맞춰 정책을 <b>추가만 아니라 회수까지</b> 한다:
 * <ul>
 *   <li>관리자(NIFI 쓰기) → 보기 + 수정/실행 정책 보장</li>
 *   <li>조회자(NIFI 읽기) → 보기 정책 보장, <b>수정/실행 정책 회수</b>(강등 반영)</li>
 *   <li>비활성(use_yn='N')/NiFi 권한 없음 → <b>전 정책에서 제거</b>(회수)</li>
 * </ul>
 *
 * <p>기본 off({@code authz.identity-sync.enabled}). 실패는 삼킨다(계정 관리를 막지 않음).
 * 관리 호출은 NifiClient 의 서비스계정 Bearer 로 하고, 콘솔 트래픽의 mTLS 대행은 nginx 담당.
 */
@Service
public class NifiTenantSyncService {

    private static final Logger log = LoggerFactory.getLogger(NifiTenantSyncService.class);

    private final NifiClient nifiClient;
    private final PermissionService permissionService;
    private final AppUserRepository userRepository;
    private final PermissionAuditService auditService;

    @Value("${authz.identity-sync.enabled:false}")
    private boolean enabled;

    public NifiTenantSyncService(NifiClient nifiClient, PermissionService permissionService,
                                 AppUserRepository userRepository, PermissionAuditService auditService) {
        this.nifiClient = nifiClient;
        this.permissionService = permissionService;
        this.userRepository = userRepository;
        this.auditService = auditService;
    }

    /**
     * 계정/역할/상태 변경 시 호출. NiFi 사용자·정책을 현재 권한에 맞춘다(추가+회수). 실패는 삼킨다.
     *
     * @return 동기화가 실제로 시도되어 성공했으면 true, 기능 off·잘못된 입력·실패면 false(일괄동기화 집계용).
     */
    public boolean syncUser(String userId) {
        return syncUserDetailed(userId).succeeded();
    }

    /** 실패 사유까지 돌려준다. 호출부가 응답에 경고를 실어 보낼 때 쓴다. */
    public SyncOutcome syncUserDetailed(String userId) {
        if (!enabled || userId == null || userId.isBlank()) {
            return SyncOutcome.skipped();
        }
        try {
            AppUser user = userRepository.findById(userId).orElse(null);
            boolean active = user != null && "Y".equalsIgnoreCase(user.getUseYn());
            boolean canView = active && permissionService.check(userId, SystemCode.NIFI, AccessBits.READ);
            boolean canModify = active && permissionService.check(userId, SystemCode.NIFI, AccessBits.WRITE);

            String rootPg = "/process-groups/" + nifiClient.getRootProcessGroupId();
            String[][] viewPolicies = {
                    {"/flow", "read"}, {rootPg, "read"}, {"/data" + rootPg, "read"}};
            String[][] writePolicies = {
                    {rootPg, "write"}, {"/operation" + rootPg, "write"}, {"/data" + rootPg, "write"}};

            if (!canView) {
                // 비활성/권한 없음 → 전 정책에서 제거(회수)
                String nid = nifiClient.findNifiUserId(userId);
                if (nid != null) {
                    for (String[] p : viewPolicies) {
                        nifiClient.removeNifiUserPolicy(p[0], p[1], nid);
                    }
                    for (String[] p : writePolicies) {
                        nifiClient.removeNifiUserPolicy(p[0], p[1], nid);
                    }
                    log.info("NiFi 개인계정 권한 회수 - {}", userId);
                }
                auditService.record("system", "SYNC_NIFI_USER", "USER", userId, "권한없음/비활성 → 정책 회수");
                return SyncOutcome.success();
            }

            String nid = nifiClient.ensureNifiUser(userId);
            if (nid == null) {
                log.warn("NiFi 사용자 동기화: id 확보 실패 - {}", userId);
                auditService.record("system", "SYNC_NIFI_USER", "USER", userId, "실패: NiFi 사용자 id 확보 실패");
                return SyncOutcome.failure("NiFi 사용자 id 확보 실패");
            }
            for (String[] p : viewPolicies) {
                nifiClient.ensureNifiUserPolicy(p[0], p[1], nid);
            }
            if (canModify) {
                for (String[] p : writePolicies) {
                    nifiClient.ensureNifiUserPolicy(p[0], p[1], nid);
                }
            } else {
                // 조회자로 강등 → 수정/실행 회수
                for (String[] p : writePolicies) {
                    nifiClient.removeNifiUserPolicy(p[0], p[1], nid);
                }
            }
            log.info("NiFi 개인계정 동기화 완료 - {} (modify={})", userId, canModify);
            auditService.record("system", "SYNC_NIFI_USER", "USER", userId,
                    canModify ? "보기+수정/실행 정책 보장" : "보기 정책 보장 · 수정/실행 회수");
            return SyncOutcome.success();
        } catch (RuntimeException ex) {
            log.warn("NiFi 개인계정 동기화 실패(무시하고 진행) - {}: {}", userId, ex.getMessage());
            auditService.record("system", "SYNC_NIFI_USER", "USER", userId, "실패: " + ex.getMessage());
            return SyncOutcome.failure(ex.getMessage());
        }
    }
}
