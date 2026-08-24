package com.company.pipeline.nifi;

import com.company.pipeline.authz.PermissionAuditService;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * NiFi 캔버스 편집을 감사 로그로 수집한다(설계서 §7.8 확장).
 *
 * <p>캔버스 편집은 NiFi UI 안에서 일어나 pipeline-api 를 안 거치므로 {@code @RequirePermission} 자동
 * 감사가 못 본다. 대신 NiFi 자체의 Flow Configuration History 를 주기적으로 읽어, per-user 프록시
 * (P5b)로 기록된 <b>실명 사용자</b>의 편집만 골라 {@code permission_audit_log} 에 남긴다.
 *
 * <ul>
 *   <li>서비스(대행) 계정이 남긴 상태 오버레이 쓰기와 Label 편집은 소음이라 제외한다.</li>
 *   <li>기동 후 첫 폴은 기준점만 잡고 과거 이력은 백필하지 않는다(감사 폭주 방지).</li>
 *   <li>커서는 인메모리라 재기동 중 발생한 편집은 유실될 수 있다(감사 보조 뷰 수준).</li>
 *   <li>per-user 프록시({@code authz.proxy.enabled})가 켜졌을 때만 동작 - 그래야 실명이 남는다.</li>
 * </ul>
 */
@Service
public class NifiCanvasAuditService {

    private static final Logger log = LoggerFactory.getLogger(NifiCanvasAuditService.class);

    private final NifiClient nifiClient;
    private final PermissionAuditService auditService;

    @Value("${authz.proxy.enabled:false}")
    private boolean enabled;

    private volatile int cursor = -1;

    public NifiCanvasAuditService(NifiClient nifiClient, PermissionAuditService auditService) {
        this.nifiClient = nifiClient;
        this.auditService = auditService;
    }

    @Scheduled(fixedRate = 60_000, initialDelay = 40_000)
    public void poll() {
        if (!enabled) {
            return;
        }
        try {
            List<NifiClient.FlowAction> actions = nifiClient.getFlowHistory(cursor);
            if (actions.isEmpty()) {
                return;
            }
            int max = cursor;
            for (NifiClient.FlowAction a : actions) {
                max = Math.max(max, a.id());
            }
            if (cursor < 0) {
                cursor = max;   // 첫 폴: 기준점만, 과거 백필 안 함
                return;
            }
            String serviceUser = nifiClient.getServiceUsername();
            for (NifiClient.FlowAction a : actions) {
                if (isNoise(a, serviceUser)) {
                    continue;
                }
                String action = "NIFI_CANVAS_" + normalizeOp(a.operation());
                String detail = (a.sourceType() == null ? "" : a.sourceType() + " · ")
                        + a.operation() + " · " + a.timestamp();
                auditService.record(a.userIdentity(), action, "NIFI", a.sourceName(), detail);
            }
            cursor = max;
        } catch (RuntimeException ex) {
            log.debug("NiFi 캔버스 감사 수집 실패(무시): {}", ex.getMessage());
        }
    }

    /** 서비스 계정의 자동 쓰기 + 상태 오버레이(Label)는 사용자 편집이 아니므로 제외. */
    private boolean isNoise(NifiClient.FlowAction a, String serviceUser) {
        return a.userIdentity() == null
                || a.userIdentity().equalsIgnoreCase(serviceUser)
                || "Label".equalsIgnoreCase(a.sourceType());
    }

    private String normalizeOp(String operation) {
        if (operation == null || operation.isBlank()) {
            return "EDIT";
        }
        return operation.trim().toUpperCase().replace(' ', '_');
    }
}
