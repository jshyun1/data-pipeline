package com.company.pipeline.notification;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * 알림 발송 아웃박스 + 디스패처 (U11/U12, 설계서 4-7절/6-2절).
 *
 * <p>판정(alert_*)과 발송(notification_*)을 분리한다. AlertEngine 은 FIRING 시 enqueueForInstance 로
 * notification_delivery 행만 INSERT(수 ms)하고, 실제 발송은 이 디스패처(controlPlaneScheduler)가
 * 가져간다. 인메모리 큐를 안 쓰므로 재기동해도 "발생은 했는데 못 받은" 알림이 생기지 않는다.
 *
 * <p>이번 슬라이스: IN_APP(화면 알림)은 즉시 SENT, EMAIL 은 릴레이 미설정 환경에서 실패로 RETRY→DEAD.
 * 심각도별 레이트리밋·서킷브레이커·배치요약·데드맨은 후속.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final JdbcTemplate jdbc;

    public NotificationService(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    /** FIRING 시 호출. 구독한 수신자별 IN_APP/EMAIL 아웃박스 행을 만든다(심각도 필터). */
    public void enqueueForInstance(long instanceId) {
        Map<String, Object> inst;
        try {
            inst = jdbc.queryForMap("SELECT id, severity, rule_type_code, target_label, summary, deep_link, "
                    + "notify_count FROM alert_instance WHERE id = ?", instanceId);
        } catch (Exception ex) {
            return;
        }
        String severity = (String) inst.get("severity");
        int rank = rank(severity);
        String category = "SYSTEM";
        String eventKey = "ALERT-" + instanceId + "-" + inst.get("notify_count");
        String summary = (String) inst.get("summary");
        String deepLink = (String) inst.get("deep_link");

        // IN_APP: 구독한 수신자별 1행. 수신자가 없어도(초기) 브로드캐스트 1행을 남겨 종 배지가 뜬다.
        List<Map<String, Object>> recips = jdbc.queryForList("""
                SELECT r.id AS rid, r.display_name FROM notification_recipient r
                JOIN notification_subscription s ON s.recipient_id = r.id AND s.channel_type = 'IN_APP' AND s.enabled
                WHERE r.deleted_at IS NULL AND r.enabled
                  AND ? >= CASE s.min_severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END
                """, rank);
        if (recips.isEmpty()) {
            insertDelivery(eventKey, "IN_APP", severity, rank, category, null, null, summary, deepLink);
        } else {
            for (Map<String, Object> r : recips) {
                insertDelivery(eventKey, "IN_APP", severity, rank, category,
                        ((Number) r.get("rid")).longValue(), null, summary, deepLink);
            }
        }
        // EMAIL: 이메일 구독자.
        List<Map<String, Object>> emailRecips = jdbc.queryForList("""
                SELECT r.id AS rid, r.email FROM notification_recipient r
                JOIN notification_subscription s ON s.recipient_id = r.id AND s.channel_type = 'EMAIL' AND s.enabled
                WHERE r.deleted_at IS NULL AND r.enabled AND r.email IS NOT NULL AND r.email_disabled_at IS NULL
                  AND ? >= CASE s.min_severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END
                """, rank);
        for (Map<String, Object> r : emailRecips) {
            insertDelivery(eventKey, "EMAIL", severity, rank, category,
                    ((Number) r.get("rid")).longValue(), maskEmail((String) r.get("email")), summary, deepLink);
        }
    }

    private void insertDelivery(String eventKey, String channel, String severity, int rank, String category,
                                Long recipientId, String address, String summary, String deepLink) {
        String dedup = sha256(eventKey + "|" + channel + "|" + recipientId + "|" + (address == null ? "" : address));
        try {
            jdbc.update("""
                    INSERT INTO notification_delivery
                        (event_key, dedup_key, channel_type, severity, severity_rank, category, recipient_id,
                         target_address, subject, body, deep_link, status, next_attempt_at, expires_at, occurred_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', now(), now() + interval '6 hours', now())
                    ON CONFLICT (dedup_key) DO NOTHING
                    """, eventKey, dedup, channel, severity, rank, category, recipientId, address,
                    "[관제] " + severity + " 알림", summary, deepLink);
        } catch (Exception ex) {
            log.warn("아웃박스 적재 실패({}, {}): {}", channel, eventKey, ex.getMessage());
        }
    }

    @Scheduled(fixedRate = 5_000, initialDelay = 50_000, scheduler = "controlPlaneScheduler")
    public void dispatch() {
        List<Map<String, Object>> batch;
        try {
            batch = jdbc.queryForList("""
                    SELECT id, channel_type FROM notification_delivery
                    WHERE status IN ('PENDING','RETRY') AND next_attempt_at <= now()
                    ORDER BY severity_rank, next_attempt_at LIMIT 20
                    """);
        } catch (Exception ex) {
            return;
        }
        for (Map<String, Object> d : batch) {
            long id = ((Number) d.get("id")).longValue();
            String channel = (String) d.get("channel_type");
            try {
                if ("IN_APP".equals(channel)) {
                    // 화면 알림은 외부 의존이 없다 - 즉시 전송 완료.
                    jdbc.update("UPDATE notification_delivery SET status='SENT', sent_at=now(), "
                            + "attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, now()), "
                            + "updated_at=now() WHERE id=?", id);
                } else {
                    // EMAIL/SMS: 릴레이 미설정 환경에서는 실패 처리(재시도 백오프는 후속 U14).
                    jdbc.update("UPDATE notification_delivery SET status='RETRY', "
                            + "attempt_count=attempt_count+1, failure_reason='NO_RELAY', "
                            + "next_attempt_at=now() + interval '5 minutes', updated_at=now() "
                            + "WHERE id=? AND attempt_count+1 < max_attempts", id);
                    jdbc.update("UPDATE notification_delivery SET status='DEAD', updated_at=now() "
                            + "WHERE id=? AND attempt_count >= max_attempts", id);
                }
            } catch (Exception ex) {
                log.warn("발송 처리 실패(id={}): {}", id, ex.getMessage());
            }
        }
    }

    private int rank(String severity) {
        return switch (severity) {
            case "CRITICAL" -> 0;
            case "WARNING" -> 1;
            default -> 2;
        };
    }

    private String maskEmail(String email) {
        int at = email.indexOf('@');
        if (at <= 1) {
            return "***" + email.substring(Math.max(0, at));
        }
        return email.charAt(0) + "***" + email.substring(at);
    }

    private String sha256(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] h = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(64);
            for (byte b : h) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception ex) {
            return Integer.toHexString(s.hashCode());
        }
    }
}
