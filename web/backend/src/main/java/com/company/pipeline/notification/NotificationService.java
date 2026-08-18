package com.company.pipeline.notification;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
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

    // 심각도 발송분기(원본 5-7): 경고는 즉시 폭주 대신 배치 창을 두고 지연 발송한다.
    private static final int WARNING_BATCH_SECONDS = 120;

    private final JdbcTemplate jdbc;
    // JavaMailSender 는 spring.mail.host 가 있을 때만 존재한다(ObjectProvider 로 부재 허용).
    private final ObjectProvider<JavaMailSender> mailSenderProvider;
    private final boolean emailEnabled;
    private final String emailFrom;
    // SMS 는 EMAIL 의 SMTP 처럼 외부 게이트웨이가 필요하다. gateway.url 이 있을 때만 실제 발송.
    private final boolean smsEnabled;
    private final String smsGatewayUrl;
    private final java.net.http.HttpClient httpClient = java.net.http.HttpClient.newBuilder()
            .connectTimeout(java.time.Duration.ofSeconds(5)).build();

    public NotificationService(DataSource dataSource,
                               ObjectProvider<JavaMailSender> mailSenderProvider,
                               @Value("${notification.email.enabled:false}") boolean emailEnabled,
                               @Value("${notification.email.from:alerts@pipeline.local}") String emailFrom,
                               @Value("${notification.sms.enabled:false}") boolean smsEnabled,
                               @Value("${notification.sms.gateway.url:}") String smsGatewayUrl) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.mailSenderProvider = mailSenderProvider;
        this.emailEnabled = emailEnabled;
        this.emailFrom = emailFrom;
        this.smsEnabled = smsEnabled;
        this.smsGatewayUrl = smsGatewayUrl;
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
                  AND ? <= CASE s.min_severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END
                """, rank);
        if (recips.isEmpty()) {
            insertDelivery(eventKey, "IN_APP", severity, rank, category, null, null, summary, deepLink, 0);
        } else {
            for (Map<String, Object> r : recips) {
                insertDelivery(eventKey, "IN_APP", severity, rank, category,
                        ((Number) r.get("rid")).longValue(), null, summary, deepLink, 0);
            }
        }
        // 심각도 발송분기(원본 5-7): 정보(INFO)는 «화면 내에만» — 외부 채널(EMAIL/SMS)로 내보내지 않는다.
        if (rank >= 2) {
            return;
        }
        // 위험(CRITICAL)=즉시 / 경고(WARNING)=배치 창 지연.
        int emailDelaySec = rank == 0 ? 0 : WARNING_BATCH_SECONDS;
        // EMAIL: 이메일 구독자. 실제 발송 주소는 원본 이메일을 저장한다(마스킹은 표시 계층에서만).
        List<Map<String, Object>> emailRecips = jdbc.queryForList("""
                SELECT r.id AS rid, r.email FROM notification_recipient r
                JOIN notification_subscription s ON s.recipient_id = r.id AND s.channel_type = 'EMAIL' AND s.enabled
                WHERE r.deleted_at IS NULL AND r.enabled AND r.email IS NOT NULL AND r.email_disabled_at IS NULL
                  AND ? <= CASE s.min_severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END
                """, rank);
        for (Map<String, Object> r : emailRecips) {
            insertDelivery(eventKey, "EMAIL", severity, rank, category,
                    ((Number) r.get("rid")).longValue(), (String) r.get("email"), summary, deepLink, emailDelaySec);
        }
    }

    private void insertDelivery(String eventKey, String channel, String severity, int rank, String category,
                                Long recipientId, String address, String summary, String deepLink, int delaySeconds) {
        String dedup = sha256(eventKey + "|" + channel + "|" + recipientId + "|" + (address == null ? "" : address));
        try {
            jdbc.update("""
                    INSERT INTO notification_delivery
                        (event_key, dedup_key, channel_type, severity, severity_rank, category, recipient_id,
                         target_address, subject, body, deep_link, status, next_attempt_at, expires_at, occurred_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING',
                            now() + (? * interval '1 second'), now() + interval '6 hours', now())
                    ON CONFLICT (dedup_key) DO NOTHING
                    """, eventKey, dedup, channel, severity, rank, category, recipientId, address,
                    "[관제] " + severity + " 알림", summary, deepLink, delaySeconds);
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
                } else if ("EMAIL".equals(channel) && emailEnabled) {
                    relayEmail(id);
                } else if ("SMS".equals(channel) && smsEnabled) {
                    relaySms(id);
                } else {
                    // 미지원 채널 또는 릴레이 비활성(EMAIL SMTP·SMS 게이트웨이 미설정): 재시도 후 DEAD.
                    markNoRelay(id);
                }
            } catch (Exception ex) {
                log.warn("발송 처리 실패(id={}): {}", id, ex.getMessage());
            }
        }
    }

    /** EMAIL 실 릴레이. JavaMailSender 로 발송하고 결과를 아웃박스에 반영한다. */
    private void relayEmail(long id) {
        JavaMailSender sender = mailSenderProvider.getIfAvailable();
        if (sender == null) {
            // enabled=true 인데 spring.mail.host 미설정 → 빈 없음. NO_RELAY 로 폴백.
            markNoRelay(id);
            return;
        }
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT target_address, subject, body FROM notification_delivery WHERE id=?", id);
        String to = (String) row.get("target_address");
        if (to == null || to.isBlank()) {
            jdbc.update("UPDATE notification_delivery SET status='DEAD', failure_reason='NO_ADDRESS', "
                    + "attempt_count=attempt_count+1, updated_at=now() WHERE id=?", id);
            return;
        }
        try {
            SimpleMailMessage msg = new SimpleMailMessage();
            msg.setFrom(emailFrom);
            msg.setTo(to);
            Object subject = row.get("subject");
            msg.setSubject(subject == null ? "[관제] 알림" : subject.toString());
            Object body = row.get("body");
            msg.setText(body == null ? "" : body.toString());
            sender.send(msg);
            jdbc.update("UPDATE notification_delivery SET status='SENT', sent_at=now(), "
                    + "attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, now()), "
                    + "updated_at=now() WHERE id=?", id);
            log.info("EMAIL 발송 완료 id={} to={}", id, maskEmail(to));
        } catch (Exception ex) {
            String detail = ex.getMessage();
            if (detail != null && detail.length() > 480) {
                detail = detail.substring(0, 480);
            }
            jdbc.update("UPDATE notification_delivery SET status='RETRY', attempt_count=attempt_count+1, "
                    + "failure_reason='SMTP_ERROR', failure_detail=?, "
                    + "next_attempt_at=now() + interval '2 minutes', updated_at=now() "
                    + "WHERE id=? AND attempt_count+1 < max_attempts", detail, id);
            jdbc.update("UPDATE notification_delivery SET status='DEAD', updated_at=now() "
                    + "WHERE id=? AND attempt_count >= max_attempts", id);
            log.warn("EMAIL 발송 실패 id={}: {}", id, ex.getMessage());
        }
    }

    /**
     * SMS 실 릴레이. 설정된 게이트웨이(notification.sms.gateway.url)로 {to,text} JSON 을 POST 한다.
     * EMAIL 의 SMTP 처럼 외부 게이트웨이가 필요하며, 미설정이면 NO_RELAY 로 폴백한다(실제 문자 안 감).
     */
    private void relaySms(long id) {
        if (smsGatewayUrl == null || smsGatewayUrl.isBlank()) {
            markNoRelay(id);
            return;
        }
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT target_address, body FROM notification_delivery WHERE id=?", id);
        String to = (String) row.get("target_address");
        if (to == null || to.isBlank()) {
            jdbc.update("UPDATE notification_delivery SET status='DEAD', failure_reason='NO_ADDRESS', "
                    + "attempt_count=attempt_count+1, updated_at=now() WHERE id=?", id);
            return;
        }
        String text = row.get("body") == null ? "" : row.get("body").toString();
        try {
            String json = "{\"to\":\"" + jsonEsc(to) + "\",\"text\":\"" + jsonEsc(text) + "\"}";
            java.net.http.HttpRequest req = java.net.http.HttpRequest.newBuilder(java.net.URI.create(smsGatewayUrl))
                    .timeout(java.time.Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                    .build();
            java.net.http.HttpResponse<String> resp = httpClient.send(
                    req, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 == 2) {
                jdbc.update("UPDATE notification_delivery SET status='SENT', sent_at=now(), "
                        + "attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, now()), "
                        + "updated_at=now() WHERE id=?", id);
                log.info("SMS 발송 완료 id={} to={}", id, maskPhone(to));
            } else {
                smsFail(id, "HTTP " + resp.statusCode());
            }
        } catch (Exception ex) {
            smsFail(id, ex.getMessage());
        }
    }

    private void smsFail(long id, String reason) {
        String detail = reason == null ? null : (reason.length() > 480 ? reason.substring(0, 480) : reason);
        jdbc.update("UPDATE notification_delivery SET status='RETRY', attempt_count=attempt_count+1, "
                + "failure_reason='SMS_ERROR', failure_detail=?, "
                + "next_attempt_at=now() + interval '2 minutes', updated_at=now() "
                + "WHERE id=? AND attempt_count+1 < max_attempts", detail, id);
        jdbc.update("UPDATE notification_delivery SET status='DEAD', updated_at=now() "
                + "WHERE id=? AND attempt_count >= max_attempts", id);
        log.warn("SMS 발송 실패 id={}: {}", id, reason);
    }

    private String jsonEsc(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "");
    }

    private String maskPhone(String phone) {
        if (phone.length() < 4) {
            return "***";
        }
        return "***" + phone.substring(phone.length() - 4);
    }

    /** 릴레이 불가(미지원 채널/미설정): 재시도 백오프 후 최대치 도달 시 DEAD. */
    private void markNoRelay(long id) {
        jdbc.update("UPDATE notification_delivery SET status='RETRY', "
                + "attempt_count=attempt_count+1, failure_reason='NO_RELAY', "
                + "next_attempt_at=now() + interval '5 minutes', updated_at=now() "
                + "WHERE id=? AND attempt_count+1 < max_attempts", id);
        jdbc.update("UPDATE notification_delivery SET status='DEAD', updated_at=now() "
                + "WHERE id=? AND attempt_count >= max_attempts", id);
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
