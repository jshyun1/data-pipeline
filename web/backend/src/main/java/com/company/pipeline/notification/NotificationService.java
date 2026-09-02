package com.company.pipeline.notification;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    private final JdbcTemplate jdbc;
    // JavaMailSender 는 spring.mail.host 가 있을 때만 존재한다(ObjectProvider 로 부재 허용).
    private final ObjectProvider<JavaMailSender> mailSenderProvider;
    private final boolean emailEnabled;
    private final String emailFrom;
    // SMS 는 EMAIL 의 SMTP 처럼 외부 게이트웨이가 필요하다. gateway.url 이 있을 때만 실제 발송.
    private final boolean smsEnabled;
    private final String smsGatewayUrl;
    // 게이트웨이 규격 흡수용(폐쇄망 대비). 사내 문자 서버·사업자 API 는 인증 헤더를 요구하고
    // 필드명도 제각각인데(to/text, receiver/msg, phone/message …), 그때마다 코드를 고치면
    // 폐쇄망에서는 이미지 재반입이 필요하다. 아래 4개를 설정으로 열어 두면 .env 만으로 붙는다.
    private final String smsHeaders;
    private final String smsContentType;
    private final String smsBodyTemplate;
    private final String smsSender;
    private final java.net.http.HttpClient httpClient = java.net.http.HttpClient.newBuilder()
            .connectTimeout(java.time.Duration.ofSeconds(5)).build();

    public NotificationService(DataSource dataSource,
                               ObjectProvider<JavaMailSender> mailSenderProvider,
                               @Value("${notification.email.enabled:false}") boolean emailEnabled,
                               @Value("${notification.email.from:alerts@pipeline.local}") String emailFrom,
                               @Value("${notification.sms.enabled:false}") boolean smsEnabled,
                               @Value("${notification.sms.gateway.url:}") String smsGatewayUrl,
                               @Value("${notification.sms.gateway.headers:}") String smsHeaders,
                               @Value("${notification.sms.gateway.content-type:application/json}") String smsContentType,
                               @Value("${notification.sms.gateway.body-template:}") String smsBodyTemplate,
                               @Value("${notification.sms.sender:}") String smsSender) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.mailSenderProvider = mailSenderProvider;
        this.emailEnabled = emailEnabled;
        this.emailFrom = emailFrom;
        this.smsEnabled = smsEnabled;
        this.smsGatewayUrl = smsGatewayUrl;
        this.smsHeaders = smsHeaders;
        this.smsContentType = smsContentType == null || smsContentType.isBlank()
                ? "application/json" : smsContentType.trim();
        this.smsBodyTemplate = smsBodyTemplate;
        this.smsSender = smsSender;
    }

    /**
     * 이 채널이 «실제로 나갈 수 있는» 상태인지. 화면(발송 채널)이 «켰는데 왜 안 오지»를
     * 판별할 수 있도록 노출한다 - 토글은 DB, 릴레이 설정은 환경변수라 둘이 어긋날 수 있다.
     */
    public boolean relayConfigured(String channelType) {
        return switch (channelType) {
            case "IN_APP" -> true;   // 외부 의존이 없다.
            case "EMAIL" -> emailEnabled && mailSenderProvider.getIfAvailable() != null;
            case "SMS" -> smsEnabled && smsGatewayUrl != null && !smsGatewayUrl.isBlank();
            default -> false;
        };
    }

    /** FIRING 시 호출. 구독한 수신자별 IN_APP/EMAIL 아웃박스 행을 만든다(심각도 필터). */
    public void enqueueForInstance(long instanceId) {
        Map<String, Object> inst;
        try {
            inst = jdbc.queryForMap("SELECT id, severity, rule_type_code, target_label, summary, deep_link, "
                    + "notify_count, rule_id FROM alert_instance WHERE id = ?", instanceId);
        } catch (Exception ex) {
            return;
        }
        String severity = (String) inst.get("severity");
        int rank = rank(severity);
        String category = "SYSTEM";
        String eventKey = "ALERT-" + instanceId + "-" + inst.get("notify_count");
        String summary = (String) inst.get("summary");
        String deepLink = (String) inst.get("deep_link");
        // 이 규칙을 «누가» 받는지(알림 규칙 화면 → 수신자).
        Routing routing = routingFor(inst.get("rule_id"));

        // IN_APP: 구독한 수신자별 1행. 수신자가 없어도(초기) 브로드캐스트 1행을 남겨 종 배지가 뜬다.
        List<Map<String, Object>> recips = jdbc.queryForList("""
                SELECT r.id AS rid, r.display_name FROM notification_recipient r
                JOIN notification_subscription s ON s.recipient_id = r.id AND s.channel_type = 'IN_APP' AND s.enabled
                WHERE r.deleted_at IS NULL AND r.enabled
                  AND ? <= CASE s.min_severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END
                """, rank);
        // 브로드캐스트 폴백은 «수신자를 아무도 등록하지 않은» 초기 상태를 위한 것이다.
        // 규칙에서 수신자를 걸러 낸 경우까지 폴백하면, 담당 아닌 사람에게 안 보내려던 설정이
        // 오히려 전체 공지가 되어 버린다. 그래서 필터 «전» 목록으로 폴백 여부를 정한다.
        List<Map<String, Object>> inAppRecips = routing.filter(recips);
        if (recips.isEmpty()) {
            insertDelivery(eventKey, "IN_APP", severity, rank, category, null, null, summary, deepLink, 0);
        } else {
            for (Map<String, Object> r : inAppRecips) {
                insertDelivery(eventKey, "IN_APP", severity, rank, category,
                        ((Number) r.get("rid")).longValue(), null, summary, deepLink, 0);
            }
        }
        // 심각도 발송분기: 외부 발송(EMAIL/SMS)은 «위험(CRITICAL)»만, 즉시 내보낸다.
        // 경고·정보는 화면 알림(조치 대기열·헤더 배지)으로만 남긴다 - 위 IN_APP 블록에서 이미
        // 만들었으므로 여기서 끝낸다.
        //
        // 예전에는 경고도 120초 모아서 메일·문자로 내보냈는데, 경고는 «봐야 하지만 당장
        // 뛰어갈 일은 아닌» 등급이라 문자로 오면 곧 무시하게 된다. 그러면 정작 위험이 왔을 때도
        // 같이 묻힌다. 외부 발송은 «지금 사람이 움직여야 하는» 위험에만 쓴다(2026-09-02 결정).
        if (rank >= 1) {
            return;
        }
        int emailDelaySec = 0;
        // EMAIL: 이메일 구독자. 실제 발송 주소는 원본 이메일을 저장한다(마스킹은 표시 계층에서만).
        List<Map<String, Object>> emailRecips = jdbc.queryForList("""
                SELECT r.id AS rid, r.email FROM notification_recipient r
                JOIN notification_subscription s ON s.recipient_id = r.id AND s.channel_type = 'EMAIL' AND s.enabled
                WHERE r.deleted_at IS NULL AND r.enabled AND r.email IS NOT NULL AND r.email_disabled_at IS NULL
                  AND ? <= CASE s.min_severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END
                """, rank);
        for (Map<String, Object> r : routing.filter(emailRecips)) {
            insertDelivery(eventKey, "EMAIL", severity, rank, category,
                    ((Number) r.get("rid")).longValue(), (String) r.get("email"), summary, deepLink, emailDelaySec);
        }
        // SMS: 문자 구독자(전화번호 보유). 게이트웨이가 설정돼 있으면 dispatch 가 실제 발송한다.
        List<Map<String, Object>> smsRecips = jdbc.queryForList("""
                SELECT r.id AS rid, r.phone FROM notification_recipient r
                JOIN notification_subscription s ON s.recipient_id = r.id AND s.channel_type = 'SMS' AND s.enabled
                WHERE r.deleted_at IS NULL AND r.enabled AND r.phone IS NOT NULL AND r.phone <> ''
                  AND ? <= CASE s.min_severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END
                """, rank);
        for (Map<String, Object> r : routing.filter(smsRecips)) {
            insertDelivery(eventKey, "SMS", severity, rank, category,
                    ((Number) r.get("rid")).longValue(), (String) r.get("phone"), summary, deepLink, emailDelaySec);
        }
    }


    /**
     * 이 알림을 누구에게 보낼지 미리 계산해 둔 결과.
     *
     * <p>규칙마다 «받는 수신자» 목록을 둔다(알림 규칙 화면 → 수신자). 목록을 한 번도 손대지
     * 않은 규칙은 종전대로 전원에게 간다 — 그렇게 안 하면 이 기능이 들어간 순간 기존 수신자
     * 전원이 조용히 알림을 못 받게 된다.
     *
     * <p>끈 수신자는 행이 지워지는 게 아니라 enabled=false 로 남는다. 전원을 껐을 때
     * "목록 없음(=전원 수신)"과 헷갈리지 않기 위해서다.
     */
    private record Routing(boolean unrestricted, Set<Long> allowed) {

        private List<Map<String, Object>> filter(List<Map<String, Object>> recips) {
            if (unrestricted) {
                return recips;
            }
            return recips.stream()
                    .filter(r -> allowed.contains(((Number) r.get("rid")).longValue()))
                    .toList();
        }
    }

    private Routing routingFor(Object ruleId) {
        long rid = ruleId instanceof Number n ? n.longValue() : -1L;
        List<Map<String, Object>> rows;
        try {
            rows = jdbc.queryForList(
                    "SELECT recipient_id, enabled FROM notification_recipient_rule WHERE rule_id = ?", rid);
        } catch (Exception ex) {
            return new Routing(true, Set.of());   // 표가 아직 없으면 종전 동작(전원 수신).
        }
        if (rows.isEmpty()) {
            return new Routing(true, Set.of());   // 규칙에 수신자 목록을 지정한 적이 없다.
        }
        Set<Long> allowed = new HashSet<>();
        for (Map<String, Object> row : rows) {
            if (Boolean.TRUE.equals(row.get("enabled"))) {
                allowed.add(((Number) row.get("recipient_id")).longValue());
            }
        }
        return new Routing(false, allowed);
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
        // 화면(발송 채널)의 on/off 토글. 예전에는 이 값을 아무도 안 봐서 «화면은 켜짐인데 안 감»,
        // 반대로 «껐는데 계속 감»이 둘 다 가능했다. 운영 중 문자 폭주를 즉시 끊을 수단이기도 하다.
        Set<String> enabledChannels = enabledChannels();
        for (Map<String, Object> d : batch) {
            long id = ((Number) d.get("id")).longValue();
            String channel = (String) d.get("channel_type");
            try {
                if (!"IN_APP".equals(channel) && enabledChannels != null && !enabledChannels.contains(channel)) {
                    // 운영자가 일부러 끈 채널이다. 재시도해 봐야 같은 결과라 바로 종료 처리한다
                    // (화면알림은 계속 쌓이므로 «알림 자체»가 사라지지는 않는다).
                    markDead(id, "CHANNEL_OFF");
                } else if ("IN_APP".equals(channel)) {
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

    /**
     * 켜져 있는 채널 목록. 표를 못 읽으면 null 을 돌려 «종전 동작(환경변수만 본다)»으로 둔다 -
     * DB 가 잠깐 흔들렸다고 알림이 통째로 멈추는 쪽이 더 위험하다.
     */
    private Set<String> enabledChannels() {
        try {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT channel_type FROM notification_channel_config WHERE enabled");
            Set<String> on = new HashSet<>();
            for (Map<String, Object> r : rows) {
                on.add((String) r.get("channel_type"));
            }
            return on;
        } catch (Exception ex) {
            return null;
        }
    }

    /** 재시도해도 결과가 같은 사유(채널 꺼짐 등)는 바로 종료 처리한다. */
    private void markDead(long id, String reason) {
        jdbc.update("UPDATE notification_delivery SET status='DEAD', failure_reason=?, "
                + "attempt_count=attempt_count+1, updated_at=now() WHERE id=?", reason, id);
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
     * SMS 실 릴레이. 설정된 게이트웨이(notification.sms.gateway.url)로 POST 한다. 본문 형식·
     * 인증 헤더·Content-Type 은 설정으로 바꿀 수 있다({@link #renderSmsBody}, {@link #applyGatewayHeaders}).
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
            java.net.http.HttpRequest.Builder builder =
                    java.net.http.HttpRequest.newBuilder(java.net.URI.create(smsGatewayUrl))
                            .timeout(java.time.Duration.ofSeconds(5))
                            .header("Content-Type", smsContentType);
            applyGatewayHeaders(builder);
            java.net.http.HttpRequest req = builder
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(
                            renderSmsBody(to, text), StandardCharsets.UTF_8))
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

    /**
     * 게이트웨이로 보낼 본문. 기본은 예전과 같은 {@code {"to","text"}} JSON 이고,
     * {@code notification.sms.gateway.body-template} 로 필드명·구조를 바꿀 수 있다.
     * 치환자는 {@code {to} {text} {sender}} 세 개.
     *
     * <pre>
     * {"receiver":"{to}","msg":"{text}","sender":"{sender}"}   ← JSON 게이트웨이
     * to={to}&amp;text={text}&amp;from={sender}                        ← form 게이트웨이
     * </pre>
     *
     * <p>{@code ${to}} 형태도 받지만 <b>권장하지 않는다</b> - docker compose 가 자기 변수로 먼저
     * 치환해 버려서 빈 문자열이 들어온다(실측). 그래서 {@code $} 없는 형태를 기본으로 안내한다.
     *
     * <p>이스케이프는 Content-Type 을 따른다 - form 이면 URL 인코딩, 아니면 JSON 이스케이프.
     * 알림 본문에는 따옴표·개행·한글이 그대로 들어오므로 이걸 빠뜨리면 게이트웨이가 400 을 준다.
     */
    private String renderSmsBody(String to, String text) {
        boolean form = smsContentType.toLowerCase().startsWith("application/x-www-form-urlencoded");
        String template = smsBodyTemplate == null || smsBodyTemplate.isBlank()
                ? (form ? "to={to}&text={text}" : "{\"to\":\"{to}\",\"text\":\"{text}\"}")
                : smsBodyTemplate;
        String encodedTo = escapeFor(to, form);
        String encodedText = escapeFor(text, form);
        String encodedSender = escapeFor(smsSender == null ? "" : smsSender, form);
        return template
                .replace("${to}", encodedTo).replace("{to}", encodedTo)
                .replace("${text}", encodedText).replace("{text}", encodedText)
                .replace("${sender}", encodedSender).replace("{sender}", encodedSender);
    }

    private String escapeFor(String value, boolean form) {
        return form ? java.net.URLEncoder.encode(value, StandardCharsets.UTF_8) : jsonEsc(value);
    }

    /**
     * 게이트웨이 인증 헤더. {@code "Authorization: Bearer x; X-API-KEY: y"} 처럼 세미콜론으로 나눈다.
     * 값에 콜론이 들어갈 수 있으므로(Bearer 토큰) 첫 콜론만 구분자로 쓴다.
     */
    private void applyGatewayHeaders(java.net.http.HttpRequest.Builder builder) {
        if (smsHeaders == null || smsHeaders.isBlank()) {
            return;
        }
        for (String entry : smsHeaders.split(";")) {
            String pair = entry.trim();
            int colon = pair.indexOf(':');
            if (pair.isEmpty() || colon <= 0) {
                continue;
            }
            String name = pair.substring(0, colon).trim();
            String value = pair.substring(colon + 1).trim();
            // Content-Type 은 따로 넣는다(여기서 또 넣으면 헤더가 두 번 붙는다).
            if (name.equalsIgnoreCase("Content-Type")) {
                continue;
            }
            try {
                builder.header(name, value);
            } catch (IllegalArgumentException ex) {
                // Host·Connection 등 JDK 가 막는 헤더. 설정 실수를 조용히 넘기지 않고 남긴다.
                log.warn("SMS 게이트웨이 헤더 무시({}): {}", name, ex.getMessage());
            }
        }
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
