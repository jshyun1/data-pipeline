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
    // 화면에서 메일 서버를 넣으면 그 값으로 발송기를 직접 만든다(아래 mailSenderFor).
    private final ObjectProvider<JavaMailSender> mailSenderProvider;
    /**
     * 채널 설정(메일 · 문자 · 카카오). 화면에서 넣은 값(DB)이 먼저, 없으면 환경변수다 -
     * 들어갈 망이 정해지지 않아 «현장에서 화면으로 설정»이 가능해야 한다(2026-09-20).
     */
    private final ChannelSettingsService channelSettings;
    private final boolean emailEnabled;
    private final boolean smsEnabled;
    private final boolean kakaoEnabled;
    private final java.net.http.HttpClient httpClient = java.net.http.HttpClient.newBuilder()
            .connectTimeout(java.time.Duration.ofSeconds(5)).build();
    /** 화면 설정으로 만든 메일 발송기 캐시. 설정이 바뀌면 키가 달라져 새로 만든다. */
    private final Map<String, JavaMailSender> mailSenders = new java.util.concurrent.ConcurrentHashMap<>();
    /**
     * 이 인스턴스 이름. 아웃박스 한 건을 «누가 집었는지»({@code claimed_by})로 남긴다.
     * 컨테이너면 HOSTNAME(컨테이너 id)이라 두 대를 띄워도 서로 다르다.
     */
    private final String instanceId = instanceId();

    private static String instanceId() {
        String host = System.getenv("HOSTNAME");
        String id = host == null || host.isBlank()
                ? "local-" + java.util.UUID.randomUUID().toString().substring(0, 8)
                : host;
        return id.length() > 40 ? id.substring(0, 40) : id;
    }

    public NotificationService(DataSource dataSource,
                               ObjectProvider<JavaMailSender> mailSenderProvider,
                               ChannelSettingsService channelSettings,
                               @Value("${notification.email.enabled:false}") boolean emailEnabled,
                               @Value("${notification.sms.enabled:false}") boolean smsEnabled,
                               @Value("${notification.kakao.enabled:false}") boolean kakaoEnabled) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.mailSenderProvider = mailSenderProvider;
        this.channelSettings = channelSettings;
        this.emailEnabled = emailEnabled;
        this.smsEnabled = smsEnabled;
        this.kakaoEnabled = kakaoEnabled;
    }

    /**
     * 채널을 실제로 쓸 수 있는가. 예전에는 «환경변수 enabled» 만 봤는데, 이제는 화면에서 채널을 켜고
     * 설정을 넣는 경로가 있어서 <b>DB 의 on/off 도 함께</b> 본다 - 화면에서 켠 채널이 .env 때문에
     * 조용히 안 나가면 «켰는데 왜 안 오지»가 된다.
     */
    private boolean channelOn(String channelType, boolean envEnabled) {
        Set<String> on = enabledChannels();
        return envEnabled || (on != null && on.contains(channelType));
    }

    /**
     * 이 채널이 «실제로 나갈 수 있는» 상태인지. 화면(발송 채널)이 «켰는데 왜 안 오지»를
     * 판별할 수 있도록 노출한다 - 토글은 DB, 릴레이 설정은 환경변수라 둘이 어긋날 수 있다.
     */
    public boolean relayConfigured(String channelType) {
        return switch (channelType) {
            case "IN_APP" -> true;   // 외부 의존이 없다.
            case "EMAIL" -> channelSettings.effective("EMAIL").hasText("host")
                    || mailSenderProvider.getIfAvailable() != null;
            case "SMS" -> channelSettings.effective("SMS").hasText("url");
            case "KAKAO" -> channelSettings.effective("KAKAO").hasText("url");
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
        // KAKAO(알림톡): 문자와 같은 휴대폰 번호로 보낸다 - 카카오 전용 식별자를 따로 받지 않는다.
        // 등급 정책도 메일 · 문자와 같다(위험만). 여기까지 온 것은 이미 rank==0 이라는 뜻이다.
        List<Map<String, Object>> kakaoRecips = jdbc.queryForList("""
                SELECT r.id AS rid, r.phone FROM notification_recipient r
                JOIN notification_subscription s ON s.recipient_id = r.id AND s.channel_type = 'KAKAO' AND s.enabled
                WHERE r.deleted_at IS NULL AND r.enabled AND r.phone IS NOT NULL AND r.phone <> ''
                  AND ? <= CASE s.min_severity WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END
                """, rank);
        for (Map<String, Object> r : routing.filter(kakaoRecips)) {
            insertDelivery(eventKey, "KAKAO", severity, rank, category,
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

    /**
     * 아웃박스를 한 바퀴 돈다(5초).
     *
     * <p><b>여러 대가 같이 떠도 한 건은 한 번만 나간다.</b> 후보를 읽은 뒤 곧바로 보내지 않고,
     * {@code status='SENDING'} 으로 <b>집고(claim)</b> 성공한 인스턴스만 발송한다 - 집기는
     * {@code WHERE status IN ('PENDING','RETRY')} 를 달아 한 행에 한 번만 성사된다(먼저 집은 쪽이 이긴다).
     * 알림톡 · 문자는 건당 과금이라 이중 발송이 곧 비용이고, MSA 는 앱을 두 대로 띄운다.
     *
     * <p>집어 놓고 죽으면({@code SENDING} 인 채로 남으면) {@code claim_expires_at} 이 지난 뒤
     * 다시 재시도로 돌린다 - 그 경우 «보냈는데 SENT 를 못 찍은» 건이 한 번 더 갈 수 있다(at-least-once).
     * 영영 안 나가는 것보다 낫다고 본다.
     *
     * <p>{@code claimed_by} · {@code claim_expires_at} 컬럼과 {@code idx_delivery_sending} 인덱스는
     * V36 이 만들어 두고 아무도 쓰지 않던 자리다.
     */
    @Scheduled(fixedRate = 5_000, initialDelay = 50_000, scheduler = "controlPlaneScheduler")
    public void dispatch() {
        releaseExpiredClaims();
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
            if (!claim(id)) {
                continue;   // 다른 인스턴스가 먼저 집었다(또는 그새 상태가 바뀌었다)
            }
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
                } else if ("EMAIL".equals(channel) && channelOn("EMAIL", emailEnabled)) {
                    relayEmail(id);
                } else if ("SMS".equals(channel) && channelOn("SMS", smsEnabled)) {
                    relaySms(id);
                } else if ("KAKAO".equals(channel) && channelOn("KAKAO", kakaoEnabled)) {
                    relayKakao(id);
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
     * 한 건을 이 인스턴스 것으로 집는다. 성사되면 true.
     *
     * <p>조건절의 {@code status IN ('PENDING','RETRY')} 가 자물쇠다 - 두 인스턴스가 같은 행을 동시에
     * 집으려 하면 한쪽의 UPDATE 가 먼저 커밋되고, 다른 쪽은 조건에 걸려 0행이 된다.
     * DB 종류를 가리지 않는다(잠금 힌트를 쓰지 않는다).
     */
    private boolean claim(long id) {
        try {
            return jdbc.update("""
                    UPDATE notification_delivery
                       SET status='SENDING', claimed_by=?, claim_expires_at=now() + INTERVAL '5' MINUTE,
                           updated_at=now()
                     WHERE id=? AND status IN ('PENDING','RETRY')
                    """, instanceId, id) == 1;
        } catch (Exception ex) {
            log.warn("발송 건 집기 실패(id={}): {}", id, ex.getMessage());
            return false;
        }
    }

    /**
     * 집어 놓고 끝내지 못한 건을 되돌린다(인스턴스 종료 · 크래시 · 배포 중 교체).
     * 이게 없으면 그 건은 {@code SENDING} 인 채로 영영 남아 아무도 보내지 않는다.
     */
    private void releaseExpiredClaims() {
        try {
            int released = jdbc.update("""
                    UPDATE notification_delivery
                       SET status='RETRY', next_attempt_at=now(), updated_at=now()
                     WHERE status='SENDING' AND claim_expires_at < now()
                    """);
            if (released > 0) {
                log.warn("발송 중 끊긴 알림 {}건을 재시도로 되돌렸다(집은 인스턴스가 끝내지 못함).", released);
            }
        } catch (Exception ex) {
            log.warn("끊긴 발송 건 회수 실패: {}", ex.getMessage());
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

    /**
     * 화면에서 넣은 메일 서버로 발송기를 만든다. 화면 설정이 없으면 기동 때 만들어진 Spring 빈(=.env) 을 쓴다.
     *
     * <p>Spring 의 JavaMailSender 빈은 기동 시점 설정으로 고정된다 - 화면에서 서버를 바꿔도 반영되지 않는다.
     * 그래서 설정 값으로 직접 만들고, 같은 설정이면 다시 만들지 않도록 캐시한다(키가 설정 자체다).
     */
    private JavaMailSender mailSenderFor(ChannelSettingsService.Settings settings) {
        if (!settings.hasText("host")) {
            return mailSenderProvider.getIfAvailable();
        }
        String key = settings.get("host", "") + "|" + settings.get("port", "25") + "|"
                + settings.get("username", "") + "|" + settings.bool("auth", false) + "|"
                + settings.bool("starttls", false) + "|" + sha256(settings.secret() == null ? "" : settings.secret());
        return mailSenders.computeIfAbsent(key, k -> {
            org.springframework.mail.javamail.JavaMailSenderImpl impl =
                    new org.springframework.mail.javamail.JavaMailSenderImpl();
            impl.setHost(settings.get("host", ""));
            impl.setPort(settings.number("port", 25));
            impl.setDefaultEncoding("UTF-8");
            String username = settings.get("username", "");
            if (!username.isBlank()) {
                impl.setUsername(username);
            }
            if (settings.secret() != null && !settings.secret().isBlank()) {
                impl.setPassword(settings.secret());
            }
            java.util.Properties props = impl.getJavaMailProperties();
            props.put("mail.smtp.auth", String.valueOf(settings.bool("auth", false)));
            props.put("mail.smtp.starttls.enable", String.valueOf(settings.bool("starttls", false)));
            props.put("mail.smtp.connectiontimeout", "5000");
            props.put("mail.smtp.timeout", "5000");
            props.put("mail.smtp.writetimeout", "5000");
            return impl;
        });
    }

    /** EMAIL 실 릴레이. JavaMailSender 로 발송하고 결과를 아웃박스에 반영한다. */
    private void relayEmail(long id) {
        ChannelSettingsService.Settings settings = channelSettings.effective("EMAIL");
        JavaMailSender sender = mailSenderFor(settings);
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
            msg.setFrom(settings.get("from", "alerts@pipeline.local"));
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
                    + "next_attempt_at=now() + INTERVAL '2' MINUTE, updated_at=now() "
                    + "WHERE id=? AND attempt_count+1 < max_attempts", detail, id);
            jdbc.update("UPDATE notification_delivery SET status='DEAD', updated_at=now() "
                    + "WHERE id=? AND attempt_count >= max_attempts", id);
            log.warn("EMAIL 발송 실패 id={}: {}", id, ex.getMessage());
        }
    }

    /**
     * SMS 실 릴레이. 설정된 게이트웨이로 POST 한다. 주소 · 본문 형식 · 인증 헤더 · Content-Type 은
     * 화면(발송 채널 설정)이나 환경변수로 바꾼다({@link #renderBody}, {@link #applyGatewayHeaders}).
     * 미설정이면 NO_RELAY 로 폴백한다(실제 문자 안 감).
     */
    private void relaySms(long id) {
        relayGateway(id, "SMS", channelSettings.effective("SMS"), "SMS_ERROR");
    }

    /**
     * KAKAO(알림톡) 실 릴레이. 문자와 같은 HTTP POST 인데 두 가지가 다르다.
     *
     * <p>① 알림톡은 <b>미리 승인받은 템플릿</b>에 변수만 채워 보낸다 - 본문 템플릿에 {@code {templateCode}}
     * {@code {senderKey}} 치환자를 둔다. ② 대행사는 HTTP 200 에 실패를 실어 보내는 일이 흔하다 -
     * {@code successPath}/{@code successValue} 가 설정돼 있으면 응답 값까지 맞아야 성공으로 본다.
     *
     * <p>대행사를 코드로 고정하지 않는 이유: 들어갈 망과 계약할 대행사가 정해져 있지 않다.
     * 규격은 화면에서 넣는 설정(주소 · 헤더 · 본문 템플릿 · 성공 판정)으로 흡수한다.
     */
    private void relayKakao(long id) {
        relayGateway(id, "KAKAO", channelSettings.effective("KAKAO"), "KAKAO_ERROR");
    }

    /** 문자 · 카카오 공통 게이트웨이 발송. 규격 차이는 전부 설정(url · headers · body · 성공 판정)으로 받는다. */
    private void relayGateway(long id, String channel, ChannelSettingsService.Settings settings, String failureReason) {
        String url = settings.get("url", "");
        if (url.isBlank()) {
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
        String text = row.get("body") == null ? "" : row.get("body").toString();
        String title = row.get("subject") == null ? "" : row.get("subject").toString();
        String contentType = settings.get("contentType", "application/json");
        try {
            java.net.http.HttpRequest.Builder builder =
                    java.net.http.HttpRequest.newBuilder(java.net.URI.create(url))
                            .timeout(java.time.Duration.ofSeconds(5))
                            .header("Content-Type", contentType);
            applyGatewayHeaders(builder, renderTemplate(settings.get("headers", ""), to, text, title, settings, false));
            java.net.http.HttpRequest req = builder
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(
                            renderBody(settings, to, text, title, contentType), StandardCharsets.UTF_8))
                    .build();
            java.net.http.HttpResponse<String> resp = httpClient.send(
                    req, java.net.http.HttpResponse.BodyHandlers.ofString());
            String mismatch = successMismatch(settings, resp);
            if (resp.statusCode() / 100 == 2 && mismatch == null) {
                jdbc.update("UPDATE notification_delivery SET status='SENT', sent_at=now(), "
                        + "attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, now()), "
                        + "updated_at=now() WHERE id=?", id);
                log.info("{} 발송 완료 id={} to={}", channel, id, maskPhone(to));
            } else if (resp.statusCode() / 100 == 2) {
                gatewayFail(id, channel, failureReason, mismatch);
            } else {
                gatewayFail(id, channel, failureReason, "HTTP " + resp.statusCode()
                        + (resp.body() == null || resp.body().isBlank() ? "" : " " + resp.body()));
            }
        } catch (Exception ex) {
            gatewayFail(id, channel, failureReason, ex.getMessage());
        }
    }

    /**
     * 대행사가 2xx 로 주면서 본문에 실패를 담는 경우를 가려낸다. successPath 가 비어 있으면 검사하지 않는다
     * (지금까지의 문자 동작과 같다). 경로는 점 표기 - 예: {@code header.resultCode}.
     *
     * @return 어긋난 이유(=실패), 문제없으면 null
     */
    private String successMismatch(ChannelSettingsService.Settings settings, java.net.http.HttpResponse<String> resp) {
        String path = settings.get("successPath", "");
        String expected = settings.get("successValue", "");
        if (path.isBlank() || expected.isBlank()) {
            return null;
        }
        String body = resp.body() == null ? "" : resp.body();
        try {
            com.fasterxml.jackson.databind.JsonNode node =
                    new com.fasterxml.jackson.databind.ObjectMapper().readTree(body);
            for (String segment : path.split("\\.")) {
                if (node == null) {
                    break;
                }
                node = node.get(segment.trim());
            }
            String actual = node == null ? null : node.asText();
            if (expected.equals(actual)) {
                return null;
            }
            return "성공 판정 불일치(" + path + "=" + actual + ", 기대 " + expected + "): "
                    + (body.length() > 200 ? body.substring(0, 200) : body);
        } catch (Exception ex) {
            return "응답을 읽지 못했습니다(" + ex.getMessage() + "): "
                    + (body.length() > 200 ? body.substring(0, 200) : body);
        }
    }

    private void gatewayFail(long id, String channel, String failureReason, String reason) {
        String detail = reason == null ? null : (reason.length() > 480 ? reason.substring(0, 480) : reason);
        jdbc.update("UPDATE notification_delivery SET status='RETRY', attempt_count=attempt_count+1, "
                + "failure_reason=?, failure_detail=?, "
                + "next_attempt_at=now() + INTERVAL '2' MINUTE, updated_at=now() "
                + "WHERE id=? AND attempt_count+1 < max_attempts", failureReason, detail, id);
        jdbc.update("UPDATE notification_delivery SET status='DEAD', updated_at=now() "
                + "WHERE id=? AND attempt_count >= max_attempts", id);
        log.warn("{} 발송 실패 id={}: {}", channel, id, reason);
    }

    /**
     * 게이트웨이로 보낼 본문. 기본은 예전과 같은 {@code {"to","text"}} JSON 이고, 설정의 본문 템플릿으로
     * 필드명 · 구조를 바꾼다. 치환자는 {@code {to} {text} {title} {sender} {senderKey} {templateCode} {secret}}.
     *
     * <pre>
     * {"receiver":"{to}","msg":"{text}","sender":"{sender}"}                     ← 문자 게이트웨이
     * {"senderKey":"{senderKey}","templateCode":"{templateCode}",
     *  "to":"{to}","text":"{text}"}                                             ← 알림톡 대행사
     * to={to}&amp;text={text}&amp;from={sender}                                          ← form 게이트웨이
     * </pre>
     *
     * <p>{@code ${to}} 형태도 받지만 <b>권장하지 않는다</b> - docker compose 가 자기 변수로 먼저
     * 치환해 버려서 빈 문자열이 들어온다(실측). 그래서 {@code $} 없는 형태를 기본으로 안내한다.
     *
     * <p>이스케이프는 Content-Type 을 따른다 - form 이면 URL 인코딩, 아니면 JSON 이스케이프.
     * 알림 본문에는 따옴표 · 개행 · 한글이 그대로 들어오므로 이걸 빠뜨리면 게이트웨이가 400 을 준다.
     */
    private String renderBody(ChannelSettingsService.Settings settings, String to, String text, String title,
                              String contentType) {
        boolean form = contentType.toLowerCase().startsWith("application/x-www-form-urlencoded");
        String template = settings.get("bodyTemplate",
                form ? "to={to}&text={text}" : "{\"to\":\"{to}\",\"text\":\"{text}\"}");
        return renderTemplate(template, to, text, title, settings, form);
    }

    /** 치환자 채우기. 헤더(이스케이프 없음)와 본문(Content-Type 에 맞춘 이스케이프) 양쪽이 쓴다. */
    private String renderTemplate(String template, String to, String text, String title,
                                  ChannelSettingsService.Settings settings, boolean form) {
        if (template == null || template.isBlank()) {
            return "";
        }
        Map<String, String> vars = new java.util.LinkedHashMap<>();
        vars.put("to", to);
        vars.put("text", text);
        vars.put("title", title);
        vars.put("sender", settings.get("sender", ""));
        vars.put("senderKey", settings.get("senderKey", ""));
        vars.put("templateCode", settings.get("templateCode", ""));
        vars.put("secret", settings.secret() == null ? "" : settings.secret());
        String out = template;
        for (Map.Entry<String, String> e : vars.entrySet()) {
            String value = escapeFor(e.getValue(), form);
            out = out.replace("${" + e.getKey() + "}", value).replace("{" + e.getKey() + "}", value);
        }
        return out;
    }

    private String escapeFor(String value, boolean form) {
        return form ? java.net.URLEncoder.encode(value, StandardCharsets.UTF_8) : jsonEsc(value);
    }

    /**
     * 게이트웨이 인증 헤더. {@code "Authorization: Bearer x; X-API-KEY: y"} 처럼 세미콜론으로 나눈다.
     * 값에 콜론이 들어갈 수 있으므로(Bearer 토큰) 첫 콜론만 구분자로 쓴다.
     */
    private void applyGatewayHeaders(java.net.http.HttpRequest.Builder builder, String headers) {
        if (headers == null || headers.isBlank()) {
            return;
        }
        for (String entry : headers.split(";")) {
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
                log.warn("게이트웨이 헤더 무시({}): {}", name, ex.getMessage());
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

    /**
     * 릴레이 불가(미지원 채널/미설정): 재시도 백오프 후 최대치 도달 시 DEAD.
     *
     * <p>간격은 표준 문법 {@code INTERVAL '5' MINUTE} 으로 쓴다 - Postgres 전용
     * {@code interval '5 minutes'} 는 단위 테스트의 H2 에서 조용히 실패해 «재시도로 안 바뀐다»는
     * 가짜 실패를 만든다(2026-09-20). 두 표기의 결과는 Postgres 에서 동일함을 확인했다.
     */
    private void markNoRelay(long id) {
        jdbc.update("UPDATE notification_delivery SET status='RETRY', "
                + "attempt_count=attempt_count+1, failure_reason='NO_RELAY', "
                + "next_attempt_at=now() + INTERVAL '5' MINUTE, updated_at=now() "
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
