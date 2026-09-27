package com.company.pipeline.notification;

import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * 발송 채널(메일 · 문자 · 카카오) 설정. <b>화면에서 넣은 값(DB)이 먼저, 없으면 환경변수</b>.
 *
 * <p>이 제품이 어느 망(폐쇄망 · 사내망 · 외부망)에 들어갈지 정해져 있지 않다. 예전에는 발송 설정이
 * .env 에만 있어서 현장마다 파일을 고치고 재기동해야 했다. 값을 DB 에 두면 화면에서 넣고 바로 쓴다 -
 * 저장 즉시 캐시를 버리므로 다음 발송부터 새 설정으로 나간다(재배포 · 재기동 없음).
 *
 * <p>환경변수 폴백을 남기는 이유: 폐쇄망 중에는 «설정을 파일로만 관리한다»는 곳이 있다. 그런 현장은
 * 화면을 안 쓰면 예전과 똑같이 동작한다.
 *
 * <p>비밀값(메일 비밀번호 · 게이트웨이 토큰 · 카카오 API 키)은 채널마다 하나이며 암호화해 저장하고,
 * 화면에는 끝 4자리 힌트만 준다(GPT 제공자 등록과 같은 방식).
 */
@Service
public class ChannelSettingsService {

    private static final Logger log = LoggerFactory.getLogger(ChannelSettingsService.class);
    /** dispatch 가 5초마다 돈다 - 매 건 조회·복호화하지 않도록 잠깐 들고 있는다. 저장하면 바로 버린다. */
    private static final Duration CACHE_TTL = Duration.ofSeconds(10);

    /** 채널별로 화면이 받는 항목. 순서가 곧 화면 입력 순서다. */
    public static final Map<String, List<String>> KEYS = Map.of(
            "EMAIL", List.of("host", "port", "username", "from", "auth", "starttls"),
            "SMS", List.of("url", "headers", "contentType", "bodyTemplate", "sender"),
            "KAKAO", List.of("url", "headers", "contentType", "bodyTemplate", "senderKey", "templateCode",
                    "sender", "successPath", "successValue"));

    private final JdbcTemplate jdbc;
    private final PasswordCryptoService crypto;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, Cached> cache = new ConcurrentHashMap<>();
    private final Map<String, Map<String, String>> envDefaults;
    private final Map<String, String> envSecrets;

    public ChannelSettingsService(DataSource dataSource, PasswordCryptoService crypto,
                                  @Value("${spring.mail.host:}") String mailHost,
                                  @Value("${spring.mail.port:}") String mailPort,
                                  @Value("${spring.mail.username:}") String mailUsername,
                                  @Value("${spring.mail.password:}") String mailPassword,
                                  @Value("${spring.mail.properties.mail.smtp.auth:false}") String mailAuth,
                                  @Value("${spring.mail.properties.mail.smtp.starttls.enable:false}") String mailStarttls,
                                  @Value("${notification.email.from:}") String emailFrom,
                                  @Value("${notification.sms.gateway.url:}") String smsUrl,
                                  @Value("${notification.sms.gateway.headers:}") String smsHeaders,
                                  @Value("${notification.sms.gateway.content-type:application/json}") String smsContentType,
                                  @Value("${notification.sms.gateway.body-template:}") String smsBodyTemplate,
                                  @Value("${notification.sms.sender:}") String smsSender,
                                  @Value("${notification.kakao.gateway.url:}") String kakaoUrl,
                                  @Value("${notification.kakao.gateway.headers:}") String kakaoHeaders,
                                  @Value("${notification.kakao.gateway.content-type:application/json}") String kakaoContentType,
                                  @Value("${notification.kakao.gateway.body-template:}") String kakaoBodyTemplate,
                                  @Value("${notification.kakao.sender-key:}") String kakaoSenderKey,
                                  @Value("${notification.kakao.template-code:}") String kakaoTemplateCode,
                                  @Value("${notification.kakao.sender:}") String kakaoSender,
                                  @Value("${notification.kakao.success-path:}") String kakaoSuccessPath,
                                  @Value("${notification.kakao.success-value:}") String kakaoSuccessValue,
                                  @Value("${notification.kakao.api-key:}") String kakaoApiKey) {
        this.jdbc = new JdbcTemplate(dataSource);
        this.crypto = crypto;
        this.envDefaults = Map.of(
                "EMAIL", ordered("host", mailHost, "port", mailPort, "username", mailUsername,
                        "from", emailFrom, "auth", mailAuth, "starttls", mailStarttls),
                "SMS", ordered("url", smsUrl, "headers", smsHeaders, "contentType", smsContentType,
                        "bodyTemplate", smsBodyTemplate, "sender", smsSender),
                "KAKAO", ordered("url", kakaoUrl, "headers", kakaoHeaders, "contentType", kakaoContentType,
                        "bodyTemplate", kakaoBodyTemplate, "senderKey", kakaoSenderKey,
                        "templateCode", kakaoTemplateCode, "sender", kakaoSender,
                        "successPath", kakaoSuccessPath, "successValue", kakaoSuccessValue));
        this.envSecrets = ordered("EMAIL", mailPassword, "SMS", "", "KAKAO", kakaoApiKey);
    }

    /** 실제로 발송에 쓸 설정. DB 값이 있으면 DB, 비어 있으면 환경변수. */
    public Settings effective(String channelType) {
        Cached cached = cache.get(channelType);
        if (cached != null && cached.until > System.nanoTime()) {
            return cached.settings;
        }
        Settings settings = load(channelType);
        cache.put(channelType, new Cached(settings, System.nanoTime() + CACHE_TTL.toNanos()));
        return settings;
    }

    /**
     * 화면에 보여 줄 설정. 비밀값은 저장 여부와 끝자리 힌트만 준다.
     * 값이 DB 에 없어 환경변수로 도는 항목도 그대로 보여 준다 - 화면에서 «지금 무엇으로 나가는지»가 보여야 한다.
     */
    public Map<String, Object> view(String channelType) {
        Settings settings = load(channelType);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("channelType", channelType);
        Map<String, String> values = new LinkedHashMap<>();
        for (String key : KEYS.getOrDefault(channelType, List.of())) {
            values.put(key, settings.get(key, ""));
        }
        out.put("values", values);
        out.put("secretSet", settings.secret() != null && !settings.secret().isBlank());
        out.put("secretHint", settings.secretHint());
        out.put("fromEnvOnly", settings.fromEnvOnly());
        return out;
    }

    /**
     * 화면에서 저장. 비밀값을 <b>비워 보내면 기존 값을 유지</b>한다(GPT 제공자 등록과 같은 규칙) -
     * 화면은 힌트만 받으므로, 다른 항목만 고칠 때 비밀값을 다시 입력하게 만들면 안 된다.
     */
    public void save(String channelType, Map<String, String> values, String secret, String actor) {
        Map<String, String> clean = new LinkedHashMap<>();
        for (String key : KEYS.getOrDefault(channelType, List.of())) {
            String value = values == null ? null : values.get(key);
            clean.put(key, value == null ? "" : value.trim());
        }
        String json;
        try {
            json = objectMapper.writeValueAsString(clean);
        } catch (Exception ex) {
            throw new IllegalArgumentException("설정을 저장할 수 없습니다: " + ex.getMessage());
        }
        if (secret != null && !secret.isBlank()) {
            String encrypted = crypto.encrypt(secret.trim());
            jdbc.update("UPDATE notification_channel_config SET config_json = ?::jsonb, encrypted_secret = ?, "
                            + "secret_hint = ?, updated_by = ?, updated_at = now() WHERE channel_type = ?",
                    json, encrypted, hint(secret.trim()), actor, channelType);
        } else {
            jdbc.update("UPDATE notification_channel_config SET config_json = ?::jsonb, updated_by = ?, "
                    + "updated_at = now() WHERE channel_type = ?", json, actor, channelType);
        }
        cache.remove(channelType);
    }

    /** 비밀값 삭제(화면의 «지운다»). 지우면 환경변수 값으로 되돌아간다. */
    public void clearSecret(String channelType, String actor) {
        jdbc.update("UPDATE notification_channel_config SET encrypted_secret = NULL, secret_hint = NULL, "
                + "updated_by = ?, updated_at = now() WHERE channel_type = ?", actor, channelType);
        cache.remove(channelType);
    }

    private Settings load(String channelType) {
        Map<String, String> fromDb = new LinkedHashMap<>();
        String secret = null;
        String secretHint = null;
        boolean rowFound = false;
        try {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "SELECT config_json, encrypted_secret, secret_hint FROM notification_channel_config "
                            + "WHERE channel_type = ?", channelType);
            if (!rows.isEmpty()) {
                rowFound = true;
                Map<String, Object> row = rows.get(0);
                Object config = row.get("config_json");
                if (config != null) {
                    Map<?, ?> parsed = objectMapper.readValue(config.toString(), Map.class);
                    parsed.forEach((k, v) -> {
                        if (v != null && !v.toString().isBlank()) {
                            fromDb.put(String.valueOf(k), v.toString());
                        }
                    });
                }
                String encrypted = (String) row.get("encrypted_secret");
                if (encrypted != null && !encrypted.isBlank()) {
                    try {
                        secret = crypto.decrypt(encrypted);
                        secretHint = (String) row.get("secret_hint");
                    } catch (Exception ex) {
                        // 암호화 키가 바뀌면 복호화가 깨진다. 발송을 막지 말고(환경변수 폴백) 로그만 남긴다.
                        log.warn("{} 채널 비밀값 복호화 실패 - 환경변수 값으로 진행합니다: {}", channelType, ex.getMessage());
                    }
                }
            }
        } catch (Exception ex) {
            log.warn("{} 채널 설정 조회 실패 - 환경변수 값으로 진행합니다: {}", channelType, ex.getMessage());
        }
        Map<String, String> merged = new LinkedHashMap<>(envDefaults.getOrDefault(channelType, Map.of()));
        merged.putAll(fromDb);
        if (secret == null || secret.isBlank()) {
            secret = envSecrets.getOrDefault(channelType, "");
        }
        boolean fromEnvOnly = rowFound && fromDb.isEmpty();
        return new Settings(merged, secret, secretHint, fromEnvOnly);
    }

    private static String hint(String secret) {
        return secret.length() <= 4 ? "****" : "****" + secret.substring(secret.length() - 4);
    }

    private static Map<String, String> ordered(String... pairs) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < pairs.length; i += 2) {
            map.put(pairs[i], pairs[i + 1] == null ? "" : pairs[i + 1]);
        }
        return map;
    }

    private record Cached(Settings settings, long until) {
    }

    /**
     * 한 채널의 «지금 쓸» 설정 한 벌.
     *
     * @param fromEnvOnly 화면에서 저장한 적이 없어 환경변수만으로 도는 상태(화면 안내용)
     */
    public record Settings(Map<String, String> values, String secret, String secretHint, boolean fromEnvOnly) {

        public String get(String key, String fallback) {
            String value = values.get(key);
            return value == null || value.isBlank() ? fallback : value;
        }

        public boolean bool(String key, boolean fallback) {
            String value = values.get(key);
            if (value == null || value.isBlank()) {
                return fallback;
            }
            return "true".equalsIgnoreCase(value.trim()) || "1".equals(value.trim());
        }

        public int number(String key, int fallback) {
            try {
                return Integer.parseInt(get(key, String.valueOf(fallback)).trim());
            } catch (NumberFormatException ex) {
                return fallback;
            }
        }

        /** 발송에 필요한 최소 항목이 있는지(채널마다 다르다). */
        public boolean hasText(String key) {
            String value = values.get(key);
            return value != null && !value.isBlank();
        }

        public Map<String, String> asMap() {
            return new HashMap<>(values);
        }
    }
}
