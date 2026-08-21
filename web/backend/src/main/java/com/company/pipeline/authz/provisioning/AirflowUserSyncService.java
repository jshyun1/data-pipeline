package com.company.pipeline.authz.provisioning;

import com.company.pipeline.authz.AccessBits;
import com.company.pipeline.authz.PermissionService;
import com.company.pipeline.authz.SystemCode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Cerebro 계정을 Airflow 개인 계정으로 동기화하고(FAB 사용자·역할, 설계서 §7.7 P5b), 콘솔 대행에
 * 필요한 <b>사용자별 세션토큰(_token)</b>을 발급한다.
 *
 * <ul>
 *   <li>인증: Airflow 3 는 JWT (POST /airflow/auth/token). FAB API 는 Bearer JWT 로 호출(실측).</li>
 *   <li>역할: AIRFLOW 쓰기=Admin / 읽기=Viewer. 강등 시 역할 교체.</li>
 *   <li>비밀번호: 사용자별 결정론적 값(HMAC(userId))이라 백엔드가 그 사용자로 로그인해 콘솔용
 *       {@code _token} 쿠키를 받을 수 있다(사람은 직접 로그인하지 않음).</li>
 * </ul>
 *
 * <p>기본 off({@code authz.identity-sync.enabled}). 실패는 삼킨다.
 */
@Service
public class AirflowUserSyncService {

    private static final Logger log = LoggerFactory.getLogger(AirflowUserSyncService.class);
    private static final long SESSION_TTL_MS = 20 * 60 * 1000L;

    private final RestClient restClient;
    private final PermissionService permissionService;
    private final String adminUsername;
    private final String adminPassword;
    private final String passwordSecret;

    private final Map<String, Cached> sessionCache = new ConcurrentHashMap<>();

    private record Cached(String token, long expiresAt) {
    }

    @Value("${authz.identity-sync.enabled:false}")
    private boolean enabled;

    public AirflowUserSyncService(
            PermissionService permissionService,
            @Value("${airflow.base-url:http://airflow-apiserver:8080}") String baseUrl,
            @Value("${airflow.admin-username:}") String adminUsername,
            @Value("${airflow.admin-password:}") String adminPassword,
            @Value("${authz.proxy.airflow-secret:cerebro-airflow-p5b-secret}") String passwordSecret) {
        this.permissionService = permissionService;
        this.adminUsername = adminUsername;
        this.adminPassword = adminPassword;
        this.passwordSecret = passwordSecret;
        // Airflow(uvicorn)는 JDK HttpClient 기본 HTTP/2 요청을 "Invalid HTTP request"로 거부하므로
        // HTTP/1.1 로 고정한다(실측).
        java.net.http.HttpClient httpClient = java.net.http.HttpClient.newBuilder()
                .version(java.net.http.HttpClient.Version.HTTP_1_1).build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(Duration.ofSeconds(6));
        // Airflow 앱은 /airflow prefix 로 마운트되어 있다(실측).
        this.restClient = RestClient.builder().baseUrl(baseUrl + "/airflow").requestFactory(factory).build();
    }

    // ----- FAB 사용자 동기화 -----------------------------------------------

    public void syncUser(String userId, String userNm, String email) {
        if (!enabled || userId == null || userId.isBlank()) {
            return;
        }
        try {
            boolean active = permissionService.check(userId, SystemCode.AIRFLOW, AccessBits.READ);
            if (!active) {
                // 접근 없음/비활성 → 역할 강등(Public). 삭제 대신 무권한 역할.
                if (userExists(userId)) {
                    setRole(userId, "Public");
                }
                return;
            }
            String role = permissionService.check(userId, SystemCode.AIRFLOW, AccessBits.WRITE) ? "Admin" : "Viewer";
            if (userExists(userId)) {
                setRole(userId, role);
            } else {
                createUser(userId, userNm, email, role);
            }
            log.info("Airflow 개인계정 동기화 완료 - {} ({})", userId, role);
        } catch (RuntimeException ex) {
            log.warn("Airflow 개인계정 동기화 실패(무시하고 진행) - {}: {}", userId, ex.getMessage());
        }
    }

    private String adminToken() {
        Map<String, Object> resp = restClient.post().uri("/auth/token")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", adminUsername, "password", adminPassword))
                .retrieve().body(Map.class);
        return resp == null ? null : (String) resp.get("access_token");
    }

    private boolean userExists(String username) {
        try {
            restClient.get().uri("/auth/fab/v1/users/{u}", username)
                    .header("Authorization", "Bearer " + adminToken())
                    .retrieve().toBodilessEntity();
            return true;
        } catch (RestClientException ex) {
            return false;
        }
    }

    private void createUser(String username, String userNm, String email, String role) {
        Map<String, Object> body = Map.of(
                "username", username,
                "password", derivePassword(username),
                "first_name", StringUtils.hasText(userNm) ? userNm : username,
                "last_name", "-",
                "email", StringUtils.hasText(email) ? email : username + "@cerebro.local",
                "roles", List.of(Map.of("name", role)));
        restClient.post().uri("/auth/fab/v1/users")
                .header("Authorization", "Bearer " + adminToken())
                .contentType(MediaType.APPLICATION_JSON)
                .body(body).retrieve().toBodilessEntity();
    }

    private void setRole(String username, String role) {
        restClient.patch().uri("/auth/fab/v1/users/{u}", username)
                .header("Authorization", "Bearer " + adminToken())
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("roles", List.of(Map.of("name", role))))
                .retrieve().toBodilessEntity();
    }

    // ----- 콘솔 대행용 사용자별 _token 세션 --------------------------------

    /** nginx auth_request 가 쓸 사용자별 Airflow 세션토큰(_token 쿠키값). 실패 시 null. 20분 캐시. */
    public String userSessionToken(String userId) {
        if (!enabled || userId == null) {
            return null;
        }
        long now = System.currentTimeMillis();
        Cached c = sessionCache.get(userId);
        if (c != null && c.expiresAt() > now) {
            return c.token();
        }
        try {
            String token = loginAndExtractToken(userId, derivePassword(userId));
            if (token != null) {
                sessionCache.put(userId, new Cached(token, now + SESSION_TTL_MS));
            }
            return token;
        } catch (RuntimeException ex) {
            log.warn("Airflow 사용자 세션 발급 실패 - {}: {}", userId, ex.getMessage());
            return null;
        }
    }

    /** CSRF 확보 → 폼 로그인 → Set-Cookie 의 _token 값을 뽑는다(refresh-credentials.sh 와 동일 방식). */
    private String loginAndExtractToken(String username, String password) {
        var loginPage = restClient.get().uri("/auth/login/").retrieve().toEntity(String.class);
        String html = loginPage.getBody();
        String csrf = extractBetween(html, "name=\"csrf_token\" type=\"hidden\" value=\"", "\"");
        List<String> setCookies = loginPage.getHeaders().get("Set-Cookie");
        String sessionCookie = firstCookie(setCookies);
        if (csrf == null) {
            return null;
        }
        String form = "username=" + enc(username) + "&password=" + enc(password) + "&csrf_token=" + enc(csrf);
        var resp = restClient.post().uri("/auth/login/")
                .header("Referer", "/airflow/auth/login/")
                .header("Cookie", sessionCookie == null ? "" : sessionCookie)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .body(form).retrieve().toBodilessEntity();
        List<String> cookies = resp.getHeaders().get("Set-Cookie");
        if (cookies != null) {
            for (String c : cookies) {
                if (c.startsWith("_token=")) {
                    return c.substring("_token=".length()).split(";", 2)[0];
                }
            }
        }
        return null;
    }

    // ----- helpers ---------------------------------------------------------

    private String derivePassword(String userId) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(passwordSecret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            byte[] out = mac.doFinal(userId.getBytes(StandardCharsets.UTF_8));
            return "Cx1!" + HexFormat.of().formatHex(out).substring(0, 24);
        } catch (Exception ex) {
            throw new IllegalStateException("Airflow 비밀번호 파생 실패", ex);
        }
    }

    private String extractBetween(String s, String start, String end) {
        if (s == null) {
            return null;
        }
        int i = s.indexOf(start);
        if (i < 0) {
            return null;
        }
        int j = s.indexOf(end, i + start.length());
        return j < 0 ? null : s.substring(i + start.length(), j);
    }

    private String firstCookie(List<String> setCookies) {
        if (setCookies == null || setCookies.isEmpty()) {
            return null;
        }
        return setCookies.get(0).split(";", 2)[0];
    }

    private String enc(String v) {
        return URLEncoder.encode(v, StandardCharsets.UTF_8);
    }
}
