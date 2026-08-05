package com.company.pipeline.nifi;

import com.company.pipeline.nifi.dto.NifiBulletinBoardResponse;
import com.company.pipeline.nifi.dto.NifiCountersResponse;
import com.company.pipeline.nifi.dto.NifiFlowResponse;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse;
import com.company.pipeline.nifi.dto.NifiParameterContextResponse;
import com.company.pipeline.nifi.dto.NifiProcessGroupEntity;
import com.company.pipeline.nifi.dto.NifiProcessGroupResponse;
import com.company.pipeline.nifi.dto.NifiProcessorDetailResponse;
import java.net.http.HttpClient;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Component
@EnableConfigurationProperties(NifiProperties.class)
public class NifiClient {

    private static final String ROOT_GROUP_ID = "root";

    private final RestClient restClient;
    private final NifiProperties properties;

    public NifiClient(NifiProperties properties) {
        this.properties = properties;
        this.restClient = RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(new JdkClientHttpRequestFactory(insecureHttpClient()))
                .build();
    }

    public NifiProcessGroupResponse createRootProcessGroup(String name) {
        String token = getToken();
        Map<String, Object> body = Map.of(
                "revision", Map.of(
                        "clientId", UUID.randomUUID().toString(),
                        "version", 0
                ),
                "component", Map.of(
                        "name", name,
                        "position", Map.of(
                                "x", 120,
                                "y", 120
                        )
                )
        );

        try {
            NifiProcessGroupEntity entity = restClient.post()
                    .uri("/nifi-api/process-groups/{groupId}/process-groups", ROOT_GROUP_ID)
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(NifiProcessGroupEntity.class);
            return toResponse(entity);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi Processor Group 생성 실패: " + ex.getMessage(), ex);
        }
    }

    /** 적재 건수 집계용 - 프로세스 그룹 트리 전체(재귀)의 프로세서 상태 스냅샷. */
    public NifiFlowStatusResponse getRootFlowStatus() {
        String token = getToken();
        try {
            return restClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/nifi-api/flow/process-groups/root/status")
                            .queryParam("recursive", true)
                            .build())
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(NifiFlowStatusResponse.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 프로세스 그룹 상태 조회 실패: " + ex.getMessage(), ex);
        }
    }

    /**
     * 잡 카탈로그 동기화용 - 그룹 한 단계의 "구성"(하위 그룹/프로세서/연결선).
     *
     * <p>상태 조회와 달리 프로세서 설정값과 연결의 관계 이름, 캔버스 좌표가 들어 있다.
     * 한 번에 한 단계만 내려오므로 하위 그룹은 호출자가 재귀로 다시 부른다.
     */
    public NifiFlowResponse getFlow(String groupId) {
        String token = getToken();
        try {
            return restClient.get()
                    .uri("/nifi-api/flow/process-groups/{groupId}", groupId)
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(NifiFlowResponse.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 프로세스 그룹 구성 조회 실패: " + ex.getMessage(), ex);
        }
    }

    /** 그룹에 바인딩된 파라미터 컨텍스트의 파라미터 목록. sensitive 값은 내려오지 않는다. */
    public NifiParameterContextResponse getParameterContext(String parameterContextId) {
        String token = getToken();
        try {
            return restClient.get()
                    .uri("/nifi-api/parameter-contexts/{id}", parameterContextId)
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(NifiParameterContextResponse.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 파라미터 컨텍스트 조회 실패: " + ex.getMessage(), ex);
        }
    }

    /** 적재 건수 집계용 - PutDatabaseRecord 등이 등록한 누적 카운터(재시작 전까지 유지). */
    public NifiCountersResponse getCounters() {
        String token = getToken();
        try {
            return restClient.get()
                    .uri("/nifi-api/counters")
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(NifiCountersResponse.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 카운터 조회 실패: " + ex.getMessage(), ex);
        }
    }

    /**
     * 프로세서 1개의 설정. status 응답에는 설정이 없어서 적재 대상 테이블 같은 값을
     * 얻으려면 이걸 따로 불러야 한다 - 프로세서마다 한 번만 부르고 캐시할 것.
     */
    public NifiProcessorDetailResponse getProcessor(String processorId) {
        String token = getToken();
        try {
            return restClient.get()
                    .uri("/nifi-api/processors/{id}", processorId)
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(NifiProcessorDetailResponse.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 프로세서 조회 실패: " + ex.getMessage(), ex);
        }
    }

    /**
     * 경고/에러 알림 조회. {@code afterId} 이후에 생긴 것만 받아서 같은 알림을 두 번
     * 처리하지 않는다.
     *
     * <p>bulletin은 NiFi 메모리에 5분 남짓만 남으므로, 이 호출 주기가 그보다 길면
     * 그 사이 발생한 실패는 조용히 유실된다.
     */
    public NifiBulletinBoardResponse getBulletins(long afterId) {
        String token = getToken();
        try {
            return restClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/nifi-api/flow/bulletin-board")
                            .queryParam("after", afterId)
                            .build())
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(NifiBulletinBoardResponse.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi bulletin 조회 실패: " + ex.getMessage(), ex);
        }
    }

    // NiFi가 Keycloak/OIDC를 제거하고 Single User 인증(공유 서비스계정)으로 전환됨에 따라,
    // /nifi-api/access/token에 username/password를 보내 JWT를 직접 발급받는다.
    // 이 엔드포인트는 응답 본문이 JSON이 아니라 JWT 문자열 그대로임에 주의.
    private String getToken() {
        if (!StringUtils.hasText(properties.username()) || !StringUtils.hasText(properties.password())) {
            throw new NifiClientException("NiFi 서비스 계정 정보가 설정되지 않았습니다.", null);
        }

        try {
            MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
            form.add("username", properties.username());
            form.add("password", properties.password());
            String token = restClient.post()
                    .uri("/nifi-api/access/token")
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .body(form)
                    .retrieve()
                    .body(String.class);
            if (!StringUtils.hasText(token)) {
                throw new NifiClientException("NiFi 토큰 응답이 비어 있습니다.", null);
            }
            return token;
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 토큰 발급 실패: " + ex.getMessage(), ex);
        }
    }

    private NifiProcessGroupResponse toResponse(NifiProcessGroupEntity entity) {
        if (entity == null) {
            throw new NifiClientException("NiFi Processor Group 생성 응답이 비어 있습니다.", null);
        }
        NifiProcessGroupEntity.Component component = entity == null ? null : entity.component();
        String id = component == null || !StringUtils.hasText(component.id()) ? entity.id() : component.id();
        String name = component == null ? null : component.name();
        String parentGroupId = component == null ? null : component.parentGroupId();
        return new NifiProcessGroupResponse(id, name, parentGroupId);
    }

    private HttpClient insecureHttpClient() {
        try {
            SSLContext sslContext = trustAllSslContext();
            // new SSLParameters()로 빈 객체를 넘기면 protocols/cipher suite가 비어있어
            // JDK HttpClient가 이를 불완전하다고 보고 플랫폼 기본값(호스트명 검증 포함)으로
            // 되돌아간다. SSLContext의 기본 파라미터에서 시작해 필드 하나만 덮어써야 한다.
            SSLParameters sslParameters = sslContext.getDefaultSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("");
            return HttpClient.newBuilder()
                    .sslContext(sslContext)
                    .sslParameters(sslParameters)
                    .build();
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            throw new NifiClientException("NiFi HTTPS 클라이언트 초기화 실패: " + ex.getMessage(), ex);
        }
    }

    private SSLContext trustAllSslContext() throws NoSuchAlgorithmException, KeyManagementException {
        TrustManager[] trustManagers = {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
        };
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagers, new SecureRandom());
        return sslContext;
    }
}
