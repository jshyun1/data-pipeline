package com.company.pipeline.nifi;

import com.company.pipeline.nifi.dto.NifiProcessGroupEntity;
import com.company.pipeline.nifi.dto.NifiProcessGroupResponse;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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

    // NiFi가 OIDC 전용 인증으로 전환되며 username/password 토큰 발급(/nifi-api/access/token)이
    // 막혀서, Keycloak에서 client_credentials로 직접 토큰을 받아 NiFi에 Bearer로 제시한다.
    // NiFi는 신뢰하는 realm이 서명한 토큰이면 발급 클라이언트를 가리지 않고 인증을 통과시키고,
    // 이후 sub 클레임을 identity로 authorizers.xml/users.xml에 등록된 권한을 확인한다.
    private String getToken() {
        if (!StringUtils.hasText(properties.serviceClientId()) || !StringUtils.hasText(properties.serviceClientSecret())) {
            throw new NifiClientException("NiFi 서비스 계정 정보가 설정되지 않았습니다.", null);
        }

        try {
            MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
            form.add("grant_type", "client_credentials");
            form.add("client_id", properties.serviceClientId());
            form.add("client_secret", properties.serviceClientSecret());
            KeycloakTokenResponse response = restClient.post()
                    .uri(properties.keycloakTokenUri())
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .body(form)
                    .retrieve()
                    .body(KeycloakTokenResponse.class);
            if (response == null || !StringUtils.hasText(response.accessToken())) {
                throw new NifiClientException("Keycloak 토큰 응답이 비어 있습니다.", null);
            }
            return response.accessToken();
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 토큰 발급 실패: " + ex.getMessage(), ex);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record KeycloakTokenResponse(@JsonProperty("access_token") String accessToken) {
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
