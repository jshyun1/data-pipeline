package com.company.pipeline.user.security;

import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.web.client.RestTemplate;

/**
 * Keycloak이 자체 서명 인증서로 HTTPS를 제공하므로(POC), JWKS를 가져오는 HTTP 클라이언트가
 * 그 인증서를 신뢰해야 한다. 표준 issuer-location 방식은 기본 RestTemplate로 JWKS를 받다가
 * SSL 검증에 실패하므로, 인증서 검증을 생략한 클라이언트로 JWKS만 가져오도록 디코더를 직접 만든다.
 * (같은 도커 네트워크 내부 IdP라는 전제의 POC 설정 - NiFi 제어에서 self-signed를 신뢰하는 것과 동일 맥락)
 * 토큰 자체의 서명/issuer/만료 검증은 그대로 수행된다.
 */
@Configuration
public class JwtDecoderConfig {

    @Value("${spring.security.oauth2.resourceserver.jwt.issuer-uri}")
    private String issuerUri;

    @Bean
    public JwtDecoder jwtDecoder() throws Exception {
        String jwkSetUri = issuerUri + "/protocol/openid-connect/certs";
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withJwkSetUri(jwkSetUri)
                .restOperations(trustAllRestTemplate())
                .build();
        // 서명뿐 아니라 iss 클레임이 우리 issuer와 일치하고 만료되지 않았는지도 검증
        decoder.setJwtValidator(JwtValidators.createDefaultWithIssuer(issuerUri));
        return decoder;
    }

    private RestTemplate trustAllRestTemplate() throws Exception {
        TrustManager[] trustAll = new TrustManager[] {
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
        sslContext.init(null, trustAll, new java.security.SecureRandom());
        var httpClient = java.net.http.HttpClient.newBuilder().sslContext(sslContext).build();
        return new RestTemplate(new JdkClientHttpRequestFactory(httpClient));
    }
}
