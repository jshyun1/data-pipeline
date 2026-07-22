package com.company.pipeline.user.security;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.ErrorCode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;

/**
 * 통합 웹 보안 설정. 인증원은 Keycloak(cerebro realm) - 이 서비스는 Keycloak이 발급한
 * 액세스 토큰을 검증하는 OAuth2 Resource Server로만 동작하고 자체 토큰을 발급하지 않는다.
 *
 * 기존 업무 API(/api/pipelines 등)는 여전히 permitAll이다: Airflow 제어 DAG와
 * (레거시)pipeline-ui가 토큰 없이 호출하므로, 이들 게이팅은 서비스 토큰 도입 후 별도 단계.
 */
@Configuration
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http, ObjectMapper objectMapper) throws Exception {
        http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // 현재 사용자 프로필 조회는 유효한 Keycloak 토큰 필요
                        .requestMatchers("/api/auth/me").authenticated()
                        // 나머지 업무 API는 이번 단계에선 개방 유지(위 주석 참고)
                        .anyRequest().permitAll())
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> jwt.jwtAuthenticationConverter(keycloakConverter())))
                .exceptionHandling(eh -> eh.authenticationEntryPoint((request, response, ex) -> {
                    response.setStatus(ErrorCode.UNAUTHORIZED.getHttpStatus().value());
                    response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                    response.setCharacterEncoding("UTF-8");
                    objectMapper.writeValue(response.getWriter(), ApiResponse.error(
                            ErrorCode.UNAUTHORIZED, ErrorCode.UNAUTHORIZED.getDefaultMessage()));
                }));
        return http.build();
    }

    /** Keycloak realm_access.roles 를 ROLE_* 권한으로 매핑한다(예: portal_admin -> ROLE_PORTAL_ADMIN). */
    private JwtAuthenticationConverter keycloakConverter() {
        JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(SecurityConfig::extractRealmRoles);
        return converter;
    }

    @SuppressWarnings("unchecked")
    private static Collection<GrantedAuthority> extractRealmRoles(Jwt jwt) {
        Map<String, Object> realmAccess = jwt.getClaim("realm_access");
        if (realmAccess == null) {
            return List.of();
        }
        Object roles = realmAccess.get("roles");
        if (!(roles instanceof Collection<?> roleList)) {
            return List.of();
        }
        return roleList.stream()
                .map(Object::toString)
                .map(role -> (GrantedAuthority) new SimpleGrantedAuthority("ROLE_" + role.toUpperCase()))
                .toList();
    }
}
