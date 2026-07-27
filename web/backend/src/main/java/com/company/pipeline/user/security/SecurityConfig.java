package com.company.pipeline.user.security;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.ErrorCode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/**
 * 통합 웹 보안 설정. Keycloak을 제거하고 이 앱이 직접 발급/검증하는 JWT
 * (PipelineJwtService/PipelineJwtAuthenticationFilter)로 전환했다 - 인증원은
 * 통합 계정 테이블(ST_USER, MySQL)이고, 토큰 서명 자체는 이 앱 단독 비밀키로 한다.
 *
 * 기존 업무 API(/api/pipelines 등)는 여전히 permitAll이다: Airflow 제어 DAG와
 * (레거시)pipeline-ui가 토큰 없이 호출하므로, 이들 게이팅은 서비스 토큰 도입 후 별도 단계.
 */
@Configuration
public class SecurityConfig {

    private final PipelineJwtAuthenticationFilter jwtAuthenticationFilter;

    public SecurityConfig(PipelineJwtAuthenticationFilter jwtAuthenticationFilter) {
        this.jwtAuthenticationFilter = jwtAuthenticationFilter;
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http, ObjectMapper objectMapper) throws Exception {
        http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // 로그인 자체는 인증 없이 호출 가능해야 함
                        .requestMatchers("/api/auth/login").permitAll()
                        // 현재 사용자 프로필 조회는 유효한 자체 토큰 필요
                        .requestMatchers("/api/auth/me").authenticated()
                        // 나머지 업무 API는 이번 단계에선 개방 유지(위 주석 참고)
                        .anyRequest().permitAll())
                .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class)
                .exceptionHandling(eh -> eh.authenticationEntryPoint((request, response, ex) -> {
                    response.setStatus(ErrorCode.UNAUTHORIZED.getHttpStatus().value());
                    response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                    response.setCharacterEncoding("UTF-8");
                    objectMapper.writeValue(response.getWriter(), ApiResponse.error(
                            ErrorCode.UNAUTHORIZED, ErrorCode.UNAUTHORIZED.getDefaultMessage()));
                }));
        return http.build();
    }
}
