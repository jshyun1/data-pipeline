package com.company.pipeline.user.security;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.ErrorCode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
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
 * 통합 웹 로그인 보안 설정. JWT 기반 stateless.
 *
 * 이번 단계 범위: 인증 시스템만 구축하고, 기존 업무 API(/api/pipelines 등)는
 * 아직 permitAll로 둔다 - 그 엔드포인트들은 Airflow 제어 DAG와 (레거시)pipeline-ui가
 * 토큰 없이 직접 호출하고 있어서, 지금 잠그면 그것들이 한꺼번에 깨진다.
 * 업무 API 인증(서비스 토큰/게이트) 적용은 별도 단계로 미룬다.
 */
@Configuration
@EnableConfigurationProperties(JwtProperties.class)
public class SecurityConfig {

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http, JwtAuthenticationFilter jwtFilter,
            ObjectMapper objectMapper) throws Exception {
        http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // 로그인/등록신청은 미인증 허용
                        .requestMatchers("/api/auth/login", "/api/auth/register").permitAll()
                        // 현재 사용자 조회는 유효한 토큰 필요(= 토큰 검증 실동작 지점)
                        .requestMatchers("/api/auth/me").authenticated()
                        // 나머지 업무 API는 이번 단계에선 개방 유지(위 주석 참고)
                        .anyRequest().permitAll())
                .exceptionHandling(eh -> eh.authenticationEntryPoint((request, response, ex) -> {
                    response.setStatus(ErrorCode.UNAUTHORIZED.getHttpStatus().value());
                    response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                    response.setCharacterEncoding("UTF-8");
                    objectMapper.writeValue(response.getWriter(), ApiResponse.error(
                            ErrorCode.UNAUTHORIZED, ErrorCode.UNAUTHORIZED.getDefaultMessage()));
                }))
                .addFilterBefore(jwtFilter, UsernamePasswordAuthenticationFilter.class);
        return http.build();
    }
}
