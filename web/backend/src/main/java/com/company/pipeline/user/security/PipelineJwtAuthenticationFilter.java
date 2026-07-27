package com.company.pipeline.user.security;

import com.company.pipeline.user.AppUser;
import com.company.pipeline.user.AppUserRepository;
import io.jsonwebtoken.Claims;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * cerebroetl-ui 자체 로그인 JWT(Authorization: Bearer ...) 검증. Keycloak OAuth2 Resource
 * Server를 대체한다 - 서명/만료만 확인하고, 사용자 활성 상태는 app_user 테이블로 재확인한다
 * (계정이 그 사이 비활성화됐을 수 있으므로).
 */
@Component
public class PipelineJwtAuthenticationFilter extends OncePerRequestFilter {

    private final PipelineJwtService jwtService;
    private final AppUserRepository appUserRepository;

    public PipelineJwtAuthenticationFilter(PipelineJwtService jwtService, AppUserRepository appUserRepository) {
        this.jwtService = jwtService;
        this.appUserRepository = appUserRepository;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String header = request.getHeader("Authorization");
        if (header != null && header.startsWith("Bearer ")) {
            try {
                Claims claims = jwtService.parse(header.substring(7));
                String userId = claims.getSubject();
                AppUser user = appUserRepository.findById(userId).orElse(null);
                if (user != null && "Y".equalsIgnoreCase(user.getUseYn())
                        && SecurityContextHolder.getContext().getAuthentication() == null) {
                    var auth = new UsernamePasswordAuthenticationToken(user, null, List.of());
                    SecurityContextHolder.getContext().setAuthentication(auth);
                }
            } catch (Exception ex) {
                logger.warn("JWT 검증 실패: " + ex.getMessage());
            }
        }
        filterChain.doFilter(request, response);
    }
}
