package com.company.pipeline.user.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.AppUser;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/** 등록자 · 요청자로 남길 «지금 요청의 주체»(2026-10-13). */
class RequestActorResolverTest {

    private final RequestActorResolver resolver = new RequestActorResolver("svc-secret");

    @AfterEach
    void clear() {
        SecurityContextHolder.clearContext();
        RequestContextHolder.resetRequestAttributes();
    }

    private static MockHttpServletRequest request(String method) {
        MockHttpServletRequest request = new MockHttpServletRequest(method, "/api/pipelines/7/start");
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
        return request;
    }

    private static void login(String userId) {
        AppUser user = mock(AppUser.class);
        when(user.getUserId()).thenReturn(userId);
        SecurityContextHolder.getContext().setAuthentication(new UsernamePasswordAuthenticationToken(user, null, List.of()));
    }

    @Test
    void 로그인_사용자면_사용자_ID() {
        request("POST");
        login("kim");

        assertThat(resolver.current()).contains("kim");
        assertThat(resolver.currentForChange()).contains("kim");
        assertThat(resolver.requireForRegistration("CDC 파이프라인")).isEqualTo("kim");
    }

    @Test
    void 서비스_토큰이_맞으면_airflow_와_DAG_이름() {
        MockHttpServletRequest request = request("POST");
        request.addHeader("X-Service-Token", "svc-secret");
        assertThat(resolver.current()).contains("airflow");

        request.addHeader("X-Requested-By", "etl_wf_dz_cdc");
        assertThat(resolver.current()).contains("airflow:etl_wf_dz_cdc");
    }

    @Test
    void 서비스_토큰이_틀리면_밝힌_이름도_믿지_않는다() {
        MockHttpServletRequest request = request("POST");
        request.addHeader("X-Service-Token", "wrong");
        request.addHeader("X-Requested-By", "admin");

        assertThat(resolver.current()).isEmpty();
        assertThatThrownBy(() -> resolver.requireForRegistration("워크플로우"))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("워크플로우 등록자를 알 수 없습니다")
                .extracting(ex -> ((BusinessException) ex).getErrorCode()).isEqualTo(ErrorCode.UNAUTHORIZED);
    }

    @Test
    void 조회_요청이나_요청_밖이면_변경_주체가_없다() {
        request("GET");
        login("kim");
        assertThat(resolver.currentForChange()).isEmpty();

        RequestContextHolder.resetRequestAttributes();   // 스케줄러 스레드
        assertThat(resolver.currentForChange()).isEmpty();
    }

    @Test
    void 서비스_토큰을_안_쓰는_환경이면_서비스_호출로_보지_않는다() {
        RequestActorResolver noToken = new RequestActorResolver("");
        MockHttpServletRequest request = request("POST");
        request.addHeader("X-Service-Token", "");

        assertThat(noToken.current()).isEmpty();
    }
}
