package com.company.pipeline.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.user.AppUser;
import com.company.pipeline.user.security.RequestActorResolver;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/** 제어 이력 요청자(2026-10-13) - 예전에는 준비 · 시작 · 정지 · 재시작 이력이 전부 요청자 없이 남았다. */
class PipelineCommandHistoryRecorderTest {

    private final PipelineCommandHistoryRepository repository = mock(PipelineCommandHistoryRepository.class);
    private final PipelineCommandHistoryRecorder recorder =
            new PipelineCommandHistoryRecorder(repository, new RequestActorResolver("svc-secret"));

    @AfterEach
    void clear() {
        SecurityContextHolder.clearContext();
        RequestContextHolder.resetRequestAttributes();
    }

    private MockHttpServletRequest post() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/pipelines/7/start");
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
        return request;
    }

    private String savedRequestedBy() {
        ArgumentCaptor<PipelineCommandHistory> saved = ArgumentCaptor.forClass(PipelineCommandHistory.class);
        verify(repository).save(saved.capture());
        return saved.getValue().getRequestedBy();
    }

    @Test
    void 화면에서_누른_제어는_로그인_사용자가_요청자() {
        post();
        AppUser user = mock(AppUser.class);
        when(user.getUserId()).thenReturn("kim");
        SecurityContextHolder.getContext().setAuthentication(new UsernamePasswordAuthenticationToken(user, null, List.of()));

        recorder.record(7L, "START", "SUCCESS", null);

        assertThat(savedRequestedBy()).isEqualTo("kim");
    }

    @Test
    void 워크플로우가_보낸_제어는_airflow_와_DAG_이름() {
        MockHttpServletRequest request = post();
        request.addHeader("X-Service-Token", "svc-secret");
        request.addHeader("X-Requested-By", "etl_wf_dz_cdc");

        recorder.record(7L, "STOP", "SUCCESS", null);

        assertThat(savedRequestedBy()).isEqualTo("airflow:etl_wf_dz_cdc");
    }

    @Test
    void 자동_감시는_요청_안에서_남아도_요청자를_채우지_않는다() {
        post();
        AppUser user = mock(AppUser.class);
        when(user.getUserId()).thenReturn("kim");
        SecurityContextHolder.getContext().setAuthentication(new UsernamePasswordAuthenticationToken(user, null, List.of()));

        recorder.record(7L, "RUNTIME_MONITOR", "SUCCESS", null);

        assertThat(savedRequestedBy()).isNull();
    }

    @Test
    void 스케줄러처럼_요청_밖이면_비워_두고_넘겨준_요청자는_그대로() {
        recorder.record(7L, "START", "SUCCESS", null);
        assertThat(savedRequestedBy()).isNull();

        PipelineCommandHistoryRepository other = mock(PipelineCommandHistoryRepository.class);
        new PipelineCommandHistoryRecorder(other, new RequestActorResolver("svc-secret"))
                .record(7L, "SKIP_RECORD", "SUCCESS", "사유", "lee");
        ArgumentCaptor<PipelineCommandHistory> saved = ArgumentCaptor.forClass(PipelineCommandHistory.class);
        verify(other).save(saved.capture());
        assertThat(saved.getValue().getRequestedBy()).isEqualTo("lee");
    }
}
