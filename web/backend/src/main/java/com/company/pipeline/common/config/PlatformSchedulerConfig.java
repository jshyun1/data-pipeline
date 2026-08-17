package com.company.pipeline.common.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * 스케줄러 스레드 풀 격리 (설계서 3-2, U1).
 *
 * <p>목적: "알림을 만들었는데 장애 시에만 정확히 안 온다"는 최악의 실패 모드를 구조적으로 막는다.
 * 상시 수집(collect-)·관제(ctrl-)·배치(batch-)·워치독(watchdog-)을 서로 다른 풀로 나눠, 한 영역이
 * 스레드를 오래 점유해도 다른 영역이 굶지 않게 한다. 특히 새벽 베이스라인 학습/보존 배치가 풀을
 * 점유하는 동안에도 수집·평가·발송이 자기 스레드를 갖는다.
 *
 * <p>배선 규칙: scheduler를 지정하지 않은 기존 @Scheduled 8개는 spring-context의 이름 폴백
 * (ScheduledAnnotationBeanPostProcessor.DEFAULT_TASK_SCHEDULER_BEAN_NAME = "taskScheduler")으로
 * 아래 taskScheduler(=collect-)에 붙는다. TaskScheduler 빈이 2개 이상이면 타입 해석이 모호해져
 * 이름 폴백이 발동하는 성질을 이용한 것이다. 신규 관제/배치 주기 작업은
 * @Scheduled(scheduler="controlPlaneScheduler" 또는 "batchScheduler")로 명시한다.
 */
@Configuration
public class PlatformSchedulerConfig {

    /**
     * 수집용. 이름을 반드시 "taskScheduler"로 둔다. 이 빈을 직접 선언하면 Spring Boot의
     * TaskSchedulingAutoConfiguration이 물러나므로 poolSize를 여기서 지정한다
     * (이 상태에서는 spring.task.scheduling.pool.size가 바인딩되지 않는다).
     * scheduler를 지정하지 않은 기존 @Scheduled가 이름 폴백으로 여기에 붙는다.
     */
    @Bean("taskScheduler")
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler s = new ThreadPoolTaskScheduler();
        s.setPoolSize(4);
        s.setThreadNamePrefix("collect-");
        s.setWaitForTasksToCompleteOnShutdown(true);
        s.setAwaitTerminationSeconds(20);
        return s;
    }

    /** 관제용. 신호수집 1 + 평가 1 + 발송 1. */
    @Bean("controlPlaneScheduler")
    public ThreadPoolTaskScheduler controlPlaneScheduler() {
        ThreadPoolTaskScheduler s = new ThreadPoolTaskScheduler();
        s.setPoolSize(3);
        s.setThreadNamePrefix("ctrl-");
        s.setErrorHandler(new AlertSchedulerErrorHandler());  // 조용히 죽는 것 금지
        return s;
    }

    /** 배치·외부호출용. 베이스라인/보존/정합성/요약. 상시 작업과 절대 섞지 않는다. */
    @Bean("batchScheduler")
    public ThreadPoolTaskScheduler batchScheduler() {
        ThreadPoolTaskScheduler s = new ThreadPoolTaskScheduler();
        s.setPoolSize(2);
        s.setThreadNamePrefix("batch-");
        s.setErrorHandler(new AlertSchedulerErrorHandler());
        return s;
    }

    /** 워치독 전용 단일 스레드. 보존정리가 수백만 행을 돌 때도 자기감시는 살아 있어야 한다. */
    @Bean("watchdogScheduler")
    public ThreadPoolTaskScheduler watchdogScheduler() {
        ThreadPoolTaskScheduler s = new ThreadPoolTaskScheduler();
        s.setPoolSize(1);
        s.setThreadNamePrefix("watchdog-");
        s.setErrorHandler(new AlertSchedulerErrorHandler());
        return s;
    }
}
