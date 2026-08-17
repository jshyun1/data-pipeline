package com.company.pipeline.common.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ErrorHandler;

/**
 * 관제/배치 스케줄러 전용 에러 핸들러 (설계서 3-2, U1).
 *
 * <p>ThreadPoolTaskScheduler에 에러 핸들러를 주지 않으면 주기 태스크에서 던져진 예외가
 * 조용히 억제되기만 하고(로그는 남지만) 관제 루프의 이상이 무증상으로 흐른다. 알림 평가/발송
 * 루프에서는 "예외가 났다"가 곧 "알림이 안 나갔다"이므로, 명시적으로 error 레벨로 표면화한다.
 * 예외를 다시 던지지 않으므로(로그만) 주기 태스크의 다음 실행은 계속된다.
 */
public class AlertSchedulerErrorHandler implements ErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(AlertSchedulerErrorHandler.class);

    @Override
    public void handleError(Throwable t) {
        log.error("스케줄 태스크에서 처리되지 않은 예외 - 관제/배치 루프 이상. 다음 실행은 계속된다.", t);
    }
}
