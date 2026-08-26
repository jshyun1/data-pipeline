package com.company.pipeline.alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 규칙 평가 이력 보존정책.
 *
 * <p>발화·해소·평가실패만 남기므로 감사 로그만큼 늘지는 않지만, 규칙이 늘고 장애가 반복되면
 * 계속 쌓이기만 한다. 권한 감사 로그와 같은 방식으로 하루 한 번 정리한다
 * ({@link com.company.pipeline.authz.PermissionAuditRetentionService}).
 *
 * <p>보존 일수 0 이하면 무제한(정리하지 않음). 정리 시각은 감사 로그(03:30)와 겹치지 않게
 * 03:40 으로 둔다 - 같은 시각에 몰면 배치 스케줄러(pool 2)를 함께 점유한다.
 */
@Service
public class AlertRuleEvalLogRetentionService {

    private static final Logger log = LoggerFactory.getLogger(AlertRuleEvalLogRetentionService.class);

    private final JdbcTemplate jdbc;

    @Value("${alert.eval-log.retention-days:90}")
    private int retentionDays;

    public AlertRuleEvalLogRetentionService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Scheduled(cron = "${alert.eval-log.retention-cron:0 40 3 * * *}")
    @Transactional
    public void purgeExpired() {
        if (retentionDays <= 0) {
            return;   // 무제한 보존
        }
        int deleted = jdbc.update(
                "DELETE FROM alert_rule_eval_log WHERE occurred_at < now() - make_interval(days => ?)",
                retentionDays);
        if (deleted > 0) {
            log.info("규칙 평가 이력 {}건 정리(보존 {}일 초과)", deleted, retentionDays);
        }
    }
}
