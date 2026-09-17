package com.company.pipeline.llm.context;

import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRun;
import com.company.pipeline.jobcatalog.EtlJobRunRepository;
import com.company.pipeline.jobcatalog.EtlJobStep;
import com.company.pipeline.jobcatalog.EtlJobStepRepository;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/**
 * ETL job 의 단계(SQL · 대상 테이블)와 최근 실행을 글로 만든다. 워크플로우 task · 워크플로우 노드 수집기가 같이 쓰고,
 * ETL 쪽 수집기(ETL_JOB)도 그대로 쓸 수 있다.
 *
 * <p>연결정보(DBCP 서비스의 접속 주소 · 계정)는 읽지 않는다. 단계에 적힌 SQL · 테이블 이름까지만.
 */
@Component
public class EtlJobContextText {

    static final int RECENT_RUNS = 5;
    private static final int MAX_SQL_CHARS = 4_000;

    private final EtlJobStepRepository stepRepository;
    private final EtlJobRunRepository runRepository;

    public EtlJobContextText(EtlJobStepRepository stepRepository, EtlJobRunRepository runRepository) {
        this.stepRepository = stepRepository;
        this.runRepository = runRepository;
    }

    public String steps(EtlJob job) {
        List<EtlJobStep> steps = stepRepository.findByJobIdAndDeletedAtIsNullOrderByStepNameAsc(job.getId());
        if (steps.isEmpty()) {
            return "(등록된 단계 정보가 없다)";
        }
        StringBuilder out = new StringBuilder();
        for (EtlJobStep step : steps) {
            if (!out.isEmpty()) {
                out.append("\n\n");
            }
            out.append("### ").append(step.getStepName()).append(" (").append(ContextText.orDash(step.getStepType())).append(")");
            if (step.getTargetTable() != null) {
                out.append("\n대상 테이블: ").append(step.getTargetTable());
            }
            if (step.getStatementType() != null) {
                out.append("\n문 유형: ").append(step.getStatementType());
            }
            if (step.getUpdateKeys() != null) {
                out.append("\nUPSERT 키: ").append(step.getUpdateKeys());
            }
            if (step.getRunStatus() != null) {
                out.append("\nNiFi 상태: ").append(step.getRunStatus());
            }
            if (step.getSqlText() != null && !step.getSqlText().isBlank()) {
                out.append("\nSQL:\n").append(ContextText.cut(step.getSqlText(), MAX_SQL_CHARS));
            }
        }
        return out.toString();
    }

    /**
     * 최근 실행(최신순).
     *
     * @param nodeKey 워크플로우 노드로 부른 실행만 보려면 노드 키, 전체면 null
     */
    public String recentRuns(EtlJob job, String nodeKey) {
        List<EtlJobRun> runs = runRepository.findTop20ByJobIdOrderByStartedAtDesc(job.getId()).stream()
                .filter(run -> nodeKey == null || Objects.equals(nodeKey, run.getNodeKey()))
                .limit(RECENT_RUNS)
                .toList();
        if (runs.isEmpty()) {
            return "(실행 기록 없음)";
        }
        return runs.stream().map(run -> {
            String line = ContextText.time(run.getStartedAt()) + " | " + ContextText.orDash(run.getStatus())
                    + " | 적재 " + run.getTotalInserted() + "건 | 실패 단계 " + run.getFailedStepCount() + "개";
            return run.getErrorMessage() == null ? line : line + " | 오류: " + ContextText.cut(run.getErrorMessage(), 300);
        }).collect(Collectors.joining("\n"));
    }
}
