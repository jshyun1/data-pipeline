package com.company.pipeline.llm.context;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * ETL 로그 오류 분석 · ETL 관리 속성창 질문의 정보 수집(대상 {@code ETL_JOB}).
 *
 * <p>화면은 jobId 또는 groupId 와 화면에 보이는 오류 문구만 보낸다. 서버는 job 정보, 단계 SQL, 최근 실행을 붙인다.
 * 연결 계정, 비밀번호, API 키 같은 접속 정보는 읽지 않는다.
 */
@Component
public class EtlJobContextCollector implements LlmContextCollector {

    private final EtlJobRepository jobRepository;
    private final EtlJobContextText jobText;

    public EtlJobContextCollector(EtlJobRepository jobRepository, EtlJobContextText jobText) {
        this.jobRepository = jobRepository;
        this.jobText = jobText;
    }

    @Override
    public LlmTargetType targetType() {
        return LlmTargetType.ETL_JOB;
    }

    @Override
    @Transactional(readOnly = true)
    public CollectedTarget collect(Map<String, String> target, String errorText) {
        String ref = targetRef(target);
        LlmContext context = new LlmContext();
        String error = etlErrorText(errorText);
        context.add("화면에 표시된 오류 내용", error, LlmContext.PRIORITY_ERROR);

        // 실행 로그는 job 이 지워진 뒤에도 남는다(ETL 로그 화면). 지워진 job 도 해석할 수 있어야 한다(2026-10-06 - 지워진 job 이면
        // «찾을 수 없음»으로 막혀 실패 로그 해석이 HTTP 500 으로 끝났다). job 기록마저 없으면 오류 내용만으로 해석한다.
        Optional<EtlJob> found = findJob(target);
        if (found.isEmpty()) {
            if (error == null) {
                throw new BusinessException(ErrorCode.LLM_TARGET_NOT_FOUND, "ETL Job을 찾을 수 없습니다: " + ref);
            }
            context.add("ETL Job", "이 실행 기록의 job 정보가 남아 있지 않습니다(" + ref + "). 화면의 오류 내용만으로 분석하십시오.",
                    LlmContext.PRIORITY_IDENTITY);
            return new CollectedTarget(ref, ref, context);
        }

        EtlJob job = found.get();
        context.add("ETL Job", identity(job), LlmContext.PRIORITY_IDENTITY);
        context.add("Job 단계", jobText.steps(job), LlmContext.PRIORITY_DETAIL);
        context.add("최근 실행 (최신순)", jobText.recentRuns(job, null), LlmContext.PRIORITY_HISTORY);
        return new CollectedTarget(String.valueOf(job.getId()), job.getJobName(), context);
    }

    /** 지워진 job 도 찾는다 - 표시는 {@link #identity} 가 «삭제됨»으로 붙인다. */
    private Optional<EtlJob> findJob(Map<String, String> target) {
        Long jobId = optionalLong(target, "jobId");
        if (jobId != null) {
            return jobRepository.findById(jobId);
        }
        String groupId = target == null ? null : target.get("groupId");
        if (groupId != null && !groupId.isBlank()) {
            return jobRepository.findByNifiPgId(groupId.trim());
        }
        throw new BusinessException(ErrorCode.VALIDATION_ERROR, "jobId 또는 groupId가 필요합니다.");
    }

    private static String targetRef(Map<String, String> target) {
        if (target == null) {
            return "-";
        }
        String jobId = target.get("jobId");
        if (jobId != null && !jobId.isBlank()) {
            return "jobId " + jobId.trim();
        }
        String groupId = target.get("groupId");
        return groupId == null || groupId.isBlank() ? "-" : "groupId " + groupId.trim();
    }

    private static String identity(EtlJob job) {
        List<String> lines = new ArrayList<>();
        lines.add("이름: " + job.getJobName());
        if (job.getDeletedAt() != null) {
            lines.add("상태: 삭제된 job(" + job.getDeletedAt() + " 삭제) - NiFi 에는 이 그룹이 더 이상 없을 수 있습니다");
        }
        lines.add("ETL 그룹 ID: " + ContextText.orDash(job.getNifiPgId()));
        lines.add("상위 그룹 ID: " + ContextText.orDash(job.getParentPgId()));
        lines.add("Airflow DAG: " + ContextText.orDash(job.getAirflowDagId()));
        lines.add("단계 수: " + job.getStepCount());
        lines.add("상태 카운트: 실행 " + job.getRunningCount()
                + " · 중지 " + job.getStoppedCount()
                + " · 검증실패 " + job.getInvalidCount());
        if (job.getComments() != null && !job.getComments().isBlank()) {
            lines.add("설명: " + ContextText.cut(job.getComments(), 500));
        }
        return String.join("\n", lines);
    }

    private static String etlErrorText(String errorText) {
        String base = ContextText.errorText(errorText);
        if (base == null || base.isBlank()) {
            return null;
        }
        return "ETL 처리 중 발생한 오류 내용입니다.\n"
                + "원인, 조치 방법, 확인할 것 형식으로 분석할 때 이 오류 원문을 우선 근거로 삼으십시오.\n\n"
                + base;
    }

    private static Long optionalLong(Map<String, String> target, String key) {
        String value = target == null ? null : target.get(key);
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, key + " 형식이 올바르지 않습니다: " + value);
        }
    }
}
