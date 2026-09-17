package com.company.pipeline.llm;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobRun;
import com.company.pipeline.jobcatalog.EtlJobRunRepository;
import com.company.pipeline.jobcatalog.EtlJobStep;
import com.company.pipeline.jobcatalog.EtlJobStepRepository;
import java.util.List;
import java.util.regex.Pattern;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(readOnly = true)
public class LlmContextService {

    private static final Pattern EMAIL = Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}");
    private static final Pattern PHONE = Pattern.compile("(?<!\\d)(?:01[016789][- ]?\\d{3,4}[- ]?\\d{4}|\\d{2,3}[- ]\\d{3,4}[- ]\\d{4})(?!\\d)");
    private static final Pattern KOREAN_RESIDENT_NO = Pattern.compile("(?<!\\d)\\d{6}[- ]?[1-8]\\d{6}(?!\\d)");
    private static final Pattern CARD_NO = Pattern.compile("(?<!\\d)(?:\\d{4}[- ]?){3}\\d{4}(?!\\d)");
    private static final Pattern SECRET_ASSIGNMENT = Pattern.compile("(?i)\\b(password|passwd|pwd|api[_-]?key|access[_-]?token|refresh[_-]?token|secret)\\b\\s*[:=]\\s*['\\\"]?[^'\\\"\\s,;)}]+");

    private final EtlJobRepository etlJobRepository;
    private final EtlJobStepRepository etlJobStepRepository;
    private final EtlJobRunRepository etlJobRunRepository;

    public LlmContextService(EtlJobRepository etlJobRepository, EtlJobStepRepository etlJobStepRepository,
                             EtlJobRunRepository etlJobRunRepository) {
        this.etlJobRepository = etlJobRepository;
        this.etlJobStepRepository = etlJobStepRepository;
        this.etlJobRunRepository = etlJobRunRepository;
    }

    public LlmContext collect(LlmTargetType targetType, String targetId, LlmProviderConfig config) {
        if (targetType == LlmTargetType.GENERAL) {
            return new LlmContext("일반 질문", """
                    대상: 일반 AI 어시스턴트 질문
                    현재 선택된 ETL Job 컨텍스트는 없습니다.
                    사용자가 NiFi/ETL 운영 전반에 대해 묻는 경우, 제공된 대화와 질문만 근거로 답하십시오.
                    특정 Job/Processor/실행 이력 값이 필요한 질문이면, 대상을 선택해야 정확히 확인할 수 있다고 안내하십시오.
                    """, config.isMaskSensitive());
        }
        if (targetType != LlmTargetType.ETL_JOB) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "아직 ETL Job 대상만 AI 어시스턴트를 지원합니다.");
        }
        Long jobId;
        try {
            jobId = Long.valueOf(targetId);
        } catch (NumberFormatException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "ETL Job 대상 ID가 올바르지 않습니다.");
        }
        EtlJob job = etlJobRepository.findById(jobId)
                .orElseThrow(() -> new BusinessException(ErrorCode.VALIDATION_ERROR, "ETL Job을 찾을 수 없습니다."));
        List<EtlJobStep> steps = etlJobStepRepository.findByJobIdAndDeletedAtIsNullOrderByStepNameAsc(jobId);
        List<EtlJobRun> runs = etlJobRunRepository.findTop20ByJobIdOrderByStartedAtDesc(jobId);

        String raw = stripSecrets(buildEtlJobContext(job, steps, runs));
        String text = config.isMaskSensitive() ? mask(raw) : raw;
        return new LlmContext(job.getJobName(), limit(text, config.getMaxInputChars()), config.isMaskSensitive());
    }

    private static String buildEtlJobContext(EtlJob job, List<EtlJobStep> steps, List<EtlJobRun> runs) {
        StringBuilder out = new StringBuilder();
        out.append("대상: ETL Job\n");
        out.append("Job ID: ").append(job.getId()).append('\n');
        out.append("이름: ").append(value(job.getJobName())).append('\n');
        out.append("NiFi 프로세스 그룹 ID: ").append(value(job.getNifiPgId())).append('\n');
        out.append("상위 그룹 ID: ").append(value(job.getParentPgId())).append('\n');
        out.append("Airflow DAG: ").append(value(job.getAirflowDagId())).append('\n');
        out.append("상태 카운트: 실행 ").append(job.getRunningCount())
                .append(", 중지 ").append(job.getStoppedCount())
                .append(", 검증실패 ").append(job.getInvalidCount()).append('\n');
        out.append("설명: ").append(value(job.getComments())).append("\n\n");

        out.append("[단계]\n");
        if (steps.isEmpty()) {
            out.append("- 단계 정보 없음\n");
        } else {
            for (EtlJobStep step : steps) {
                out.append("- ").append(value(step.getStepName()))
                        .append(" / ").append(value(step.getStepType())).append('\n');
                out.append("  실행상태: ").append(value(step.getRunStatus()))
                        .append(", 검증: ").append(value(step.getValidationStatus())).append('\n');
                out.append("  스케줄: ").append(value(step.getSchedulingStrategy()))
                        .append(" / ").append(value(step.getSchedulingPeriod())).append('\n');
                if (hasText(step.getStatementType()) || hasText(step.getTargetTable()) || hasText(step.getUpdateKeys())) {
                    out.append("  SQL 유형: ").append(value(step.getStatementType()))
                            .append(", 타깃 테이블: ").append(value(step.getTargetTable()))
                            .append(", UPSERT 키: ").append(value(step.getUpdateKeys())).append('\n');
                }
                if (hasText(step.getSqlText())) {
                    out.append("  SQL:\n").append(indent(step.getSqlText())).append('\n');
                }
            }
        }

        out.append("\n[최근 실행]\n");
        if (runs.isEmpty()) {
            out.append("- 실행 이력 없음\n");
        } else {
            for (EtlJobRun run : runs.stream().limit(8).toList()) {
                out.append("- ").append(run.getStatus())
                        .append(" / 시작: ").append(value(run.getStartedAt()))
                        .append(" / 종료: ").append(value(run.getEndedAt()))
                        .append(" / 적재: ").append(run.getRowsProcessed() != null ? run.getRowsProcessed() : run.getTotalInserted())
                        .append(" / 실패단계: ").append(run.getFailedStepCount()).append('\n');
                if (hasText(run.getErrorMessage())) {
                    out.append("  오류: ").append(run.getErrorMessage()).append('\n');
                }
            }
        }
        return out.toString();
    }

    private static String mask(String value) {
        String out = EMAIL.matcher(value).replaceAll("<이메일>");
        out = PHONE.matcher(out).replaceAll("<전화번호>");
        out = KOREAN_RESIDENT_NO.matcher(out).replaceAll("<주민번호>");
        out = CARD_NO.matcher(out).replaceAll("<카드번호>");
        return out;
    }

    private static String stripSecrets(String value) {
        return SECRET_ASSIGNMENT.matcher(value).replaceAll("$1=<비밀값 제외>");
    }

    private static String limit(String value, int maxInputChars) {
        if (maxInputChars <= 0 || value.length() <= maxInputChars) {
            return value;
        }
        return value.substring(0, maxInputChars) + "\n\n[입력 길이 제한으로 이후 정보는 생략됨]";
    }

    private static String indent(String value) {
        return "    " + value.strip().replace("\n", "\n    ");
    }

    private static String value(Object value) {
        return value == null ? "-" : String.valueOf(value);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
