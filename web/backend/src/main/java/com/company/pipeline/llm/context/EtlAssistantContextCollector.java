package com.company.pipeline.llm.context;

import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobStep;
import com.company.pipeline.jobcatalog.EtlJobStepRepository;
import com.company.pipeline.nifi.NifiProcessGroupMetadata;
import com.company.pipeline.nifi.NifiProcessGroupMetadataRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * ETL 관리 화면의 AI 어시스턴트 컨텍스트 수집기.
 *
 * <p>현재 선택된 프로세스 그룹을 컨텍스트로 보낸다. 선택된 그룹이 JOB이면 JOB 상세까지 붙이고,
 * 일반 그룹이면 그룹 메타데이터만 붙여 운영자가 보고 있는 캔버스와 AI 답변의 기준을 맞춘다.
 */
@Component
public class EtlAssistantContextCollector implements LlmContextCollector {

    private final EtlJobRepository jobRepository;
    private final EtlJobStepRepository stepRepository;
    private final NifiProcessGroupMetadataRepository metadataRepository;
    private final EtlJobContextText jobText;

    public EtlAssistantContextCollector(EtlJobRepository jobRepository, EtlJobStepRepository stepRepository,
                                        NifiProcessGroupMetadataRepository metadataRepository,
                                        EtlJobContextText jobText) {
        this.jobRepository = jobRepository;
        this.stepRepository = stepRepository;
        this.metadataRepository = metadataRepository;
        this.jobText = jobText;
    }

    @Override
    public LlmTargetType targetType() {
        return LlmTargetType.ETL_ASSISTANT;
    }

    @Override
    @Transactional(readOnly = true)
    public CollectedTarget collect(Map<String, String> target, String errorText) {
        String selectedType = value(target, "selectedType");
        String selectedId = value(target, "selectedId");
        String selectedName = value(target, "selectedName");
        String currentPath = value(target, "currentPath");
        Optional<EtlJob> currentJob = resolveCurrentJob(target, selectedId);

        LlmContext context = new LlmContext();
        context.add("어시스턴트 역할", roleText(), LlmContext.PRIORITY_IDENTITY);
        context.add("현재 화면 선택", selectionText(selectedType, selectedId, selectedName, currentPath),
                LlmContext.PRIORITY_IDENTITY);
        if (currentJob.isPresent()) {
            EtlJob job = currentJob.get();
            List<EtlJobStep> steps = stepRepository.findByJobIdAndDeletedAtIsNullOrderByStepNameAsc(job.getId());
            Optional<NifiProcessGroupMetadata> metadata = metadataRepository.findById(job.getNifiPgId());
            context.add("현재 JOB 기본 정보", jobOverview(
                            job, steps, metadata.orElse(null), value(target, "dagSchedule"), value(target, "createdBy")),
                    LlmContext.PRIORITY_DETAIL);
            context.add("현재 JOB 단계", jobText.steps(job), LlmContext.PRIORITY_DETAIL);
            context.add("현재 JOB 최근 실행", jobText.recentRuns(job, null), LlmContext.PRIORITY_DETAIL);
        } else {
            Optional<NifiProcessGroupMetadata> metadata = selectedId == null || selectedId.isBlank()
                    ? Optional.empty()
                    : metadataRepository.findById(selectedId);
            context.add("현재 프로세스 그룹 기본 정보",
                    groupOverview(selectedId, selectedName, currentPath, metadata.orElse(null)),
                    LlmContext.PRIORITY_DETAIL);
        }
        context.add("화면에 표시된 오류 내용", ContextText.errorText(errorText), LlmContext.PRIORITY_ERROR);

        String label = currentJob.map(job -> "ETL Job · " + job.getJobName())
                .orElse(selectedName != null && !selectedName.isBlank()
                        ? "ETL Process Group · " + selectedName
                        : "ETL Process Group");
        String id = currentJob.map(job -> String.valueOf(job.getId()))
                .orElse(selectedId == null || selectedId.isBlank() ? "ETL_ASSISTANT" : selectedId);
        return new CollectedTarget(id, label, context);
    }

    private Optional<EtlJob> resolveCurrentJob(Map<String, String> target, String selectedId) {
        String jobId = value(target, "jobId");
        if (jobId != null) {
            try {
                Optional<EtlJob> job = jobRepository.findById(Long.parseLong(jobId));
                if (job.isPresent() && job.get().getDeletedAt() == null) {
                    return job;
                }
            } catch (NumberFormatException ignored) {
                // selectedId/jobGroupId fallback below.
            }
        }
        String jobGroupId = value(target, "jobGroupId");
        String groupId = jobGroupId != null ? jobGroupId : selectedId;
        if (groupId == null || groupId.isBlank() || "root".equalsIgnoreCase(groupId)) {
            return Optional.empty();
        }
        return jobRepository.findByNifiPgId(groupId).filter(job -> job.getDeletedAt() == null);
    }

    private String jobOverview(EtlJob job, List<EtlJobStep> steps, NifiProcessGroupMetadata metadata,
                               String dagSchedule, String createdBy) {
        List<String> lines = new ArrayList<>();
        lines.add("JOB명: " + job.getJobName());
        lines.add("JOB ID: " + job.getId());
        lines.add("부모 그룹 ID: " + ContextText.orDash(job.getParentPgId()));
        lines.add("적재 유형: " + loadModeText(steps));
        lines.add("스케줄: " + ContextText.orDash(dagSchedule));
        lines.add("Processor 수: " + job.getStepCount());
        lines.add("상태 카운트: 실행 " + job.getRunningCount()
                + " / 중지 " + job.getStoppedCount()
                + " / 검증실패 " + job.getInvalidCount());
        lines.add("생성일자: " + ContextText.time(job.getFirstSeenAt()));
        lines.add("작성자: " + ContextText.orDash(createdBy != null ? createdBy : metadata == null ? null : metadata.getCreatedBy()));
        lines.add("마지막 동기화: " + ContextText.time(job.getLastSyncedAt()));
        if (metadata != null) {
            lines.add("생성자/생성일: " + metadata.getCreatedBy() + " / " + ContextText.time(metadata.getCreatedAt()));
            lines.add("수정자/수정일: " + metadata.getUpdatedBy() + " / " + ContextText.time(metadata.getUpdatedAt()));
        }
        if (job.getComments() != null && !job.getComments().isBlank()) {
            lines.add("설명: " + ContextText.cut(job.getComments(), 500));
        }
        return String.join("\n", lines);
    }

    private String groupOverview(String selectedId, String selectedName, String currentPath,
                                 NifiProcessGroupMetadata metadata) {
        List<String> lines = new ArrayList<>();
        lines.add("프로세스 그룹명: " + ContextText.orDash(
                metadata == null ? selectedName : metadata.getProcessGroupName()));
        lines.add("프로세스 그룹 ID: " + ContextText.orDash(
                metadata == null ? selectedId : metadata.getProcessGroupId()));
        lines.add("부모 그룹 ID: " + ContextText.orDash(metadata == null ? null : metadata.getParentGroupId()));
        lines.add("화면 경로: " + ContextText.orDash(currentPath));
        if (metadata != null) {
            lines.add("생성자/생성일: " + metadata.getCreatedBy() + " / " + ContextText.time(metadata.getCreatedAt()));
            lines.add("수정자/수정일: " + metadata.getUpdatedBy() + " / " + ContextText.time(metadata.getUpdatedAt()));
            if (metadata.getComments() != null && !metadata.getComments().isBlank()) {
                lines.add("설명: " + ContextText.cut(metadata.getComments(), 500));
            }
        }
        return String.join("\n", lines);
    }

    private static String loadModeText(List<EtlJobStep> steps) {
        List<String> modes = steps.stream()
                .map(EtlAssistantContextCollector::loadMode)
                .filter(mode -> !mode.isBlank())
                .distinct()
                .toList();
        return modes.isEmpty() ? "-" : String.join(", ", modes);
    }

    private static String loadMode(EtlJobStep step) {
        String text = (step.getStepName() + " " + step.getStepType() + " "
                + ContextText.orDash(step.getStatementType()) + " "
                + ContextText.orDash(step.getSqlText())).toLowerCase(Locale.ROOT);
        if (text.contains("truncate") || text.contains("initial")) {
            return "초기적재";
        }
        if (text.contains("merge") || text.contains("upsert") || text.contains("update")
                || text.contains("increment")) {
            return "증분적재";
        }
        if (text.contains("insert")) {
            return "일반적재";
        }
        return "";
    }

    private static String scheduleText(List<EtlJobStep> steps) {
        List<String> schedules = steps.stream()
                .map(step -> {
                    String strategy = ContextText.orDash(step.getSchedulingStrategy());
                    String period = ContextText.orDash(step.getSchedulingPeriod());
                    return Objects.equals(strategy, "-") && Objects.equals(period, "-") ? "" : strategy + " / " + period;
                })
                .filter(schedule -> !schedule.isBlank())
                .distinct()
                .collect(Collectors.toList());
        return schedules.isEmpty() ? "-" : String.join(", ", schedules);
    }

    private static String roleText() {
        return """
                너는 Cerebro ETL 운영 어시스턴트다.
                반드시 현재 선택된 프로세스 그룹 또는 JOB 컨텍스트만 근거로 답한다.
                다른 JOB, 다른 그룹, 전체 현황을 추측하거나 섞어서 설명하지 않는다.
                컨텍스트에 없는 실시간 사실은 추측하지 말고 확인이 필요하다고 말한다.
                DB 비밀번호, API 키, 인증 토큰, 불필요한 접속 계정 정보는 답변하지 않는다.
                """;
    }

    private static String selectionText(String selectedType, String selectedId, String selectedName, String currentPath) {
        List<String> lines = new ArrayList<>();
        lines.add("선택 유형: " + ContextText.orDash(selectedType));
        lines.add("선택 이름: " + ContextText.orDash(selectedName));
        lines.add("선택 ID: " + ContextText.orDash(selectedId));
        lines.add("화면 경로: " + ContextText.orDash(currentPath));
        return String.join("\n", lines);
    }

    private static String value(Map<String, String> target, String key) {
        String value = target == null ? null : target.get(key);
        return value == null || value.isBlank() ? null : value.trim();
    }
}
