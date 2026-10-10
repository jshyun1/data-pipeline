package com.company.pipeline.llm.context;

import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobLink;
import com.company.pipeline.jobcatalog.EtlJobLinkRepository;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobRun;
import com.company.pipeline.jobcatalog.EtlJobRunRepository;
import com.company.pipeline.jobcatalog.EtlJobStep;
import com.company.pipeline.jobcatalog.EtlJobStepRepository;
import com.company.pipeline.nifi.NifiProcessGroupMetadata;
import com.company.pipeline.nifi.NifiProcessGroupMetadataRepository;
import com.company.pipeline.nifi.NifiProcessGroupTreeService;
import com.company.pipeline.nifi.dto.NifiProcessGroupTreeResponse;
import com.company.pipeline.workflow.EtlWorkflow;
import com.company.pipeline.workflow.EtlWorkflowNode;
import com.company.pipeline.workflow.EtlWorkflowNodeRepository;
import com.company.pipeline.workflow.EtlWorkflowRepository;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * ETL 관리 화면의 AI 어시스턴트 컨텍스트 수집기.
 *
 * <p>JOB이 선택되면 해당 JOB만 좁게 수집한다. 일반 그룹이면 질문 의도를 규칙 기반으로 분류하고,
 * 선택 그룹 하위 트리 요약과 질문 관련 JOB 상세만 선별해 붙인다.
 *
 * <p>2026-10-10: 화면이 워크플로우 속성창과 같은 대화 창(탭을 열면 GPT 가 요약)으로 바뀌었다. 예전 화면 고정 보고서에 있던 값
 * (소스 · 타깃 · 처리 흐름 · 스케줄 · 최근 5일 실행 · 소요 시간)은 {@link EtlJobFacts} 로 여기서 계산해 붙인다 - 요약 답에 같은 정보가 나오게.
 * 질문은 화면이 target 에 넣지 않고 요청의 질문을 받는다({@link #collect(Map, String, String, String)}).
 */
@Component
public class EtlAssistantContextCollector implements LlmContextCollector {

    private static final int MAX_DESCENDANT_LINES = 80;
    private static final int MAX_RELEVANT_JOBS = 5;
    private static final int MAX_DETAIL_STEPS_PER_JOB = 20;

    private final EtlJobRepository jobRepository;
    private final EtlJobStepRepository stepRepository;
    private final EtlJobLinkRepository linkRepository;
    private final EtlJobRunRepository runRepository;
    private final NifiProcessGroupMetadataRepository metadataRepository;
    private final NifiProcessGroupTreeService treeService;
    private final EtlWorkflowRepository workflowRepository;
    private final EtlWorkflowNodeRepository workflowNodeRepository;
    private final EtlJobContextText jobText;

    public EtlAssistantContextCollector(EtlJobRepository jobRepository, EtlJobStepRepository stepRepository,
                                        EtlJobLinkRepository linkRepository, EtlJobRunRepository runRepository,
                                        NifiProcessGroupMetadataRepository metadataRepository,
                                        NifiProcessGroupTreeService treeService,
                                        EtlWorkflowRepository workflowRepository,
                                        EtlWorkflowNodeRepository workflowNodeRepository,
                                        EtlJobContextText jobText) {
        this.jobRepository = jobRepository;
        this.stepRepository = stepRepository;
        this.linkRepository = linkRepository;
        this.runRepository = runRepository;
        this.metadataRepository = metadataRepository;
        this.treeService = treeService;
        this.workflowRepository = workflowRepository;
        this.workflowNodeRepository = workflowNodeRepository;
        this.jobText = jobText;
    }

    @Override
    public LlmTargetType targetType() {
        return LlmTargetType.ETL_ASSISTANT;
    }

    @Override
    @Transactional(readOnly = true)
    public CollectedTarget collect(Map<String, String> target, String errorText) {
        return collectFor(target, errorText, value(target, "question"));
    }

    /** 속성창 대화: 이번 질문으로 그룹 질문 의도를 가른다(예전 화면은 target 의 question 으로 보냈다 - 그것도 받는다). */
    @Override
    @Transactional(readOnly = true)
    public CollectedTarget collect(Map<String, String> target, String errorText, String draft, String question) {
        return collectFor(target, errorText, question != null && !question.isBlank() ? question : value(target, "question"));
    }

    private CollectedTarget collectFor(Map<String, String> target, String errorText, String question) {
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
            collectJobContext(context, currentJob.get(), target);
        } else {
            collectGroupContext(context, selectedId, selectedName, currentPath, question);
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

    private void collectJobContext(LlmContext context, EtlJob job, Map<String, String> target) {
        List<EtlJobStep> steps = stepRepository.findByJobIdAndDeletedAtIsNullOrderByStepNameAsc(job.getId());
        List<EtlJobLink> links = linkRepository.findByJobIdAndDeletedAtIsNull(job.getId());
        List<EtlJobRun> runs = runRepository.findTop20ByJobIdOrderByStartedAtDesc(job.getId());
        Optional<NifiProcessGroupMetadata> metadata = metadataRepository.findById(job.getNifiPgId());
        // 스케줄은 서버가 게시된 워크플로우에서 찾는다. 못 찾으면 화면이 보낸 값(예전 호환)을 쓴다.
        String schedule = workflowScheduleText(collectWorkflowSchedules(Set.of(job.getId())).get(job.getId()));
        if ("없음".equals(schedule) && value(target, "dagSchedule") != null) {
            schedule = value(target, "dagSchedule");
        }
        context.add("현재 JOB 기본 정보", jobOverview(job, steps, metadata.orElse(null), schedule, value(target, "createdBy")),
                LlmContext.PRIORITY_DETAIL);
        context.add("현재 JOB 흐름 · 최근 실행 요약", jobFacts(job, steps, links, runs), LlmContext.PRIORITY_DETAIL);
        context.add("현재 JOB 단계", jobText.steps(job), LlmContext.PRIORITY_DETAIL);
        context.add("현재 JOB 최근 실행", jobText.recentRuns(job, null), LlmContext.PRIORITY_DETAIL);
    }

    private void collectGroupContext(LlmContext context, String selectedId, String selectedName,
                                     String currentPath, String question) {
        Optional<NifiProcessGroupMetadata> metadata = selectedId == null || selectedId.isBlank()
                ? Optional.empty()
                : metadataRepository.findById(selectedId);
        context.add("현재 프로세스 그룹 기본 정보",
                groupOverview(selectedId, selectedName, currentPath, metadata.orElse(null)),
                LlmContext.PRIORITY_DETAIL);

        GroupIntent intent = classifyIntent(question);
        context.add("그룹 질문 해석", intentText(intent), LlmContext.PRIORITY_DETAIL);

        GroupScope scope = collectDescendantSummary(selectedId, intent);
        context.add("선택 그룹 하위 요약", scope.summaryText(), LlmContext.PRIORITY_DETAIL);
        if (!scope.relevantJobs().isEmpty()) {
            context.add("질문 관련 하위 JOB 상세", collectRelevantDetails(scope.relevantJobs(), intent),
                    LlmContext.PRIORITY_DETAIL);
        }
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
        lines.add("스케줄: " + ("없음".equals(dagSchedule) || dagSchedule == null
                ? "없음 - ETL job 은 자체 스케줄이 없고, 워크플로우에 넣어 게시해야 실행 일정이 생긴다"
                : dagSchedule));
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

    /** 예전 화면 보고서(구성 · 지금 상태)에 있던 값 - {@link EtlJobFacts} 와 같은 규칙. */
    private static String jobFacts(EtlJob job, List<EtlJobStep> steps, List<EtlJobLink> links, List<EtlJobRun> runs) {
        List<String> lines = new ArrayList<>();
        lines.add("소스: " + EtlJobFacts.source(steps, links));
        lines.add("타깃(적재 테이블): " + EtlJobFacts.target(steps, links));
        lines.add("처리 흐름(캔버스 위치 순): " + EtlJobFacts.flow(steps));
        lines.add("연결된 Airflow DAG: " + ContextText.orDash(job.getAirflowDagId()));
        lines.add("최근 실행: " + EtlJobFacts.recentRuns(runs, LocalDate.now()));
        lines.add("최근 실행 소요 시간: " + EtlJobFacts.latestDuration(runs));
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

    private GroupScope collectDescendantSummary(String selectedId, GroupIntent intent) {
        NifiProcessGroupTreeResponse root;
        try {
            root = treeService.getTree();
        } catch (RuntimeException ex) {
            return new GroupScope("(NiFi 그룹 트리 조회 실패: " + ContextText.cut(ex.getMessage(), 300) + ")",
                    List.of());
        }

        NifiProcessGroupTreeResponse selected = findTreeNode(root, selectedId).orElse(root);
        List<NifiProcessGroupTreeResponse> descendants = new ArrayList<>();
        collectDescendants(selected, descendants);
        List<NifiProcessGroupTreeResponse> jobNodes = descendants.stream()
                .filter(node -> "JOB".equalsIgnoreCase(node.groupType()))
                .toList();
        List<String> jobGroupIds = jobNodes.stream().map(NifiProcessGroupTreeResponse::id).toList();
        Map<String, EtlJob> jobByGroupId = jobGroupIds.isEmpty()
                ? Map.of()
                : jobRepository.findByNifiPgIdIn(jobGroupIds).stream()
                        .filter(job -> job.getDeletedAt() == null)
                        .collect(Collectors.toMap(EtlJob::getNifiPgId, Function.identity(), (a, b) -> a));
        Map<Long, List<WorkflowSchedule>> schedulesByJobId = collectWorkflowSchedules(jobByGroupId.values().stream()
                .map(EtlJob::getId).collect(Collectors.toSet()));

        List<EtlJob> relevantJobs = selectRelevantNodes(jobNodes, jobByGroupId, schedulesByJobId, intent);

        int running = 0;
        int stopped = 0;
        int failed = 0;
        int waiting = 0;
        int unknown = 0;
        for (NifiProcessGroupTreeResponse node : jobNodes) {
            switch (ContextText.orDash(node.jobStatus()).toUpperCase(Locale.ROOT)) {
                case "RUNNING" -> running++;
                case "STOPPED" -> stopped++;
                case "FAILED" -> failed++;
                case "WAITING" -> waiting++;
                default -> unknown++;
            }
        }

        List<String> lines = new ArrayList<>();
        lines.add("수집 범위: 현재 선택 그룹 `" + ContextText.orDash(selected.name()) + "` 하위 전체");
        lines.add("하위 그룹 수: " + descendants.stream()
                .filter(node -> !"JOB".equalsIgnoreCase(node.groupType())).count());
        lines.add("하위 JOB 수: " + jobNodes.size());
        lines.add("JOB 상태: 실행 " + running + " / 대기 " + waiting + " / 중지 " + stopped
                + " / 실패 " + failed + (unknown > 0 ? " / 미확인 " + unknown : ""));
        lines.add("관련 상세 후보: " + relevantJobs.size() + "개"
                + (relevantJobs.size() >= MAX_RELEVANT_JOBS ? " (상위 " + MAX_RELEVANT_JOBS + "개 제한)" : ""));

        if (jobNodes.isEmpty()) {
            lines.add("하위 JOB 목록: (없음)");
        } else {
            lines.add("하위 JOB/그룹 목록(최대 " + MAX_DESCENDANT_LINES + "개):");
            jobNodes.stream()
                    .sorted(Comparator.comparing(NifiProcessGroupTreeResponse::name, String.CASE_INSENSITIVE_ORDER))
                    .limit(MAX_DESCENDANT_LINES)
                    .forEach(node -> {
                        EtlJob job = jobByGroupId.get(node.id());
                        String schedule = job == null ? "없음" : workflowScheduleText(schedulesByJobId.get(job.getId()));
                        lines.add("- " + node.name()
                                + " | 상태 " + jobStatusText(node.jobStatus())
                                + " | Processor " + node.processorCount()
                                + " | 스케줄 " + schedule
                                + " | ID " + node.id());
                    });
            if (jobNodes.size() > MAX_DESCENDANT_LINES) {
                lines.add("... 외 " + (jobNodes.size() - MAX_DESCENDANT_LINES) + "개 생략");
            }
        }
        return new GroupScope(String.join("\n", lines), relevantJobs);
    }

    private String collectRelevantDetails(List<EtlJob> jobs, GroupIntent intent) {
        if (jobs.isEmpty()) {
            return "(질문과 직접 관련된 하위 JOB을 찾지 못했다)";
        }
        Map<Long, List<WorkflowSchedule>> schedulesByJobId = collectWorkflowSchedules(
                jobs.stream().map(EtlJob::getId).collect(Collectors.toSet()));
        List<String> blocks = new ArrayList<>();
        for (EtlJob job : jobs.stream().limit(MAX_RELEVANT_JOBS).toList()) {
            List<EtlJobStep> steps = stepRepository.findByJobIdAndDeletedAtIsNullOrderByStepNameAsc(job.getId());
            Optional<NifiProcessGroupMetadata> metadata = metadataRepository.findById(job.getNifiPgId());
            List<String> lines = new ArrayList<>();
            lines.add("### " + job.getJobName());
            lines.add("JOB ID: " + job.getId() + " / NiFi 그룹 ID: " + job.getNifiPgId());
            lines.add("상태 카운트: 실행 " + job.getRunningCount()
                    + " / 중지 " + job.getStoppedCount()
                    + " / 검증실패 " + job.getInvalidCount());
            List<EtlJobLink> links = linkRepository.findByJobIdAndDeletedAtIsNull(job.getId());
            lines.add("소스 → 타깃: " + EtlJobFacts.source(steps, links) + " → " + EtlJobFacts.target(steps, links));
            lines.add("Processor 수: " + job.getStepCount() + " / 연결된 Airflow DAG: " + ContextText.orDash(job.getAirflowDagId()));
            lines.add("적재 유형: " + loadModeText(steps));
            lines.add("스케줄: " + workflowScheduleText(schedulesByJobId.get(job.getId())));
            lines.add("생성일자: " + ContextText.time(job.getFirstSeenAt()));
            lines.add("작성자: " + ContextText.orDash(metadata.map(NifiProcessGroupMetadata::getCreatedBy).orElse(null)));
            lines.add("최근 실행: " + inline(jobText.recentRuns(job, null), 1_200));
            if (intent.kind() == IntentKind.ERROR || intent.kind() == IntentKind.CONFIG || intent.kind() == IntentKind.GENERAL) {
                lines.add("단계 요약:");
                steps.stream().limit(MAX_DETAIL_STEPS_PER_JOB).forEach(step -> lines.add("- "
                        + step.getStepName()
                        + " | " + ContextText.orDash(step.getStepType())
                        + " | 문유형 " + ContextText.orDash(step.getStatementType())
                        + " | 상태 " + ContextText.orDash(step.getRunStatus())
                        + " | 대상 " + ContextText.orDash(step.getTargetTable())));
                if (steps.size() > MAX_DETAIL_STEPS_PER_JOB) {
                    lines.add("- ... 외 " + (steps.size() - MAX_DETAIL_STEPS_PER_JOB) + "개 단계 생략");
                }
            }
            blocks.add(String.join("\n", lines));
        }
        return String.join("\n\n", blocks);
    }

    private List<EtlJob> selectRelevantNodes(List<NifiProcessGroupTreeResponse> jobNodes, Map<String, EtlJob> jobByGroupId,
                                             Map<Long, List<WorkflowSchedule>> schedulesByJobId, GroupIntent intent) {
        Set<String> keywords = intent.keywords();
        return jobNodes.stream()
                .map(node -> jobByGroupId.get(node.id()))
                .filter(Objects::nonNull)
                .sorted(Comparator
                        .comparingInt((EtlJob job) -> relevantScore(job, schedulesByJobId.get(job.getId()), intent, keywords))
                        .reversed()
                        .thenComparing(EtlJob::getJobName, String.CASE_INSENSITIVE_ORDER))
                .filter(job -> relevantScore(job, schedulesByJobId.get(job.getId()), intent, keywords) > 0
                        || intent.kind() == IntentKind.STRUCTURE
                        || intent.kind() == IntentKind.STATUS
                        || intent.kind() == IntentKind.SCHEDULE
                        || intent.kind() == IntentKind.GENERAL)
                .limit(MAX_RELEVANT_JOBS)
                .toList();
    }

    private int relevantScore(EtlJob job, List<WorkflowSchedule> schedules, GroupIntent intent, Set<String> keywords) {
        int score = 0;
        if (matchesKeyword(job.getJobName(), keywords) || matchesKeyword(job.getComments(), keywords)) {
            score += 40;
        }
        if (intent.kind() == IntentKind.ERROR && job.getInvalidCount() > 0) {
            score += 80;
        }
        if (intent.kind() == IntentKind.STATUS) {
            if (job.getRunningCount() > 0 || job.getStoppedCount() > 0 || job.getInvalidCount() > 0) {
                score += 50;
            }
        }
        if (intent.kind() == IntentKind.SCHEDULE && schedules != null && !schedules.isEmpty()) {
            score += 50;
        }
        if (intent.kind() == IntentKind.OWNER) {
            score += 20;
        }
        if (intent.kind() == IntentKind.CONFIG && job.getStepCount() > 0) {
            score += 20;
        }
        if (intent.kind() == IntentKind.GENERAL) {
            score += job.getInvalidCount() > 0 ? 40 : job.getRunningCount() > 0 ? 30 : 10;
        }
        return score;
    }

    private Map<Long, List<WorkflowSchedule>> collectWorkflowSchedules(Set<Long> jobIds) {
        if (jobIds == null || jobIds.isEmpty()) {
            return Map.of();
        }
        Map<Long, List<WorkflowSchedule>> out = jobIds.stream().collect(Collectors.toMap(
                Function.identity(), ignored -> new ArrayList<WorkflowSchedule>(), (a, b) -> a));
        List<EtlWorkflow> workflows = workflowRepository.findByDeletedAtIsNullAndPublishedAtIsNotNull();
        for (EtlWorkflow workflow : workflows) {
            List<EtlWorkflowNode> nodes = workflowNodeRepository.findByWorkflowIdAndDeletedAtIsNull(workflow.getId());
            for (EtlWorkflowNode node : nodes) {
                if (!EtlWorkflowNode.TYPE_JOB.equals(node.getNodeType()) || node.getJobId() == null
                        || !jobIds.contains(node.getJobId())) {
                    continue;
                }
                out.computeIfAbsent(node.getJobId(), ignored -> new ArrayList<>())
                        .add(new WorkflowSchedule(workflow.getName(), workflow.dagId(), workflow.getScheduleCron()));
            }
        }
        return out;
    }

    private Optional<NifiProcessGroupTreeResponse> findTreeNode(NifiProcessGroupTreeResponse node, String selectedId) {
        if (node == null) {
            return Optional.empty();
        }
        if (selectedId == null || selectedId.isBlank() || "root".equalsIgnoreCase(selectedId)) {
            return Optional.of(node);
        }
        if (selectedId.equals(node.id())) {
            return Optional.of(node);
        }
        if (node.children() == null) {
            return Optional.empty();
        }
        for (NifiProcessGroupTreeResponse child : node.children()) {
            Optional<NifiProcessGroupTreeResponse> found = findTreeNode(child, selectedId);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    private void collectDescendants(NifiProcessGroupTreeResponse node, List<NifiProcessGroupTreeResponse> out) {
        if (node == null || node.children() == null) {
            return;
        }
        for (NifiProcessGroupTreeResponse child : node.children()) {
            out.add(child);
            collectDescendants(child, out);
        }
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

    private GroupIntent classifyIntent(String question) {
        String text = question == null ? "" : question.toLowerCase(Locale.ROOT);
        IntentKind kind;
        if (containsAny(text, "오류", "에러", "실패", "안됨", "안돼", "문제", "원인", "조치", "경고", "invalid")) {
            kind = IntentKind.ERROR;
        } else if (containsAny(text, "스케줄", "schedule", "cron", "주기", "언제", "예약", "수동")) {
            kind = IntentKind.SCHEDULE;
        } else if (containsAny(text, "상태", "실행", "중지", "대기", "성공", "running", "stopped", "waiting")) {
            kind = IntentKind.STATUS;
        } else if (containsAny(text, "구조", "목록", "하위", "트리", "구성", "전체", "job", "그룹")) {
            kind = IntentKind.STRUCTURE;
        } else if (containsAny(text, "작성자", "생성자", "누가", "생성일", "수정자", "수정일")) {
            kind = IntentKind.OWNER;
        } else if (containsAny(text, "processor", "프로세서", "sql", "테이블", "설정", "적재", "초기", "증분", "upsert")) {
            kind = IntentKind.CONFIG;
        } else {
            kind = IntentKind.GENERAL;
        }
        return new GroupIntent(kind, keywords(question));
    }

    private static Set<String> keywords(String question) {
        if (question == null || question.isBlank()) {
            return Set.of();
        }
        Set<String> excluded = Set.of(
                "어떤", "무엇", "뭐", "좀", "알려줘", "보여줘", "확인", "해줘", "현재", "하위",
                "그룹", "상태", "오류", "에러", "실패", "스케줄", "작성자", "생성자", "job", "JOB");
        return java.util.Arrays.stream(question.split("[^0-9A-Za-z가-힣_\\-]+"))
                .map(String::trim)
                .filter(token -> token.length() >= 2)
                .filter(token -> !excluded.contains(token))
                .map(token -> token.toLowerCase(Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static boolean containsAny(String text, String... needles) {
        for (String needle : needles) {
            if (text.contains(needle.toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesKeyword(String value, Set<String> keywords) {
        if (value == null || value.isBlank() || keywords == null || keywords.isEmpty()) {
            return false;
        }
        String normalized = value.toLowerCase(Locale.ROOT);
        return keywords.stream().anyMatch(normalized::contains);
    }

    private static String intentText(GroupIntent intent) {
        return "질문 의도: " + switch (intent.kind()) {
            case ERROR -> "오류/실패 원인";
            case SCHEDULE -> "스케줄";
            case STATUS -> "상태";
            case STRUCTURE -> "하위 구조";
            case OWNER -> "작성자/생성·수정 이력";
            case CONFIG -> "프로세서/적재 설정";
            case GENERAL -> "일반 질의";
        } + "\n키워드: " + (intent.keywords().isEmpty() ? "(없음)" : String.join(", ", intent.keywords()));
    }

    private static String workflowScheduleText(List<WorkflowSchedule> schedules) {
        if (schedules == null || schedules.isEmpty()) {
            return "없음";
        }
        return schedules.stream()
                .map(schedule -> schedule.workflowName() + " / "
                        + (schedule.cron() == null || schedule.cron().isBlank() ? "수동실행" : schedule.cron()))
                .distinct()
                .collect(Collectors.joining(", "));
    }

    private static String jobStatusText(String status) {
        if (status == null) {
            return "대기";
        }
        return switch (status.toUpperCase(Locale.ROOT)) {
            case "STOPPED" -> "중지";
            case "RUNNING" -> "실행";
            case "FAILED" -> "실패";
            default -> "대기";
        };
    }

    private static String inline(String text, int maxChars) {
        if (text == null || text.isBlank()) {
            return "-";
        }
        return ContextText.cut(text.replace("\n", " / "), maxChars);
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
                JOB이 선택된 경우 반드시 현재 선택된 JOB 컨텍스트만 근거로 답한다.
                GROUP이 선택된 경우 현재 선택된 그룹과 그 하위 그룹/JOB 요약 및 선별 상세 컨텍스트 안에서만 답한다.
                선택 범위 밖의 다른 JOB, 다른 그룹, 전체 현황을 추측하거나 섞어서 설명하지 않는다.
                GROUP 답변에서는 확인한 하위 범위와 관련 JOB명을 명시한다.
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

    private enum IntentKind {
        STATUS,
        ERROR,
        SCHEDULE,
        STRUCTURE,
        OWNER,
        CONFIG,
        GENERAL
    }

    private record GroupIntent(IntentKind kind, Set<String> keywords) {
    }

    private record GroupScope(String summaryText, List<EtlJob> relevantJobs) {
    }

    private record WorkflowSchedule(String workflowName, String dagId, String cron) {
    }
}
