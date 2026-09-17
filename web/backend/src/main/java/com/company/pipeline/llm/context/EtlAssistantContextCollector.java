package com.company.pipeline.llm.context;

import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobStep;
import com.company.pipeline.jobcatalog.EtlJobStepRepository;
import com.company.pipeline.nifi.NifiProcessGroupTreeService;
import com.company.pipeline.nifi.dto.NifiProcessGroupTreeResponse;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * ETL 관리 화면의 범용 AI 어시스턴트 컨텍스트 수집기.
 *
 * <p>Root/그룹/Job/Processor 어느 곳에서 질문해도 깨지지 않도록 단일 Job 조회에 묶이지 않는다.
 * 전체 현황은 요약으로, 선택 대상과 질문 키워드에 걸린 Job만 상세로 붙인다.
 */
@Component
public class EtlAssistantContextCollector implements LlmContextCollector {

    private static final int MAX_TREE_LINES = 80;
    private static final int MAX_JOB_SUMMARY = 80;
    private static final int MAX_RELATED_JOBS = 6;
    private static final int MAX_SELECTED_CHILDREN = 24;

    private final EtlJobRepository jobRepository;
    private final EtlJobStepRepository stepRepository;
    private final EtlJobContextText jobText;
    private final NifiProcessGroupTreeService treeService;

    public EtlAssistantContextCollector(EtlJobRepository jobRepository, EtlJobStepRepository stepRepository,
                                        EtlJobContextText jobText, NifiProcessGroupTreeService treeService) {
        this.jobRepository = jobRepository;
        this.stepRepository = stepRepository;
        this.jobText = jobText;
        this.treeService = treeService;
    }

    @Override
    public LlmTargetType targetType() {
        return LlmTargetType.ETL_ASSISTANT;
    }

    @Override
    @Transactional(readOnly = true)
    public CollectedTarget collect(Map<String, String> target, String errorText) {
        String question = value(target, "question");
        String selectedType = value(target, "selectedType");
        String selectedId = value(target, "selectedId");
        String selectedName = value(target, "selectedName");
        String currentPath = value(target, "currentPath");

        List<EtlJob> jobs = jobRepository.findByDeletedAtIsNullOrderByJobNameAsc();
        NifiProcessGroupTreeResponse tree = safeTree();
        Optional<NifiProcessGroupTreeResponse> selectedNode = findNode(tree, selectedId);
        Optional<EtlJob> selectedJob = resolveSelectedJob(selectedType, selectedId, jobs);
        Optional<EtlJobStep> selectedStep = resolveSelectedStep(selectedType, selectedId);

        List<EtlJob> relatedJobs = relatedJobs(question, selectedId, selectedName, selectedNode.orElse(null),
                selectedJob.orElse(null), selectedStep.orElse(null), jobs);

        LlmContext context = new LlmContext();
        context.add("어시스턴트 역할", roleText(), LlmContext.PRIORITY_IDENTITY);
        context.add("현재 화면 선택", selectionText(selectedType, selectedId, selectedName, currentPath,
                selectedNode.orElse(null), selectedJob.orElse(null), selectedStep.orElse(null)), LlmContext.PRIORITY_IDENTITY);
        context.add("ETL 전체 현황", overallText(tree, jobs), LlmContext.PRIORITY_DETAIL);
        context.add("ETL 그룹 트리 요약", treeText(tree), LlmContext.PRIORITY_DETAIL);
        context.add("관련 Job 후보", relatedJobSummary(relatedJobs), LlmContext.PRIORITY_DETAIL);

        selectedNode.ifPresent(node -> context.add("선택 그룹 주변", selectedGroupText(node, jobs),
                LlmContext.PRIORITY_DETAIL));
        selectedStep.ifPresent(step -> context.add("선택 Processor", stepText(step, selectedJob.orElse(null)),
                LlmContext.PRIORITY_DETAIL));
        for (EtlJob job : relatedJobs.stream().limit(3).toList()) {
            context.add("Job 상세: " + job.getJobName(), jobDetail(job), LlmContext.PRIORITY_DETAIL);
        }
        context.add("화면에 표시된 오류 내용", ContextText.errorText(errorText), LlmContext.PRIORITY_ERROR);

        String label = selectedName != null && !selectedName.isBlank()
                ? "ETL 어시스턴트 · " + selectedName
                : "ETL 어시스턴트";
        return new CollectedTarget(selectedId == null || selectedId.isBlank() ? "ETL_ASSISTANT" : selectedId, label, context);
    }

    private static String roleText() {
        return """
                너는 Cerebro ETL 운영 어시스턴트다.
                일반 질문에는 ETL 운영 도우미로서 자연스럽게 답하고, 구성·상태·오류 질문에는 아래 컨텍스트를 근거로 답한다.
                컨텍스트에 없는 실시간 사실은 추측하지 말고 확인이 필요하다고 말한다.
                DB 비밀번호, API 키, 인증 토큰, 불필요한 접속 계정 정보는 답변하지 않는다.
                """;
    }

    private static String selectionText(String selectedType, String selectedId, String selectedName, String currentPath,
                                        NifiProcessGroupTreeResponse node, EtlJob job, EtlJobStep step) {
        List<String> lines = new ArrayList<>();
        lines.add("선택 유형: " + ContextText.orDash(selectedType));
        lines.add("선택 이름: " + ContextText.orDash(selectedName));
        lines.add("선택 ID: " + ContextText.orDash(selectedId));
        lines.add("화면 경로: " + ContextText.orDash(currentPath));
        if (node != null) {
            lines.add("그룹 유형: " + node.groupType());
            lines.add("그룹 상태: " + node.jobStatus());
            lines.add("하위 Job 수: " + node.processorCount());
            lines.add("실행/중지/검증실패: " + node.runningCount() + "/" + node.stoppedCount() + "/" + node.invalidCount());
            lines.add("대기 FlowFile: " + node.flowFilesQueued());
        }
        if (job != null) {
            lines.add("매핑 Job: " + job.getJobName() + " (#" + job.getId() + ")");
        }
        if (step != null) {
            lines.add("선택 Processor: " + step.getStepName() + " (" + step.getStepType() + ")");
        }
        return String.join("\n", lines);
    }

    private static String overallText(NifiProcessGroupTreeResponse tree, List<EtlJob> jobs) {
        long running = jobs.stream().filter(job -> job.getRunningCount() > 0).count();
        long invalid = jobs.stream().filter(job -> job.getInvalidCount() > 0).count();
        long stopped = jobs.stream().filter(job -> job.getStoppedCount() > 0).count();
        return "전체 Job 수: " + jobs.size()
                + "\n실행 중 Job: " + running
                + "\n중지 포함 Job: " + stopped
                + "\n검증 실패 포함 Job: " + invalid
                + (tree == null ? "" : "\nRoot 상태: " + tree.jobStatus()
                + "\nRoot 대기 FlowFile: " + tree.flowFilesQueued()
                + "\nRoot 활성 스레드: " + tree.activeThreadCount());
    }

    private static String treeText(NifiProcessGroupTreeResponse tree) {
        if (tree == null) {
            return "(그룹 트리 정보를 수집하지 못했다)";
        }
        List<String> lines = new ArrayList<>();
        appendTree(lines, tree, 0);
        if (lines.size() > MAX_TREE_LINES) {
            return String.join("\n", lines.subList(0, MAX_TREE_LINES))
                    + "\n…(그룹 " + (lines.size() - MAX_TREE_LINES) + "개 생략)";
        }
        return String.join("\n", lines);
    }

    private static void appendTree(List<String> lines, NifiProcessGroupTreeResponse node, int depth) {
        String indent = "  ".repeat(Math.min(depth, 8));
        lines.add(indent + "- " + node.name() + " [" + node.groupType() + ", " + node.jobStatus()
                + ", jobs=" + node.processorCount() + ", invalid=" + node.invalidCount()
                + ", queued=" + node.flowFilesQueued() + "]");
        for (NifiProcessGroupTreeResponse child : node.children()) {
            appendTree(lines, child, depth + 1);
        }
    }

    private static String selectedGroupText(NifiProcessGroupTreeResponse node, List<EtlJob> jobs) {
        Set<String> descendantIds = new HashSet<>();
        collectIds(node, descendantIds);
        List<String> lines = new ArrayList<>();
        lines.add("그룹명: " + node.name());
        lines.add("그룹 유형/상태: " + node.groupType() + " / " + node.jobStatus());
        lines.add("하위 Job 수: " + node.processorCount());
        lines.add("직접/하위 그룹:");
        node.children().stream().limit(MAX_SELECTED_CHILDREN).forEach(child -> lines.add("- " + child.name()
                + " [" + child.groupType() + ", " + child.jobStatus() + ", jobs=" + child.processorCount() + "]"));
        List<EtlJob> childJobs = jobs.stream()
                .filter(job -> descendantIds.contains(job.getNifiPgId()))
                .limit(MAX_RELATED_JOBS)
                .toList();
        if (!childJobs.isEmpty()) {
            lines.add("관련 Job:");
            childJobs.forEach(job -> lines.add(jobSummaryLine(job)));
        }
        return String.join("\n", lines);
    }

    private static String relatedJobSummary(List<EtlJob> jobs) {
        if (jobs.isEmpty()) {
            return "(질문/선택과 직접 매칭된 Job이 없다. 전체 현황과 그룹 트리 요약을 기준으로 답한다)";
        }
        return jobs.stream().limit(MAX_JOB_SUMMARY).map(EtlAssistantContextCollector::jobSummaryLine)
                .collect(Collectors.joining("\n"));
    }

    private static String jobSummaryLine(EtlJob job) {
        return "- " + job.getJobName() + " (#" + job.getId() + ", ETL group=" + job.getNifiPgId()
                + ", parent=" + ContextText.orDash(job.getParentPgId())
                + ", steps=" + job.getStepCount()
                + ", running=" + job.getRunningCount()
                + ", stopped=" + job.getStoppedCount()
                + ", invalid=" + job.getInvalidCount()
                + ", dag=" + ContextText.orDash(job.getAirflowDagId()) + ")";
    }

    private String stepText(EtlJobStep step, EtlJob job) {
        List<String> lines = new ArrayList<>();
        lines.add("이름: " + step.getStepName());
        lines.add("유형: " + step.getStepType());
        lines.add("Processor ID: " + step.getNifiProcessorId());
        lines.add("소속 Job: " + (job == null ? ContextText.orDash(step.getJobId()) : job.getJobName()));
        lines.add("스케줄: " + ContextText.orDash(step.getSchedulingStrategy()) + " / " + ContextText.orDash(step.getSchedulingPeriod()));
        lines.add("상태: 실행 " + ContextText.orDash(step.getRunStatus()) + " · 검증 " + ContextText.orDash(step.getValidationStatus()));
        lines.add("타깃 테이블: " + ContextText.orDash(step.getTargetTable()));
        lines.add("문 유형: " + ContextText.orDash(step.getStatementType()));
        if (step.getSqlText() != null && !step.getSqlText().isBlank()) {
            lines.add("SQL:\n" + ContextText.cut(step.getSqlText(), 2_000));
        }
        return String.join("\n", lines);
    }

    private String jobDetail(EtlJob job) {
        return identity(job)
                + "\n\n[단계]\n" + jobText.steps(job)
                + "\n\n[최근 실행]\n" + jobText.recentRuns(job, null);
    }

    private static String identity(EtlJob job) {
        return jobSummaryLine(job)
                + (job.getComments() == null || job.getComments().isBlank()
                ? "" : "\n설명: " + ContextText.cut(job.getComments(), 500));
    }

    private List<EtlJob> relatedJobs(String question, String selectedId, String selectedName,
                                     NifiProcessGroupTreeResponse selectedNode, EtlJob selectedJob,
                                     EtlJobStep selectedStep, List<EtlJob> jobs) {
        LinkedHashSet<EtlJob> related = new LinkedHashSet<>();
        if (selectedJob != null) {
            related.add(selectedJob);
        }
        if (selectedStep != null) {
            jobs.stream().filter(job -> Objects.equals(job.getId(), selectedStep.getJobId())).findFirst().ifPresent(related::add);
        }
        Set<String> selectedGroupIds = new HashSet<>();
        if (selectedNode != null) {
            collectIds(selectedNode, selectedGroupIds);
        } else if (selectedId != null && !selectedId.isBlank()) {
            selectedGroupIds.add(selectedId);
        }
        jobs.stream()
                .filter(job -> selectedGroupIds.contains(job.getNifiPgId()) || selectedGroupIds.contains(job.getParentPgId()))
                .sorted(Comparator.comparingInt(EtlJob::getInvalidCount).reversed().thenComparing(EtlJob::getJobName))
                .limit(MAX_RELATED_JOBS)
                .forEach(related::add);

        List<String> tokens = questionTokens(question + " " + ContextText.orDash(selectedName));
        if (!tokens.isEmpty()) {
            jobs.stream()
                    .filter(job -> matches(job, tokens))
                    .limit(MAX_RELATED_JOBS)
                    .forEach(related::add);
            List<EtlJobStep> matchedSteps = stepRepository.findAll().stream()
                    .filter(step -> step.getDeletedAt() == null && stepMatches(step, tokens))
                    .limit(MAX_RELATED_JOBS)
                    .toList();
            Set<Long> stepJobIds = matchedSteps.stream().map(EtlJobStep::getJobId).collect(Collectors.toSet());
            jobs.stream().filter(job -> stepJobIds.contains(job.getId())).forEach(related::add);
        }

        if (related.isEmpty()) {
            jobs.stream()
                    .sorted(Comparator.comparingInt(EtlJob::getInvalidCount).reversed()
                            .thenComparing(Comparator.comparingInt(EtlJob::getRunningCount).reversed())
                            .thenComparing(EtlJob::getJobName))
                    .limit(MAX_RELATED_JOBS)
                    .forEach(related::add);
        }
        return related.stream().limit(MAX_RELATED_JOBS).toList();
    }

    private static boolean matches(EtlJob job, List<String> tokens) {
        String haystack = (job.getJobName() + " " + job.getNifiPgId() + " " + ContextText.orDash(job.getComments()))
                .toLowerCase(Locale.ROOT);
        return tokens.stream().anyMatch(haystack::contains);
    }

    private static boolean stepMatches(EtlJobStep step, List<String> tokens) {
        String haystack = (step.getStepName() + " " + step.getStepType() + " " + step.getNifiProcessorId() + " "
                + ContextText.orDash(step.getTargetTable()) + " " + ContextText.orDash(step.getStatementType()))
                .toLowerCase(Locale.ROOT);
        return tokens.stream().anyMatch(haystack::contains);
    }

    private static List<String> questionTokens(String text) {
        if (text == null || text.isBlank()) {
            return List.of();
        }
        return java.util.Arrays.stream(text.toLowerCase(Locale.ROOT).split("[^0-9a-zA-Z가-힣_\\-]+"))
                .map(String::trim)
                .filter(token -> token.length() >= 2)
                .filter(token -> !Set.of("etl", "job", "root", "그룹", "정보", "설명", "어디", "무엇", "누구", "안녕").contains(token))
                .distinct()
                .limit(12)
                .toList();
    }

    private Optional<EtlJob> resolveSelectedJob(String selectedType, String selectedId, List<EtlJob> jobs) {
        if (selectedId == null || selectedId.isBlank()) {
            return Optional.empty();
        }
        if ("JOB".equalsIgnoreCase(selectedType)) {
            return jobs.stream().filter(job -> Objects.equals(job.getNifiPgId(), selectedId)).findFirst();
        }
        return jobs.stream().filter(job -> Objects.equals(job.getNifiPgId(), selectedId)).findFirst();
    }

    private Optional<EtlJobStep> resolveSelectedStep(String selectedType, String selectedId) {
        if (selectedId == null || selectedId.isBlank() || !"PROCESSOR".equalsIgnoreCase(selectedType)) {
            return Optional.empty();
        }
        return stepRepository.findByNifiProcessorId(selectedId).filter(step -> step.getDeletedAt() == null);
    }

    private NifiProcessGroupTreeResponse safeTree() {
        try {
            return treeService.getTree();
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static Optional<NifiProcessGroupTreeResponse> findNode(NifiProcessGroupTreeResponse root, String id) {
        if (root == null || id == null || id.isBlank()) {
            return Optional.empty();
        }
        ArrayDeque<NifiProcessGroupTreeResponse> queue = new ArrayDeque<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            NifiProcessGroupTreeResponse node = queue.removeFirst();
            if (Objects.equals(node.id(), id) || ("root".equals(id) && Objects.equals(node.id(), "root"))) {
                return Optional.of(node);
            }
            queue.addAll(node.children());
        }
        return Optional.empty();
    }

    private static void collectIds(NifiProcessGroupTreeResponse node, Set<String> ids) {
        if (node == null) {
            return;
        }
        ids.add(node.id());
        node.children().forEach(child -> collectIds(child, ids));
    }

    private static String value(Map<String, String> target, String key) {
        String value = target == null ? null : target.get(key);
        return value == null || value.isBlank() ? null : value.trim();
    }
}
