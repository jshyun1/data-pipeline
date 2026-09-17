package com.company.pipeline.llm;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.jobcatalog.EtlJob;
import com.company.pipeline.jobcatalog.EtlJobLink;
import com.company.pipeline.jobcatalog.EtlJobLinkRepository;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobRun;
import com.company.pipeline.jobcatalog.EtlJobRunRepository;
import com.company.pipeline.jobcatalog.EtlJobStep;
import com.company.pipeline.jobcatalog.EtlJobStepRepository;
import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.NifiClientException;
import com.company.pipeline.nifi.NifiProcessGroupMetadata;
import com.company.pipeline.nifi.NifiProcessGroupMetadataRepository;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
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
    private final EtlJobLinkRepository etlJobLinkRepository;
    private final EtlJobRunRepository etlJobRunRepository;
    private final NifiProcessGroupMetadataRepository groupMetadataRepository;
    private final NifiClient nifiClient;

    public LlmContextService(EtlJobRepository etlJobRepository, EtlJobStepRepository etlJobStepRepository,
                             EtlJobLinkRepository etlJobLinkRepository, EtlJobRunRepository etlJobRunRepository,
                             NifiProcessGroupMetadataRepository groupMetadataRepository, NifiClient nifiClient) {
        this.etlJobRepository = etlJobRepository;
        this.etlJobStepRepository = etlJobStepRepository;
        this.etlJobLinkRepository = etlJobLinkRepository;
        this.etlJobRunRepository = etlJobRunRepository;
        this.groupMetadataRepository = groupMetadataRepository;
        this.nifiClient = nifiClient;
    }

    public LlmContext collect(LlmTargetType targetType, String targetId, LlmProviderConfig config) {
        if (targetType == LlmTargetType.GENERAL) {
            return new LlmContext("일반 질문", """
                    대상: Cerebro ETL/NiFi 운영 어시스턴트
                    역할: 사용자의 ETL 관리, NiFi Flow, Job/Processor, 실행 이력, 오류 원인 분석을 돕는다.
                    답변 원칙:
                    - 일반 질문에는 Cerebro ETL/NiFi 운영 어시스턴트로서 답한다.
                    - 특정 Job/Processor/그룹의 값은 서버가 제공한 컨텍스트에 있을 때만 단정한다.
                    - 컨텍스트에 없는 값은 '현재 제공된 정보로는 확인할 수 없음'이라고 말한다.
                    - DB 비밀번호, API 키, 인증 토큰, 접속 계정 정보는 답변에 포함하지 않는다.
                    """, config.isMaskSensitive());
        }
        if (targetType == LlmTargetType.NIFI_GROUP) {
            return collectNifiKnowledge(targetId, config);
        }
        if (targetType != LlmTargetType.ETL_JOB) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "아직 ETL/NiFi 그룹과 ETL Job 대상만 AI 어시스턴트를 지원합니다.");
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
        List<EtlJobLink> links = etlJobLinkRepository.findByJobIdAndDeletedAtIsNull(jobId);
        List<EtlJobRun> runs = etlJobRunRepository.findTop20ByJobIdOrderByStartedAtDesc(jobId);

        String raw = stripSecrets(buildEtlJobContext(job, steps, links, runs));
        String text = config.isMaskSensitive() ? mask(raw) : raw;
        return new LlmContext(job.getJobName(), limit(text, config.getMaxInputChars()), config.isMaskSensitive());
    }

    private LlmContext collectNifiKnowledge(String targetId, LlmProviderConfig config) {
        try {
            List<NifiProcessGroupMetadata> groups = groupMetadataRepository.findAll();
            List<EtlJob> allJobs = etlJobRepository.findByDeletedAtIsNullOrderByJobNameAsc();
            Map<String, NifiProcessGroupMetadata> groupById = groups.stream()
                    .collect(Collectors.toMap(NifiProcessGroupMetadata::getProcessGroupId, g -> g, (a, b) -> a));
            List<EtlJob> scopedJobs = allJobs.stream()
                    .sorted(Comparator.comparing(EtlJob::getJobName, String.CASE_INSENSITIVE_ORDER))
                    .toList();
            Map<String, RuntimeStatus> runtime = runtimeStatusByProcessor();
            String raw = stripSecrets(buildNifiKnowledgeContext(targetId, groupById, groups, scopedJobs, runtime, allJobs.size()));
            String text = config.isMaskSensitive() ? mask(raw) : raw;
            String title = groupById.get(targetId) == null ? "ETL 전체" : groupById.get(targetId).getProcessGroupName();
            return new LlmContext(title, limit(text, config.getMaxInputChars()), config.isMaskSensitive());
        } catch (NifiClientException ex) {
            throw new BusinessException(ErrorCode.LLM_CHAT_ERROR, "NiFi 실시간 정보 수집 실패: " + ex.getMessage(), ex);
        } catch (RuntimeException ex) {
            throw new BusinessException(ErrorCode.LLM_CHAT_ERROR, "NiFi 그룹 컨텍스트 수집 실패: " + ex.getMessage(), ex);
        }
    }

    private static String buildEtlJobContext(EtlJob job, List<EtlJobStep> steps, List<EtlJobLink> links, List<EtlJobRun> runs) {
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

        if (!links.isEmpty()) {
            out.append("\n[연결]\n");
            for (EtlJobLink link : links) {
                out.append("- ").append(value(link.getFromName()))
                        .append(" --").append(value(link.getRelationships()))
                        .append("--> ").append(value(link.getToName())).append('\n');
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

    private String buildNifiKnowledgeContext(String targetId,
                                             Map<String, NifiProcessGroupMetadata> groupById,
                                             List<NifiProcessGroupMetadata> groups,
                                             List<EtlJob> jobs,
                                             Map<String, RuntimeStatus> runtime,
                                             int totalJobCount) {
        StringBuilder out = new StringBuilder();
        NifiProcessGroupMetadata selected = groupById.get(targetId);
        out.append("대상: ETL/NiFi 운영 지식 컨텍스트\n");
        out.append("선택 그룹: ").append(selected == null ? "ETL Root/전체" : selected.getProcessGroupName()).append('\n');
        out.append("선택 그룹 ID: ").append(value(targetId)).append('\n');
        out.append("전체 그룹 수: ").append(groups.size()).append(", 전체 Job 수: ").append(totalJobCount).append('\n');
        out.append("전달 Job 수: ").append(jobs.size()).append(" (선택 그룹은 포커스이며, 전체 Job/Processor 지식도 함께 제공)\n\n");

        out.append("[역할]\n");
        out.append("- 너는 Cerebro ETL/NiFi 운영 어시스턴트다.\n");
        out.append("- 일반 질문에는 이 정체성으로 답한다.\n");
        out.append("- ETL/NiFi 구조, 적재 방향, Processor 상태 질문은 아래 Job/Processor 정보를 근거로 답한다.\n");
        out.append("- 모르는 값은 확인 불가라고 말하고, 필요한 선택/동기화/로그 확인을 안내한다.\n\n");

        out.append("[그룹 목록 요약]\n");
        groups.stream()
                .sorted(Comparator.comparing(NifiProcessGroupMetadata::getProcessGroupName, String.CASE_INSENSITIVE_ORDER))
                .limit(80)
                .forEach(group -> out.append("- ")
                        .append(group.getProcessGroupName())
                        .append(" (id=").append(group.getProcessGroupId())
                        .append(", parent=").append(value(group.getParentGroupId())).append(")\n"));

        out.append("\n[Job/Processor 상세]\n");
        if (jobs.isEmpty()) {
            out.append("- 선택 범위에 Job이 없습니다. 하위 Job 동기화가 아직 안 되었거나 Processor가 없는 그룹일 수 있습니다.\n");
            return out.toString();
        }
        for (EtlJob job : jobs.stream().limit(40).toList()) {
            List<EtlJobStep> steps = etlJobStepRepository.findByJobIdAndDeletedAtIsNullOrderByStepNameAsc(job.getId());
            List<EtlJobLink> links = etlJobLinkRepository.findByJobIdAndDeletedAtIsNull(job.getId());
            NifiProcessGroupMetadata group = groupById.get(job.getNifiPgId());
            out.append("\n## Job: ").append(job.getJobName()).append('\n');
            out.append("- 그룹: ").append(group == null ? "-" : group.getProcessGroupName())
                    .append(" / ID: ").append(job.getNifiPgId()).append('\n');
            out.append("- Airflow DAG: ").append(value(job.getAirflowDagId())).append('\n');
            out.append("- 상태 카운트: 실행 ").append(job.getRunningCount())
                    .append(", 중지 ").append(job.getStoppedCount())
                    .append(", 검증실패 ").append(job.getInvalidCount()).append('\n');
            out.append("- Processor:\n");
            for (EtlJobStep step : steps) {
                RuntimeStatus rt = runtime.get(step.getNifiProcessorId());
                out.append("  - ").append(step.getStepName())
                        .append(" / ").append(step.getStepType())
                        .append(" / 저장상태=").append(value(step.getRunStatus()));
                if (rt != null) {
                    out.append(" / 실시간=").append(value(rt.runStatus()))
                            .append(" / activeThreads=").append(rt.activeThreads())
                            .append(" / in=").append(rt.received())
                            .append(" / out=").append(rt.sent());
                }
                out.append('\n');
                if (hasText(step.getTargetTable()) || hasText(step.getStatementType()) || hasText(step.getUpdateKeys())) {
                    out.append("    target=").append(value(step.getTargetTable()))
                            .append(", statement=").append(value(step.getStatementType()))
                            .append(", keys=").append(value(step.getUpdateKeys())).append('\n');
                }
                if (hasText(step.getSqlText())) {
                    out.append("    SQL:\n").append(indent(step.getSqlText())).append('\n');
                }
            }
            if (!links.isEmpty()) {
                out.append("- 연결:\n");
                for (EtlJobLink link : links) {
                    out.append("  - ").append(value(link.getFromName()))
                            .append(" --").append(value(link.getRelationships()))
                            .append("--> ").append(value(link.getToName())).append('\n');
                }
            }
        }
        return out.toString();
    }

    private static Set<String> scopedGroupIds(String targetId, List<NifiProcessGroupMetadata> groups) {
        if (targetId == null || targetId.isBlank() || "root".equalsIgnoreCase(targetId)) {
            return Set.of();
        }
        Map<String, List<NifiProcessGroupMetadata>> childrenByParent = groups.stream()
                .filter(group -> group.getParentGroupId() != null)
                .collect(Collectors.groupingBy(NifiProcessGroupMetadata::getParentGroupId));
        Set<String> result = new HashSet<>();
        collectChildren(targetId, childrenByParent, result);
        result.add(targetId);
        return result;
    }

    private static void collectChildren(String groupId, Map<String, List<NifiProcessGroupMetadata>> childrenByParent, Set<String> out) {
        for (NifiProcessGroupMetadata child : childrenByParent.getOrDefault(groupId, List.of())) {
            if (out.add(child.getProcessGroupId())) {
                collectChildren(child.getProcessGroupId(), childrenByParent, out);
            }
        }
    }

    private Map<String, RuntimeStatus> runtimeStatusByProcessor() {
        Map<String, RuntimeStatus> result = new HashMap<>();
        NifiFlowStatusResponse response = nifiClient.getRootFlowStatus();
        if (response == null || response.processGroupStatus() == null
                || response.processGroupStatus().aggregateSnapshot() == null) {
            return result;
        }
        collectRuntime(response.processGroupStatus().aggregateSnapshot().processorStatusSnapshots(),
                response.processGroupStatus().aggregateSnapshot().processGroupStatusSnapshots(), result);
        return result;
    }

    private static void collectRuntime(List<NifiFlowStatusResponse.ProcessorStatusEntry> processors,
                                       List<NifiFlowStatusResponse.ProcessGroupStatusEntry> groups,
                                       Map<String, RuntimeStatus> out) {
        if (processors != null) {
            for (var entry : processors) {
                var p = entry.processorStatusSnapshot();
                if (p != null && p.id() != null) {
                    out.put(p.id(), new RuntimeStatus(p.runStatus(), p.activeThreads(),
                            p.flowFilesReceived() == null ? 0 : p.flowFilesReceived(),
                            p.flowFilesSent() == null ? 0 : p.flowFilesSent()));
                }
            }
        }
        if (groups != null) {
            for (var entry : groups) {
                var g = entry.processGroupStatusSnapshot();
                if (g != null) {
                    collectRuntime(g.processorStatusSnapshots(), g.processGroupStatusSnapshots(), out);
                }
            }
        }
    }

    private record RuntimeStatus(String runStatus, int activeThreads, int received, int sent) {
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
