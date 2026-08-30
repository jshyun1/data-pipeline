package com.company.pipeline.jobcatalog;

import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.NifiProcessGroupMetadataService;
import com.company.pipeline.nifi.dto.NifiFlowResponse;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ConnectionEntity;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessGroupComponent;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessGroupEntity;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessorComponent;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessorEntity;
import com.company.pipeline.nifi.dto.NifiParameterContextResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * NiFi 캔버스를 읽어 잡 카탈로그 테이블에 미러링한다(NiFi 원본, DB 사본).
 *
 * <p>모든 프로세스 그룹은 metadata로 남기고, Template 하위가 아니면서 직계 프로세서를 가진
 * 그룹만 etl_job으로 미러링한다. 상위 그룹/그룹핑 그룹과 실제 JOB그룹의 경계를 DB에서도
 * 분리하기 위해 JOB의 스텝/연결은 직계 구성만 담는다.
 *
 * <p>안전장치 두 가지가 있다.
 * <ul>
 *   <li>NiFi에서 사라진 잡은 지우지 않고 {@code deleted_at}만 찍는다. 캔버스 유실과
 *       의도적 삭제를 구분할 방법이 없기 때문이다(메모리 압박 하 재시작에서 실제로 겪음).</li>
 *   <li>한 번에 여러 잡이 동시에 사라지면 삭제 표시 자체를 건너뛰고 ERROR 로그만 남긴다.
 *       캔버스가 통째로 비는 사고에서 미러까지 같이 비면 복구 근거가 사라진다.</li>
 * </ul>
 */
@Service
public class NifiJobMirrorService {

    private static final Logger log = LoggerFactory.getLogger(NifiJobMirrorService.class);

    private static final String ROOT_GROUP_ID = "root";
    /** 잡 정의는 자주 바뀌지 않는다. 상태 폴링(15~30초)과 달리 5분이면 충분하다. */
    private static final long SYNC_INTERVAL_MS = 300_000L;
    /** 이 개수를 넘겨 한 번에 사라지면 삭제 표시를 보류한다(캔버스 유실 의심). */
    private static final int MASS_DELETION_THRESHOLD = 3;
    private static final int SNAPSHOT_RETENTION_DAYS = 90;
    private static final String TEMPLATE_GROUP_NAME = "Template";

    private final NifiClient nifiClient;
    private final NifiProcessGroupMetadataService processGroupMetadataService;
    private final EtlJobRepository jobRepository;
    private final EtlJobStepRepository stepRepository;
    private final EtlJobLinkRepository linkRepository;
    private final EtlJobParamRepository paramRepository;
    private final EtlJobSnapshotRepository snapshotRepository;
    private final JobLookup jobLookup;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public NifiJobMirrorService(NifiClient nifiClient,
                                NifiProcessGroupMetadataService processGroupMetadataService,
                                EtlJobRepository jobRepository,
                                EtlJobStepRepository stepRepository,
                                EtlJobLinkRepository linkRepository,
                                EtlJobParamRepository paramRepository,
                                EtlJobSnapshotRepository snapshotRepository,
                                JobLookup jobLookup) {
        this.nifiClient = nifiClient;
        this.processGroupMetadataService = processGroupMetadataService;
        this.jobRepository = jobRepository;
        this.stepRepository = stepRepository;
        this.linkRepository = linkRepository;
        this.paramRepository = paramRepository;
        this.snapshotRepository = snapshotRepository;
        this.jobLookup = jobLookup;
    }

    @Scheduled(fixedRate = SYNC_INTERVAL_MS, initialDelay = 60_000L)
    public void scheduledSync() {
        try {
            SyncResult result = sync();
            log.debug("잡 카탈로그 동기화 완료: {}", result);
        } catch (RuntimeException ex) {
            // NiFi가 잠깐 죽어 있는 것으로 스케줄러를 멈추지 않는다. 다음 주기에 다시 시도.
            log.warn("잡 카탈로그 동기화 실패(다음 주기에 재시도): {}", ex.getMessage());
        }
    }

    /** 수동 재동기화(화면/API에서 호출). 예외를 그대로 올려 호출자가 실패를 알 수 있게 한다. */
    @Transactional
    public SyncResult sync() {
        NifiFlowResponse rootFlow = nifiClient.getFlow(ROOT_GROUP_ID);
        List<MirroredGroup> jobGroups = new ArrayList<>();
        Set<String> allDiscoveredGroupIds = new HashSet<>();
        String rootGroupId = rootFlow == null || rootFlow.processGroupFlow() == null
                || rootFlow.processGroupFlow().id() == null
                ? ROOT_GROUP_ID : rootFlow.processGroupFlow().id();
        allDiscoveredGroupIds.add(rootGroupId);
        processGroupMetadataService.ensureDiscovered(rootGroupId, "ETL Root", null, null);
        collectGroupsForMirror(rootGroupId, childProcessGroups(rootFlow),
                false, allDiscoveredGroupIds, jobGroups);

        Set<String> seenJobGroupIds = new HashSet<>();
        int created = 0;
        int updated = 0;
        int snapshots = 0;

        for (MirroredGroup group : jobGroups) {
            var component = group.component();
            String pgId = group.groupId();
            seenJobGroupIds.add(pgId);

            Optional<EtlJob> existing = jobRepository.findByNifiPgId(pgId);
            EtlJob job = existing.orElseGet(() -> new EtlJob(pgId, component.name()));
            boolean isNew = existing.isEmpty();
            if (isNew) {
                job = jobRepository.save(job);
                created++;
            } else {
                updated++;
            }

            GroupContents contents = group.contents();
            job.applySnapshot(
                    component.name(),
                    group.parentGroupId(),
                    component.comments(),
                    component.parameterContext() == null ? null : component.parameterContext().id(),
                    component.parameterContext() == null ? null : component.parameterContext().name(),
                    component.position() == null ? null : component.position().x(),
                    component.position() == null ? null : component.position().y(),
                    contents.processors().size(),
                    zeroIfNull(component.runningCount()),
                    zeroIfNull(component.stoppedCount()),
                    zeroIfNull(component.invalidCount()));
            jobRepository.save(job);

            mirrorSteps(job, contents.processors());
            mirrorLinks(job, contents.connections());
            mirrorParams(job);

            if (captureSnapshotIfChanged(job, contents)) {
                snapshots++;
            }
        }

        int deleted = markMissingJobsDeleted(seenJobGroupIds, allDiscoveredGroupIds);
        // 구성이 바뀌었을 수 있으므로 프로세서 -> 잡 캐시를 비운다(이력 귀속이 새 구성을 따르도록).
        jobLookup.invalidate();
        return new SyncResult(jobGroups.size(), created, updated, deleted, snapshots);
    }

    private void collectGroupsForMirror(String parentGroupId, List<ProcessGroupEntity> groups,
                                        boolean templateBranch, Set<String> allDiscoveredGroupIds,
                                        List<MirroredGroup> jobGroups) {
        for (ProcessGroupEntity group : groups) {
            var component = group.component();
            if (component == null) {
                continue;
            }
            String groupId = component.id() != null ? component.id() : group.id();
            if (groupId == null) {
                continue;
            }

            String groupName = component.name();
            boolean inTemplateBranch = templateBranch || isTemplateGroup(groupName);
            allDiscoveredGroupIds.add(groupId);
            processGroupMetadataService.ensureDiscovered(groupId, groupName, parentGroupId, component.comments());

            NifiFlowResponse groupFlow = nifiClient.getFlow(groupId);
            GroupContents contents = directContents(groupFlow);
            if (!inTemplateBranch && !contents.processors().isEmpty()) {
                jobGroups.add(new MirroredGroup(groupId, parentGroupId, component, contents));
            }
            collectGroupsForMirror(groupId, childProcessGroups(groupFlow),
                    inTemplateBranch, allDiscoveredGroupIds, jobGroups);
        }
    }

    private List<ProcessGroupEntity> childProcessGroups(NifiFlowResponse flow) {
        if (flow == null || flow.processGroupFlow() == null
                || flow.processGroupFlow().flow() == null
                || flow.processGroupFlow().flow().processGroups() == null) {
            return List.of();
        }
        return flow.processGroupFlow().flow().processGroups();
    }

    private GroupContents directContents(NifiFlowResponse flow) {
        if (flow == null || flow.processGroupFlow() == null || flow.processGroupFlow().flow() == null) {
            return new GroupContents(List.of(), List.of());
        }
        var inner = flow.processGroupFlow().flow();
        return new GroupContents(
                inner.processors() == null ? List.of() : inner.processors(),
                inner.connections() == null ? List.of() : inner.connections());
    }

    private static boolean isTemplateGroup(String groupName) {
        return TEMPLATE_GROUP_NAME.equalsIgnoreCase(groupName) || "템플릿".equals(groupName);
    }

    private void mirrorSteps(EtlJob job, List<ProcessorEntity> processors) {
        Map<String, EtlJobStep> existing = new HashMap<>();
        for (EtlJobStep step : stepRepository.findByJobIdAndDeletedAtIsNull(job.getId())) {
            existing.put(step.getNifiProcessorId(), step);
        }

        for (ProcessorEntity entity : processors) {
            ProcessorComponent component = entity.component();
            if (component == null) {
                continue;
            }
            String processorId = component.id() != null ? component.id() : entity.id();
            Map<String, String> props = component.config() == null || component.config().properties() == null
                    ? Map.of() : component.config().properties();

            EtlJobStep step = existing.remove(processorId);
            if (step == null) {
                step = stepRepository.findByNifiProcessorId(processorId)
                        .orElseGet(() -> new EtlJobStep(job.getId(), processorId,
                                component.name(), component.shortType()));
            }
            step.applySnapshot(
                    job.getId(),
                    component.name(),
                    component.shortType(),
                    component.config() == null ? null : component.config().schedulingStrategy(),
                    component.config() == null ? null : component.config().schedulingPeriod(),
                    sqlText(props),
                    targetTable(props),
                    props.get("put-db-record-statement-type"),
                    props.get("put-db-record-update-keys"),
                    dbcpServiceId(props),
                    component.position() == null ? null : component.position().x(),
                    component.position() == null ? null : component.position().y(),
                    component.validationStatus(),
                    component.state(),
                    toJson(props));
            stepRepository.save(step);
        }

        // 이번 조회에 없던 스텝 = 캔버스에서 지워진 것
        existing.values().forEach(step -> {
            step.markDeleted();
            stepRepository.save(step);
        });
    }

    private void mirrorLinks(EtlJob job, List<ConnectionEntity> connections) {
        Map<String, EtlJobLink> existing = new HashMap<>();
        for (EtlJobLink link : linkRepository.findByJobIdAndDeletedAtIsNull(job.getId())) {
            existing.put(link.getNifiConnectionId(), link);
        }

        for (ConnectionEntity entity : connections) {
            var component = entity.component();
            if (component == null || component.source() == null || component.destination() == null) {
                continue;
            }
            String connectionId = component.id() != null ? component.id() : entity.id();
            EtlJobLink link = existing.remove(connectionId);
            if (link == null) {
                link = linkRepository.findByNifiConnectionId(connectionId)
                        .orElseGet(() -> new EtlJobLink(job.getId(), connectionId,
                                component.source().id(), component.destination().id()));
            }
            link.applySnapshot(job.getId(),
                    component.source().id(), component.source().name(),
                    component.destination().id(), component.destination().name(),
                    component.selectedRelationships() == null
                            ? null : String.join(",", component.selectedRelationships()));
            linkRepository.save(link);
        }

        existing.values().forEach(link -> {
            link.markDeleted();
            linkRepository.save(link);
        });
    }

    private void mirrorParams(EtlJob job) {
        Map<String, EtlJobParam> existing = new HashMap<>();
        for (EtlJobParam param : paramRepository.findByJobIdAndDeletedAtIsNull(job.getId())) {
            existing.put(param.getParamName(), param);
        }

        if (job.getParameterContextId() != null) {
            NifiParameterContextResponse context = nifiClient.getParameterContext(job.getParameterContextId());
            List<NifiParameterContextResponse.ParameterEntity> parameters =
                    context == null || context.component() == null || context.component().parameters() == null
                            ? List.of() : context.component().parameters();
            for (var entity : parameters) {
                var parameter = entity.parameter();
                if (parameter == null || parameter.name() == null) {
                    continue;
                }
                EtlJobParam param = existing.remove(parameter.name());
                if (param == null) {
                    param = new EtlJobParam(job.getId(), parameter.name());
                }
                param.applySnapshot(parameter.value(),
                        Boolean.TRUE.equals(parameter.sensitive()),
                        parameter.description());
                paramRepository.save(param);
            }
        }

        existing.values().forEach(param -> {
            param.markDeleted();
            paramRepository.save(param);
        });
    }

    /**
     * 구조가 직전과 달라졌을 때만 스냅샷 1행. 5분마다 같은 내용을 쌓지 않기 위해
     * 정렬된 표현을 해시해서 비교한다.
     */
    private boolean captureSnapshotIfChanged(EtlJob job, GroupContents contents) {
        String serialized = serializeForHash(job, contents);
        String hash = sha256(serialized);
        Optional<EtlJobSnapshot> latest = snapshotRepository.findFirstByJobIdOrderByCapturedAtDesc(job.getId());
        if (latest.isPresent() && hash.equals(latest.get().getContentHash())) {
            return false;
        }
        snapshotRepository.save(new EtlJobSnapshot(job.getId(), hash, serialized));
        log.info("잡 구조 변경 감지 - {}({}) 스냅샷 저장", job.getJobName(), job.getNifiPgId());
        return true;
    }

    private String serializeForHash(EtlJob job, GroupContents contents) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("jobName", job.getJobName());
        root.put("nifiPgId", job.getNifiPgId());

        Map<String, Object> steps = new TreeMap<>();
        for (ProcessorEntity entity : contents.processors()) {
            ProcessorComponent component = entity.component();
            if (component == null) {
                continue;
            }
            Map<String, Object> step = new LinkedHashMap<>();
            step.put("name", component.name());
            step.put("type", component.type());
            step.put("properties", component.config() == null ? Map.of()
                    : new TreeMap<>(component.config().properties() == null
                            ? Map.of() : component.config().properties()));
            steps.put(component.id() != null ? component.id() : entity.id(), step);
        }
        root.put("steps", steps);

        Map<String, Object> links = new TreeMap<>();
        for (ConnectionEntity entity : contents.connections()) {
            var component = entity.component();
            if (component == null) {
                continue;
            }
            Map<String, Object> link = new LinkedHashMap<>();
            link.put("from", component.source() == null ? null : component.source().id());
            link.put("to", component.destination() == null ? null : component.destination().id());
            link.put("relationships", component.selectedRelationships());
            links.put(component.id() != null ? component.id() : entity.id(), link);
        }
        root.put("links", links);
        return toJson(root);
    }

    /**
     * etl_job 조건에서 빠졌거나 NiFi에 더 이상 없는 잡을 삭제 표시한다. NiFi에서 아예
     * 사라진 잡이 한 번에 여러 개면 캔버스 유실을 의심해 삭제 표시를 보류한다.
     */
    private int markMissingJobsDeleted(Set<String> seenJobGroupIds, Set<String> allDiscoveredGroupIds) {
        List<EtlJob> live = jobRepository.findByDeletedAtIsNullOrderByJobNameAsc();
        List<EtlJob> noLongerJobs = live.stream()
                .filter(job -> !seenJobGroupIds.contains(job.getNifiPgId()))
                .filter(job -> allDiscoveredGroupIds.contains(job.getNifiPgId()))
                .toList();
        List<EtlJob> missing = live.stream()
                .filter(job -> !seenJobGroupIds.contains(job.getNifiPgId()))
                .filter(job -> !allDiscoveredGroupIds.contains(job.getNifiPgId()))
                .toList();
        if (noLongerJobs.isEmpty() && missing.isEmpty()) {
            return 0;
        }
        if (missing.size() > MASS_DELETION_THRESHOLD) {
            log.error("잡 {}개가 한 번에 NiFi에서 사라졌습니다(임계 {}개 초과). 캔버스 유실을 의심해 "
                            + "삭제 표시를 보류합니다 - NiFi 상태를 확인하세요: {}",
                    missing.size(), MASS_DELETION_THRESHOLD,
                    missing.stream().map(EtlJob::getJobName).toList());
            missing = List.of();
        }
        noLongerJobs.forEach(job -> {
            markJobDeleted(job);
            log.info("프로세스 그룹은 존재하지만 etl_job 조건에서 제외되어 삭제 표시 - {}({})",
                    job.getJobName(), job.getNifiPgId());
        });
        missing.forEach(job -> {
            markJobDeleted(job);
            log.info("잡이 NiFi에서 사라져 삭제 표시 - {}({})", job.getJobName(), job.getNifiPgId());
        });
        return noLongerJobs.size() + missing.size();
    }

    private void markJobDeleted(EtlJob job) {
        stepRepository.findByJobIdAndDeletedAtIsNull(job.getId()).forEach(step -> {
            step.markDeleted();
            stepRepository.save(step);
        });
        linkRepository.findByJobIdAndDeletedAtIsNull(job.getId()).forEach(link -> {
            link.markDeleted();
            linkRepository.save(link);
        });
        paramRepository.findByJobIdAndDeletedAtIsNull(job.getId()).forEach(param -> {
            param.markDeleted();
            paramRepository.save(param);
        });
        job.markDeleted();
        jobRepository.save(job);
    }

    /** 스냅샷 보존 90일. 하루 한 번만 돌면 충분하다. */
    @Scheduled(fixedRate = 86_400_000L, initialDelay = 3_600_000L)
    @Transactional
    public void purgeOldSnapshots() {
        int removed = snapshotRepository.deleteOlderThan(
                LocalDateTime.now().minusDays(SNAPSHOT_RETENTION_DAYS));
        if (removed > 0) {
            log.info("잡 스냅샷 {}건 정리(보존 {}일)", removed, SNAPSHOT_RETENTION_DAYS);
        }
    }

    private static String sqlText(Map<String, String> props) {
        String select = props.get("SQL select query");
        if (select != null && !select.isBlank()) {
            return select;
        }
        String statement = props.get("putsql-sql-statement");
        return statement == null || statement.isBlank() ? null : statement;
    }

    private static String targetTable(Map<String, String> props) {
        String table = props.get("put-db-record-table-name");
        if (table == null || table.isBlank()) {
            return null;
        }
        String schema = props.get("put-db-record-schema-name");
        return schema == null || schema.isBlank() ? table : schema + "." + table;
    }

    private static String dbcpServiceId(Map<String, String> props) {
        // 프로세서 타입마다 커넥션 풀 프로퍼티 키가 다르다.
        for (String key : List.of("Database Connection Pooling Service", "put-db-record-dcbp-service",
                "JDBC Connection Pool")) {
            String value = props.get(key);
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static int zeroIfNull(Integer value) {
        return value == null ? 0 : value;
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            log.warn("잡 카탈로그 JSON 직렬화 실패: {}", ex.getMessage());
            return "{}";
        }
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashed = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(hashed.length * 2);
            for (byte b : hashed) {
                builder.append(Character.forDigit((b >> 4) & 0xF, 16));
                builder.append(Character.forDigit(b & 0xF, 16));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256을 쓸 수 없습니다", ex);
        }
    }

    private record GroupContents(List<ProcessorEntity> processors, List<ConnectionEntity> connections) {
    }

    private record MirroredGroup(String groupId, String parentGroupId, ProcessGroupComponent component,
                                 GroupContents contents) {
    }

    /** 동기화 1회 결과. 수동 재동기화 API 응답으로도 쓴다. */
    public record SyncResult(int jobsSeen, int created, int updated, int deleted, int snapshots) {
    }
}
