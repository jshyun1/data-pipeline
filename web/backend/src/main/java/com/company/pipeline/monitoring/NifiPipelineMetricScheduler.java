package com.company.pipeline.monitoring;

import com.company.pipeline.jobcatalog.JobLookup;
import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.dto.NifiBulletinBoardResponse;
import com.company.pipeline.nifi.dto.NifiCountersResponse;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 대시보드 "NiFi 적재 건수"를 Airflow DAG(nifi_pipelines_metrics_collector, 타겟 DB를
 * 타임스탬프로 훑는 방식) 없이 NiFi 자신의 REST API만으로 직접 채운다. 적재 프로세서는
 * "INSERT updates performed"라는 누적 카운터를 등록하는데(NiFi 재시작 전까지 유지),
 * 프로세서별 마지막 값을 nifi_counter_snapshot에 남겨두고 직전 값과의 증가분만
 * 반영한다 - Kafka의 committed offset 스냅샷과 동일한 델타 방식.
 *
 * ETL 로그 화면(nifi_execution_log)도 같은 델타 감지를 "그 주기(60초)에 한 번 실행됨"으로
 * 간주해 행을 하나씩 남긴다. NiFi에 Provenance(이 환경에서 인덱스/이벤트파일 불일치로
 * 조회 불가 확인됨)나 프로세서 단위 Status History(항상 비어있음, 확인됨) 같은 "실행
 * 이력" 개념이 없어서 택한 차선책 - 정확히 "실행 1번 = 행 1개"는 아니고 "60초 구간 안에
 * 증가가 있었다 = 행 1개"이지만, 지금 파이프라인들의 실행 빈도상 대부분 근접하게 맞는다.
 *
 * <p>NiFi 카운터는 재시작하면 초기화되므로, 스냅샷과 비교하는 이 방식은 초기화를
 * 반드시 인지해야 한다. 놓치면 두 방향으로 다 틀어진다 - 낡은 기준점이 남아 증가분을
 * 통째로 놓치거나(같은 값까지 다시 올라오면 delta가 0), 반대로 이미 센 구간을 다시
 * 세서 부풀린다. 초기화 감지는 두 경로로 한다.
 * <ul>
 *   <li>카운터가 응답에서 사라짐 → 초기화된 것이므로 기준점을 0으로 내린다.</li>
 *   <li>카운터가 이전 값보다 작아짐 → 이미 다시 세기 시작한 것이므로 현재값 전체를
 *       증가분으로 잡는다.</li>
 * </ul>
 */
@Component
public class NifiPipelineMetricScheduler {

    private static final Logger log = LoggerFactory.getLogger(NifiPipelineMetricScheduler.class);
    private static final String SOURCE = "NIFI";
    // 이름은 PutDatabaseRecord만 보던 시절의 잔재지만 그대로 둔다 - 이미 쌓인
    // pipeline_daily_load_metric 행들의 task_key라 바꾸면 같은 파이프라인의 이력이 갈린다.
    private static final String TASK_KEY = "put_database_record_counter";
    // /nifi-api/flow/process-groups/.../status(recursive)는 프로세서 타입을 전체 클래스명이
    // 아니라 짧은 이름으로 돌려준다(다른 NiFi API, 예: /process-groups/{id}/processors는
    // 전체 클래스명을 쓰는 것과 다름 - 실측으로 확인).
    //
    // "INSERT updates performed" 카운터를 올리는 프로세서 타입들. PutDatabaseRecord는
    // NiFi가 알아서 올려주고, ExecuteGroovyScript는 비정형 그룹의 이미지/동영상 적재가
    // 스크립트 안에서 session.adjustCounter()로 같은 이름을 직접 올린다(바이너리는
    // 레코드 기반인 PutDatabaseRecord로 넣으면 파일 전체가 힙에 올라와서 못 씀).
    //
    // 카운터를 안 올리는 ExecuteGroovyScript가 섞여 있어도 문제되지 않는다 - 카운터가
    // 없으면 스냅샷도 없어서 아래 초기화 감지가 아무것도 하지 않는다.
    private static final Set<String> LOAD_PROCESSOR_TYPES = Set.of("PutDatabaseRecord", "ExecuteGroovyScript");
    private static final String INSERT_COUNTER_NAME = "INSERT updates performed";
    // 같은 bulletin id를 "이미 넣은 것"으로 볼 시간 범위. NiFi의 bulletin 링버퍼가
    // 5분치라 그보다 넉넉히 잡으면 같은 세션의 중복은 확실히 걸러지고, 재시작 뒤
    // 재사용된 id는 예전 행이 이 범위 밖이라 새 실패로 정상 기록된다.
    private static final long BULLETIN_DEDUPE_WINDOW_MINUTES = 30;
    private static final Pattern PROCESSOR_ID_IN_CONTEXT = Pattern.compile("\\(([0-9a-fA-F-]{36})\\)\\s*$");

    private final NifiClient nifiClient;
    private final NifiCounterSnapshotRepository snapshotRepository;
    private final PipelineDailyLoadMetricService dailyLoadMetricService;
    private final NifiExecutionLogEntryRepository executionLogRepository;
    /** 이력을 어느 잡에 귀속시킬지 해석한다(V18). 미러가 못 본 프로세서는 null. */
    private final JobLookup jobLookup;

    public NifiPipelineMetricScheduler(
            NifiClient nifiClient,
            NifiCounterSnapshotRepository snapshotRepository,
            PipelineDailyLoadMetricService dailyLoadMetricService,
            NifiExecutionLogEntryRepository executionLogRepository,
            JobLookup jobLookup) {
        this.nifiClient = nifiClient;
        this.snapshotRepository = snapshotRepository;
        this.dailyLoadMetricService = dailyLoadMetricService;
        this.executionLogRepository = executionLogRepository;
        this.jobLookup = jobLookup;
    }

    private record ProcessorRef(String id, String name, String groupId, String groupName) {
    }

    /**
     * NiFi bulletin(경고/에러)을 긁어 실패 이력으로 남긴다.
     *
     * <p>카운터 델타 방식은 "적재가 늘었을 때"만 행을 남기므로 실패를 기록할 방법이
     * 원래 없었다 - 화면의 실패 배지가 도달 불가능한 코드였던 이유. 실제로 DB 인증
     * 실패로 truncate가 30초마다 롤백되던 날에도, ExecuteSQL이 OOM으로 34번 죽던
     * 날에도 이 표에는 아무것도 남지 않았다.
     *
     * <p>bulletin은 NiFi 메모리에 5분 남짓만 남는 링버퍼라 카운터(60초)보다 자주
     * 가져온다. 놓치면 그 실패는 어디에도 남지 않는다.
     *
     * <p>예전에는 "마지막으로 본 id 이후"만 요청했는데(after 파라미터), bulletin id가
     * NiFi 프로세스 안에서만 단조 증가하고 재시작하면 1부터 다시 시작한다는 걸 놓쳤다.
     * 재시작 뒤에는 새 id가 전부 예전 커서보다 작아서 NiFi가 통째로 걸러버렸고, 수집이
     * 조용히 멈췄다 - DZ 5개 테이블이 비워진 사고가 로그에 한 줄도 안 남았다(2026-07-29).
     * 지금은 커서를 쓰지 않고 링버퍼 전체를 가져온 뒤 (id, 최근 시간대)로 중복을
     * 거른다. 버퍼가 5분치로 애초에 작아서 매번 다 읽어도 부담이 없다.
     */
    @Scheduled(fixedRate = 30_000, initialDelay = 30_000)
    public void collectBulletins() {
        NifiBulletinBoardResponse response;
        try {
            response = nifiClient.getBulletins(0L);
        } catch (Exception ex) {
            // NiFi가 부하로 응답을 못 하는 상황 자체가 흔하다(그때가 오히려 실패가
            // 쌓이는 때다). 다음 주기에 같은 afterId로 다시 시도하면 되므로 조용히 넘긴다.
            log.warn("NiFi bulletin 조회 실패(다음 주기에 재시도): {}", ex.getMessage());
            return;
        }
        var board = response == null ? null : response.bulletinBoard();
        var bulletins = board == null ? null : board.bulletins();
        if (bulletins == null || bulletins.isEmpty()) {
            return;
        }

        LocalDateTime dedupeSince = LocalDateTime.now().minusMinutes(BULLETIN_DEDUPE_WINDOW_MINUTES);
        for (var entity : bulletins) {
            var bulletin = entity.bulletin();
            if (bulletin == null) {
                continue;
            }
            String level = bulletin.level();
            if (!"ERROR".equals(level) && !"WARNING".equals(level)) {
                continue;
            }
            Long bulletinId = bulletin.id() != null ? bulletin.id() : entity.id();
            if (bulletinId == null
                    || executionLogRepository.existsByBulletinIdAndOccurredAtAfter(bulletinId, dedupeSince)) {
                continue;
            }
            String sourceId = bulletin.sourceId() != null ? bulletin.sourceId() : entity.sourceId();
            String groupId = bulletin.groupId() != null ? bulletin.groupId() : entity.groupId();
            NifiExecutionLogEntry failureEntry = NifiExecutionLogEntry.fromBulletin(
                    sourceId == null ? "unknown" : sourceId,
                    bulletin.sourceName() == null ? "unknown" : bulletin.sourceName(),
                    groupId,
                    // bulletin은 그룹 "이름"을 주지 않는다. 화면에서 그룹명이 필요하면
                    // group_id로 조인해야 한다(여기서 매번 조회하면 호출이 배로 늘어남).
                    null,
                    // bulletin.timestamp는 "HH:mm:ss z" 표시용 문자열이라 날짜가 없다.
                    // 30초 주기로 즉시 수거하므로 수집 시각을 발생 시각으로 근사한다.
                    LocalDateTime.now(),
                    bulletinId,
                    level,
                    bulletin.message());
            // 실패 로그도 잡에 귀속시킨다 - 잡 실행을 FAILED로 닫는 판정이 이 값을 본다.
            failureEntry.assignJob(jobLookup.resolveJobId(sourceId, groupId).orElse(null));
            executionLogRepository.save(failureEntry);
        }
    }

    /**
     * 이 수집 루프가 마지막으로 정상 완주한 시각. 대시보드 인프라 구역이 "적재 지표 수집"
     * 생존 표시에 쓴다.
     *
     * <p>DB(nifi_counter_snapshot.updated_at)로는 이걸 판단할 수 없다: NiFi 카운터가
     * 초기화된 뒤 적재가 한 번도 없으면 스냅샷을 새로 쓸 일이 없어서, 수집이 정상인데도
     * 시각이 몇 시간 전에 멈춘 것처럼 보인다(실측: NiFi 재기동 5시간 뒤 09:03에 멈춘 채).
     */
    private volatile LocalDateTime lastCounterCheckAt;

    public LocalDateTime getLastCounterCheckAt() {
        return lastCounterCheckAt;
    }

    @Scheduled(fixedRate = 60_000, initialDelay = 60_000)
    public void checkCounters() {
        NifiFlowStatusResponse flow;
        NifiCountersResponse counters;
        try {
            flow = nifiClient.getRootFlowStatus();
            counters = nifiClient.getCounters();
        } catch (Exception ex) {
            // NiFi가 일시적으로 응답 안 해도(재시작 등) 다음 주기에 다시 시도되므로
            // 여기서 죽지 않고 조용히 넘어간다. NiFi를 못 본 주기는 "정상 완주"가
            // 아니므로 heartbeat도 갱신하지 않는다.
            log.warn("NiFi 상태/카운터 조회 실패(다음 주기에 재시도): {}", ex.getMessage());
            return;
        }

        lastCounterCheckAt = LocalDateTime.now();

        List<ProcessorRef> loadProcessors = collectLoadProcessors(flow);
        if (loadProcessors.isEmpty()) {
            return;
        }

        Map<String, Long> insertCountByProcessorId = extractInsertCounters(counters);

        for (ProcessorRef ref : loadProcessors) {
            Long currentValue = insertCountByProcessorId.get(ref.id());
            if (currentValue == null) {
                // 카운터가 응답에 통째로 없다 = NiFi가 재시작되며 카운터가 초기화된 것
                // (프로세서는 그대로 있는데 카운터만 사라진다).
                //
                // 예전에는 여기서 그냥 넘어갔는데, 그러면 재시작 이전의 낡은 스냅샷이
                // 그대로 남는다. 이 플로우는 TRUNCATE 후 전량 재적재라 카운터가 결국
                // 예전과 "똑같은 값"까지 올라오고, 그 시점에 delta가 0으로 계산되어
                // 적재가 통째로 누락됐다(실측: 430만건 적재가 로그에 한 줄도 안 남음).
                // 그래서 초기화를 감지한 시점에 스냅샷도 0으로 내려둔다.
                resetSnapshotForClearedCounter(ref);
                continue;
            }
            checkOne(ref, currentValue);
        }
    }

    private void resetSnapshotForClearedCounter(ProcessorRef ref) {
        snapshotRepository.findById(ref.id())
                .filter(snapshot -> snapshot.getLastValue() != null && snapshot.getLastValue() != 0L)
                .ifPresent(snapshot -> {
                    log.info("NiFi 카운터 초기화 감지 - {} 기준점을 0으로 재설정(이전 {})",
                            ref.name(), snapshot.getLastValue());
                    snapshotRepository.save(new NifiCounterSnapshot(ref.id(), ref.name(), 0L));
                });
    }

    private void checkOne(ProcessorRef ref, long currentValue) {
        Optional<NifiCounterSnapshot> previous = snapshotRepository.findById(ref.id());
        if (previous.isPresent()) {
            long previousValue = previous.get().getLastValue();
            // NiFi 재시작 등으로 카운터가 리셋되면 currentValue < previousValue가 될 수 있는데,
            // 이땐 음수 delta로 깎지 않고 리셋 이후 누적값 전체를 이번 증가분으로 잡는다.
            long delta = currentValue >= previousValue ? currentValue - previousValue : currentValue;
            if (delta > 0) {
                dailyLoadMetricService.incrementLoadedCount(SOURCE, ref.id(), TASK_KEY, ref.name(), delta);
                NifiExecutionLogEntry entry = new NifiExecutionLogEntry(ref.id(), ref.name(), ref.groupId(),
                        ref.groupName(), LocalDateTime.now(), delta, NifiExecutionLogEntry.STATUS_SUCCESS);
                entry.assignJob(jobLookup.resolveJobId(ref.id(), ref.groupId()).orElse(null));
                executionLogRepository.save(entry);
            }
        }
        // 처음 보는 프로세서는 이번 값을 기준점으로만 잡고(그 이전 이력은 알 방법이 없음)
        // 증가분은 다음 주기부터 반영한다.
        snapshotRepository.save(new NifiCounterSnapshot(ref.id(), ref.name(), currentValue));
    }

    private Map<String, Long> extractInsertCounters(NifiCountersResponse counters) {
        Map<String, Long> result = new HashMap<>();
        var aggregateSnapshot = counters.counters() == null ? null : counters.counters().aggregateSnapshot();
        var counterList = aggregateSnapshot == null ? null : aggregateSnapshot.counters();
        if (counterList == null) {
            return result;
        }
        for (var counter : counterList) {
            if (!INSERT_COUNTER_NAME.equals(counter.name())) {
                continue;
            }
            Matcher matcher = PROCESSOR_ID_IN_CONTEXT.matcher(counter.context() == null ? "" : counter.context());
            if (matcher.find()) {
                result.put(matcher.group(1), counter.valueCount());
            }
        }
        return result;
    }

    private List<ProcessorRef> collectLoadProcessors(NifiFlowStatusResponse flow) {
        List<ProcessorRef> result = new ArrayList<>();
        var aggregateSnapshot = flow.processGroupStatus() == null ? null : flow.processGroupStatus().aggregateSnapshot();
        if (aggregateSnapshot == null) {
            return result;
        }
        collect(aggregateSnapshot.processorStatusSnapshots(), aggregateSnapshot.processGroupStatusSnapshots(),
                "root", "root", result);
        return result;
    }

    private void collect(
            List<NifiFlowStatusResponse.ProcessorStatusEntry> processors,
            List<NifiFlowStatusResponse.ProcessGroupStatusEntry> groups,
            String currentGroupId,
            String currentGroupName,
            List<ProcessorRef> out) {
        if (processors != null) {
            for (var entry : processors) {
                var processor = entry.processorStatusSnapshot();
                if (processor != null && LOAD_PROCESSOR_TYPES.contains(processor.type())) {
                    out.add(new ProcessorRef(processor.id(), processor.name(), currentGroupId, currentGroupName));
                }
            }
        }
        if (groups != null) {
            for (var entry : groups) {
                var group = entry.processGroupStatusSnapshot();
                if (group != null) {
                    collect(group.processorStatusSnapshots(), group.processGroupStatusSnapshots(),
                            group.id(), group.name(), out);
                }
            }
        }
    }
}
