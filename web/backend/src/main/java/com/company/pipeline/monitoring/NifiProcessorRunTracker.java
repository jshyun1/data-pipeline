package com.company.pipeline.monitoring;

import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.dto.NifiCountersResponse;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 적재 프로세서의 "실행 구간"(시작~종료)을 폴링으로 관측해 {@link NifiProcessorRun}에 남긴다.
 *
 * <p>왜 관측해서 만드는가: NiFi에 실행 이력 개념이 없다. Provenance는 이 환경에서 이벤트를
 * 0건 반환하고(재시작/저장소 재구축 후에도 재현), 프로세서 단위 Status History는 항상 빈
 * 응답이다. 그룹 단위 Status History는 되지만 그룹 안 여러 테이블이 섞여 프로세서별로
 * 나눌 수 없다.
 *
 * <p>판정 방법: 매 주기 두 가지를 본다.
 * <ul>
 *   <li>{@code activeThreadCount > 0} - 지금 스레드를 잡고 돌고 있다</li>
 *   <li>적재 카운터 증가 - 방금 행이 들어갔다</li>
 * </ul>
 * 둘 중 하나라도 있으면 "활동 중"으로 보고 열린 구간이 없으면 새로 연다. 카운터만 보면
 * 추출이 오래 걸리는 동안(아직 한 행도 안 넣은 구간)이 빠져서 소요시간이 실제보다 짧게
 * 나오므로 스레드도 같이 본다.
 *
 * <p>정밀도는 폴링 주기에 묶인다. 15초마다 보므로 시작/종료가 최대 15초 늦고, 15초 안에
 * 끝난 적재는 시작=종료로 기록돼 처리량을 계산할 수 없다.
 */
@Component
public class NifiProcessorRunTracker {

    private static final Logger log = LoggerFactory.getLogger(NifiProcessorRunTracker.class);
    private static final String INSERT_COUNTER_NAME = "INSERT updates performed";
    private static final Pattern PROCESSOR_ID_IN_CONTEXT = Pattern.compile("\\(([0-9a-fA-F-]{36})\\)\\s*$");
    /** {@link NifiPipelineMetricScheduler}와 같은 기준 - 적재를 수행하는 프로세서 타입. */
    private static final Set<String> LOAD_PROCESSOR_TYPES = Set.of("PutDatabaseRecord", "ExecuteGroovyScript");
    /**
     * 활동을 마지막으로 본 뒤 이만큼 조용하면 구간을 닫는다.
     *
     * <p>폴링 주기의 3배. 한 주기만 놓쳐도 닫아버리면 배치 사이 잠깐의 공백에서 한
     * 실행이 여러 조각으로 쪼개진다 - 원래 화면이 29페이지가 됐던 이유가 그거다.
     */
    private static final long IDLE_CLOSE_SECONDS = 45;

    private final NifiClient nifiClient;
    private final NifiProcessorRunRepository runRepository;

    /** 프로세서별 직전 카운터값. 증가분만 구간에 더하기 위해 들고 있는다. */
    private final Map<String, Long> lastCounterValue = new ConcurrentHashMap<>();
    /** 프로세서별 적재 대상 테이블. 설정은 잘 안 바뀌니 한 번 읽고 재사용한다. */
    private final Map<String, String> targetTableCache = new ConcurrentHashMap<>();

    private final Clock clock;

    // 생성자가 둘이라 어느 쪽으로 주입할지 명시해야 한다(없으면 기동 시 기본
    // 생성자를 찾다가 실패한다 - 실제로 겪음).
    @Autowired
    public NifiProcessorRunTracker(NifiClient nifiClient, NifiProcessorRunRepository runRepository) {
        this(nifiClient, runRepository, Clock.systemDefaultZone());
    }

    /** 유휴 판정이 시간에 의존해서 테스트에서 시계를 갈아끼울 수 있어야 한다. */
    NifiProcessorRunTracker(NifiClient nifiClient, NifiProcessorRunRepository runRepository, Clock clock) {
        this.nifiClient = nifiClient;
        this.runRepository = runRepository;
        this.clock = clock;
    }

    private record ProcessorSnapshot(String id, String name, String type, String groupId, String groupName,
            int activeThreads) {
    }

    @Scheduled(fixedRate = 15_000, initialDelay = 20_000)
    @Transactional
    public void track() {
        NifiFlowStatusResponse flow;
        NifiCountersResponse counters;
        try {
            flow = nifiClient.getRootFlowStatus();
            counters = nifiClient.getCounters();
        } catch (Exception ex) {
            // NiFi가 잠깐 응답을 못 해도 다음 주기에 이어서 본다. 이때 구간을 닫아버리면
            // 멀쩡히 돌고 있는 적재가 끊긴 것처럼 기록된다.
            log.debug("NiFi 상태/카운터 조회 실패(다음 주기에 재시도): {}", ex.getMessage());
            return;
        }

        LocalDateTime now = LocalDateTime.now(clock);
        List<ProcessorSnapshot> processors = collectLoadProcessors(flow);
        Map<String, Long> counterByProcessor = extractInsertCounters(counters);
        Map<String, NifiProcessorRun> openRuns = new HashMap<>();
        for (NifiProcessorRun run : runRepository.findByEndedAtIsNull()) {
            openRuns.put(run.getProcessorId(), run);
        }

        for (ProcessorSnapshot processor : processors) {
            Long currentValue = counterByProcessor.get(processor.id());
            long delta = deltaFor(processor.id(), currentValue);
            boolean active = processor.activeThreads() > 0 || delta > 0;
            NifiProcessorRun open = openRuns.remove(processor.id());

            if (active) {
                if (open == null) {
                    open = new NifiProcessorRun(processor.id(), processor.name(), processor.type(),
                            processor.groupId(), processor.groupName(), targetTableOf(processor), now);
                } else {
                    open.describe(processor.name(), processor.type(), processor.groupName(), targetTableOf(processor));
                }
                open.observeActivity(now, delta);
                runRepository.save(open);
            } else if (open != null && open.getLastSeenAt().plusSeconds(IDLE_CLOSE_SECONDS).isBefore(now)) {
                open.close();
                runRepository.save(open);
                log.info("NiFi 적재 구간 종료 - {} {}건 ({}~{})", open.getProcessorName(), open.getInsertedCount(),
                        open.getStartedAt(), open.getEndedAt());
            }
        }

        // 캔버스에서 지워진 프로세서의 구간이 영원히 열린 채로 남지 않게 정리한다.
        for (NifiProcessorRun orphan : openRuns.values()) {
            if (orphan.getLastSeenAt().plusSeconds(IDLE_CLOSE_SECONDS).isBefore(now)) {
                orphan.close();
                runRepository.save(orphan);
            }
        }
    }

    /**
     * 직전 값과의 증가분.
     *
     * <p>카운터가 응답에 없을 때 기준점을 <b>지우면 안 되고 0으로 내려야</b> 한다.
     * NiFi 카운터는 프로세서가 한 번이라도 적재하기 전까지는 응답에 아예 없고,
     * PutDatabaseRecord는 배치를 다 넣은 뒤 세션 커밋 시점에 한 번에 올리므로
     * "없음 → 1,660,590"처럼 단번에 나타난다. 기준점을 지워두면 그 순간이 "처음 보는
     * 프로세서"로 취급돼 증가분이 통째로 0이 된다 - 실제로 12:56 실행에서 8개 적재가
     * 전부 0건으로 기록됐고, 그중 5개는 활동으로 인정되지 않아 구간조차 안 생겼다.
     *
     * <p>반면 앱이 막 떠서 이 프로세서를 <b>한 번도 못 본</b> 경우(previous == null)는
     * 기준점만 잡는다. 그때 카운터에 이미 쌓여 있는 값은 이번 실행분이 아니기 때문이다.
     */
    private long deltaFor(String processorId, Long currentValue) {
        if (currentValue == null) {
            // 아직 한 번도 안 돌았거나 NiFi 재시작으로 초기화됨. 다음에 나타나는 값이
            // 곧 그때까지 적재한 양이므로 기준점을 0으로 둔다.
            lastCounterValue.put(processorId, 0L);
            return 0L;
        }
        Long previous = lastCounterValue.put(processorId, currentValue);
        if (previous == null) {
            return 0L;
        }
        return currentValue >= previous ? currentValue - previous : currentValue;
    }

    private String targetTableOf(ProcessorSnapshot processor) {
        String cached = targetTableCache.get(processor.id());
        if (cached != null) {
            // ConcurrentHashMap은 null을 못 담아서, "조회했지만 알 수 없음"(스크립트
            // 적재 등)을 빈 문자열로 캐시해 매 주기 다시 부르지 않게 한다.
            return cached.isBlank() ? null : cached;
        }
        try {
            String table = nifiClient.getProcessor(processor.id()).targetTable();
            targetTableCache.put(processor.id(), table == null ? "" : table);
            return table;
        } catch (Exception ex) {
            // 일시적 실패는 캐시하지 않는다 - 다음 주기에 다시 시도한다.
            log.debug("적재 대상 테이블 조회 실패 - {}: {}", processor.name(), ex.getMessage());
            return null;
        }
    }

    private Map<String, Long> extractInsertCounters(NifiCountersResponse counters) {
        Map<String, Long> result = new HashMap<>();
        var aggregate = counters == null || counters.counters() == null ? null : counters.counters().aggregateSnapshot();
        var list = aggregate == null ? null : aggregate.counters();
        if (list == null) {
            return result;
        }
        for (var counter : list) {
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

    private List<ProcessorSnapshot> collectLoadProcessors(NifiFlowStatusResponse flow) {
        List<ProcessorSnapshot> result = new ArrayList<>();
        var aggregate = flow.processGroupStatus() == null ? null : flow.processGroupStatus().aggregateSnapshot();
        if (aggregate == null) {
            return result;
        }
        collect(aggregate.processorStatusSnapshots(), aggregate.processGroupStatusSnapshots(), "root", "root", result);
        return result;
    }

    private void collect(
            List<NifiFlowStatusResponse.ProcessorStatusEntry> processors,
            List<NifiFlowStatusResponse.ProcessGroupStatusEntry> groups,
            String groupId,
            String groupName,
            List<ProcessorSnapshot> out) {
        if (processors != null) {
            for (var entry : processors) {
                var processor = entry.processorStatusSnapshot();
                if (processor != null && LOAD_PROCESSOR_TYPES.contains(processor.type())) {
                    out.add(new ProcessorSnapshot(processor.id(), processor.name(), processor.type(),
                            groupId, groupName, processor.activeThreads()));
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
