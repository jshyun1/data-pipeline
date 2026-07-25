package com.company.pipeline.monitoring;

import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.dto.NifiCountersResponse;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 대시보드 "NiFi 적재 건수"를 Airflow DAG(nifi_pipelines_metrics_collector, 타겟 DB를
 * 타임스탬프로 훑는 방식) 없이 NiFi 자신의 REST API만으로 직접 채운다. PutDatabaseRecord
 * 프로세서는 "INSERT updates performed"라는 누적 카운터를 등록하는데(NiFi 재시작 전까지
 * 유지), 프로세서별 마지막 값을 nifi_counter_snapshot에 남겨두고 직전 값과의 증가분만
 * 반영한다 - Kafka의 committed offset 스냅샷과 동일한 델타 방식.
 */
@Component
public class NifiPipelineMetricScheduler {

    private static final Logger log = LoggerFactory.getLogger(NifiPipelineMetricScheduler.class);
    private static final String SOURCE = "NIFI";
    private static final String TASK_KEY = "put_database_record_counter";
    // /nifi-api/flow/process-groups/.../status(recursive)는 프로세서 타입을 전체 클래스명이
    // 아니라 짧은 이름으로 돌려준다(다른 NiFi API, 예: /process-groups/{id}/processors는
    // 전체 클래스명을 쓰는 것과 다름 - 실측으로 확인).
    private static final String PUT_DATABASE_RECORD_TYPE = "PutDatabaseRecord";
    private static final String INSERT_COUNTER_NAME = "INSERT updates performed";
    private static final Pattern PROCESSOR_ID_IN_CONTEXT = Pattern.compile("\\(([0-9a-fA-F-]{36})\\)\\s*$");

    private final NifiClient nifiClient;
    private final NifiCounterSnapshotRepository snapshotRepository;
    private final PipelineDailyLoadMetricService dailyLoadMetricService;

    public NifiPipelineMetricScheduler(
            NifiClient nifiClient,
            NifiCounterSnapshotRepository snapshotRepository,
            PipelineDailyLoadMetricService dailyLoadMetricService) {
        this.nifiClient = nifiClient;
        this.snapshotRepository = snapshotRepository;
        this.dailyLoadMetricService = dailyLoadMetricService;
    }

    private record ProcessorRef(String id, String name) {
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
            // 여기서 죽지 않고 조용히 넘어간다.
            log.warn("NiFi 상태/카운터 조회 실패(다음 주기에 재시도): {}", ex.getMessage());
            return;
        }

        List<ProcessorRef> putDatabaseRecordProcessors = collectPutDatabaseRecordProcessors(flow);
        if (putDatabaseRecordProcessors.isEmpty()) {
            return;
        }

        Map<String, Long> insertCountByProcessorId = extractInsertCounters(counters);

        for (ProcessorRef ref : putDatabaseRecordProcessors) {
            Long currentValue = insertCountByProcessorId.get(ref.id());
            if (currentValue == null) {
                // 이 프로세서가 마지막 NiFi 재시작 이후 아직 한 번도 실행되지 않아
                // 카운터 자체가 생성되지 않은 상태 - 다음 실행 후 카운터가 생기면 잡힌다.
                continue;
            }
            checkOne(ref, currentValue);
        }
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

    private List<ProcessorRef> collectPutDatabaseRecordProcessors(NifiFlowStatusResponse flow) {
        List<ProcessorRef> result = new ArrayList<>();
        var aggregateSnapshot = flow.processGroupStatus() == null ? null : flow.processGroupStatus().aggregateSnapshot();
        if (aggregateSnapshot == null) {
            return result;
        }
        collect(aggregateSnapshot.processorStatusSnapshots(), aggregateSnapshot.processGroupStatusSnapshots(), result);
        return result;
    }

    private void collect(
            List<NifiFlowStatusResponse.ProcessorStatusEntry> processors,
            List<NifiFlowStatusResponse.ProcessGroupStatusEntry> groups,
            List<ProcessorRef> out) {
        if (processors != null) {
            for (var entry : processors) {
                var processor = entry.processorStatusSnapshot();
                if (processor != null && PUT_DATABASE_RECORD_TYPE.equals(processor.type())) {
                    out.add(new ProcessorRef(processor.id(), processor.name()));
                }
            }
        }
        if (groups != null) {
            for (var entry : groups) {
                var group = entry.processGroupStatusSnapshot();
                if (group != null) {
                    collect(group.processorStatusSnapshots(), group.processGroupStatusSnapshots(), out);
                }
            }
        }
    }
}
