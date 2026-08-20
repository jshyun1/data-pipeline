package com.company.pipeline.infra;

import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import com.company.pipeline.connector.dto.ConnectorTaskStatus;
import com.company.pipeline.connector.dto.KafkaConnectWorkerInfo;
import com.company.pipeline.heartbeat.HeartbeatService;
import com.company.pipeline.infra.dto.AirflowHealthResponse;
import com.company.pipeline.infra.dto.ProcessHealthResponse;
import com.company.pipeline.infra.dto.ProcessHealthResponse.ProcessGroup;
import com.company.pipeline.infra.dto.ProcessHealthResponse.ProcessItem;
import com.company.pipeline.infra.dto.ProcessStatus;
import com.company.pipeline.monitoring.KafkaBrokerHealthChecker;
import com.company.pipeline.monitoring.KafkaBrokerProperties;
import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.dto.NifiBulletinBoardResponse;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse.ProcessGroupStatusEntry;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse.ProcessorStatusEntry;
import com.company.pipeline.settings.SettingKey;
import com.company.pipeline.settings.SettingService;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

/**
 * 대시보드 인프라 구역의 "주요 프로세스 현황". CDC/NiFi/Airflow 각각에서 떠 있어야 하는
 * 구성요소를 직접 물어본다.
 *
 * <p>한 축이 죽어도 나머지 축은 그대로 보여야 하므로 모든 조회는 개별적으로 감싸고,
 * 실패는 예외로 올리지 않고 DOWN/UNKNOWN 항목으로 바꿔서 내린다.
 */
@Service
@Transactional(readOnly = true)
public class ProcessHealthService {

    private static final Logger log = LoggerFactory.getLogger(ProcessHealthService.class);

    private static final DateTimeFormatter HEARTBEAT_FORMAT = DateTimeFormatter.ofPattern("MM.dd HH:mm:ss");
    // 수집 신선도 임계(적재 카운터 60초 주기, 기존 3분/10분)는
    // SettingKey(health.stale/dead.seconds.nifi)로 이관됨(U2, 설계서 4-2).
    private static final String AIRFLOW_HEALTHY = "healthy";

    private final KafkaBrokerHealthChecker kafkaBrokerHealthChecker;
    private final KafkaBrokerProperties kafkaBrokerProperties;
    private final KafkaConnectClient kafkaConnectClient;
    private final NifiClient nifiClient;
    private final AirflowHealthClient airflowHealthClient;
    private final HeartbeatService heartbeat;
    private final SettingService settings;

    public ProcessHealthService(
            KafkaBrokerHealthChecker kafkaBrokerHealthChecker,
            KafkaBrokerProperties kafkaBrokerProperties,
            KafkaConnectClient kafkaConnectClient,
            NifiClient nifiClient,
            AirflowHealthClient airflowHealthClient,
            HeartbeatService heartbeat,
            SettingService settings) {
        this.kafkaBrokerHealthChecker = kafkaBrokerHealthChecker;
        this.kafkaBrokerProperties = kafkaBrokerProperties;
        this.kafkaConnectClient = kafkaConnectClient;
        this.nifiClient = nifiClient;
        this.airflowHealthClient = airflowHealthClient;
        this.heartbeat = heartbeat;
        this.settings = settings;
    }

    public ProcessHealthResponse collect() {
        return new ProcessHealthResponse(
                LocalDateTime.now(),
                List.of(cdcGroup(), nifiGroup(), airflowGroup()));
    }

    // ------------------------------------------------------------------ CDC

    private ProcessGroup cdcGroup() {
        List<ProcessItem> items = new ArrayList<>();

        boolean brokerHealthy = kafkaBrokerHealthChecker.isHealthy();
        items.add(new ProcessItem(
                "Kafka Broker",
                brokerHealthy ? ProcessStatus.UP : ProcessStatus.DOWN,
                kafkaBrokerProperties.bootstrapServers()));

        KafkaConnectWorkerInfo workerInfo = null;
        try {
            workerInfo = kafkaConnectClient.getWorkerInfo();
        } catch (RuntimeException ex) {
            log.debug("Kafka Connect 워커 조회 실패: {}", ex.getMessage());
        }
        items.add(new ProcessItem(
                "Kafka Connect Worker",
                workerInfo != null ? ProcessStatus.UP : ProcessStatus.DOWN,
                workerInfo != null ? "버전 " + workerInfo.version() : "REST 응답 없음"));

        if (workerInfo == null) {
            // 워커가 안 뜨면 커넥터 상태 자체를 물어볼 수 없다("죽었다"고 단정하지 않는다).
            items.add(new ProcessItem("Source Connector (Debezium)", ProcessStatus.UNKNOWN, "워커 응답 없음"));
            items.add(new ProcessItem("Sink Connector (JDBC)", ProcessStatus.UNKNOWN, "워커 응답 없음"));
        } else {
            List<ConnectorStatusResponse> statuses = connectorStatuses();
            items.add(connectorItem("Source Connector (Debezium)", statuses, "source"));
            items.add(connectorItem("Sink Connector (JDBC)", statuses, "sink"));
        }

        return ProcessGroup.of("CDC", "CDC", items);
    }

    private List<ConnectorStatusResponse> connectorStatuses() {
        List<ConnectorStatusResponse> statuses = new ArrayList<>();
        try {
            for (String name : kafkaConnectClient.listConnectors()) {
                try {
                    statuses.add(kafkaConnectClient.getStatus(name));
                } catch (RuntimeException ex) {
                    log.debug("커넥터 상태 조회 실패 - {}: {}", name, ex.getMessage());
                }
            }
        } catch (RuntimeException ex) {
            log.debug("커넥터 목록 조회 실패: {}", ex.getMessage());
        }
        return statuses;
    }

    /**
     * 커넥터는 태스크 하나만 죽어도 connector.state가 RUNNING으로 남는 경우가 많아서
     * 태스크 상태까지 같이 본다(대시보드 상단 카드와 같은 판정 기준).
     *
     * <p>PAUSED/STOPPED는 사람이 화면에서 일부러 멈춘 상태라 장애와 구분한다 - 이 스택은
     * "Sink만 멈추고 Source는 계속 Kafka에 쌓는" 운영을 정상 시나리오로 쓰기 때문에
     * 중지된 Sink를 빨간 중단으로 표시하면 안 된다.
     */
    private ProcessItem connectorItem(String name, List<ConnectorStatusResponse> statuses, String type) {
        List<ConnectorStatusResponse> matched = statuses.stream()
                .filter(status -> type.equalsIgnoreCase(status.type()))
                .toList();
        if (matched.isEmpty()) {
            return new ProcessItem(name, ProcessStatus.UNKNOWN, "등록된 커넥터 없음");
        }

        int running = 0;
        int stopped = 0;
        int failedTasks = 0;
        int failedConnectors = 0;
        for (ConnectorStatusResponse status : matched) {
            List<ConnectorTaskStatus> tasks = status.tasks() == null ? List.of() : status.tasks();
            long failed = tasks.stream().filter(task -> "FAILED".equalsIgnoreCase(task.state())).count();
            failedTasks += (int) failed;
            String state = status.connector() == null ? null : status.connector().state();
            if ("RUNNING".equalsIgnoreCase(state) && failed == 0) {
                running++;
            } else if ("PAUSED".equalsIgnoreCase(state) || "STOPPED".equalsIgnoreCase(state)) {
                stopped++;
            } else {
                failedConnectors++;
            }
        }

        ProcessStatus status;
        if (failedConnectors > 0 || failedTasks > 0) {
            status = running > 0 ? ProcessStatus.DEGRADED : ProcessStatus.DOWN;
        } else if (running == 0) {
            status = ProcessStatus.STOPPED;
        } else {
            status = ProcessStatus.UP;
        }

        String detail = "%d/%d 실행".formatted(running, matched.size())
                + (stopped > 0 ? " · 중지 %d".formatted(stopped) : "")
                + (failedTasks > 0 ? " · 실패 태스크 %d".formatted(failedTasks) : "");
        return new ProcessItem(name, status, detail);
    }

    // ----------------------------------------------------------------- NiFi

    private ProcessGroup nifiGroup() {
        List<ProcessItem> items = new ArrayList<>();

        NifiFlowStatusResponse flow = null;
        try {
            flow = nifiClient.getRootFlowStatus();
        } catch (RuntimeException ex) {
            log.debug("NiFi 상태 조회 실패: {}", ex.getMessage());
        }

        if (flow == null) {
            items.add(new ProcessItem("NiFi 서비스", ProcessStatus.DOWN, "REST 응답 없음"));
            items.add(new ProcessItem("NiFi 플로우 처리", ProcessStatus.UNKNOWN, "NiFi 응답 없음"));
        } else {
            FlowCounts counts = countFlow(flow);
            items.add(new ProcessItem(
                    "NiFi 서비스",
                    ProcessStatus.UP,
                    "프로세스 그룹 %d · 프로세서 %d".formatted(counts.groups(), counts.processors())));

            Integer errorBulletins = countErrorBulletins();
            ProcessStatus flowStatus = errorBulletins == null
                    ? ProcessStatus.UNKNOWN
                    : errorBulletins > 0 ? ProcessStatus.DEGRADED : ProcessStatus.UP;
            String flowDetail = "활성 스레드 %d".formatted(counts.activeThreads())
                    + (errorBulletins == null ? " · 오류 확인 불가" : " · 최근 오류 %d건".formatted(errorBulletins));
            items.add(new ProcessItem("NiFi 플로우 처리", flowStatus, flowDetail));
        }

        items.add(loadCollectorItem());
        return ProcessGroup.of("NIFI", "ETL", items);
    }

    /** bulletin 조회 실패는 0건과 구분해야 하므로 null로 돌려준다. */
    private Integer countErrorBulletins() {
        try {
            NifiBulletinBoardResponse response = nifiClient.getBulletins(0L);
            var board = response == null ? null : response.bulletinBoard();
            var bulletins = board == null || board.bulletins() == null ? List.<NifiBulletinBoardResponse.BulletinEntity>of() : board.bulletins();
            return (int) bulletins.stream()
                    .filter(entity -> entity.bulletin() != null && "ERROR".equalsIgnoreCase(entity.bulletin().level()))
                    .count();
        } catch (RuntimeException ex) {
            log.debug("NiFi bulletin 조회 실패: {}", ex.getMessage());
            return null;
        }
    }

    /**
     * NiFi 적재 건수를 만드는 건 NiFi가 아니라 이 앱의 60초 주기 수집이다(NiFi엔 실행
     * 이력 개념이 없어 카운터 증가분을 직접 관측한다). 이게 멈추면 화면 숫자만 조용히
     * 멈추므로 프로세스 목록에 같이 세워 둔다.
     *
     * <p>판단 근거는 수집 루프가 마지막으로 완주한 시각이다. 적재 결과 테이블로 보면
     * "적재가 없어서 조용한 것"과 "수집이 죽은 것"을 구분할 수 없다.
     */
    private ProcessItem loadCollectorItem() {
        // 하트비트(system_heartbeat, key=nifi-counter)에서 마지막 완주 시각을 읽는다(U5). JVM 필드가
        // 아니라 DB라 재기동을 넘어 유지된다. TIMESTAMPTZ 라 OffsetDateTime.
        OffsetDateTime lastRun = heartbeat.lastBeat("nifi-counter");
        if (lastRun == null) {
            // 기동 직후 첫 주기(60초) 전. 아직 모른다는 뜻이지 죽은 게 아니다.
            return new ProcessItem("적재 지표 수집", ProcessStatus.UNKNOWN, "첫 수집 주기 대기 중");
        }
        Duration elapsed = Duration.between(lastRun.toInstant(), Instant.now());
        int deadSeconds = settings.getInt(SettingKey.HEALTH_DEAD_SECONDS_NIFI);
        int staleSeconds = settings.getInt(SettingKey.HEALTH_STALE_SECONDS_NIFI);
        Duration deadAfter = Duration.ofSeconds(deadSeconds);
        Duration staleAfter = Duration.ofSeconds(staleSeconds);
        ProcessStatus status = elapsed.compareTo(deadAfter) > 0
                ? ProcessStatus.DOWN
                : elapsed.compareTo(staleAfter) > 0 ? ProcessStatus.DEGRADED : ProcessStatus.UP;
        // OffsetDateTime(DB는 UTC 오프셋)을 앱 타임존(KST)으로 변환해 표시한다.
        var localBeat = lastRun.atZoneSameInstant(java.time.ZoneId.systemDefault());
        // 원본 5-4 "정상 판정 기준 불명 → 허용 지연을 툴팁으로": 임계값을 응답에 실어 보낸다.
        return ProcessItem.withThresholds("적재 지표 수집", status,
                "마지막 수집 " + HEARTBEAT_FORMAT.format(localBeat),
                localBeat.toLocalDateTime(), staleSeconds, deadSeconds);
    }

    private FlowCounts countFlow(NifiFlowStatusResponse flow) {
        var status = flow.processGroupStatus();
        var snapshot = status == null ? null : status.aggregateSnapshot();
        if (snapshot == null) {
            return new FlowCounts(0, 0, 0);
        }
        FlowCounts root = new FlowCounts(0, countProcessors(snapshot.processorStatusSnapshots()),
                countActiveThreads(snapshot.processorStatusSnapshots()));
        return addGroups(root, snapshot.processGroupStatusSnapshots());
    }

    private FlowCounts addGroups(FlowCounts accumulated, List<ProcessGroupStatusEntry> groups) {
        if (groups == null) {
            return accumulated;
        }
        FlowCounts current = accumulated;
        for (ProcessGroupStatusEntry entry : groups) {
            var snapshot = entry.processGroupStatusSnapshot();
            if (snapshot == null) {
                continue;
            }
            current = new FlowCounts(
                    current.groups() + 1,
                    current.processors() + countProcessors(snapshot.processorStatusSnapshots()),
                    current.activeThreads() + countActiveThreads(snapshot.processorStatusSnapshots()));
            current = addGroups(current, snapshot.processGroupStatusSnapshots());
        }
        return current;
    }

    private int countProcessors(List<ProcessorStatusEntry> processors) {
        return processors == null ? 0 : (int) processors.stream()
                .filter(entry -> entry.processorStatusSnapshot() != null)
                .count();
    }

    private int countActiveThreads(List<ProcessorStatusEntry> processors) {
        return processors == null ? 0 : processors.stream()
                .filter(entry -> entry.processorStatusSnapshot() != null)
                .mapToInt(entry -> entry.processorStatusSnapshot().activeThreads())
                .sum();
    }

    // -------------------------------------------------------------- Airflow

    private ProcessGroup airflowGroup() {
        Optional<AirflowHealthResponse> health = airflowHealthClient.getHealth();
        if (health.isEmpty()) {
            return ProcessGroup.of("AIRFLOW", "Airflow", List.of(
                    new ProcessItem("API 서버", ProcessStatus.DOWN, "health 응답 없음"),
                    new ProcessItem("스케줄러", ProcessStatus.UNKNOWN, "API 서버 응답 없음"),
                    new ProcessItem("DAG 프로세서", ProcessStatus.UNKNOWN, "API 서버 응답 없음")));
        }

        AirflowHealthResponse response = health.get();
        List<ProcessItem> items = new ArrayList<>();
        items.add(new ProcessItem("API 서버", ProcessStatus.UP, "health 응답 정상"));
        items.add(componentItem("스케줄러",
                response.scheduler() == null ? null : response.scheduler().status(),
                response.scheduler() == null ? null : response.scheduler().latestHeartbeat()));
        items.add(componentItem("DAG 프로세서",
                response.dagProcessor() == null ? null : response.dagProcessor().status(),
                response.dagProcessor() == null ? null : response.dagProcessor().latestHeartbeat()));

        // 이 배포에 없는 triggerer는 Airflow가 status=null로 내려준다. 미구성 컴포넌트는
        // 인프라 상태에 표시하지 않고, 실제로 구성되어 상태가 있을 때만 노출한다.
        String triggererStatus = response.triggerer() == null ? null : response.triggerer().status();
        if (StringUtils.hasText(triggererStatus)) {
            items.add(componentItem("트리거러", triggererStatus,
                    response.triggerer().latestHeartbeat()));
        }

        return ProcessGroup.of("AIRFLOW", "Airflow", items);
    }

    private ProcessItem componentItem(String name, String status, String heartbeat) {
        if (!StringUtils.hasText(status)) {
            // 상태를 물어봤는데 안 준 것 - "안 쓴다"와 다르므로 configured=true 를 유지한다.
            return new ProcessItem(name, ProcessStatus.UNKNOWN, "상태 미보고");
        }
        ProcessStatus mapped = AIRFLOW_HEALTHY.equalsIgnoreCase(status) ? ProcessStatus.UP : ProcessStatus.DOWN;
        String detail = StringUtils.hasText(heartbeat) ? "heartbeat " + formatHeartbeat(heartbeat) : status;
        // Airflow 자체 판정이라 우리 임계값은 없다. 시각만 실어서 툴팁이 "마지막 성공"을 쓰게 한다.
        return ProcessItem.withThresholds(name, mapped, detail, parseHeartbeat(heartbeat), null, null);
    }

    /** 툴팁이 쓸 구조화된 시각. 파싱 실패는 null(표시 생략)로 떨어뜨린다. */
    private java.time.LocalDateTime parseHeartbeat(String isoTimestamp) {
        if (!StringUtils.hasText(isoTimestamp)) {
            return null;
        }
        try {
            return java.time.OffsetDateTime.parse(isoTimestamp)
                    .atZoneSameInstant(java.time.ZoneId.systemDefault())
                    .toLocalDateTime();
        } catch (java.time.format.DateTimeParseException ex) {
            return null;
        }
    }

    /** Airflow는 UTC ISO 시각을 주므로 화면 표기용으로만 서버 시간대로 옮긴다. */
    private String formatHeartbeat(String isoTimestamp) {
        try {
            return HEARTBEAT_FORMAT.format(java.time.OffsetDateTime.parse(isoTimestamp)
                    .atZoneSameInstant(java.time.ZoneId.systemDefault()));
        } catch (java.time.format.DateTimeParseException ex) {
            return isoTimestamp;
        }
    }

    private record FlowCounts(int groups, int processors, int activeThreads) {
    }
}
