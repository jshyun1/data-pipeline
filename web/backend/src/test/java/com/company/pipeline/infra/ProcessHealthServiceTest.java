package com.company.pipeline.infra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.KafkaConnectClientException;
import com.company.pipeline.connector.dto.ConnectorState;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import com.company.pipeline.connector.dto.ConnectorTaskStatus;
import com.company.pipeline.connector.dto.KafkaConnectWorkerInfo;
import com.company.pipeline.infra.dto.AirflowHealthResponse;
import com.company.pipeline.infra.dto.ProcessHealthResponse.ProcessGroup;
import com.company.pipeline.infra.dto.ProcessHealthResponse.ProcessItem;
import com.company.pipeline.infra.dto.ProcessStatus;
import com.company.pipeline.heartbeat.HeartbeatService;
import com.company.pipeline.monitoring.KafkaBrokerHealthChecker;
import com.company.pipeline.monitoring.KafkaBrokerProperties;
import com.company.pipeline.settings.SettingKey;
import com.company.pipeline.settings.SettingService;
import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.NifiClientException;
import com.company.pipeline.nifi.dto.NifiBulletinBoardResponse;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ProcessHealthServiceTest {

    @Mock
    private KafkaBrokerHealthChecker kafkaBrokerHealthChecker;
    @Mock
    private KafkaConnectClient kafkaConnectClient;
    @Mock
    private NifiClient nifiClient;
    @Mock
    private AirflowHealthClient airflowHealthClient;
    @Mock
    private HeartbeatService heartbeat;
    @Mock
    private SettingService settings;

    private ProcessHealthService service;

    @BeforeEach
    void setUp() {
        service = new ProcessHealthService(
                kafkaBrokerHealthChecker,
                new KafkaBrokerProperties("kafka:9092"),
                kafkaConnectClient,
                nifiClient,
                airflowHealthClient,
                heartbeat,
                settings);

        // 각 테스트가 관심 있는 축만 세팅하도록, 기본은 "전부 정상"으로 둔다.
        when(kafkaBrokerHealthChecker.isHealthy()).thenReturn(true);
        when(kafkaConnectClient.getWorkerInfo()).thenReturn(new KafkaConnectWorkerInfo("7.7.1-ccs", "abc", "cluster1"));
        when(kafkaConnectClient.listConnectors()).thenReturn(List.of("pg-source", "jdbc-sink"));
        when(kafkaConnectClient.getStatus("pg-source")).thenReturn(runningConnector("pg-source", "source"));
        when(kafkaConnectClient.getStatus("jdbc-sink")).thenReturn(runningConnector("jdbc-sink", "sink"));
        when(nifiClient.getRootFlowStatus()).thenReturn(flowWithOneGroup());
        when(nifiClient.getBulletins(anyLong())).thenReturn(new NifiBulletinBoardResponse(
                new NifiBulletinBoardResponse.BulletinBoard(List.of())));
        when(heartbeat.lastBeat("nifi-counter")).thenReturn(OffsetDateTime.now());
        when(settings.getInt(SettingKey.HEALTH_STALE_SECONDS_NIFI)).thenReturn(180);
        when(settings.getInt(SettingKey.HEALTH_DEAD_SECONDS_NIFI)).thenReturn(600);
        when(airflowHealthClient.getHealth()).thenReturn(Optional.of(healthyAirflow()));
    }

    @Test
    void collect_allHealthy_reportsThreeGroupsUp() {
        var response = service.collect();

        assertThat(response.groups()).extracting(ProcessGroup::key).containsExactly("CDC", "NIFI", "AIRFLOW");
        assertThat(response.groups()).extracting(ProcessGroup::status)
                .containsOnly(ProcessStatus.UP);
    }

    @Test
    void collect_brokerDown_marksOnlyCdcGroupDown() {
        when(kafkaBrokerHealthChecker.isHealthy()).thenReturn(false);

        var response = service.collect();

        assertThat(group(response.groups(), "CDC").status()).isEqualTo(ProcessStatus.DOWN);
        assertThat(item(response.groups(), "CDC", "Kafka Broker").detail()).isEqualTo("kafka:9092");
        assertThat(group(response.groups(), "NIFI").status()).isEqualTo(ProcessStatus.UP);
        assertThat(group(response.groups(), "AIRFLOW").status()).isEqualTo(ProcessStatus.UP);
    }

    @Test
    void collect_workerDown_leavesConnectorsUnknownInsteadOfDown() {
        when(kafkaConnectClient.getWorkerInfo()).thenThrow(new KafkaConnectClientException("connection refused", null));

        var response = service.collect();

        assertThat(item(response.groups(), "CDC", "Kafka Connect Worker").status()).isEqualTo(ProcessStatus.DOWN);
        assertThat(item(response.groups(), "CDC", "Source Connector (Debezium)").status())
                .isEqualTo(ProcessStatus.UNKNOWN);
        assertThat(item(response.groups(), "CDC", "Sink Connector (JDBC)").status()).isEqualTo(ProcessStatus.UNKNOWN);
    }

    @Test
    void collect_failedSinkTask_marksConnectorDegradedEvenWhenConnectorStateIsRunning() {
        var sinkWithFailedTask = new ConnectorStatusResponse(
                "jdbc-sink",
                new ConnectorState("RUNNING", "worker1"),
                List.of(new ConnectorTaskStatus(0, "RUNNING", "worker1", null),
                        new ConnectorTaskStatus(1, "FAILED", "worker1", "boom")),
                "sink");
        when(kafkaConnectClient.listConnectors()).thenReturn(List.of("jdbc-sink", "jdbc-sink-2"));
        when(kafkaConnectClient.getStatus("jdbc-sink")).thenReturn(sinkWithFailedTask);
        when(kafkaConnectClient.getStatus("jdbc-sink-2")).thenReturn(runningConnector("jdbc-sink-2", "sink"));

        var response = service.collect();

        ProcessItem sink = item(response.groups(), "CDC", "Sink Connector (JDBC)");
        assertThat(sink.status()).isEqualTo(ProcessStatus.DEGRADED);
        assertThat(sink.detail()).isEqualTo("1/2 실행 · 실패 태스크 1");
    }

    @Test
    void collect_noConnectorsRegistered_isUnknownNotDown() {
        when(kafkaConnectClient.listConnectors()).thenReturn(List.of());

        var response = service.collect();

        ProcessItem source = item(response.groups(), "CDC", "Source Connector (Debezium)");
        assertThat(source.status()).isEqualTo(ProcessStatus.UNKNOWN);
        assertThat(source.detail()).isEqualTo("등록된 커넥터 없음");
    }

    @Test
    void collect_nifiDown_reportsServiceDownAndFlowUnknown() {
        when(nifiClient.getRootFlowStatus()).thenThrow(new NifiClientException("timeout", null));

        var response = service.collect();

        assertThat(item(response.groups(), "NIFI", "NiFi 서비스").status()).isEqualTo(ProcessStatus.DOWN);
        assertThat(item(response.groups(), "NIFI", "NiFi 플로우 처리").status()).isEqualTo(ProcessStatus.UNKNOWN);
    }

    @Test
    void collect_nifiErrorBulletins_marksFlowDegraded() {
        var bulletin = new NifiBulletinBoardResponse.Bulletin(
                1L, "Log Message", "group1", "processor1", "PutDatabaseRecord", "ERROR", "insert 실패", "10:00:00 KST");
        when(nifiClient.getBulletins(anyLong())).thenReturn(new NifiBulletinBoardResponse(
                new NifiBulletinBoardResponse.BulletinBoard(
                        List.of(new NifiBulletinBoardResponse.BulletinEntity(1L, "group1", "processor1", bulletin)))));

        var response = service.collect();

        ProcessItem flow = item(response.groups(), "NIFI", "NiFi 플로우 처리");
        assertThat(flow.status()).isEqualTo(ProcessStatus.DEGRADED);
        assertThat(flow.detail()).contains("최근 오류 1건");
    }

    @Test
    void collect_collectorLoopStalled_marksLoadCollectorDown() {
        when(heartbeat.lastBeat("nifi-counter")).thenReturn(OffsetDateTime.now().minusMinutes(30));

        var response = service.collect();

        assertThat(item(response.groups(), "NIFI", "적재 지표 수집").status()).isEqualTo(ProcessStatus.DOWN);
    }

    /**
     * 적재가 한 건도 없어도 수집 루프는 정상이다. 예전엔 적재 결과 테이블의 마지막 시각으로
     * 판단해서 NiFi 재기동 후 조용한 몇 시간을 "수집 중단"으로 오보했다.
     */
    @Test
    void collect_collectorRunningWithoutAnyLoad_staysUp() {
        when(heartbeat.lastBeat("nifi-counter")).thenReturn(OffsetDateTime.now().minusSeconds(40));

        var response = service.collect();

        ProcessItem collector = item(response.groups(), "NIFI", "적재 지표 수집");
        assertThat(collector.status()).isEqualTo(ProcessStatus.UP);
        assertThat(group(response.groups(), "NIFI").status()).isEqualTo(ProcessStatus.UP);
    }

    @Test
    void collect_beforeFirstCollectorCycle_isUnknownNotDown() {
        when(heartbeat.lastBeat("nifi-counter")).thenReturn(null);

        var response = service.collect();

        ProcessItem collector = item(response.groups(), "NIFI", "적재 지표 수집");
        assertThat(collector.status()).isEqualTo(ProcessStatus.UNKNOWN);
        assertThat(collector.detail()).isEqualTo("첫 수집 주기 대기 중");
    }

    /**
     * 이 스택은 "Sink만 멈추고 Source는 계속 쌓기"를 정상 운영 시나리오로 쓴다 - 일부러
     * 멈춘 Sink가 CDC 그룹 전체를 빨갛게 만들면 안 된다.
     */
    @Test
    void collect_deliberatelyStoppedSink_isStoppedAndDoesNotAlarmTheGroup() {
        when(kafkaConnectClient.listConnectors()).thenReturn(List.of("pg-source", "jdbc-sink", "jdbc-sink-2"));
        when(kafkaConnectClient.getStatus("jdbc-sink")).thenReturn(stoppedConnector("jdbc-sink"));
        when(kafkaConnectClient.getStatus("jdbc-sink-2")).thenReturn(stoppedConnector("jdbc-sink-2"));

        var response = service.collect();

        ProcessItem sink = item(response.groups(), "CDC", "Sink Connector (JDBC)");
        assertThat(sink.status()).isEqualTo(ProcessStatus.STOPPED);
        assertThat(sink.detail()).isEqualTo("0/2 실행 · 중지 2");
        assertThat(group(response.groups(), "CDC").status()).isEqualTo(ProcessStatus.UP);
    }

    @Test
    void collect_unassignedConnector_isDownNotStopped() {
        var unassigned = new ConnectorStatusResponse(
                "jdbc-sink", new ConnectorState("UNASSIGNED", null), List.of(), "sink");
        when(kafkaConnectClient.listConnectors()).thenReturn(List.of("jdbc-sink"));
        when(kafkaConnectClient.getStatus("jdbc-sink")).thenReturn(unassigned);

        var response = service.collect();

        assertThat(item(response.groups(), "CDC", "Sink Connector (JDBC)").status()).isEqualTo(ProcessStatus.DOWN);
        assertThat(group(response.groups(), "CDC").status()).isEqualTo(ProcessStatus.DOWN);
    }

    @Test
    void collect_airflowApiServerDown_leavesComponentsUnknown() {
        when(airflowHealthClient.getHealth()).thenReturn(Optional.empty());

        var response = service.collect();

        ProcessGroup airflow = group(response.groups(), "AIRFLOW");
        assertThat(airflow.status()).isEqualTo(ProcessStatus.DOWN);
        assertThat(item(response.groups(), "AIRFLOW", "API 서버").status()).isEqualTo(ProcessStatus.DOWN);
        assertThat(item(response.groups(), "AIRFLOW", "스케줄러").status()).isEqualTo(ProcessStatus.UNKNOWN);
    }

    /** 메타데이터 DB는 API 서버가 응답하는 것으로 이미 확인되는 값이라 목록에서 뺐다. */
    @Test
    void collect_airflowGroup_doesNotListMetadataDatabase() {
        var response = service.collect();

        assertThat(group(response.groups(), "AIRFLOW").processes())
                .extracting(ProcessItem::name)
                .doesNotContain("메타데이터 DB");
    }

    @Test
    void collect_unhealthyScheduler_marksSchedulerDown() {
        when(airflowHealthClient.getHealth()).thenReturn(Optional.of(new AirflowHealthResponse(
                new AirflowHealthResponse.Metadatabase("healthy"),
                new AirflowHealthResponse.Scheduler("unhealthy", "2026-07-30T04:18:17.557964+00:00"),
                new AirflowHealthResponse.Triggerer(null, null),
                new AirflowHealthResponse.DagProcessor("healthy", "2026-07-30T04:18:21.113034+00:00"))));

        var response = service.collect();

        assertThat(item(response.groups(), "AIRFLOW", "스케줄러").status()).isEqualTo(ProcessStatus.DOWN);
        assertThat(group(response.groups(), "AIRFLOW").status()).isEqualTo(ProcessStatus.DOWN);
    }

    @Test
    void collect_triggererNotConfigured_isNotListed() {
        var response = service.collect();

        assertThat(group(response.groups(), "AIRFLOW").processes())
                .extracting(ProcessItem::name)
                .doesNotContain("트리거러");
        assertThat(group(response.groups(), "AIRFLOW").status()).isEqualTo(ProcessStatus.UP);
    }

    private static AirflowHealthResponse healthyAirflow() {
        return new AirflowHealthResponse(
                new AirflowHealthResponse.Metadatabase("healthy"),
                new AirflowHealthResponse.Scheduler("healthy", "2026-07-30T04:18:17.557964+00:00"),
                new AirflowHealthResponse.Triggerer(null, null),
                new AirflowHealthResponse.DagProcessor("healthy", "2026-07-30T04:18:21.113034+00:00"));
    }

    /** 화면에서 중지시킨 Sink의 실제 응답 형태(state=STOPPED, tasks 비어 있음). */
    private static ConnectorStatusResponse stoppedConnector(String name) {
        return new ConnectorStatusResponse(name, new ConnectorState("STOPPED", "kafka-connect:8083"), List.of(), "sink");
    }

    private static ConnectorStatusResponse runningConnector(String name, String type) {
        return new ConnectorStatusResponse(
                name,
                new ConnectorState("RUNNING", "worker1"),
                List.of(new ConnectorTaskStatus(0, "RUNNING", "worker1", null)),
                type);
    }

    private static NifiFlowStatusResponse flowWithOneGroup() {
        var processor = new NifiFlowStatusResponse.ProcessorStatus("p1", "PutDatabaseRecord", "PutDatabaseRecord", 2);
        var processorEntry = new NifiFlowStatusResponse.ProcessorStatusEntry(processor);
        var group = new NifiFlowStatusResponse.ProcessGroupStatusSnapshot(
                "g1", "logfile", List.of(processorEntry), List.of());
        var aggregate = new NifiFlowStatusResponse.AggregateSnapshot(
                List.of(), List.of(new NifiFlowStatusResponse.ProcessGroupStatusEntry(group)));
        return new NifiFlowStatusResponse(new NifiFlowStatusResponse.ProcessGroupStatus(aggregate));
    }

    private static ProcessGroup group(List<ProcessGroup> groups, String key) {
        return groups.stream().filter(candidate -> candidate.key().equals(key)).findFirst().orElseThrow();
    }

    private static ProcessItem item(List<ProcessGroup> groups, String groupKey, String name) {
        return group(groups, groupKey).processes().stream()
                .filter(candidate -> candidate.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError(name + " 항목이 없습니다."));
    }

    @Test
    void collect_connectorStatusCallFails_ignoresThatConnectorOnly() {
        when(kafkaConnectClient.getStatus(anyString())).thenThrow(new KafkaConnectClientException("boom", null));

        var response = service.collect();

        assertThat(item(response.groups(), "CDC", "Source Connector (Debezium)").detail())
                .isEqualTo("등록된 커넥터 없음");
        assertThat(item(response.groups(), "CDC", "Kafka Connect Worker").status()).isEqualTo(ProcessStatus.UP);
    }
}
