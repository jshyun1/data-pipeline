package com.company.pipeline.pipeline;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.connection.ConnectionRepository;
import com.company.pipeline.connection.PipelineConnection;
import com.company.pipeline.connector.ConnectorConfigRenderer;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.KafkaConnectTimeoutException;
import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
import com.company.pipeline.connector.dto.ConnectorTaskStatus;
import com.company.pipeline.connector.dto.LogSinkConnectorRequest;
import com.company.pipeline.connector.dto.RenderedConnectorConfig;
import com.company.pipeline.connector.dto.SinkConnectorRequest;
import com.company.pipeline.connector.dto.SourceConnectorRequest;
import com.company.pipeline.logpipeline.FilebeatConfigRenderer;
import com.company.pipeline.logpipeline.FilebeatInputFileService;
import com.company.pipeline.logpipeline.LogPipelineSource;
import com.company.pipeline.logpipeline.LogPipelineSourceRepository;
import com.company.pipeline.pipeline.dto.PipelineResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * docs/kafka-webservice-design.md §6.2 흐름 그대로:
 * validate -> connector config 생성 -> Kafka Connect 등록 -> 상태 조회 -> metadata 갱신.
 * 실패하면 pipeline 상태를 FAILED로 바꾸고 command_history에 에러를 남긴다.
 *
 * 의도적으로 이 메서드 전체를 하나의 @Transactional로 묶지 않는다: Kafka Connect REST
 * 호출은 DB 트랜잭션으로 되돌릴 수 없는 외부 부작용이라, 중간에 실패해도 "여기까지는
 * 실제로 벌어졌다"는 각 단계의 DB 기록(DEPLOYING -> 커넥터별 등록 결과 -> FAILED/DEPLOYED)이
 * 그 자체로 독립적으로 남아야 한다. 하나로 묶으면 실패 시 FAILED 상태 기록 자체도
 * 롤백되어버리는 문제가 있다.
 */
@Service
public class PipelineDeployService {

    private static final Logger log = LoggerFactory.getLogger(PipelineDeployService.class);

    // Oracle Debezium은 최초 시작 시 DB 접속·스키마 히스토리 준비로 수 초 이상 걸릴 수
    // 있다. Connector만 RUNNING이고 task가 아직 비어 있는 정상 초기화 구간을 실패로
    // 오판하지 않도록 최대 60초를 기다린다.
    private static final int STATUS_VERIFY_ATTEMPTS = 120;
    private static final long STATUS_VERIFY_INTERVAL_MILLIS = 500L;

    private final PipelineDefinitionRepository pipelineDefinitionRepository;
    private final PipelineConnectorRepository pipelineConnectorRepository;
    private final PipelineCommandHistoryRecorder commandHistoryRecorder;
    private final ConnectionRepository connectionRepository;
    private final PasswordCryptoService passwordCryptoService;
    private final ConnectorConfigRenderer connectorConfigRenderer;
    private final KafkaConnectClient kafkaConnectClient;
    private final ObjectMapper objectMapper;
    private final LogPipelineSourceRepository logPipelineSourceRepository;
    private final FilebeatConfigRenderer filebeatConfigRenderer;
    private final FilebeatInputFileService filebeatInputFileService;
    private final DeltaTargetTableService deltaTargetTableService;
    private final ConcurrentHashMap<Long, ReentrantLock> pipelineLocks = new ConcurrentHashMap<>();

    /**
     * Debezium task가 최초 RUNNING을 보고한 직후에도 Oracle snapshot 준비(테이블 lock 등)에서
     * 실패할 수 있다. 이 구간을 통과하기 전에는 파이프라인을 DEPLOYED로 확정하지 않는다.
     */
    @Value("${pipeline.cdc.start-stability-millis:10000}")
    private long startStabilityMillis = 10_000L;

    @Value("${pipeline.cdc.start-stability-check-interval-millis:500}")
    private long startStabilityCheckIntervalMillis = 500L;

    public PipelineDeployService(PipelineDefinitionRepository pipelineDefinitionRepository,
            PipelineConnectorRepository pipelineConnectorRepository,
            PipelineCommandHistoryRecorder commandHistoryRecorder,
            ConnectionRepository connectionRepository,
            PasswordCryptoService passwordCryptoService,
            ConnectorConfigRenderer connectorConfigRenderer,
            KafkaConnectClient kafkaConnectClient,
            ObjectMapper objectMapper,
            LogPipelineSourceRepository logPipelineSourceRepository,
            FilebeatConfigRenderer filebeatConfigRenderer,
            FilebeatInputFileService filebeatInputFileService,
            DeltaTargetTableService deltaTargetTableService) {
        this.pipelineDefinitionRepository = pipelineDefinitionRepository;
        this.pipelineConnectorRepository = pipelineConnectorRepository;
        this.commandHistoryRecorder = commandHistoryRecorder;
        this.connectionRepository = connectionRepository;
        this.passwordCryptoService = passwordCryptoService;
        this.connectorConfigRenderer = connectorConfigRenderer;
        this.kafkaConnectClient = kafkaConnectClient;
        this.objectMapper = objectMapper;
        this.logPipelineSourceRepository = logPipelineSourceRepository;
        this.filebeatConfigRenderer = filebeatConfigRenderer;
        this.filebeatInputFileService = filebeatInputFileService;
        this.deltaTargetTableService = deltaTargetTableService;
    }

    public PipelineResponse deploy(Long pipelineId) {
        return withPipelineLock(pipelineId, () -> deployLocked(pipelineId));
    }

    private PipelineResponse deployLocked(Long pipelineId) {
        PipelineDefinition pipeline = pipelineDefinitionRepository.findById(pipelineId)
                .orElseThrow(() -> new PipelineNotFoundException(pipelineId));
        // 배포는 두 타입 모두 "만들되 실행하지 않는" 단계다 - 실행 지시는 오직 Airflow가
        // 한다는 원칙(NiFi의 autoResumeState=false와 같은 규칙)을 로그 파이프라인에도
        // 적용한다. 예전에는 LOG_FILE만 배포 즉시 적재가 시작돼서, 아무도 시작을
        // 지시하지 않았는데 데이터가 흐르는 상태가 만들어졌다.
        String command = "PREPARE";

        pipeline.setStatus(PipelineStatus.DEPLOYING);
        pipelineDefinitionRepository.save(pipeline);

        try {
            if ("LOG_FILE".equals(pipeline.getPipelineType())) {
                deployLogFilePipeline(pipeline);
            } else {
                deployTableCdcPipeline(pipeline);
            }

            pipeline.setStatus(PipelineStatus.READY);
            pipelineDefinitionRepository.save(pipeline);
            commandHistoryRecorder.record(pipeline.getId(), command, "SUCCESS", null);
        } catch (Exception ex) {
            pipeline.setStatus(PipelineStatus.FAILED);
            pipelineDefinitionRepository.save(pipeline);
            commandHistoryRecorder.record(pipeline.getId(), command, "FAILED", ex.getMessage());
            if (ex instanceof BusinessException businessException) {
                throw businessException;
            }
            throw new BusinessException(ErrorCode.INTERNAL_ERROR,
                    "파이프라인 실행 대기 준비 실패: " + ex.getMessage());
        }

        return PipelineResponse.from(pipeline, pipelineConnectorRepository.findByPipelineId(pipeline.getId()));
    }

    private void deployTableCdcPipeline(PipelineDefinition pipeline) {
        PipelineConnection source = findConnectionOrThrow(pipeline.getSourceConnectionId());
        PipelineConnection target = findConnectionOrThrow(pipeline.getTargetConnectionId());

        // 델타 적재는 커넥터 등록 전에 타깃 테이블을 준비/검증한다(APPEND: 구분컬럼이 맨 앞인 뼈대
        // 생성, UPSERT: 소스 PK 와 같은 키 존재 확인). 여기서 실패하면 아직 아무 커넥터도 등록되지
        // 않은 상태라 되돌릴 것이 없다.
        if (PipelineLoadMode.from(pipeline.getLoadMode()).isDelta()) {
            deltaTargetTableService.ensureDeltaTable(pipeline, target);
        }

        RenderedConnectorConfig sourceConfig = connectorConfigRenderer.renderSource(new SourceConnectorRequest(
                pipeline.getId(), source.getDbType(), source.getHost(), source.getPort(),
                source.getUsername(), passwordCryptoService.decrypt(source.getEncryptedPassword()),
                source.getDatabaseName(), source.getServiceName(),
                pipeline.getSourceSchema(), pipeline.getSourceTable(), pipeline.getTopicName(),
                PipelineSnapshotMode.from(pipeline.getSnapshotMode()).name(), pipeline.getExcludedColumns(),
                pipeline.getMaskedColumns()));
        prepareStoppedConnector(pipeline.getId(), "SOURCE", sourceConfig);

        RenderedConnectorConfig sinkConfig = connectorConfigRenderer.renderSink(new SinkConnectorRequest(
                pipeline.getId(), target.getDbType(), target.getHost(), target.getPort(),
                target.getUsername(), passwordCryptoService.decrypt(target.getEncryptedPassword()),
                target.getDatabaseName(), target.getServiceName(),
                pipeline.getTargetSchema(), pipeline.getTargetTable(),
                source.getDbType(), pipeline.getSourceSchema(), pipeline.getSourceTable(),
                pipeline.getTopicName(), Boolean.TRUE.equals(pipeline.getDeleteEnabled()),
                pipeline.getLoadMode(), pipeline.getDeltaOpColumn()));
        prepareStoppedConnector(pipeline.getId(), "SINK", sinkConfig);
    }

    /**
     * 로그 파이프라인은 소스 커넥터가 없다 - Filebeat가 소스 역할을 하고(output.kafka로
     * Kafka Connect 없이 직접 Kafka에 씀), 여기선 싱크 커넥터 등록과 Filebeat 입력 설정
     * 파일 쓰기만 한다. 파일 쓰기도 Kafka Connect REST 호출과 마찬가지로 되돌릴 수 없는
     * 외부 부작용이라 deploy() 전체를 감싸는 트랜잭션 밖에서 처리한다(클래스 상단 설명 참고).
     */
    private void deployLogFilePipeline(PipelineDefinition pipeline) {
        LogPipelineSource source = logPipelineSourceRepository.findByPipelineId(pipeline.getId())
                .orElseThrow(() -> new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "로그 소스 설정을 찾을 수 없습니다. pipelineId=" + pipeline.getId()));
        PipelineConnection target = findConnectionOrThrow(pipeline.getTargetConnectionId());

        RenderedConnectorConfig sinkConfig = connectorConfigRenderer.renderLogSink(new LogSinkConnectorRequest(
                pipeline.getId(), target.getDbType(), target.getHost(), target.getPort(),
                target.getUsername(), passwordCryptoService.decrypt(target.getEncryptedPassword()),
                target.getDatabaseName(), target.getServiceName(),
                pipeline.getTargetSchema(), pipeline.getTargetTable(), source.getTopicName()));
        prepareStoppedConnector(pipeline.getId(), "SINK", sinkConfig);

        String filebeatYaml = filebeatConfigRenderer.render(pipeline.getId(), source);
        filebeatInputFileService.write(pipeline.getId(), filebeatYaml);
    }

    private void deployConnector(Long pipelineId, String role, RenderedConnectorConfig rendered) {
        kafkaConnectClient.upsertConfig(rendered.connectorName(), rendered.config());

        PipelineConnector connector = pipelineConnectorRepository
                .findByPipelineIdAndConnectorRole(pipelineId, role)
                .orElseGet(() -> new PipelineConnector(pipelineId, role,
                        rendered.connectorName(), rendered.connectorClass(), "{}"));
        connector.setConnectorName(rendered.connectorName());
        connector.setConnectorClass(rendered.connectorClass());
        connector.setConnectorConfigJson(toJson(rendered.config()));
        connector.setDeployedAt(LocalDateTime.now());

        refreshConnectorStatus(connector);
        pipelineConnectorRepository.save(connector);
    }

    /**
     * CDC 커넥터를 등록만 하고 실행하지 않는다. 이미 같은 이름의 커넥터가 있으면 먼저
     * STOPPED를 확인한 뒤 설정을 갱신하므로 재준비 중 Source가 몰래 실행되는 구간도 없다.
     */
    private void prepareStoppedConnector(Long pipelineId, String role, RenderedConnectorConfig rendered) {
        if (kafkaConnectClient.listConnectors().contains(rendered.connectorName())) {
            tolerateTimeout("stop", rendered.connectorName(),
                    () -> kafkaConnectClient.stop(rendered.connectorName()));
            awaitConnectorState(rendered.connectorName(), "STOPPED", false);
            tolerateTimeout("upsertConfig", rendered.connectorName(),
                    () -> kafkaConnectClient.upsertConfig(rendered.connectorName(), rendered.config()));
            tolerateTimeout("stop", rendered.connectorName(),
                    () -> kafkaConnectClient.stop(rendered.connectorName()));
        } else {
            tolerateTimeout("createStopped", rendered.connectorName(),
                    () -> kafkaConnectClient.createStopped(rendered.connectorName(), rendered.config()));
        }

        ConnectorStatusResponse status = awaitConnectorState(rendered.connectorName(), "STOPPED", false);
        PipelineConnector connector = pipelineConnectorRepository
                .findByPipelineIdAndConnectorRole(pipelineId, role)
                .orElseGet(() -> new PipelineConnector(pipelineId, role,
                        rendered.connectorName(), rendered.connectorClass(), "{}"));
        connector.setConnectorName(rendered.connectorName());
        connector.setConnectorClass(rendered.connectorClass());
        connector.setConnectorConfigJson(toJson(rendered.config()));
        connector.setDeployedAt(LocalDateTime.now());
        updateConnectorStatus(connector, status);
        pipelineConnectorRepository.save(connector);
    }

    /**
     * pause/start(resume)/stop/restart 공통 처리: 파이프라인에 딸린 모든 커넥터에
     * 같은 Kafka Connect 액션을 적용하고, 성공하면 파이프라인 상태를 targetStatus로,
     * 실패하면 FAILED로 바꾼다. 설계서 §6.1의 UI 버튼 -> 내부 동작 매핑표 그대로:
     * 일시정지=pause, 중지=pause(다른 라벨), 재시작=task restart, 시작(재개)=resume.
     */
    private PipelineResponse applyToAllConnectors(Long pipelineId, String command,
            PipelineStatus targetStatus, Consumer<String> connectAction) {
        PipelineDefinition pipeline = pipelineDefinitionRepository.findById(pipelineId)
                .orElseThrow(() -> new PipelineNotFoundException(pipelineId));
        List<PipelineConnector> connectors = pipelineConnectorRepository.findByPipelineId(pipelineId);
        if (connectors.isEmpty()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "아직 배포된 적 없는 파이프라인입니다. 먼저 /deploy를 호출하세요.");
        }

        try {
            for (PipelineConnector connector : connectors) {
                connectAction.accept(connector.getConnectorName());
                refreshConnectorStatus(connector);
                pipelineConnectorRepository.save(connector);
            }
            pipeline.setStatus(targetStatus);
            pipelineDefinitionRepository.save(pipeline);
            commandHistoryRecorder.record(pipelineId, command, "SUCCESS", null);
        } catch (Exception ex) {
            pipeline.setStatus(PipelineStatus.FAILED);
            pipelineDefinitionRepository.save(pipeline);
            commandHistoryRecorder.record(pipelineId, command, "FAILED", ex.getMessage());
            if (ex instanceof BusinessException businessException) {
                throw businessException;
            }
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, command + " 실패: " + ex.getMessage());
        }

        return PipelineResponse.from(pipeline, pipelineConnectorRepository.findByPipelineId(pipelineId));
    }

    /** 일시정지: 커넥터를 실행 상태로 두되 잠깐 멈춰둠 (다시 /start로 재개 가능). */
    public PipelineResponse pause(Long pipelineId) {
        return applyToAllConnectors(pipelineId, "PAUSE", PipelineStatus.PAUSED, kafkaConnectClient::pause);
    }

    /**
     * TABLE_CDC 시작은 READY(최초 실행) 또는 STOPPED(Sink만 재개) 상태에서만 허용한다.
     * Source와 Sink가 실제 RUNNING이고 모든 태스크가 RUNNING인 것을 확인한 뒤에만
     * 파이프라인을 DEPLOYED로 기록한다.
     */
    public PipelineResponse resume(Long pipelineId) {
        return withPipelineLock(pipelineId, () -> {
            PipelineDefinition pipeline = findPipeline(pipelineId);
            if (!isTableCdc(pipeline)) {
                return applyToAllConnectors(pipelineId, "START",
                        PipelineStatus.DEPLOYED, kafkaConnectClient::resume);
            }
            return startTableCdc(pipeline);
        });
    }

    /**
     * TABLE_CDC 중지는 Sink에만 Kafka Connect stop을 보낸다. Source는 계속 RUNNING 상태로
     * DB 변경을 Kafka topic에 적재하므로, 다음 start 때 Sink가 밀린 데이터를 이어받는다.
     */
    public PipelineResponse stop(Long pipelineId) {
        return withPipelineLock(pipelineId, () -> {
            PipelineDefinition pipeline = findPipeline(pipelineId);
            if (!isTableCdc(pipeline)) {
                return applyToAllConnectors(pipelineId, "STOP",
                        PipelineStatus.STOPPED, kafkaConnectClient::stop);
            }
            return stopTableCdc(pipeline);
        });
    }

    private PipelineResponse startTableCdc(PipelineDefinition pipeline) {
        Long pipelineId = pipeline.getId();
        PipelineStatus previousStatus = pipeline.getStatus();
        PipelineConnector source = requiredConnector(pipelineId, "SOURCE");
        PipelineConnector sink = requiredConnector(pipelineId, "SINK");

        try {
            ConnectorStatusResponse sourceBefore = kafkaConnectClient.getStatus(source.getConnectorName());
            ConnectorStatusResponse sinkBefore = kafkaConnectClient.getStatus(sink.getConnectorName());

            if (previousStatus == PipelineStatus.DEPLOYED) {
                assertState(sourceBefore, "RUNNING", true, "Source");
                assertState(sinkBefore, "RUNNING", true, "Sink");
                saveVerifiedStatus(source, sourceBefore);
                saveVerifiedStatus(sink, sinkBefore);
                throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "CDC 파이프라인이 이미 실행 중입니다. 중복 start는 허용되지 않습니다.");
            } else if (previousStatus == PipelineStatus.READY) {
                assertState(sourceBefore, "STOPPED", false, "Source");
                assertState(sinkBefore, "STOPPED", false, "Sink");
                kafkaConnectClient.resume(source.getConnectorName());
                saveVerifiedStatus(source,
                        awaitConnectorState(source.getConnectorName(), "RUNNING", true));
                kafkaConnectClient.resume(sink.getConnectorName());
                saveVerifiedStatus(sink,
                        awaitConnectorState(sink.getConnectorName(), "RUNNING", true));
            } else if (previousStatus == PipelineStatus.STOPPED) {
                assertState(sourceBefore, "RUNNING", true, "Source");
                assertState(sinkBefore, "STOPPED", false, "Sink");
                saveVerifiedStatus(source, sourceBefore);
                kafkaConnectClient.resume(sink.getConnectorName());
                saveVerifiedStatus(sink,
                        awaitConnectorState(sink.getConnectorName(), "RUNNING", true));
            } else {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                        "CDC 시작은 실행 대기(READY) 또는 중지(STOPPED) 상태에서만 가능합니다. 현재 상태="
                                + previousStatus);
            }

            verifyStableRunning(source, sink);
            pipeline.setStatus(PipelineStatus.DEPLOYED);
            pipelineDefinitionRepository.save(pipeline);
            commandHistoryRecorder.record(pipelineId, "START", "SUCCESS", null);
            return response(pipeline);
        } catch (Exception startFailure) {
            boolean rollbackSucceeded = compensateFailedStart(source, sink, previousStatus);
            pipeline.setStatus(rollbackSucceeded ? previousStatus : PipelineStatus.FAILED);
            pipelineDefinitionRepository.save(pipeline);
            String detail = startFailure.getMessage()
                    + (rollbackSucceeded ? " (이전 상태로 복구됨)" : " (안전 상태 복구 실패)");
            commandHistoryRecorder.record(pipelineId, "START", "FAILED", detail);
            if (startFailure instanceof BusinessException businessException) {
                throw businessException;
            }
            throw new BusinessException(ErrorCode.KAFKA_CONNECT_ERROR, "CDC 시작 실패: " + detail);
        }
    }

    /**
     * 두 커넥터와 모든 task가 일정 시간 연속 RUNNING인 경우에만 시작 성공으로 확정한다.
     * Oracle snapshot의 LOCK TABLE 같은 초기화 오류가 task 시작 직후 발생해도 여기서 잡아
     * startTableCdc의 보상 로직이 Source/Sink를 이전 안전 상태로 되돌리게 한다.
     */
    private void verifyStableRunning(PipelineConnector source, PipelineConnector sink) {
        long requiredNanos = Math.max(0L, startStabilityMillis) * 1_000_000L;
        long startedAt = System.nanoTime();

        while (true) {
            ConnectorStatusResponse sourceStatus =
                    kafkaConnectClient.getStatus(source.getConnectorName());
            ConnectorStatusResponse sinkStatus =
                    kafkaConnectClient.getStatus(sink.getConnectorName());
            assertState(sourceStatus, "RUNNING", true, "Source");
            assertState(sinkStatus, "RUNNING", true, "Sink");
            saveVerifiedStatus(source, sourceStatus);
            saveVerifiedStatus(sink, sinkStatus);

            long elapsedNanos = System.nanoTime() - startedAt;
            if (elapsedNanos >= requiredNanos) {
                return;
            }

            long remainingMillis = Math.max(1L,
                    (requiredNanos - elapsedNanos + 999_999L) / 1_000_000L);
            long sleepMillis = Math.min(
                    Math.max(1L, startStabilityCheckIntervalMillis), remainingMillis);
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new BusinessException(ErrorCode.KAFKA_CONNECT_ERROR,
                        "CDC 시작 안정화 확인이 중단되었습니다.");
            }
        }
    }

    private PipelineResponse stopTableCdc(PipelineDefinition pipeline) {
        Long pipelineId = pipeline.getId();
        PipelineConnector source = requiredConnector(pipelineId, "SOURCE");
        PipelineConnector sink = requiredConnector(pipelineId, "SINK");

        try {
            ConnectorStatusResponse sourceBefore = kafkaConnectClient.getStatus(source.getConnectorName());
            ConnectorStatusResponse sinkBefore = kafkaConnectClient.getStatus(sink.getConnectorName());

            if (pipeline.getStatus() == PipelineStatus.STOPPED) {
                assertState(sourceBefore, "RUNNING", true, "Source");
                assertState(sinkBefore, "STOPPED", false, "Sink");
            } else {
                if (pipeline.getStatus() != PipelineStatus.DEPLOYED) {
                    throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                            "CDC 중지는 실행 중(DEPLOYED) 상태에서만 가능합니다. 현재 상태="
                                    + pipeline.getStatus());
                }
                assertState(sourceBefore, "RUNNING", true, "Source");
                assertState(sinkBefore, "RUNNING", true, "Sink");
                kafkaConnectClient.stop(sink.getConnectorName());
                sinkBefore = awaitConnectorState(sink.getConnectorName(), "STOPPED", false);
                sourceBefore = awaitConnectorState(source.getConnectorName(), "RUNNING", true);
            }

            saveVerifiedStatus(source, sourceBefore);
            saveVerifiedStatus(sink, sinkBefore);
            pipeline.setStatus(PipelineStatus.STOPPED);
            pipelineDefinitionRepository.save(pipeline);
            commandHistoryRecorder.record(pipelineId, "STOP", "SUCCESS", null);
            return response(pipeline);
        } catch (Exception ex) {
            pipeline.setStatus(PipelineStatus.FAILED);
            pipelineDefinitionRepository.save(pipeline);
            commandHistoryRecorder.record(pipelineId, "STOP", "FAILED", ex.getMessage());
            if (ex instanceof BusinessException businessException) {
                throw businessException;
            }
            throw new BusinessException(ErrorCode.KAFKA_CONNECT_ERROR, "CDC 중지 실패: " + ex.getMessage());
        }
    }

    private boolean compensateFailedStart(PipelineConnector source, PipelineConnector sink,
            PipelineStatus previousStatus) {
        try {
            if (previousStatus == PipelineStatus.DEPLOYED) {
                ConnectorStatusResponse sourceStatus =
                        awaitConnectorState(source.getConnectorName(), "RUNNING", true);
                ConnectorStatusResponse sinkStatus =
                        awaitConnectorState(sink.getConnectorName(), "RUNNING", true);
                saveVerifiedStatus(source, sourceStatus);
                saveVerifiedStatus(sink, sinkStatus);
                return true;
            }
            if (previousStatus != PipelineStatus.READY && previousStatus != PipelineStatus.STOPPED) {
                return false;
            }

            kafkaConnectClient.stop(sink.getConnectorName());
            ConnectorStatusResponse sinkStatus =
                    awaitConnectorState(sink.getConnectorName(), "STOPPED", false);
            saveVerifiedStatus(sink, sinkStatus);

            ConnectorStatusResponse sourceStatus;
            if (previousStatus == PipelineStatus.STOPPED) {
                sourceStatus = awaitConnectorState(source.getConnectorName(), "RUNNING", true);
            } else {
                kafkaConnectClient.stop(source.getConnectorName());
                sourceStatus = awaitConnectorState(source.getConnectorName(), "STOPPED", false);
            }
            saveVerifiedStatus(source, sourceStatus);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    /** 재시작: 커넥터의 태스크만 재시작한다 (설정을 다시 만들지 않음, tasks.max=1 고정이라 task 0). */
    public PipelineResponse restart(Long pipelineId) {
        return applyToAllConnectors(pipelineId, "RESTART", PipelineStatus.DEPLOYED,
                connectorName -> kafkaConnectClient.restartTask(connectorName, 0));
    }

    /**
     * Kafka Connect 제어 호출을 실행하되, <b>타임아웃은 실패로 단정하지 않는다.</b>
     *
     * <p>커넥터 등록/갱신·정지는 클러스터 리밸런스를 유발해 응답이 늦는데, 그 사이 Connect 는
     * 요청을 이미 반영해 둔 경우가 많다. 실제로 2026-08-26 에 {@code PUT /config} 가 타임아웃
     * 됐지만 설정은 정상 반영됐고, 그럼에도 파이프라인만 FAILED 로 떨어져 이후 start 가 막혔다.
     *
     * <p>타임아웃은 "결과를 모름"이므로, 여기서 삼키고 <b>바로 뒤의 awaitConnectorState 가
     * 실제 상태를 확인해</b> 판정하게 한다. 변경 요청을 다시 보내지는 않는다 - 재시도는
     * 리밸런스를 겹치게 만들어 상황을 악화시킨다. 확인은 횟수 제한이 있고, 커넥터가 확정
     * 실패면 즉시 중단된다.
     */
    private void tolerateTimeout(String operation, String connectorName, Runnable call) {
        try {
            call.run();
        } catch (KafkaConnectTimeoutException ex) {
            log.warn("Kafka Connect {} 응답 지연 - 실제 상태로 판정한다. connector={}, 사유={}",
                    operation, connectorName, ex.getMessage());
        }
    }

    private ConnectorStatusResponse awaitConnectorState(String connectorName, String expectedState,
            boolean requireRunningTasks) {
        ConnectorStatusResponse lastStatus = null;
        BusinessException lastFailure = null;
        for (int attempt = 0; attempt < STATUS_VERIFY_ATTEMPTS; attempt++) {
            try {
                lastStatus = kafkaConnectClient.getStatus(connectorName);
                lastFailure = null;
                if (matchesState(lastStatus, expectedState, requireRunningTasks)) {
                    return lastStatus;
                }
                if (hasTerminalFailure(lastStatus)) {
                    throw new BusinessException(ErrorCode.KAFKA_CONNECT_ERROR,
                            "Kafka Connect가 실패 상태입니다. connector=" + connectorName
                                    + ", connectorState=" + connectorState(lastStatus)
                                    + ", taskStates=" + taskStates(lastStatus));
                }
            } catch (BusinessException ex) {
                if (lastStatus != null && hasTerminalFailure(lastStatus)) {
                    throw ex;
                }
                // 생성/stop/resume 직후 분산 워커의 상태 엔드포인트가 잠시 404/409를
                // 반환할 수 있다. 정해진 시간 동안만 재시도하고 끝까지 안 맞으면 실패시킨다.
                lastFailure = ex;
            }
            if (attempt + 1 < STATUS_VERIFY_ATTEMPTS) {
                try {
                    Thread.sleep(STATUS_VERIFY_INTERVAL_MILLIS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new BusinessException(ErrorCode.KAFKA_CONNECT_ERROR,
                            "Kafka Connect 상태 확인이 중단되었습니다. connector=" + connectorName);
                }
            }
        }
        throw new BusinessException(ErrorCode.KAFKA_CONNECT_ERROR,
                "Kafka Connect 상태가 동기화되지 않았습니다. connector=" + connectorName
                        + ", expected=" + expectedState
                        + ", connectorState=" + connectorState(lastStatus)
                        + ", taskStates=" + taskStates(lastStatus)
                        + (lastFailure != null ? ", lastError=" + lastFailure.getMessage() : ""));
    }

    private void assertState(ConnectorStatusResponse status, String expectedState,
            boolean requireRunningTasks, String role) {
        if (!matchesState(status, expectedState, requireRunningTasks)) {
            String actual = status != null && status.connector() != null
                    ? status.connector().state() : "UNKNOWN";
            throw new BusinessException(ErrorCode.KAFKA_CONNECT_ERROR,
                    role + " Connector 상태 불일치: expected=" + expectedState
                            + ", connectorState=" + actual
                            + ", taskStates=" + taskStates(status));
        }
    }

    private boolean matchesState(ConnectorStatusResponse status, String expectedState,
            boolean requireRunningTasks) {
        if (!hasConnectorState(status, expectedState)) {
            return false;
        }
        if (!requireRunningTasks) {
            return true;
        }
        List<ConnectorTaskStatus> tasks = status.tasks();
        return tasks != null && !tasks.isEmpty()
                && tasks.stream().allMatch(task -> "RUNNING".equalsIgnoreCase(task.state()));
    }

    private boolean hasConnectorState(ConnectorStatusResponse status, String expectedState) {
        return status != null && status.connector() != null
                && expectedState.equalsIgnoreCase(status.connector().state());
    }

    private boolean hasTerminalFailure(ConnectorStatusResponse status) {
        return hasConnectorState(status, "FAILED")
                || (status != null && status.tasks() != null
                && status.tasks().stream().anyMatch(task -> "FAILED".equalsIgnoreCase(task.state())));
    }

    private String connectorState(ConnectorStatusResponse status) {
        return status != null && status.connector() != null
                ? status.connector().state() : "UNKNOWN";
    }

    private List<String> taskStates(ConnectorStatusResponse status) {
        return status != null && status.tasks() != null
                ? status.tasks().stream().map(ConnectorTaskStatus::state).toList()
                : List.of();
    }

    private PipelineConnector requiredConnector(Long pipelineId, String role) {
        return pipelineConnectorRepository.findByPipelineIdAndConnectorRole(pipelineId, role)
                .orElseThrow(() -> new BusinessException(ErrorCode.VALIDATION_ERROR,
                        role + " Connector가 준비되지 않았습니다. 파이프라인을 다시 준비하세요."));
    }

    private void saveVerifiedStatus(PipelineConnector connector, ConnectorStatusResponse status) {
        updateConnectorStatus(connector, status);
        pipelineConnectorRepository.save(connector);
    }

    private void updateConnectorStatus(PipelineConnector connector, ConnectorStatusResponse status) {
        connector.setStatus(status.connector() != null ? status.connector().state() : "UNKNOWN");
        connector.setLastStatusJson(toJson(status));
    }

    private void refreshConnectorStatus(PipelineConnector connector) {
        try {
            ConnectorStatusResponse status = kafkaConnectClient.getStatus(connector.getConnectorName());
            updateConnectorStatus(connector, status);
        } catch (BusinessException ex) {
            // 등록/일시정지 직후라 아직 상태 조회가 안 될 수 있음 - 호출 자체를 실패로 보지 않는다.
            connector.setStatus("UNKNOWN");
        }
    }

    private PipelineDefinition findPipeline(Long pipelineId) {
        return pipelineDefinitionRepository.findById(pipelineId)
                .orElseThrow(() -> new PipelineNotFoundException(pipelineId));
    }

    private boolean isTableCdc(PipelineDefinition pipeline) {
        return "TABLE_CDC".equals(pipeline.getPipelineType());
    }

    private PipelineResponse response(PipelineDefinition pipeline) {
        return PipelineResponse.from(pipeline,
                pipelineConnectorRepository.findByPipelineId(pipeline.getId()));
    }

    private PipelineResponse withPipelineLock(Long pipelineId, Supplier<PipelineResponse> action) {
        ReentrantLock lock = pipelineLocks.computeIfAbsent(pipelineId, ignored -> new ReentrantLock());
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    private PipelineConnection findConnectionOrThrow(Long id) {
        return connectionRepository.findById(id)
                .orElseThrow(() -> new BusinessException(ErrorCode.CONNECTION_NOT_FOUND,
                        "연결정보를 찾을 수 없습니다. id=" + id));
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            return "{}";
        }
    }
}
