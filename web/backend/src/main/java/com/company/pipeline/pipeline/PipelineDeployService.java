package com.company.pipeline.pipeline;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.connection.ConnectionRepository;
import com.company.pipeline.connection.PipelineConnection;
import com.company.pipeline.connector.ConnectorConfigRenderer;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.connector.dto.ConnectorStatusResponse;
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
import java.util.function.Consumer;
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
            FilebeatInputFileService filebeatInputFileService) {
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
    }

    public PipelineResponse deploy(Long pipelineId) {
        PipelineDefinition pipeline = pipelineDefinitionRepository.findById(pipelineId)
                .orElseThrow(() -> new PipelineNotFoundException(pipelineId));

        pipeline.setStatus(PipelineStatus.DEPLOYING);
        pipelineDefinitionRepository.save(pipeline);

        try {
            if ("LOG_FILE".equals(pipeline.getPipelineType())) {
                deployLogFilePipeline(pipeline);
            } else {
                deployTableCdcPipeline(pipeline);
            }

            pipeline.setStatus(PipelineStatus.DEPLOYED);
            pipelineDefinitionRepository.save(pipeline);
            commandHistoryRecorder.record(pipeline.getId(), "DEPLOY", "SUCCESS", null);
        } catch (Exception ex) {
            pipeline.setStatus(PipelineStatus.FAILED);
            pipelineDefinitionRepository.save(pipeline);
            commandHistoryRecorder.record(pipeline.getId(), "DEPLOY", "FAILED", ex.getMessage());
            if (ex instanceof BusinessException businessException) {
                throw businessException;
            }
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "파이프라인 배포 실패: " + ex.getMessage());
        }

        return PipelineResponse.from(pipeline, pipelineConnectorRepository.findByPipelineId(pipeline.getId()));
    }

    private void deployTableCdcPipeline(PipelineDefinition pipeline) {
        PipelineConnection source = findConnectionOrThrow(pipeline.getSourceConnectionId());
        PipelineConnection target = findConnectionOrThrow(pipeline.getTargetConnectionId());

        RenderedConnectorConfig sourceConfig = connectorConfigRenderer.renderSource(new SourceConnectorRequest(
                pipeline.getId(), source.getDbType(), source.getHost(), source.getPort(),
                source.getUsername(), passwordCryptoService.decrypt(source.getEncryptedPassword()),
                source.getDatabaseName(), source.getServiceName(),
                pipeline.getSourceSchema(), pipeline.getSourceTable(), pipeline.getTopicName()));
        deployConnector(pipeline.getId(), "SOURCE", sourceConfig);

        RenderedConnectorConfig sinkConfig = connectorConfigRenderer.renderSink(new SinkConnectorRequest(
                pipeline.getId(), target.getDbType(), target.getHost(), target.getPort(),
                target.getUsername(), passwordCryptoService.decrypt(target.getEncryptedPassword()),
                target.getDatabaseName(), target.getServiceName(),
                pipeline.getTargetSchema(), pipeline.getTargetTable(),
                source.getDbType(), pipeline.getSourceSchema(), pipeline.getSourceTable(),
                pipeline.getTopicName(), Boolean.TRUE.equals(pipeline.getDeleteEnabled())));
        deployConnector(pipeline.getId(), "SINK", sinkConfig);
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
        deployConnector(pipeline.getId(), "SINK", sinkConfig);

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

    /** 시작/재개: PAUSED/STOPPED 상태의 파이프라인을 다시 돌린다. */
    public PipelineResponse resume(Long pipelineId) {
        return applyToAllConnectors(pipelineId, "START", PipelineStatus.DEPLOYED, kafkaConnectClient::resume);
    }

    /**
     * 중지: Kafka Connect에는 pause와 별개의 "정지" 개념이 없어서 내부 동작은 pause와
     * 동일하다 - 다만 화면/이력에는 "중지"로 남겨서 사용자가 의도적으로 멈춘 것과
     * 일시정지를 구분할 수 있게 한다.
     */
    public PipelineResponse stop(Long pipelineId) {
        return applyToAllConnectors(pipelineId, "STOP", PipelineStatus.STOPPED, kafkaConnectClient::pause);
    }

    /** 재시작: 커넥터의 태스크만 재시작한다 (설정을 다시 만들지 않음, tasks.max=1 고정이라 task 0). */
    public PipelineResponse restart(Long pipelineId) {
        return applyToAllConnectors(pipelineId, "RESTART", PipelineStatus.DEPLOYED,
                connectorName -> kafkaConnectClient.restartTask(connectorName, 0));
    }

    private void refreshConnectorStatus(PipelineConnector connector) {
        try {
            ConnectorStatusResponse status = kafkaConnectClient.getStatus(connector.getConnectorName());
            connector.setStatus(status.connector() != null ? status.connector().state() : "UNKNOWN");
            connector.setLastStatusJson(toJson(status));
        } catch (BusinessException ex) {
            // 등록/일시정지 직후라 아직 상태 조회가 안 될 수 있음 - 호출 자체를 실패로 보지 않는다.
            connector.setStatus("UNKNOWN");
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
