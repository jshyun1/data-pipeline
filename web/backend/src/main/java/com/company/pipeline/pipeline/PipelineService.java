package com.company.pipeline.pipeline;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.connection.ConnectionRepository;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.connection.PipelineConnection;
import com.company.pipeline.connector.DebeziumOracleTemplate;
import com.company.pipeline.connector.KafkaConnectClient;
import com.company.pipeline.connector.KafkaTopicCleanupService;
import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.connector.PipelineConnectorRepository;
import com.company.pipeline.connector.PostgresReplicationCleanupService;
import com.company.pipeline.logpipeline.FilebeatInputFileService;
import com.company.pipeline.logpipeline.LogPipelineSource;
import com.company.pipeline.logpipeline.LogPipelineSourceRepository;
import com.company.pipeline.logpipeline.dto.LogPipelineCreateRequest;
import com.company.pipeline.pipeline.dto.PipelineCreateRequest;
import com.company.pipeline.pipeline.dto.PipelineResponse;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(readOnly = true)
public class PipelineService {

    private final PipelineDefinitionRepository pipelineDefinitionRepository;
    private final PipelineConnectorRepository pipelineConnectorRepository;
    private final ConnectionRepository connectionRepository;
    private final KafkaConnectClient kafkaConnectClient;
    private final LogPipelineSourceRepository logPipelineSourceRepository;
    private final FilebeatInputFileService filebeatInputFileService;
    private final PostgresReplicationCleanupService postgresReplicationCleanupService;
    private final KafkaTopicCleanupService kafkaTopicCleanupService;
    private final PipelineMetadataArchiveRepository metadataArchiveRepository;

    public PipelineService(PipelineDefinitionRepository pipelineDefinitionRepository,
            PipelineConnectorRepository pipelineConnectorRepository,
            ConnectionRepository connectionRepository,
            KafkaConnectClient kafkaConnectClient,
            LogPipelineSourceRepository logPipelineSourceRepository,
            FilebeatInputFileService filebeatInputFileService,
            PostgresReplicationCleanupService postgresReplicationCleanupService,
            KafkaTopicCleanupService kafkaTopicCleanupService,
            PipelineMetadataArchiveRepository metadataArchiveRepository) {
        this.pipelineDefinitionRepository = pipelineDefinitionRepository;
        this.pipelineConnectorRepository = pipelineConnectorRepository;
        this.connectionRepository = connectionRepository;
        this.kafkaConnectClient = kafkaConnectClient;
        this.logPipelineSourceRepository = logPipelineSourceRepository;
        this.filebeatInputFileService = filebeatInputFileService;
        this.postgresReplicationCleanupService = postgresReplicationCleanupService;
        this.kafkaTopicCleanupService = kafkaTopicCleanupService;
        this.metadataArchiveRepository = metadataArchiveRepository;
    }

    @Transactional
    public PipelineResponse create(PipelineCreateRequest request) {
        PipelineDefinition entity = buildDefinition(request);
        pipelineDefinitionRepository.save(entity);
        return PipelineResponse.from(entity, List.of());
    }

    /** 모든 항목을 먼저 검증한 뒤 한 DB 트랜잭션에서 정의를 저장한다. Connector 준비는 별도 보상 단계다. */
    @Transactional
    public List<PipelineResponse> createBatchDefinitions(List<PipelineCreateRequest> requests) {
        if (requests == null || requests.isEmpty() || requests.size() > 50) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "배치 생성은 1건 이상 50건 이하만 가능합니다.");
        }
        Set<String> names = new HashSet<>();
        for (PipelineCreateRequest request : requests) {
            if (!names.add(request.name())) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR, "배치 안에 중복된 파이프라인명이 있습니다: " + request.name());
            }
        }
        List<PipelineDefinition> entities = requests.stream().map(this::buildDefinition).toList();
        return pipelineDefinitionRepository.saveAllAndFlush(entities).stream()
                .map(entity -> PipelineResponse.from(entity, List.of())).toList();
    }

    private PipelineDefinition buildDefinition(PipelineCreateRequest request) {
        if (pipelineDefinitionRepository.existsByName(request.name())) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "이미 사용 중인 파이프라인명입니다: " + request.name());
        }
        PipelineConnection source = findConnectionOrThrow(request.sourceConnectionId());
        PipelineConnection target = findConnectionOrThrow(request.targetConnectionId());

        // Oracle LogMiner는 CDB 공통 사용자(C##...)만 접속을 허용한다. 배포 시점에
        // Kafka Connect에 보내면 500 NPE로만 나와서 원인 파악이 어려우니 생성 시점에 미리 막는다.
        if (source.getDbType() == DbType.ORACLE && !DebeziumOracleTemplate.isOracleCommonUser(source.getUsername())) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "Oracle을 CDC 소스로 사용하려면 공통 사용자(C##으로 시작하는 계정)여야 합니다. "
                            + "선택한 연결정보('" + source.getName() + "')의 사용자명: " + source.getUsername());
        }

        PipelineDefinition entity = new PipelineDefinition(
                request.name(),
                "TABLE_CDC",
                source.getId(),
                target.getId(),
                source.getDbType(),
                target.getDbType(),
                request.sourceSchema(),
                request.sourceTable(),
                request.targetSchema(),
                request.targetTable(),
                request.topicPrefix(),
                request.deleteEnabled(),
                request.description(),
                null
        );
        entity.setSnapshotMode(PipelineSnapshotMode.from(request.snapshotMode()).name());
        PipelineLoadMode loadMode = PipelineLoadMode.from(request.loadMode());
        entity.setLoadMode(loadMode.name());
        // 델타 적재: 구분컬럼명은 생성 시점에 확정해 저장한다(배포·화면이 같은 이름을 봐야 함).
        // 소스 컬럼과 이름이 겹치면 싱크가 같은 이름의 필드를 두 개 받게 되므로 미리 막는다.
        entity.setDeltaOpColumn(loadMode.isDelta()
                ? PipelineLoadMode.normalizeDeltaOpColumn(request.deltaOpColumn()) : null);
        entity.setExcludedColumns(request.excludedColumns() == null ? null
                : request.excludedColumns().stream().map(String::trim).filter(value -> !value.isEmpty())
                        .distinct().sorted().reduce((left, right) -> left + "," + right).orElse(null));
        entity.setMaskedColumns(request.maskedColumns() == null ? null
                : request.maskedColumns().stream().map(String::trim).filter(value -> !value.isEmpty())
                        .distinct().sorted().reduce((left, right) -> left + "," + right).orElse(null));
        if (entity.getExcludedColumns() != null && entity.getMaskedColumns() != null) {
            Set<String> excluded = Set.of(entity.getExcludedColumns().toLowerCase().split(","));
            boolean overlap = java.util.Arrays.stream(entity.getMaskedColumns().toLowerCase().split(","))
                    .anyMatch(excluded::contains);
            if (overlap) throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "같은 컬럼을 제외와 마스킹에 동시에 지정할 수 없습니다.");
        }
        return entity;
    }

    /**
     * 로그파일 적재 파이프라인 생성. 소스는 DB 연결이 아니라 파일이라
     * source_connection_id/source_db_type은 NULL로 저장한다 - Filebeat가 소스 역할을
     * 하고 여기선 랜딩(타겟) DB 연결만 검증하면 된다.
     */
    @Transactional
    public PipelineResponse createLogFilePipeline(LogPipelineCreateRequest request) {
        PipelineConnection target = findConnectionOrThrow(request.targetConnectionId());

        String parseType = request.parseType() != null ? request.parseType() : "PLAIN";
        if (!"PLAIN".equalsIgnoreCase(parseType)) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "이번 버전은 parseType=PLAIN만 지원합니다. 입력값: " + parseType);
        }
        if (Boolean.TRUE.equals(request.multilineEnabled())) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "이번 버전은 multiline 로그를 지원하지 않습니다.");
        }

        PipelineDefinition entity = new PipelineDefinition(
                request.name(),
                "LOG_FILE",
                null,
                target.getId(),
                null,
                target.getDbType(),
                null,
                null,
                request.targetSchema(),
                request.targetTable(),
                null,
                false,
                request.description(),
                null
        );
        entity = pipelineDefinitionRepository.saveAndFlush(entity);

        String topicName = (request.topicName() != null && !request.topicName().isBlank())
                ? request.topicName()
                : "log-" + entity.getId();
        entity.setTopicName(topicName);

        LogPipelineSource source = new LogPipelineSource(
                entity.getId(), request.agentHost(), request.filePath(), request.filePattern(),
                request.readFrom(), parseType, request.encoding(), false, topicName);
        logPipelineSourceRepository.save(source);

        return PipelineResponse.from(entity, List.of());
    }

    public PipelineResponse get(Long id) {
        PipelineDefinition entity = findOrThrow(id);
        return PipelineResponse.from(entity, pipelineConnectorRepository.findByPipelineId(id));
    }

    public List<PipelineResponse> list() {
        return pipelineDefinitionRepository.findAll().stream()
                .map(entity -> PipelineResponse.from(entity, pipelineConnectorRepository.findByPipelineId(entity.getId())))
                .toList();
    }

    /**
     * 배포된 커넥터가 있으면 Kafka Connect에서도 먼저 삭제한 뒤 메타데이터를 지운다.
     * 소스가 Postgres인 CDC 파이프라인이면 Debezium이 만든 replication slot/publication도
     * 같이 정리하고(PostgresReplicationCleanupService), 실제 쓰던 Kafka 토픽(+ Oracle 소스면
     * 스키마 이력 토픽)도 같이 정리한다(KafkaTopicCleanupService) - 둘 다 Kafka Connect
     * 리소스가 아니라서 커넥터 삭제만으로는 안 지워짐. 정리 안 하면 아무도 안 쓰는 토픽/slot이
     * 계속 쌓이는 문제가 있어서(실제로 겪음) 삭제 시점에 확실하게 정리한다.
     */
    @Transactional
    public void delete(Long id) {
        PipelineDefinition entity = findOrThrow(id);
        List<PipelineConnector> connectors = pipelineConnectorRepository.findByPipelineId(id);
        for (PipelineConnector connector : connectors) {
            try {
                kafkaConnectClient.delete(connector.getConnectorName());
            } catch (BusinessException ex) {
                // 이미 지워졌거나 Kafka Connect가 일시적으로 응답 안 하는 경우에도
                // 메타데이터 정리는 계속 진행한다 (커넥터 자체는 REST API로 별도 재시도 가능).
            }
        }
        if (entity.getSourceConnectionId() != null) {
            connectionRepository.findById(entity.getSourceConnectionId()).ifPresent(source ->
                    connectors.stream()
                            .filter(c -> "SOURCE".equals(c.getConnectorRole()))
                            .forEach(c -> postgresReplicationCleanupService.cleanup(c, source)));
        }
        kafkaTopicCleanupService.cleanup(entity, connectors, pipelineDefinitionRepository.findAll());
        pipelineConnectorRepository.deleteAll(connectors);
        if ("LOG_FILE".equals(entity.getPipelineType())) {
            filebeatInputFileService.delete(id);
            logPipelineSourceRepository.findByPipelineId(id).ifPresent(logPipelineSourceRepository::delete);
        }
        // 삭제 후에도 CDC 처리로그가 스냅샷 보존정리(30일) 전까지 이름·경로를 표시할 수 있도록 메타 보존.
        metadataArchiveRepository.save(new PipelineMetadataArchive(
                entity.getId(), entity.getName(), entity.getPipelineType(),
                sourcePathOf(entity), targetPathOf(entity)));
        pipelineDefinitionRepository.delete(entity);
    }

    // CdcLogService 의 표시 형식과 동일하게 소스/타겟 경로를 만든다(삭제 후 표시용).
    private String sourcePathOf(PipelineDefinition p) {
        if ("LOG_FILE".equalsIgnoreCase(p.getPipelineType())) {
            return "Filebeat";
        }
        return p.getSourceDbType() + " · " + p.getSourceSchema() + "." + p.getSourceTable();
    }

    private String targetPathOf(PipelineDefinition p) {
        return p.getTargetDbType() + " · " + p.getTargetSchema() + "." + p.getTargetTable();
    }

    private PipelineDefinition findOrThrow(Long id) {
        return pipelineDefinitionRepository.findById(id)
                .orElseThrow(() -> new PipelineNotFoundException(id));
    }

    private PipelineConnection findConnectionOrThrow(Long id) {
        return connectionRepository.findById(id)
                .orElseThrow(() -> new BusinessException(ErrorCode.CONNECTION_NOT_FOUND,
                        "연결정보를 찾을 수 없습니다. id=" + id));
    }
}
