package com.company.pipeline.connection;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.connection.dto.ConnectionCreateRequest;
import com.company.pipeline.connection.dto.ConnectionResponse;
import com.company.pipeline.connection.dto.ConnectionUpdateRequest;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.pipeline.PipelineStatus;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(readOnly = true)
public class ConnectionService {

    // READY는 실행에 필요한 설정이 준비되어 있고, STOPPED도 Source가 계속 CDC 변경분을
    // Kafka에 적재하므로 연결정보 삭제를 막아야 한다. CREATED/FAILED만 삭제를 허용한다.
    private static final Set<PipelineStatus> ACTIVE_STATUSES =
            Set.of(PipelineStatus.DEPLOYED, PipelineStatus.DEPLOYING, PipelineStatus.READY,
                    PipelineStatus.PAUSED, PipelineStatus.STOPPED);

    private final ConnectionRepository connectionRepository;
    private final PasswordCryptoService passwordCryptoService;
    private final SchemaDiscoveryService schemaDiscoveryService;
    private final PipelineDefinitionRepository pipelineDefinitionRepository;

    public ConnectionService(ConnectionRepository connectionRepository,
            PasswordCryptoService passwordCryptoService,
            SchemaDiscoveryService schemaDiscoveryService,
            PipelineDefinitionRepository pipelineDefinitionRepository) {
        this.connectionRepository = connectionRepository;
        this.passwordCryptoService = passwordCryptoService;
        this.schemaDiscoveryService = schemaDiscoveryService;
        this.pipelineDefinitionRepository = pipelineDefinitionRepository;
    }

    @Transactional
    public ConnectionResponse create(ConnectionCreateRequest request) {
        PipelineConnection entity = new PipelineConnection(
                request.name(),
                request.dbType(),
                request.host(),
                request.port(),
                request.databaseName(),
                request.serviceName(),
                request.schemaName(),
                request.username(),
                passwordCryptoService.encrypt(request.password()),
                null
        );
        return ConnectionResponse.from(connectionRepository.save(entity));
    }

    public ConnectionResponse get(Long id) {
        return ConnectionResponse.from(findOrThrow(id));
    }

    public List<ConnectionResponse> list() {
        return connectionRepository.findAll().stream()
                .map(ConnectionResponse::from)
                .toList();
    }

    @Transactional
    public ConnectionResponse update(Long id, ConnectionUpdateRequest request) {
        PipelineConnection entity = findOrThrow(id);
        entity.setName(request.name());
        entity.setDbType(request.dbType());
        entity.setHost(request.host());
        entity.setPort(request.port());
        entity.setDatabaseName(request.databaseName());
        entity.setServiceName(request.serviceName());
        entity.setSchemaName(request.schemaName());
        entity.setUsername(request.username());
        // password가 없으면 기존 encrypted_password를 유지한다.
        if (request.password() != null && !request.password().isBlank()) {
            entity.setEncryptedPassword(passwordCryptoService.encrypt(request.password()));
        }
        return ConnectionResponse.from(entity);
    }

    /**
     * 실제로 접속해서 status(SUCCESS/FAILED)·lastTestedAt을 갱신한다. 이전엔 이 필드가
     * 엔티티 생성 시점에 UNKNOWN으로만 박히고 그 뒤로 갱신하는 코드가 전혀 없었다.
     */
    @Transactional
    public ConnectionResponse testConnection(Long id) {
        PipelineConnection entity = findOrThrow(id);
        boolean reachable = schemaDiscoveryService.testConnection(id);
        entity.setStatus(reachable ? ConnectionStatus.SUCCESS : ConnectionStatus.FAILED);
        entity.setLastTestedAt(LocalDateTime.now());
        return ConnectionResponse.from(entity);
    }

    @Transactional
    public void delete(Long id) {
        PipelineConnection entity = findOrThrow(id);

        List<PipelineDefinition> referencingPipelines = pipelineDefinitionRepository
                .findBySourceConnectionIdOrTargetConnectionId(id, id);
        List<String> activePipelineNames = referencingPipelines.stream()
                .filter(p -> ACTIVE_STATUSES.contains(p.getStatus()))
                .map(PipelineDefinition::getName)
                .toList();
        if (!activePipelineNames.isEmpty()) {
            throw new BusinessException(ErrorCode.CONNECTION_IN_USE,
                    "이 연결정보를 사용 중인 파이프라인이 있어 삭제할 수 없습니다: "
                            + activePipelineNames.stream().collect(Collectors.joining(", ")));
        }

        connectionRepository.delete(entity);
    }

    private PipelineConnection findOrThrow(Long id) {
        return connectionRepository.findById(id)
                .orElseThrow(() -> new ConnectionNotFoundException(id));
    }
}
