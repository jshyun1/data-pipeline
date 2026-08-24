package com.company.pipeline.connection;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.connection.dto.ConnectionCreateRequest;
import com.company.pipeline.connection.dto.ConnectionResponse;
import com.company.pipeline.connection.dto.ConnectionUpdateRequest;
import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.dto.NifiControllerServiceEntity;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

@Service
@Transactional(readOnly = true)
public class ConnectionService {

    private final ConnectionRepository connectionRepository;
    private final PasswordCryptoService passwordCryptoService;
    private final SchemaDiscoveryService schemaDiscoveryService;
    private final NifiClient nifiClient;
    private final ConnectionUsageService connectionUsageService;

    public ConnectionService(ConnectionRepository connectionRepository,
            PasswordCryptoService passwordCryptoService,
            SchemaDiscoveryService schemaDiscoveryService,
            NifiClient nifiClient,
            ConnectionUsageService connectionUsageService) {
        this.connectionRepository = connectionRepository;
        this.passwordCryptoService = passwordCryptoService;
        this.schemaDiscoveryService = schemaDiscoveryService;
        this.nifiClient = nifiClient;
        this.connectionUsageService = connectionUsageService;
    }

    @Transactional
    public ConnectionResponse create(ConnectionCreateRequest request) {
        validateForNifiControllerService(request);
        String encryptedPassword = passwordCryptoService.encrypt(request.password());
        PipelineConnection entity = new PipelineConnection(
                request.name(),
                request.dbType(),
                request.host(),
                request.port(),
                request.databaseName(),
                request.serviceName(),
                request.schemaName(),
                request.username(),
                encryptedPassword,
                null
        );
        PipelineConnection saved = connectionRepository.save(entity);

        if (request.dbType() == DbType.POSTGRESQL) {
            NifiControllerServiceEntity service = nifiClient.createPostgresDbcpControllerService(
                    saved.getId(),
                    saved.getName(),
                    saved.getHost(),
                    saved.getPort(),
                    saved.getDatabaseName(),
                    saved.getUsername(),
                    request.password()
            );
            saved.setNifiControllerServiceId(service.component().id());
            saved.setNifiControllerServiceName(service.component().name());
        } else if (request.dbType() == DbType.MYSQL) {
            NifiControllerServiceEntity service = nifiClient.createMysqlDbcpControllerService(
                    saved.getId(),
                    saved.getName(),
                    saved.getHost(),
                    saved.getPort(),
                    saved.getDatabaseName(),
                    saved.getUsername(),
                    request.password()
            );
            saved.setNifiControllerServiceId(service.component().id());
            saved.setNifiControllerServiceName(service.component().name());
        } else if (request.dbType() == DbType.ORACLE) {
            NifiControllerServiceEntity service = nifiClient.createOracleDbcpControllerService(
                    saved.getId(),
                    saved.getName(),
                    saved.getHost(),
                    saved.getPort(),
                    saved.getServiceName(),
                    saved.getUsername(),
                    request.password()
            );
            saved.setNifiControllerServiceId(service.component().id());
            saved.setNifiControllerServiceName(service.component().name());
        }

        return ConnectionResponse.from(saved);
    }

    private void validateForNifiControllerService(ConnectionCreateRequest request) {
        if (request.dbType() == DbType.POSTGRESQL && !StringUtils.hasText(request.databaseName())) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "PostgreSQL 연결은 databaseName이 필요합니다.");
        }
        if (request.dbType() == DbType.MYSQL && !StringUtils.hasText(request.databaseName())) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "MySQL 연결은 databaseName이 필요합니다.");
        }
        if (request.dbType() == DbType.ORACLE && !StringUtils.hasText(request.serviceName())) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "Oracle 연결은 serviceName이 필요합니다.");
        }
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

        var usage = connectionUsageService.get(id);
        if (!usage.deletable()) {
            throw new BusinessException(ErrorCode.CONNECTION_IN_USE,
                    "이 연결정보를 사용 중인 파이프라인이 있어 삭제할 수 없습니다: "
                            + usage.references().stream().map(reference -> reference.name()).distinct().toList());
        }

        connectionRepository.delete(entity);
    }

    private PipelineConnection findOrThrow(Long id) {
        return connectionRepository.findById(id)
                .orElseThrow(() -> new ConnectionNotFoundException(id));
    }
}
