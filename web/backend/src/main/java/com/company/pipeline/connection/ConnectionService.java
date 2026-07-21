package com.company.pipeline.connection;

import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.connection.dto.ConnectionCreateRequest;
import com.company.pipeline.connection.dto.ConnectionResponse;
import com.company.pipeline.connection.dto.ConnectionUpdateRequest;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(readOnly = true)
public class ConnectionService {

    private final ConnectionRepository connectionRepository;
    private final PasswordCryptoService passwordCryptoService;

    public ConnectionService(ConnectionRepository connectionRepository,
            PasswordCryptoService passwordCryptoService) {
        this.connectionRepository = connectionRepository;
        this.passwordCryptoService = passwordCryptoService;
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

    @Transactional
    public void delete(Long id) {
        PipelineConnection entity = findOrThrow(id);
        // TODO: pipeline_definition이 이 연결정보를 참조하고 있는지 가드하는 로직은
        // PipelineDefinition 서비스가 생기는 다음 증분에서 추가한다.
        connectionRepository.delete(entity);
    }

    private PipelineConnection findOrThrow(Long id) {
        return connectionRepository.findById(id)
                .orElseThrow(() -> new ConnectionNotFoundException(id));
    }
}
