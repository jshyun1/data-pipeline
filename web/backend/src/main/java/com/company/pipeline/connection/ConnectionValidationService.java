package com.company.pipeline.connection;

import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.connection.dto.ConnectionTestRequest;
import com.company.pipeline.connection.dto.ConnectionTestResponse;
import com.company.pipeline.connection.dto.ConnectionUpdateRequest;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Service;

@Service
public class ConnectionValidationService {

    private final ConnectionRepository connectionRepository;
    private final PasswordCryptoService passwordCryptoService;
    private final SchemaDiscoveryService schemaDiscoveryService;

    public ConnectionValidationService(ConnectionRepository connectionRepository,
            PasswordCryptoService passwordCryptoService,
            SchemaDiscoveryService schemaDiscoveryService) {
        this.connectionRepository = connectionRepository;
        this.passwordCryptoService = passwordCryptoService;
        this.schemaDiscoveryService = schemaDiscoveryService;
    }

    public ConnectionTestResponse validate(ConnectionTestRequest request) {
        return test(request.dbType(), request.host(), request.port(), request.databaseName(),
                request.serviceName(), request.username(), request.password());
    }

    public ConnectionTestResponse validate(Long id, ConnectionUpdateRequest request) {
        PipelineConnection saved = connectionRepository.findById(id)
                .orElseThrow(() -> new ConnectionNotFoundException(id));
        String password = request.password() == null || request.password().isBlank()
                ? passwordCryptoService.decrypt(saved.getEncryptedPassword())
                : request.password();
        return test(request.dbType(), request.host(), request.port(), request.databaseName(),
                request.serviceName(), request.username(), password);
    }

    private ConnectionTestResponse test(DbType dbType, String host, Integer port,
            String databaseName, String serviceName, String username, String password) {
        long started = System.nanoTime();
        LocalDateTime testedAt = LocalDateTime.now();
        try (Connection jdbc = schemaDiscoveryService.open(
                dbType, host, port, databaseName, serviceName, username, password)) {
            boolean valid = jdbc.isValid(5);
            return new ConnectionTestResponse(valid, testedAt, elapsedMillis(started),
                    valid ? "연결에 성공했습니다." : "데이터베이스가 연결을 유효하지 않은 상태로 응답했습니다.");
        } catch (Exception ex) {
            return new ConnectionTestResponse(false, testedAt, elapsedMillis(started), safeMessage(ex));
        }
    }

    private long elapsedMillis(long started) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    }

    private String safeMessage(Exception ex) {
        String message = ex.getMessage();
        if (message == null || message.isBlank()) return ex.getClass().getSimpleName();
        return message.replaceAll("(?i)(password|passwd|pwd)=[^&\\s]+", "$1=***");
    }
}
