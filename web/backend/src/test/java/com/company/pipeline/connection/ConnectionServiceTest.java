package com.company.pipeline.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.connection.dto.ConnectionCreateRequest;
import com.company.pipeline.connection.dto.ConnectionResponse;
import com.company.pipeline.connection.dto.ConnectionUpdateRequest;
<<<<<<< HEAD
import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.dto.NifiControllerServiceEntity;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.pipeline.PipelineStatus;
=======
import com.company.pipeline.connection.dto.ConnectionUsageResponse;
import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.dto.NifiControllerServiceEntity;
>>>>>>> 694dbde (Add CDC connection safety checks)
import java.lang.reflect.Field;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConnectionServiceTest {

    @Mock
    private ConnectionRepository connectionRepository;

    @Mock
    private PasswordCryptoService passwordCryptoService;

    @Mock
    private SchemaDiscoveryService schemaDiscoveryService;

    @Mock
<<<<<<< HEAD
    private PipelineDefinitionRepository pipelineDefinitionRepository;
    @Mock
    private NifiClient nifiClient;
=======
    private NifiClient nifiClient;

    @Mock
    private ConnectionUsageService connectionUsageService;
>>>>>>> 694dbde (Add CDC connection safety checks)

    private ConnectionService connectionService;

    @BeforeEach
    void setUp() {
        connectionService = new ConnectionService(
<<<<<<< HEAD
                connectionRepository, passwordCryptoService, schemaDiscoveryService, pipelineDefinitionRepository,
                nifiClient);
=======
                connectionRepository, passwordCryptoService, schemaDiscoveryService,
                nifiClient, connectionUsageService);
>>>>>>> 694dbde (Add CDC connection safety checks)
    }

    @Test
    void create_encryptsPasswordBeforeSaving() {
        ConnectionCreateRequest request = new ConnectionCreateRequest(
                "oracle-source-poc", DbType.ORACLE, "oracle-db", 1521,
                null, "XEPDB1", null, "c##dbzuser", "plaintext-pw");
        when(passwordCryptoService.encrypt("plaintext-pw")).thenReturn("cipher-text");
        when(connectionRepository.save(any(PipelineConnection.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(nifiClient.createOracleDbcpControllerService(any(), any(), any(), any(), any(), any(), any()))
<<<<<<< HEAD
                .thenReturn(new NifiControllerServiceEntity("svc-1", null,
                        new NifiControllerServiceEntity.Component("svc-1", "oracle-source-poc-dbcp",
                                "DBCPConnectionPool", "ENABLED")));
=======
                .thenReturn(new NifiControllerServiceEntity("service-id", null,
                        new NifiControllerServiceEntity.Component("service-id", "service-name", null, null)));
>>>>>>> 694dbde (Add CDC connection safety checks)

        ConnectionResponse response = connectionService.create(request);

        assertThat(response.name()).isEqualTo("oracle-source-poc");
        verify(passwordCryptoService).encrypt("plaintext-pw");
        verify(connectionRepository).save(any(PipelineConnection.class));
    }

    @Test
    void update_withoutPassword_keepsExistingEncryptedPassword() throws Exception {
        PipelineConnection existing = newConnection(1L, "cipher-original");
        when(connectionRepository.findById(1L)).thenReturn(java.util.Optional.of(existing));

        ConnectionUpdateRequest request = new ConnectionUpdateRequest(
                "renamed", DbType.ORACLE, "oracle-db", 1521,
                null, "XEPDB1", null, "c##dbzuser", null);

        connectionService.update(1L, request);

        assertThat(existing.getEncryptedPassword()).isEqualTo("cipher-original");
        assertThat(existing.getName()).isEqualTo("renamed");
        verifyNoMoreInteractions(passwordCryptoService);
    }

    @Test
    void update_withNewPassword_reEncrypts() throws Exception {
        PipelineConnection existing = newConnection(1L, "cipher-original");
        when(connectionRepository.findById(1L)).thenReturn(java.util.Optional.of(existing));
        when(passwordCryptoService.encrypt("new-pw")).thenReturn("cipher-new");

        ConnectionUpdateRequest request = new ConnectionUpdateRequest(
                "renamed", DbType.ORACLE, "oracle-db", 1521,
                null, "XEPDB1", null, "c##dbzuser", "new-pw");

        connectionService.update(1L, request);

        assertThat(existing.getEncryptedPassword()).isEqualTo("cipher-new");
    }

    @Test
    void delete_withActivePipelineReference_throwsBusinessException() throws Exception {
        PipelineConnection existing = newConnection(1L, "cipher-original");
        when(connectionRepository.findById(1L)).thenReturn(java.util.Optional.of(existing));
        when(connectionUsageService.get(1L)).thenReturn(new ConnectionUsageResponse(
                1L, 1, 0, 0, false, java.util.List.of(
                        new com.company.pipeline.connection.dto.ConnectionReferenceResponse(
                                "CDC", 10L, "customers-cdc", "SOURCE", "DEPLOYED"))));

        assertThatThrownBy(() -> connectionService.delete(1L))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("customers-cdc");
        verify(connectionRepository, org.mockito.Mockito.never()).delete(any());
    }

    @Test
    void delete_withStoppedCdcPipelineReference_isRejectedBecauseSourceStillRuns() throws Exception {
        PipelineConnection existing = newConnection(1L, "cipher-original");
        when(connectionRepository.findById(1L)).thenReturn(java.util.Optional.of(existing));
        when(connectionUsageService.get(1L)).thenReturn(new ConnectionUsageResponse(
                1L, 1, 0, 0, false, java.util.List.of(
                        new com.company.pipeline.connection.dto.ConnectionReferenceResponse(
                                "CDC", 11L, "old-pipeline", "SOURCE", "STOPPED"))));

        assertThatThrownBy(() -> connectionService.delete(1L))
                .isInstanceOf(BusinessException.class)
                .hasMessageContaining("old-pipeline");
        verify(connectionRepository, org.mockito.Mockito.never()).delete(any());
    }

    @Test
    void get_missingId_throwsConnectionNotFoundException() {
        when(connectionRepository.findById(999L)).thenReturn(java.util.Optional.empty());

        org.junit.jupiter.api.Assertions.assertThrows(
                ConnectionNotFoundException.class, () -> connectionService.get(999L));
    }

    // 프로텍티드 생성자로 만든 엔티티에 테스트용 id를 세팅하기 위한 리플렉션 헬퍼.
    private PipelineConnection newConnection(Long id, String encryptedPassword) throws Exception {
        PipelineConnection entity = new PipelineConnection(
                "original", DbType.ORACLE, "oracle-db", 1521,
                null, "XEPDB1", null, "c##dbzuser", encryptedPassword, null);
        Field idField = PipelineConnection.class.getDeclaredField("id");
        idField.setAccessible(true);
        idField.set(entity, id);
        return entity;
    }
}
