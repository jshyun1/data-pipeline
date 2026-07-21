package com.company.pipeline.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.company.pipeline.common.crypto.PasswordCryptoService;
import com.company.pipeline.connection.dto.ConnectionCreateRequest;
import com.company.pipeline.connection.dto.ConnectionResponse;
import com.company.pipeline.connection.dto.ConnectionUpdateRequest;
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

    private ConnectionService connectionService;

    @BeforeEach
    void setUp() {
        connectionService = new ConnectionService(connectionRepository, passwordCryptoService);
    }

    @Test
    void create_encryptsPasswordBeforeSaving() {
        ConnectionCreateRequest request = new ConnectionCreateRequest(
                "oracle-source-poc", DbType.ORACLE, "oracle-db", 1521,
                null, "XEPDB1", null, "c##dbzuser", "plaintext-pw");
        when(passwordCryptoService.encrypt("plaintext-pw")).thenReturn("cipher-text");
        when(connectionRepository.save(any(PipelineConnection.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

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
