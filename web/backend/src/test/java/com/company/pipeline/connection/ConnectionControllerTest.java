package com.company.pipeline.connection;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.connection.dto.ConnectionResponse;
import com.company.pipeline.user.security.SecurityConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(ConnectionController.class)
@Import(SecurityConfig.class)
class ConnectionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private ConnectionService connectionService;

    @MockBean
    private SchemaDiscoveryService schemaDiscoveryService;

    @Test
    void create_validRequest_returns200WithEnvelope() throws Exception {
        ConnectionResponse response = new ConnectionResponse(1L, "oracle-source-poc", DbType.ORACLE,
                "oracle-db", 1521, null, "XEPDB1", null, "c##dbzuser",
                ConnectionStatus.UNKNOWN, null, LocalDateTime.now(), LocalDateTime.now());
        when(connectionService.create(any())).thenReturn(response);

        String body = """
                {"name":"oracle-source-poc","dbType":"ORACLE","host":"oracle-db","port":1521,
                 "serviceName":"XEPDB1","username":"c##dbzuser","password":"ChangeMe_Cdc_2026!"}
                """;

        mockMvc.perform(post("/api/connections")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.name").value("oracle-source-poc"))
                .andExpect(jsonPath("$.data.password").doesNotExist())
                .andExpect(jsonPath("$.data.encryptedPassword").doesNotExist());
    }

    @Test
    void create_blankName_returns400ValidationError() throws Exception {
        String body = """
                {"name":"","dbType":"ORACLE","host":"oracle-db","port":1521,
                 "username":"c##dbzuser","password":"pw"}
                """;

        mockMvc.perform(post("/api/connections")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("VALIDATION_ERROR"));
    }

    @Test
    void get_missingId_returns404() throws Exception {
        when(connectionService.get(eq(999L))).thenThrow(new ConnectionNotFoundException(999L));

        mockMvc.perform(get("/api/connections/{id}", 999L))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error.code").value("CONNECTION_NOT_FOUND"));
    }

    @Test
    void delete_existingId_returns200() throws Exception {
        mockMvc.perform(delete("/api/connections/{id}", 1L))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));
    }

    @Test
    void delete_connectionInUse_returns409() throws Exception {
        doThrow(new BusinessException(ErrorCode.CONNECTION_IN_USE, "이 연결정보를 사용 중인 파이프라인이 있어 삭제할 수 없습니다: customers-cdc"))
                .when(connectionService).delete(1L);

        mockMvc.perform(delete("/api/connections/{id}", 1L))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error.code").value("CONNECTION_IN_USE"));
    }
}
