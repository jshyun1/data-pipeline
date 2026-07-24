package com.company.pipeline.connection;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.connection.dto.ConnectionCreateRequest;
import com.company.pipeline.connection.dto.ConnectionResponse;
import com.company.pipeline.connection.dto.ConnectionUpdateRequest;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/connections")
public class ConnectionController {

    private final ConnectionService connectionService;
    private final SchemaDiscoveryService schemaDiscoveryService;

    public ConnectionController(ConnectionService connectionService,
            SchemaDiscoveryService schemaDiscoveryService) {
        this.connectionService = connectionService;
        this.schemaDiscoveryService = schemaDiscoveryService;
    }

    @PostMapping
    public ApiResponse<ConnectionResponse> create(@Valid @RequestBody ConnectionCreateRequest request) {
        return ApiResponse.success(connectionService.create(request));
    }

    @GetMapping
    public ApiResponse<List<ConnectionResponse>> list() {
        return ApiResponse.success(connectionService.list());
    }

    @GetMapping("/{id}")
    public ApiResponse<ConnectionResponse> get(@PathVariable Long id) {
        return ApiResponse.success(connectionService.get(id));
    }

    @PutMapping("/{id}")
    public ApiResponse<ConnectionResponse> update(@PathVariable Long id,
            @Valid @RequestBody ConnectionUpdateRequest request) {
        return ApiResponse.success(connectionService.update(id, request));
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        connectionService.delete(id);
        return ApiResponse.success(null);
    }

    @GetMapping("/{id}/schemas")
    public ApiResponse<List<String>> listSchemas(@PathVariable Long id) {
        return ApiResponse.success(schemaDiscoveryService.listSchemas(id));
    }

    @GetMapping("/{id}/tables")
    public ApiResponse<List<String>> listTables(@PathVariable Long id, @RequestParam String schema) {
        return ApiResponse.success(schemaDiscoveryService.listTables(id, schema));
    }
}
