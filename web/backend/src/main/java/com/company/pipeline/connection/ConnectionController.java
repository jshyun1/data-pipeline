package com.company.pipeline.connection;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.connection.dto.ConnectionCreateRequest;
import com.company.pipeline.connection.dto.ConnectionResponse;
import com.company.pipeline.connection.dto.ConnectionTestRequest;
import com.company.pipeline.connection.dto.ConnectionTestResponse;
import com.company.pipeline.connection.dto.ConnectionUpdateRequest;
import com.company.pipeline.connection.dto.ConnectionUsageResponse;
import com.company.pipeline.connection.dto.CdcPrerequisiteResponse;
import com.company.pipeline.connection.dto.ColumnMetadataResponse;
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
    private final ConnectionValidationService connectionValidationService;
    private final ConnectionUsageService connectionUsageService;
    private final CdcPrerequisiteService cdcPrerequisiteService;

    public ConnectionController(ConnectionService connectionService,
            SchemaDiscoveryService schemaDiscoveryService,
            ConnectionValidationService connectionValidationService,
            ConnectionUsageService connectionUsageService,
            CdcPrerequisiteService cdcPrerequisiteService) {
        this.connectionService = connectionService;
        this.schemaDiscoveryService = schemaDiscoveryService;
        this.connectionValidationService = connectionValidationService;
        this.connectionUsageService = connectionUsageService;
        this.cdcPrerequisiteService = cdcPrerequisiteService;
    }

    @PostMapping
    public ApiResponse<ConnectionResponse> create(@Valid @RequestBody ConnectionCreateRequest request) {
        return ApiResponse.success(connectionService.create(request));
    }

    @GetMapping
    public ApiResponse<List<ConnectionResponse>> list() {
        return ApiResponse.success(connectionService.list());
    }

    @PostMapping("/validate")
    public ApiResponse<ConnectionTestResponse> validate(
            @Valid @RequestBody ConnectionTestRequest request) {
        return ApiResponse.success(connectionValidationService.validate(request));
    }

    @GetMapping("/usages")
    public ApiResponse<List<ConnectionUsageResponse>> usages() {
        return ApiResponse.success(connectionUsageService.list());
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

    @PostMapping("/{id}/test")
    public ApiResponse<ConnectionResponse> testConnection(@PathVariable Long id) {
        return ApiResponse.success(connectionService.testConnection(id));
    }

    @PostMapping("/{id}/validate")
    public ApiResponse<ConnectionTestResponse> validateUpdate(@PathVariable Long id,
            @Valid @RequestBody ConnectionUpdateRequest request) {
        return ApiResponse.success(connectionValidationService.validate(id, request));
    }

    @GetMapping("/{id}/usage")
    public ApiResponse<ConnectionUsageResponse> usage(@PathVariable Long id) {
        return ApiResponse.success(connectionUsageService.get(id));
    }

    @PostMapping("/{id}/cdc-prerequisites")
    public ApiResponse<CdcPrerequisiteResponse> cdcPrerequisites(@PathVariable Long id) {
        return ApiResponse.success(cdcPrerequisiteService.check(id));
    }

    @GetMapping("/{id}/schemas")
    public ApiResponse<List<String>> listSchemas(@PathVariable Long id) {
        return ApiResponse.success(schemaDiscoveryService.listSchemas(id));
    }

    @GetMapping("/{id}/tables")
    public ApiResponse<List<String>> listTables(@PathVariable Long id, @RequestParam String schema) {
        return ApiResponse.success(schemaDiscoveryService.listTables(id, schema));
    }

    @GetMapping("/{id}/columns")
    public ApiResponse<List<ColumnMetadataResponse>> listColumns(@PathVariable Long id, @RequestParam String schema,
            @RequestParam String table) {
        return ApiResponse.success(schemaDiscoveryService.listColumns(id, schema, table));
    }
}
