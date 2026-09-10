package com.company.pipeline.connection;

import com.company.pipeline.authz.RequirePermission;
import com.company.pipeline.authz.SystemCode;
import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.connection.dto.ConnectionCreateRequest;
import com.company.pipeline.connection.dto.ConnectionResponse;
import com.company.pipeline.connection.dto.ConnectionTestRequest;
import com.company.pipeline.connection.dto.ConnectionTestResponse;
import com.company.pipeline.connection.dto.ConnectionUpdateRequest;
import com.company.pipeline.connection.dto.ConnectionUsageResponse;
import com.company.pipeline.connection.dto.CdcPrerequisiteResponse;
import com.company.pipeline.connection.dto.CdcTableReadinessResponse;
import com.company.pipeline.connection.dto.ColumnMetadataResponse;
import com.company.pipeline.connection.dto.SqlValidationRequest;
import com.company.pipeline.connection.dto.SqlValidationResponse;
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
@RequirePermission(system = SystemCode.KAFKA)
public class ConnectionController {

    private final ConnectionService connectionService;
    private final SchemaDiscoveryService schemaDiscoveryService;
    private final ConnectionValidationService connectionValidationService;
    private final ConnectionUsageService connectionUsageService;
    private final CdcPrerequisiteService cdcPrerequisiteService;
    private final SqlValidationService sqlValidationService;

    public ConnectionController(ConnectionService connectionService,
            SchemaDiscoveryService schemaDiscoveryService,
            ConnectionValidationService connectionValidationService,
            ConnectionUsageService connectionUsageService,
            CdcPrerequisiteService cdcPrerequisiteService,
            SqlValidationService sqlValidationService) {
        this.connectionService = connectionService;
        this.schemaDiscoveryService = schemaDiscoveryService;
        this.connectionValidationService = connectionValidationService;
        this.connectionUsageService = connectionUsageService;
        this.cdcPrerequisiteService = cdcPrerequisiteService;
        this.sqlValidationService = sqlValidationService;
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

    /** 선택한 테이블 하나의 CDC 조건(Oracle 보충 로깅 / Postgres REPLICA IDENTITY)을 점검한다. */
    @GetMapping("/{id}/cdc-table-readiness")
    public ApiResponse<CdcTableReadinessResponse> cdcTableReadiness(@PathVariable Long id,
            @RequestParam String schema, @RequestParam String table) {
        return ApiResponse.success(cdcPrerequisiteService.checkTable(id, schema, table));
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

    @PostMapping("/{id}/sql-validation")
    public ApiResponse<SqlValidationResponse> validateSql(@PathVariable Long id,
            @Valid @RequestBody SqlValidationRequest request) {
        return ApiResponse.success(sqlValidationService.validateSelect(id, request.sql()));
    }
}
