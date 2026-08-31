package com.company.pipeline.nifi;

import com.company.pipeline.nifi.dto.NifiBulletinBoardResponse;
import com.company.pipeline.nifi.dto.NifiCountersResponse;
import com.company.pipeline.nifi.dto.NifiControllerServiceEntity;
import com.company.pipeline.nifi.dto.NifiFileLoadCreateRequest;
import com.company.pipeline.nifi.dto.NifiFlowResponse;
import com.company.pipeline.nifi.dto.NifiFlowStatusResponse;
import com.company.pipeline.nifi.dto.NifiInitialDbToDbCreateRequest;
import com.company.pipeline.nifi.dto.NifiLabelEntity;
import com.company.pipeline.nifi.dto.NifiParameterContextResponse;
import com.company.pipeline.nifi.dto.NifiProcessGroupEntity;
import com.company.pipeline.nifi.dto.NifiProcessGroupResponse;
import com.company.pipeline.nifi.dto.NifiProcessorDetailResponse;
import java.net.http.HttpClient;
import java.time.Duration;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Component
@EnableConfigurationProperties(NifiProperties.class)
public class NifiClient {

    private static final String ROOT_GROUP_ID = "root";
    private static final String POSTGRES_DBCP_TYPE = "org.apache.nifi.dbcp.DBCPConnectionPool";
    private static final String POSTGRES_DBCP_BUNDLE_GROUP = "org.apache.nifi";
    private static final String POSTGRES_DBCP_BUNDLE_ARTIFACT = "nifi-dbcp-service-nar";
    private static final String POSTGRES_DBCP_BUNDLE_VERSION = "2.2.0";
    private static final String POSTGRES_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String POSTGRES_DRIVER_LOCATION = "/opt/nifi/nifi-current/drivers/postgresql-42.7.4.jar";
    private static final String ORACLE_DRIVER_CLASS = "oracle.jdbc.OracleDriver";
    private static final String ORACLE_DRIVER_LOCATION = "/opt/nifi/nifi-current/drivers/ojdbc11-23.26.2.0.0.jar";
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String MYSQL_DRIVER_LOCATION = "/opt/nifi/nifi-current/drivers/mysql-connector-j-8.4.0.jar";
    private static final String INITIAL_TEMPLATE_GROUP_NAME = "Initial";
    private static final String TRUNCATE_TEMPLATE_GROUP_NAME = "truncate_initial";
    private static final String TEMPLATE_GROUP_NAME = "Template";
    private static final String INCREMENTAL_TEMPLATE_GROUP_NAME = "Incremental";
    private static final String FILE_TEMPLATE_GROUP_NAME = "File";
    private static final List<String> LIST_FILE_INPUT_DIRECTORY_KEYS = List.of(
            "Input Directory",
            "input-directory"
    );
    private static final List<String> QUERY_RECORD_SQL_KEYS = List.of(
            "DB_DATA"
    );
    private static final List<String> QUERY_DBCP_KEYS = List.of(
            "Database Connection Pooling Service",
            "dbcp-service"
    );
    private static final List<String> QUERY_DATABASE_TYPE_KEYS = List.of(
            "db-type",
            "Database Type"
    );
    private static final List<String> QUERY_TABLE_KEYS = List.of(
            "Table Name",
            "table-name"
    );
    private static final List<String> QUERY_ADDITIONAL_WHERE_KEYS = List.of(
            "Additional WHERE clause",
            "Additional Where Clause",
            "additional-where-clause"
    );
    private static final List<String> PUT_DBCP_KEYS = List.of(
            "put-db-record-dcbp-service",
            "Database Connection Pooling Service"
    );
    private static final List<String> PUT_DATABASE_TYPE_KEYS = List.of(
            "db-type",
            "Database Type"
    );
    private static final List<String> PUT_SCHEMA_KEYS = List.of(
            "put-db-record-schema-name",
            "Schema Name"
    );
    private static final List<String> PUT_TABLE_KEYS = List.of(
            "put-db-record-table-name",
            "Table Name"
    );
    private static final List<String> PUT_STATEMENT_KEYS = List.of(
            "put-db-record-statement-type",
            "Statement Type"
    );
    private static final List<String> PUT_UPDATE_KEYS = List.of(
            "put-db-record-update-keys",
            "Update Keys"
    );
    private static final List<String> TRUNCATE_DBCP_KEYS = List.of(
            "JDBC Connection Pool",
            "JDBC Connection Pooling Service"
    );
    private static final List<String> SELECT_DBCP_KEYS = List.of(
            "Database Connection Pooling Service",
            "dbcp-service"
    );
    private static final List<String> SELECT_SQL_KEYS = List.of(
            "SQL select query",
            "SQL Select Query",
            "sql-select-query"
    );
    private static final List<String> REPLACE_TEXT_VALUE_KEYS = List.of(
            "Replacement Value",
            "replacement-value"
    );

    private final RestClient restClient;
    private final NifiProperties properties;

    public NifiClient(NifiProperties properties) {
        this.properties = properties;
        // 외부 클라이언트 타임아웃 규약(설계서 3-2): connect 2s / read 5s.
        // root status recursive 응답이 무거워 read 3s는 위험하므로 5s로 둔다.
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(insecureHttpClient());
        requestFactory.setReadTimeout(Duration.ofSeconds(5));
        this.restClient = RestClient.builder()
                .baseUrl(properties.baseUrl())
                .requestFactory(requestFactory)
                .build();
    }

    public NifiProcessGroupResponse createRootProcessGroup(String name) {
        String token = getToken();
        Map<String, Object> body = Map.of(
                "revision", Map.of(
                        "clientId", UUID.randomUUID().toString(),
                        "version", 0
                ),
                "component", Map.of(
                        "name", name,
                        "position", Map.of(
                                "x", 120,
                                "y", 120
                        )
                )
        );

        try {
            NifiProcessGroupEntity entity = restClient.post()
                    .uri("/nifi-api/process-groups/{groupId}/process-groups", ROOT_GROUP_ID)
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(NifiProcessGroupEntity.class);
            return toResponse(entity);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi Processor Group 생성 실패: " + ex.getMessage(), ex);
        }
    }

    public void deleteRootProcessGroupByIdPrefix(String idPrefix) {
        List<NifiFlowResponse.ProcessGroupEntity> matches = childProcessGroups(ROOT_GROUP_ID).stream()
                .filter(candidate -> componentId(candidate).startsWith(idPrefix))
                .toList();
        if (matches.size() != 1) {
            throw new NifiClientException(
                    "NiFi 최상위 Processor Group 식별 결과가 1건이 아닙니다: "
                            + idPrefix + " (" + matches.size() + "건)", null);
        }
        NifiFlowResponse.ProcessGroupEntity group = matches.getFirst();
        long version = group.revision() == null || group.revision().version() == null
                ? 0L
                : group.revision().version();
        try {
            restClient.delete()
                    .uri(uri -> uri.path("/nifi-api/process-groups/{id}")
                            .queryParam("version", version)
                            .build(componentId(group)))
                    .header("Authorization", "Bearer " + getToken())
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException ex) {
            throw new NifiClientException(
                    "NiFi Processor Group 삭제 실패(실행 중이면 먼저 중지해야 합니다): " + ex.getMessage(), ex);
        }
    }

    public NifiProcessGroupResponse createInitialDbToDbFlow(NifiInitialDbToDbCreateRequest request) {
        String loadMode = nullToBlank(request.loadMode()).trim().toUpperCase();
        if (!"INSERT".equals(loadMode) && !"TRUNCATE".equals(loadMode) && !"UPSERT".equals(loadMode)) {
            throw new NifiClientException("DB -> DB 템플릿은 INSERT, TRUNCATE, UPSERT 적재 방식만 지원합니다.", null);
        }
        if ("UPSERT".equals(loadMode) && !StringUtils.hasText(request.updateExtractQuery())) {
            throw new NifiClientException("UPSERT 적재 방식은 UPDATE행 추출 쿼리가 필요합니다.", null);
        }
        if ("UPSERT".equals(loadMode) && !StringUtils.hasText(request.primaryKeys())) {
            throw new NifiClientException("UPSERT 적재 방식은 Target Primary Keys가 필요합니다.", null);
        }

        String token = getToken();
        TemplateSelection templateSelection = templateSelection(loadMode);
        String snippetId = createProcessGroupSnippet(
                token,
                templateSelection.parentGroupId(),
                templateSelection.templateGroupId(),
                templateSelection.templateGroupVersion()
        );

        try {
            Set<String> existingChildGroupIds = childProcessGroupIds(request.parentGroupId().trim());
            String createdGroupId = instantiateSnippet(token, request.parentGroupId().trim(), snippetId);
            if (!StringUtils.hasText(createdGroupId)) {
                createdGroupId = findNewChildProcessGroupId(request.parentGroupId().trim(), existingChildGroupIds);
            }
            if (!StringUtils.hasText(createdGroupId)) {
                throw new NifiClientException("NiFi Initial 그룹 복제 응답에서 새 그룹 ID를 찾지 못했습니다.", null);
            }

            NifiFlowResponse.ProcessGroupEntity createdGroup =
                    findChildProcessGroupById(request.parentGroupId().trim(), createdGroupId);
            updateProcessGroup(token, createdGroup, request.jobName().trim(), nullToBlank(request.comments()));
            updateInitialDbToDbProcessors(token, createdGroupId, request, loadMode);
            return new NifiProcessGroupResponse(
                    createdGroupId,
                    request.jobName().trim(),
                    request.parentGroupId().trim(),
                    directProcessorCount(createdGroupId)
            );
        } finally {
            deleteSnippetQuietly(token, snippetId);
        }
    }

    public NifiProcessGroupResponse createFileLoadFlow(NifiFileLoadCreateRequest request) {
        String token = getToken();
        TemplateSelection templateSelection = fileTemplateSelection();
        String snippetId = createProcessGroupSnippet(
                token,
                templateSelection.parentGroupId(),
                templateSelection.templateGroupId(),
                templateSelection.templateGroupVersion()
        );

        try {
            Set<String> existingChildGroupIds = childProcessGroupIds(request.parentGroupId().trim());
            String createdGroupId = instantiateSnippet(token, request.parentGroupId().trim(), snippetId);
            if (!StringUtils.hasText(createdGroupId)) {
                createdGroupId = findNewChildProcessGroupId(request.parentGroupId().trim(), existingChildGroupIds);
            }
            if (!StringUtils.hasText(createdGroupId)) {
                throw new NifiClientException("NiFi File 그룹 복제 응답에서 새 그룹 ID를 찾지 못했습니다.", null);
            }

            NifiFlowResponse.ProcessGroupEntity createdGroup =
                    findChildProcessGroupById(request.parentGroupId().trim(), createdGroupId);
            updateProcessGroup(token, createdGroup, request.jobName().trim(), nullToBlank(request.comments()));
            updateFileLoadProcessors(token, createdGroupId, request);
            return new NifiProcessGroupResponse(
                    createdGroupId,
                    request.jobName().trim(),
                    request.parentGroupId().trim(),
                    directProcessorCount(createdGroupId)
            );
        } finally {
            deleteSnippetQuietly(token, snippetId);
        }
    }

    public NifiControllerServiceEntity createPostgresDbcpControllerService(
            Long connectionId,
            String connectionName,
            String host,
            Integer port,
            String databaseName,
            String username,
            String password
    ) {
        String jdbcUrl = "jdbc:postgresql://%s:%d/%s".formatted(host, port, databaseName);
        return createDbcpControllerService(
                connectionId,
                connectionName,
                jdbcUrl,
                POSTGRES_DRIVER_CLASS,
                POSTGRES_DRIVER_LOCATION,
                username,
                password
        );
    }

    public NifiControllerServiceEntity createOracleDbcpControllerService(
            Long connectionId,
            String connectionName,
            String host,
            Integer port,
            String serviceName,
            String username,
            String password
    ) {
        String jdbcUrl = "jdbc:oracle:thin:@%s:%d/%s".formatted(host, port, serviceName);
        return createDbcpControllerService(
                connectionId,
                connectionName,
                jdbcUrl,
                ORACLE_DRIVER_CLASS,
                ORACLE_DRIVER_LOCATION,
                username,
                password
        );
    }

    public NifiControllerServiceEntity createMysqlDbcpControllerService(
            Long connectionId,
            String connectionName,
            String host,
            Integer port,
            String databaseName,
            String username,
            String password
    ) {
        String jdbcUrl = "jdbc:mysql://%s:%d/%s".formatted(host, port, databaseName);
        return createDbcpControllerService(
                connectionId,
                connectionName,
                jdbcUrl,
                MYSQL_DRIVER_CLASS,
                MYSQL_DRIVER_LOCATION,
                username,
                password
        );
    }

    private NifiControllerServiceEntity createDbcpControllerService(
            Long connectionId,
            String connectionName,
            String jdbcUrl,
            String driverClass,
            String driverLocation,
            String username,
            String password
    ) {
        String token = getToken();
        String serviceName = "cdc-%d-%s".formatted(connectionId, connectionName);
        Map<String, Object> body = Map.of(
                "revision", Map.of(
                        "clientId", UUID.randomUUID().toString(),
                        "version", 0
                ),
                "component", Map.of(
                        "name", serviceName,
                        "type", POSTGRES_DBCP_TYPE,
                        "bundle", Map.of(
                                "group", POSTGRES_DBCP_BUNDLE_GROUP,
                                "artifact", POSTGRES_DBCP_BUNDLE_ARTIFACT,
                                "version", POSTGRES_DBCP_BUNDLE_VERSION
                        ),
                        "properties", Map.of(
                                "Database Connection URL", jdbcUrl,
                                "Database Driver Class Name", driverClass,
                                "database-driver-locations", driverLocation,
                                "Database User", username,
                                "Password", password
                        )
                )
        );

        try {
            NifiControllerServiceEntity created = restClient.post()
                    .uri("/nifi-api/process-groups/{groupId}/controller-services", ROOT_GROUP_ID)
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(NifiControllerServiceEntity.class);
            return enableControllerService(created, token);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi DBCPConnectionPool 생성 실패: " + ex.getMessage(), ex);
        }
    }

    private NifiControllerServiceEntity enableControllerService(NifiControllerServiceEntity service, String token) {
        if (service == null || service.component() == null || !StringUtils.hasText(service.component().id())) {
            throw new NifiClientException("NiFi Controller Service 생성 응답이 비어 있습니다.", null);
        }
        String serviceId = service.component().id();
        long version = service.revision() == null || service.revision().version() == null
                ? 0L : service.revision().version();
        Map<String, Object> body = Map.of(
                "revision", Map.of(
                        "clientId", UUID.randomUUID().toString(),
                        "version", version
                ),
                "state", "ENABLED"
        );

        try {
            return restClient.put()
                    .uri("/nifi-api/controller-services/{id}/run-status", serviceId)
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(NifiControllerServiceEntity.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi DBCPConnectionPool 활성화 실패: " + ex.getMessage(), ex);
        }
    }

    /** 적재 건수 집계용 - 프로세스 그룹 트리 전체(재귀)의 프로세서 상태 스냅샷. */
    public NifiFlowStatusResponse getRootFlowStatus() {
        String token = getToken();
        try {
            return restClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/nifi-api/flow/process-groups/root/status")
                            .queryParam("recursive", true)
                            .build())
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(NifiFlowStatusResponse.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 프로세스 그룹 상태 조회 실패: " + ex.getMessage(), ex);
        }
    }

    /** 서비스(대행) 계정 사용자명. 캔버스 감사에서 "자동 오버레이 쓰기"를 걸러내는 데 쓴다. */
    public String getServiceUsername() {
        return properties.username();
    }

    /** NiFi Flow Configuration History 중 action id 가 {@code afterActionId} 초과인 것을 id 오름차순으로. */
    public java.util.List<FlowAction> getFlowHistory(int afterActionId) {
        String token = getToken();
        try {
            HistoryResponse resp = restClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/nifi-api/flow/history")
                            .queryParam("offset", 0)
                            .queryParam("count", 200)
                            .queryParam("sortColumn", "timestamp")
                            .queryParam("sortOrder", "desc")
                            .build())
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(HistoryResponse.class);
            if (resp == null || resp.history() == null || resp.history().actions() == null) {
                return java.util.List.of();
            }
            return resp.history().actions().stream()
                    .map(HistoryActionEntry::action)
                    .filter(a -> a != null && a.id() > afterActionId)
                    .map(a -> new FlowAction(a.id(), a.userIdentity(), a.timestamp(),
                            a.sourceName(), a.sourceType(), a.operation()))
                    .sorted(java.util.Comparator.comparingInt(FlowAction::id))
                    .toList();
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 이력 조회 실패: " + ex.getMessage(), ex);
        }
    }

    public record FlowAction(int id, String userIdentity, String timestamp,
                             String sourceName, String sourceType, String operation) {
    }

    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    private record HistoryResponse(History history) {
    }

    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    private record History(java.util.List<HistoryActionEntry> actions) {
    }

    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    private record HistoryActionEntry(HistoryAction action) {
    }

    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    private record HistoryAction(int id, String userIdentity, String timestamp,
                                 String sourceName, String sourceType, String operation) {
    }

    /**
     * 잡 카탈로그 동기화용 - 그룹 한 단계의 "구성"(하위 그룹/프로세서/연결선).
     *
     * <p>상태 조회와 달리 프로세서 설정값과 연결의 관계 이름, 캔버스 좌표가 들어 있다.
     * 한 번에 한 단계만 내려오므로 하위 그룹은 호출자가 재귀로 다시 부른다.
     */
    public NifiFlowResponse getFlow(String groupId) {
        String token = getToken();
        try {
            return restClient.get()
                    .uri("/nifi-api/flow/process-groups/{groupId}", groupId)
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(NifiFlowResponse.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 프로세스 그룹 구성 조회 실패: " + ex.getMessage(), ex);
        }
    }

    public NifiLabelEntity createLabel(String parentGroupId, String label,
            double x, double y, Map<String, String> style) {
        String token = getToken();
        Map<String, Object> body = Map.of(
                "revision", Map.of(
                        "clientId", UUID.randomUUID().toString(),
                        "version", 0
                ),
                "component", Map.of(
                        "label", label,
                        "position", Map.of("x", x, "y", y),
                        "style", style == null ? Map.of() : style
                )
        );

        try {
            return restClient.post()
                    .uri("/nifi-api/process-groups/{groupId}/labels", parentGroupId)
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(NifiLabelEntity.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 라벨 생성 실패: " + ex.getMessage(), ex);
        }
    }

    public NifiLabelEntity updateLabel(String labelId, long version, String label,
            double x, double y, Map<String, String> style) {
        String token = getToken();
        Map<String, Object> body = Map.of(
                "revision", Map.of(
                        "clientId", UUID.randomUUID().toString(),
                        "version", version
                ),
                "component", Map.of(
                        "id", labelId,
                        "label", label,
                        "position", Map.of("x", x, "y", y),
                        "style", style == null ? Map.of() : style
                )
        );

        try {
            return restClient.put()
                    .uri("/nifi-api/labels/{id}", labelId)
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(NifiLabelEntity.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 라벨 수정 실패: " + ex.getMessage(), ex);
        }
    }

    public void deleteLabel(String labelId, long version) {
        String token = getToken();
        try {
            restClient.delete()
                    .uri(uri -> uri.path("/nifi-api/labels/{id}")
                            .queryParam("version", version)
                            .build(labelId))
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 라벨 삭제 실패: " + ex.getMessage(), ex);
        }
    }

    /** 그룹에 바인딩된 파라미터 컨텍스트의 파라미터 목록. sensitive 값은 내려오지 않는다. */
    public NifiParameterContextResponse getParameterContext(String parameterContextId) {
        String token = getToken();
        try {
            return restClient.get()
                    .uri("/nifi-api/parameter-contexts/{id}", parameterContextId)
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(NifiParameterContextResponse.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 파라미터 컨텍스트 조회 실패: " + ex.getMessage(), ex);
        }
    }

    /** 적재 건수 집계용 - PutDatabaseRecord 등이 등록한 누적 카운터(재시작 전까지 유지). */
    public NifiCountersResponse getCounters() {
        String token = getToken();
        try {
            return restClient.get()
                    .uri("/nifi-api/counters")
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(NifiCountersResponse.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 카운터 조회 실패: " + ex.getMessage(), ex);
        }
    }

    /**
     * 프로세서 1개의 설정. status 응답에는 설정이 없어서 적재 대상 테이블 같은 값을
     * 얻으려면 이걸 따로 불러야 한다 - 프로세서마다 한 번만 부르고 캐시할 것.
     */
    public NifiProcessorDetailResponse getProcessor(String processorId) {
        String token = getToken();
        try {
            return restClient.get()
                    .uri("/nifi-api/processors/{id}", processorId)
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(NifiProcessorDetailResponse.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 프로세서 조회 실패: " + ex.getMessage(), ex);
        }
    }

    /**
     * 경고/에러 알림 조회. {@code afterId} 이후에 생긴 것만 받아서 같은 알림을 두 번
     * 처리하지 않는다.
     *
     * <p>bulletin은 NiFi 메모리에 5분 남짓만 남으므로, 이 호출 주기가 그보다 길면
     * 그 사이 발생한 실패는 조용히 유실된다.
     */
    public NifiBulletinBoardResponse getBulletins(long afterId) {
        String token = getToken();
        try {
            return restClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/nifi-api/flow/bulletin-board")
                            .queryParam("after", afterId)
                            .build())
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(NifiBulletinBoardResponse.class);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi bulletin 조회 실패: " + ex.getMessage(), ex);
        }
    }

    private NifiFlowResponse.ProcessGroupEntity findChildProcessGroup(String parentGroupId, String childName) {
        return childProcessGroups(parentGroupId).stream()
                .filter(group -> group.component() != null)
                .filter(group -> childName.equalsIgnoreCase(nullToBlank(group.component().name()).trim()))
                .findFirst()
                .orElseThrow(() -> new NifiClientException(
                        "NiFi Processor Group '%s' 아래에서 '%s' 그룹을 찾지 못했습니다.".formatted(parentGroupId, childName),
                        null));
    }

    private NifiFlowResponse.ProcessGroupEntity findChildProcessGroupById(String parentGroupId, String childId) {
        NifiFlowResponse flow = getFlow(parentGroupId);
        List<NifiFlowResponse.ProcessGroupEntity> groups = flow == null
                || flow.processGroupFlow() == null
                || flow.processGroupFlow().flow() == null
                || flow.processGroupFlow().flow().processGroups() == null
                ? List.of()
                : flow.processGroupFlow().flow().processGroups();
        return groups.stream()
                .filter(group -> childId.equals(componentId(group)))
                .findFirst()
                .orElseThrow(() -> new NifiClientException(
                        "NiFi Processor Group '%s' 아래에서 복제된 그룹 '%s'을 찾지 못했습니다."
                                .formatted(parentGroupId, childId),
                        null));
    }

    private Set<String> childProcessGroupIds(String parentGroupId) {
        return childProcessGroups(parentGroupId).stream()
                .map(group -> componentId(group))
                .filter(StringUtils::hasText)
                .collect(Collectors.toSet());
    }

    private List<NifiFlowResponse.ProcessGroupEntity> childProcessGroups(String parentGroupId) {
        NifiFlowResponse flow = getFlow(parentGroupId);
        return flow == null
                || flow.processGroupFlow() == null
                || flow.processGroupFlow().flow() == null
                || flow.processGroupFlow().flow().processGroups() == null
                ? List.of()
                : flow.processGroupFlow().flow().processGroups();
    }

    private String findNewChildProcessGroupId(String parentGroupId, Set<String> existingChildGroupIds) {
        return childProcessGroupIds(parentGroupId).stream()
                .filter(id -> !existingChildGroupIds.contains(id))
                .findFirst()
                .orElse(null);
    }

    @SuppressWarnings("unchecked")
    private String createProcessGroupSnippet(String token, String sourceParentGroupId, String sourceGroupId,
            long sourceGroupVersion) {
        Map<String, Object> selectedGroupRevision = Map.of(
                "clientId", UUID.randomUUID().toString(),
                "version", sourceGroupVersion
        );
        Map<String, Object> snippet = new LinkedHashMap<>();
        snippet.put("parentGroupId", sourceParentGroupId);
        snippet.put("processGroups", Map.of(sourceGroupId, selectedGroupRevision));
        Map<String, Object> body = Map.of("snippet", snippet);

        try {
            Map<String, Object> response = restClient.post()
                    .uri("/nifi-api/snippets")
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(Map.class);
            String snippetId = stringAt(response, "snippet", "id");
            if (!StringUtils.hasText(snippetId)) {
                snippetId = stringAt(response, "id");
            }
            if (!StringUtils.hasText(snippetId)) {
                throw new NifiClientException("NiFi snippet 생성 응답이 비어 있습니다.", null);
            }
            return snippetId;
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi Initial 그룹 snippet 생성 실패: " + ex.getMessage(), ex);
        }
    }

    @SuppressWarnings("unchecked")
    private String instantiateSnippet(String token, String targetParentGroupId, String snippetId) {
        Map<String, Object> body = Map.of(
                "snippetId", snippetId,
                "originX", 120,
                "originY", 120
        );

        try {
            Map<String, Object> response = restClient.post()
                    .uri("/nifi-api/process-groups/{groupId}/snippet-instance", targetParentGroupId)
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .body(Map.class);
            return firstProcessGroupId(response);
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi Initial 그룹 복제 실패: " + ex.getMessage(), ex);
        }
    }

    private void deleteSnippetQuietly(String token, String snippetId) {
        if (!StringUtils.hasText(snippetId)) {
            return;
        }
        try {
            restClient.delete()
                    .uri("/nifi-api/snippets/{id}", snippetId)
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException ignored) {
            // 복제 완료 뒤 snippet 정리에 실패해도 생성된 플로우 자체는 유효하다.
        }
    }

    private void updateProcessGroup(String token, NifiFlowResponse.ProcessGroupEntity group,
            String name, String comments) {
        String groupId = componentId(group);
        Map<String, Object> component = new LinkedHashMap<>();
        component.put("id", groupId);
        component.put("name", name);
        component.put("comments", comments);
        Map<String, Object> body = Map.of(
                "revision", Map.of(
                        "clientId", UUID.randomUUID().toString(),
                        "version", revisionVersion(group.revision())
                ),
                "component", component
        );

        try {
            restClient.put()
                    .uri("/nifi-api/process-groups/{id}", groupId)
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi Processor Group 정보 수정 실패: " + ex.getMessage(), ex);
        }
    }

    private TemplateSelection templateSelection(String loadMode) {
        NifiFlowResponse.ProcessGroupEntity templateGroup = findChildProcessGroup(ROOT_GROUP_ID, TEMPLATE_GROUP_NAME);
        if ("UPSERT".equals(loadMode)) {
            NifiFlowResponse.ProcessGroupEntity incrementalGroup =
                    findChildProcessGroup(componentId(templateGroup), INCREMENTAL_TEMPLATE_GROUP_NAME);
            return new TemplateSelection(
                    componentId(templateGroup),
                    componentId(incrementalGroup),
                    revisionVersion(incrementalGroup.revision())
            );
        }

        String templateGroupName = "TRUNCATE".equals(loadMode)
                ? TRUNCATE_TEMPLATE_GROUP_NAME
                : INITIAL_TEMPLATE_GROUP_NAME;
        NifiFlowResponse.ProcessGroupEntity initialGroup =
                findChildProcessGroup(componentId(templateGroup), templateGroupName);
        return new TemplateSelection(
                componentId(templateGroup),
                componentId(initialGroup),
                revisionVersion(initialGroup.revision())
        );
    }

    private TemplateSelection fileTemplateSelection() {
        NifiFlowResponse.ProcessGroupEntity templateGroup = findChildProcessGroup(ROOT_GROUP_ID, TEMPLATE_GROUP_NAME);
        NifiFlowResponse.ProcessGroupEntity fileGroup =
                findChildProcessGroup(componentId(templateGroup), FILE_TEMPLATE_GROUP_NAME);
        return new TemplateSelection(
                componentId(templateGroup),
                componentId(fileGroup),
                revisionVersion(fileGroup.revision())
        );
    }

    private void updateFileLoadProcessors(String token, String groupId, NifiFileLoadCreateRequest request) {
        NifiFlowResponse flow = getFlow(groupId);
        List<NifiFlowResponse.ProcessorEntity> processors = flow == null
                || flow.processGroupFlow() == null
                || flow.processGroupFlow().flow() == null
                || flow.processGroupFlow().flow().processors() == null
                ? List.of()
                : flow.processGroupFlow().flow().processors();
        NifiFlowResponse.ProcessorEntity listFile = findProcessor(processors, "ListFile");
        NifiFlowResponse.ProcessorEntity queryRecord = findProcessor(processors, "QueryRecord");
        NifiFlowResponse.ProcessorEntity put = findProcessor(processors, "PutDatabaseRecord");

        Map<String, String> listFileProperties = mergedProperties(listFile);
        putProperty(listFileProperties, LIST_FILE_INPUT_DIRECTORY_KEYS, request.inputDirectory().trim());
        updateProcessorProperties(token, listFile, listFileProperties);

        Map<String, String> queryProperties = mergedProperties(queryRecord);
        putProperty(queryProperties, QUERY_RECORD_SQL_KEYS, queryRecordSql(request.columns()));
        updateProcessorProperties(token, queryRecord, queryProperties);

        Map<String, String> putProperties = mergedProperties(put);
        putProperty(putProperties, PUT_DBCP_KEYS, request.targetServiceId().trim());
        putProperty(putProperties, PUT_DATABASE_TYPE_KEYS, request.targetDatabaseType().trim());
        putProperty(putProperties, PUT_SCHEMA_KEYS, request.targetSchema().trim());
        putProperty(putProperties, PUT_TABLE_KEYS, request.targetTable().trim());
        updateProcessorProperties(token, put, putProperties);
    }

    private String queryRecordSql(List<String> columns) {
        String projection = columns.stream()
                .map(column -> column.trim().toUpperCase())
                .filter(StringUtils::hasText)
                .distinct()
                .collect(Collectors.joining(",\n    "));
        if (!StringUtils.hasText(projection)) {
            throw new NifiClientException("파일 컬럼을 찾지 못해 QueryRecord SQL을 만들 수 없습니다.", null);
        }
        return "SELECT\n    %s\nFROM FLOWFILE".formatted(projection);
    }

    private void updateInitialDbToDbProcessors(String token, String groupId,
            NifiInitialDbToDbCreateRequest request, String loadMode) {
        NifiFlowResponse flow = getFlow(groupId);
        List<NifiFlowResponse.ProcessorEntity> processors = flow == null
                || flow.processGroupFlow() == null
                || flow.processGroupFlow().flow() == null
                || flow.processGroupFlow().flow().processors() == null
                ? List.of()
                : flow.processGroupFlow().flow().processors();
        if ("TRUNCATE".equals(loadMode)) {
            updateTruncateDbToDbProcessors(token, processors, request);
            return;
        }
        if ("UPSERT".equals(loadMode)) {
            updateUpsertDbToDbProcessors(token, processors, request);
            return;
        }

        NifiFlowResponse.ProcessorEntity query = findProcessor(processors, "QueryDatabaseTableRecord");
        NifiFlowResponse.ProcessorEntity put = findProcessor(processors, "PutDatabaseRecord");

        Map<String, String> queryProperties = mergedProperties(query);
        putProperty(queryProperties, QUERY_DBCP_KEYS, request.sourceServiceId().trim());
        putProperty(queryProperties, QUERY_DATABASE_TYPE_KEYS, request.sourceDatabaseType().trim());
        putProperty(queryProperties, QUERY_TABLE_KEYS, "%s.%s".formatted(
                request.sourceSchema().trim(), request.sourceTable().trim()));
        updateProcessorProperties(token, query, queryProperties);

        Map<String, String> putProperties = mergedProperties(put);
        putProperty(putProperties, PUT_DBCP_KEYS, request.targetServiceId().trim());
        putProperty(putProperties, PUT_DATABASE_TYPE_KEYS, request.targetDatabaseType().trim());
        putProperty(putProperties, PUT_SCHEMA_KEYS, request.targetSchema().trim());
        putProperty(putProperties, PUT_TABLE_KEYS, request.targetTable().trim());
        putProperty(putProperties, PUT_STATEMENT_KEYS, "INSERT");
        updateProcessorProperties(token, put, putProperties);
    }

    private void updateUpsertDbToDbProcessors(String token, List<NifiFlowResponse.ProcessorEntity> processors,
            NifiInitialDbToDbCreateRequest request) {
        NifiFlowResponse.ProcessorEntity source = findProcessorByName(processors, "incremental_sourceDB");
        NifiFlowResponse.ProcessorEntity upsert = findProcessorByName(processors, "targetDB_UPSERT");
        NifiFlowResponse.ProcessorEntity delete = findOptionalProcessorByName(processors, "targetDB_DELETE");

        Map<String, String> sourceProperties = mergedProperties(source);
        putProperty(sourceProperties, QUERY_DBCP_KEYS, request.sourceServiceId().trim());
        putProperty(sourceProperties, QUERY_DATABASE_TYPE_KEYS, request.sourceDatabaseType().trim());
        putProperty(sourceProperties, QUERY_TABLE_KEYS, "%s.%s".formatted(
                request.sourceSchema().trim(), request.sourceTable().trim()));
        putProperty(sourceProperties, QUERY_ADDITIONAL_WHERE_KEYS, request.updateExtractQuery().trim());
        updateProcessorProperties(token, source, sourceProperties);

        updateTargetDbRecordProcessor(token, upsert, request, "UPSERT");
        if (delete != null) {
            updateTargetDbRecordProcessor(token, delete, request, null);
        }
    }

    private void updateTargetDbRecordProcessor(String token, NifiFlowResponse.ProcessorEntity processor,
            NifiInitialDbToDbCreateRequest request, String statementType) {
        Map<String, String> properties = mergedProperties(processor);
        putProperty(properties, PUT_DBCP_KEYS, request.targetServiceId().trim());
        putProperty(properties, PUT_DATABASE_TYPE_KEYS, request.targetDatabaseType().trim());
        putProperty(properties, PUT_SCHEMA_KEYS, request.targetSchema().trim());
        putProperty(properties, PUT_TABLE_KEYS, request.targetTable().trim());
        if (StringUtils.hasText(request.primaryKeys())) {
            putProperty(properties, PUT_UPDATE_KEYS, request.primaryKeys().trim());
        }
        if (StringUtils.hasText(statementType)) {
            putProperty(properties, PUT_STATEMENT_KEYS, statementType);
        }
        updateProcessorProperties(token, processor, properties);
    }

    private void updateTruncateDbToDbProcessors(String token, List<NifiFlowResponse.ProcessorEntity> processors,
            NifiInitialDbToDbCreateRequest request) {
        NifiFlowResponse.ProcessorEntity replaceText = findProcessor(processors, "ReplaceText");
        NifiFlowResponse.ProcessorEntity truncate = findProcessorByName(processors, "03_TRUNCATE_DZ");
        NifiFlowResponse.ProcessorEntity select = findProcessorByName(processors, "04_SELECT");
        NifiFlowResponse.ProcessorEntity insert = findProcessorByName(processors, "05_INSERT");

        Map<String, String> replaceProperties = mergedProperties(replaceText);
        putProperty(replaceProperties, REPLACE_TEXT_VALUE_KEYS, truncateSql(request));
        updateProcessorProperties(token, replaceText, replaceProperties);

        Map<String, String> truncateProperties = mergedProperties(truncate);
        putProperty(truncateProperties, TRUNCATE_DBCP_KEYS, request.targetServiceId().trim());
        updateProcessorProperties(token, truncate, truncateProperties);

        Map<String, String> selectProperties = mergedProperties(select);
        putProperty(selectProperties, SELECT_DBCP_KEYS, request.sourceServiceId().trim());
        putProperty(selectProperties, SELECT_SQL_KEYS, "SELECT * FROM %s.%s".formatted(
                request.sourceSchema().trim(), request.sourceTable().trim()));
        updateProcessorProperties(token, select, selectProperties);

        Map<String, String> insertProperties = mergedProperties(insert);
        putProperty(insertProperties, PUT_DATABASE_TYPE_KEYS, request.targetDatabaseType().trim());
        putProperty(insertProperties, PUT_DBCP_KEYS, request.targetServiceId().trim());
        putProperty(insertProperties, PUT_SCHEMA_KEYS, request.targetSchema().trim());
        putProperty(insertProperties, PUT_TABLE_KEYS, request.targetTable().trim());
        updateProcessorProperties(token, insert, insertProperties);
    }

    private String truncateSql(NifiInitialDbToDbCreateRequest request) {
        if (StringUtils.hasText(request.truncateSql())) {
            return request.truncateSql().trim();
        }
        return "TRUNCATE TABLE %s.%s".formatted(request.targetSchema().trim(), request.targetTable().trim());
    }

    private int directProcessorCount(String groupId) {
        NifiFlowResponse flow = getFlow(groupId);
        return flow == null
                || flow.processGroupFlow() == null
                || flow.processGroupFlow().flow() == null
                || flow.processGroupFlow().flow().processors() == null
                ? 0
                : flow.processGroupFlow().flow().processors().size();
    }

    private NifiFlowResponse.ProcessorEntity findProcessor(List<NifiFlowResponse.ProcessorEntity> processors,
            String shortType) {
        return processors.stream()
                .filter(processor -> processor.component() != null)
                .filter(processor -> shortType.equals(processor.component().shortType()))
                .findFirst()
                .orElseThrow(() -> new NifiClientException(
                        "복제된 Initial 그룹에서 %s 프로세서를 찾지 못했습니다.".formatted(shortType),
                        null));
    }

    private NifiFlowResponse.ProcessorEntity findProcessorByName(List<NifiFlowResponse.ProcessorEntity> processors,
            String name) {
        return processors.stream()
                .filter(processor -> processor.component() != null)
                .filter(processor -> name.equalsIgnoreCase(nullToBlank(processor.component().name()).trim()))
                .findFirst()
                .orElseThrow(() -> new NifiClientException(
                        "복제된 Initial 그룹에서 '%s' 프로세서를 찾지 못했습니다.".formatted(name),
                        null));
    }

    private NifiFlowResponse.ProcessorEntity findOptionalProcessorByName(
            List<NifiFlowResponse.ProcessorEntity> processors, String name) {
        return processors.stream()
                .filter(processor -> processor.component() != null)
                .filter(processor -> name.equalsIgnoreCase(nullToBlank(processor.component().name()).trim()))
                .findFirst()
                .orElse(null);
    }

    private Map<String, String> mergedProperties(NifiFlowResponse.ProcessorEntity processor) {
        Map<String, String> properties = new LinkedHashMap<>();
        if (processor.component() != null
                && processor.component().config() != null
                && processor.component().config().properties() != null) {
            properties.putAll(processor.component().config().properties());
        }
        return properties;
    }

    private void putProperty(Map<String, String> properties, List<String> candidateKeys, String value) {
        String matchedKey = properties.keySet().stream()
                .filter(key -> candidateKeys.stream().anyMatch(candidate -> samePropertyKey(key, candidate)))
                .findFirst()
                .orElse(candidateKeys.get(0));
        properties.put(matchedKey, value);
    }

    private boolean samePropertyKey(String left, String right) {
        return left != null && right != null
                && left.trim().replace(" ", "").replace("-", "").equalsIgnoreCase(
                right.trim().replace(" ", "").replace("-", ""));
    }

    private void updateProcessorProperties(String token, NifiFlowResponse.ProcessorEntity processor,
            Map<String, String> properties) {
        String processorId = componentId(processor);
        Map<String, Object> component = new LinkedHashMap<>();
        component.put("id", processorId);
        component.put("config", Map.of("properties", properties));
        Map<String, Object> body = Map.of(
                "revision", Map.of(
                        "clientId", UUID.randomUUID().toString(),
                        "version", revisionVersion(processor.revision())
                ),
                "component", component
        );

        try {
            restClient.put()
                    .uri("/nifi-api/processors/{id}", processorId)
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 프로세서 설정 수정 실패: " + ex.getMessage(), ex);
        }
    }

    @SuppressWarnings("unchecked")
    private String firstProcessGroupId(Map<String, Object> response) {
        Object groups = valueAt(response, "flow", "processGroups");
        if (!(groups instanceof List<?> groupList) || groupList.isEmpty()) {
            groups = valueAt(response, "processGroups");
        }
        if (groups instanceof List<?> groupList && !groupList.isEmpty()
                && groupList.get(0) instanceof Map<?, ?> firstGroup) {
            String componentId = stringAt((Map<String, Object>) firstGroup, "component", "id");
            if (StringUtils.hasText(componentId)) {
                return componentId;
            }
            return stringAt((Map<String, Object>) firstGroup, "id");
        }
        return null;
    }

    private Object valueAt(Map<String, Object> source, String... path) {
        Object current = source;
        for (String key : path) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            current = map.get(key);
        }
        return current;
    }

    private String stringAt(Map<String, Object> source, String... path) {
        Object value = valueAt(source, path);
        return value == null ? null : value.toString();
    }

    private String componentId(NifiFlowResponse.ProcessGroupEntity group) {
        if (group == null) {
            return null;
        }
        return group.component() != null && StringUtils.hasText(group.component().id())
                ? group.component().id()
                : group.id();
    }

    private String componentId(NifiFlowResponse.ProcessorEntity processor) {
        if (processor == null) {
            return null;
        }
        return processor.component() != null && StringUtils.hasText(processor.component().id())
                ? processor.component().id()
                : processor.id();
    }

    private long revisionVersion(NifiFlowResponse.Revision revision) {
        return revision == null || revision.version() == null ? 0L : revision.version();
    }

    private String nullToBlank(String value) {
        return value == null ? "" : value;
    }

    private record TemplateSelection(String parentGroupId, String templateGroupId, long templateGroupVersion) {
    }

    // NiFi가 Keycloak/OIDC를 제거하고 Single User 인증(공유 서비스계정)으로 전환됨에 따라,
    // /nifi-api/access/token에 username/password를 보내 JWT를 직접 발급받는다.
    // 이 엔드포인트는 응답 본문이 JSON이 아니라 JWT 문자열 그대로임에 주의.
    // ---- P5b: 개인계정(테넌트) + 정책 동기화 (스파이크 P5a에서 검증한 API 시퀀스) ----

    /** 루트 프로세스 그룹 UUID(정책 리소스 "/process-groups/{id}"에 필요). 실패 시 "root". */
    @SuppressWarnings("unchecked")
    public String getRootProcessGroupId() {
        String token = getToken();
        try {
            Map<String, Object> resp = restClient.get()
                    .uri("/nifi-api/flow/process-groups/root")
                    .header("Authorization", "Bearer " + token)
                    .retrieve().body(Map.class);
            Map<String, Object> pgf = resp == null ? null : (Map<String, Object>) resp.get("processGroupFlow");
            String id = pgf == null ? null : (String) pgf.get("id");
            return StringUtils.hasText(id) ? id : "root";
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 루트 그룹 조회 실패: " + ex.getMessage(), ex);
        }
    }

    /** NiFi 사용자(테넌트) 보장: 있으면 id, 없으면 생성 후 id. 관리 호출이라 서비스계정 Bearer 사용. */
    @SuppressWarnings("unchecked")
    public String ensureNifiUser(String identity) {
        String token = getToken();
        try {
            Map<String, Object> resp = restClient.get()
                    .uri("/nifi-api/tenants/users")
                    .header("Authorization", "Bearer " + token)
                    .retrieve().body(Map.class);
            java.util.List<Map<String, Object>> users = resp == null ? java.util.List.of()
                    : (java.util.List<Map<String, Object>>) resp.getOrDefault("users", java.util.List.of());
            for (Map<String, Object> u : users) {
                Map<String, Object> comp = (Map<String, Object>) u.get("component");
                if (comp != null && identity.equals(comp.get("identity"))) {
                    return (String) u.get("id");
                }
            }
            Map<String, Object> body = Map.of("revision", Map.of("version", 0),
                    "component", Map.of("identity", identity));
            Map<String, Object> created = restClient.post()
                    .uri("/nifi-api/tenants/users")
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body).retrieve().body(Map.class);
            return created == null ? null : (String) created.get("id");
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 사용자 동기화 실패(" + identity + "): " + ex.getMessage(), ex);
        }
    }

    /**
     * 사용자에게 리소스/액션 정책 부여(없으면 생성, 있으면 사용자 추가). 정책 조회는 %2F 인코딩
     * 슬래시를 Jetty 가 거부하므로 비인코딩 경로를 쓴다(P5a 스파이크에서 확인).
     */
    @SuppressWarnings("unchecked")
    public void ensureNifiUserPolicy(String resource, String action, String userId) {
        String token = getToken();
        Map<String, Object> body = Map.of("revision", Map.of("version", 0),
                "component", Map.of("resource", resource, "action", action,
                        "users", java.util.List.of(Map.of("id", userId))));
        try {
            restClient.post().uri("/nifi-api/policies")
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body).retrieve().toBodilessEntity();
            return;   // 새로 생성됨
        } catch (RestClientException ignore) {
            // 이미 존재 → 조회 후 PUT 로 사용자 추가
        }
        String resPath = resource.startsWith("/") ? resource.substring(1) : resource;
        try {
            Map<String, Object> existing = restClient.get()
                    .uri("/nifi-api/policies/" + action + "/" + resPath)
                    .header("Authorization", "Bearer " + token)
                    .retrieve().body(Map.class);
            if (existing == null) {
                return;
            }
            Map<String, Object> comp = (Map<String, Object>) existing.get("component");
            java.util.List<Map<String, Object>> users = comp.get("users") == null
                    ? new java.util.ArrayList<>()
                    : new java.util.ArrayList<>((java.util.List<Map<String, Object>>) comp.get("users"));
            if (users.stream().anyMatch(u -> userId.equals(u.get("id")))) {
                return;
            }
            users.add(Map.of("id", userId));
            Map<String, Object> putBody = Map.of("revision", existing.get("revision"),
                    "component", Map.of("id", comp.get("id"), "resource", resource, "action", action, "users", users));
            restClient.put().uri("/nifi-api/policies/" + existing.get("id"))
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(putBody).retrieve().toBodilessEntity();
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 정책 부여 실패(" + resource + " " + action + "): " + ex.getMessage(), ex);
        }
    }

    /** identity 로 NiFi 사용자 id 를 찾는다(없으면 null, 생성하지 않음). 회수 경로용. */
    @SuppressWarnings("unchecked")
    public String findNifiUserId(String identity) {
        String token = getToken();
        try {
            Map<String, Object> resp = restClient.get()
                    .uri("/nifi-api/tenants/users")
                    .header("Authorization", "Bearer " + token)
                    .retrieve().body(Map.class);
            java.util.List<Map<String, Object>> users = resp == null ? java.util.List.of()
                    : (java.util.List<Map<String, Object>>) resp.getOrDefault("users", java.util.List.of());
            for (Map<String, Object> u : users) {
                Map<String, Object> comp = (Map<String, Object>) u.get("component");
                if (comp != null && identity.equals(comp.get("identity"))) {
                    return (String) u.get("id");
                }
            }
        } catch (RestClientException ignore) {
            // 조회 실패 → null
        }
        return null;
    }

    /** 사용자를 정책에서 제거(정책이 없거나 사용자가 없으면 무동작). 강등/비활성 시 권한 회수용. */
    @SuppressWarnings("unchecked")
    public void removeNifiUserPolicy(String resource, String action, String userId) {
        String token = getToken();
        String resPath = resource.startsWith("/") ? resource.substring(1) : resource;
        try {
            Map<String, Object> existing = restClient.get()
                    .uri("/nifi-api/policies/" + action + "/" + resPath)
                    .header("Authorization", "Bearer " + token)
                    .retrieve().body(Map.class);
            if (existing == null) {
                return;
            }
            Map<String, Object> comp = (Map<String, Object>) existing.get("component");
            java.util.List<Map<String, Object>> users = comp.get("users") == null
                    ? new java.util.ArrayList<>()
                    : new java.util.ArrayList<>((java.util.List<Map<String, Object>>) comp.get("users"));
            java.util.List<Map<String, Object>> filtered = new java.util.ArrayList<>();
            for (Map<String, Object> u : users) {
                if (!userId.equals(u.get("id"))) {
                    filtered.add(Map.of("id", u.get("id")));
                }
            }
            if (filtered.size() == users.size()) {
                return;   // 사용자가 정책에 없었음
            }
            Map<String, Object> putBody = Map.of("revision", existing.get("revision"),
                    "component", Map.of("id", comp.get("id"), "resource", resource, "action", action, "users", filtered));
            restClient.put().uri("/nifi-api/policies/" + existing.get("id"))
                    .header("Authorization", "Bearer " + token)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(putBody).retrieve().toBodilessEntity();
        } catch (RestClientException ex) {
            // 정책 없음(404/400) 등 → 회수할 것 없음
        }
    }

    private String getToken() {
        if (!StringUtils.hasText(properties.username()) || !StringUtils.hasText(properties.password())) {
            throw new NifiClientException("NiFi 서비스 계정 정보가 설정되지 않았습니다.", null);
        }

        try {
            MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
            form.add("username", properties.username());
            form.add("password", properties.password());
            String token = restClient.post()
                    .uri("/nifi-api/access/token")
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .body(form)
                    .retrieve()
                    .body(String.class);
            if (!StringUtils.hasText(token)) {
                throw new NifiClientException("NiFi 토큰 응답이 비어 있습니다.", null);
            }
            return token;
        } catch (RestClientException ex) {
            throw new NifiClientException("NiFi 토큰 발급 실패: " + ex.getMessage(), ex);
        }
    }

    private NifiProcessGroupResponse toResponse(NifiProcessGroupEntity entity) {
        if (entity == null) {
            throw new NifiClientException("NiFi Processor Group 생성 응답이 비어 있습니다.", null);
        }
        NifiProcessGroupEntity.Component component = entity == null ? null : entity.component();
        String id = component == null || !StringUtils.hasText(component.id()) ? entity.id() : component.id();
        String name = component == null ? null : component.name();
        String parentGroupId = component == null ? null : component.parentGroupId();
        return new NifiProcessGroupResponse(id, name, parentGroupId, null);
    }

    private HttpClient insecureHttpClient() {
        try {
            SSLContext sslContext = trustAllSslContext();
            // new SSLParameters()로 빈 객체를 넘기면 protocols/cipher suite가 비어있어
            // JDK HttpClient가 이를 불완전하다고 보고 플랫폼 기본값(호스트명 검증 포함)으로
            // 되돌아간다. SSLContext의 기본 파라미터에서 시작해 필드 하나만 덮어써야 한다.
            SSLParameters sslParameters = sslContext.getDefaultSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("");
            return HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(2))
                    .sslContext(sslContext)
                    .sslParameters(sslParameters)
                    .build();
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            throw new NifiClientException("NiFi HTTPS 클라이언트 초기화 실패: " + ex.getMessage(), ex);
        }
    }

    private SSLContext trustAllSslContext() throws NoSuchAlgorithmException, KeyManagementException {
        TrustManager[] trustManagers = {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) {
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
        };
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagers, new SecureRandom());
        return sslContext;
    }
}
