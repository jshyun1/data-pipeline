package com.company.pipeline.nifi;

import com.company.pipeline.nifi.dto.NifiBulletinBoardResponse;
import com.company.pipeline.nifi.dto.NifiCountersResponse;
import com.company.pipeline.nifi.dto.NifiControllerServiceEntity;
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
    private static final String FORMAT_TEMPLATE_GROUP_NAME = "FORMAT";
    private static final String INITIAL_TEMPLATE_GROUP_NAME = "Initial";
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

    public NifiProcessGroupResponse createInitialDbToDbFlow(NifiInitialDbToDbCreateRequest request) {
        if (!"INSERT".equals(request.loadMode())) {
            throw new NifiClientException("Initial 템플릿은 현재 INSERT 적재 방식만 지원합니다.", null);
        }

        String token = getToken();
        NifiFlowResponse.ProcessGroupEntity formatGroup = findChildProcessGroup(ROOT_GROUP_ID, FORMAT_TEMPLATE_GROUP_NAME);
        NifiFlowResponse.ProcessGroupEntity initialGroup =
                findChildProcessGroup(componentId(formatGroup), INITIAL_TEMPLATE_GROUP_NAME);
        String snippetId = createProcessGroupSnippet(token, componentId(formatGroup), componentId(initialGroup),
                revisionVersion(initialGroup.revision()));

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
            updateInitialDbToDbProcessors(token, createdGroupId, request);
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
        NifiFlowResponse flow = getFlow(parentGroupId);
        List<NifiFlowResponse.ProcessGroupEntity> groups = flow == null
                || flow.processGroupFlow() == null
                || flow.processGroupFlow().flow() == null
                || flow.processGroupFlow().flow().processGroups() == null
                ? List.of()
                : flow.processGroupFlow().flow().processGroups();
        return groups.stream()
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
        NifiFlowResponse flow = getFlow(parentGroupId);
        List<NifiFlowResponse.ProcessGroupEntity> groups = flow == null
                || flow.processGroupFlow() == null
                || flow.processGroupFlow().flow() == null
                || flow.processGroupFlow().flow().processGroups() == null
                ? List.of()
                : flow.processGroupFlow().flow().processGroups();
        return groups.stream()
                .map(group -> componentId(group))
                .filter(StringUtils::hasText)
                .collect(Collectors.toSet());
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

    private void updateInitialDbToDbProcessors(String token, String groupId,
            NifiInitialDbToDbCreateRequest request) {
        NifiFlowResponse flow = getFlow(groupId);
        List<NifiFlowResponse.ProcessorEntity> processors = flow == null
                || flow.processGroupFlow() == null
                || flow.processGroupFlow().flow() == null
                || flow.processGroupFlow().flow().processors() == null
                ? List.of()
                : flow.processGroupFlow().flow().processors();
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

    // NiFi가 Keycloak/OIDC를 제거하고 Single User 인증(공유 서비스계정)으로 전환됨에 따라,
    // /nifi-api/access/token에 username/password를 보내 JWT를 직접 발급받는다.
    // 이 엔드포인트는 응답 본문이 JSON이 아니라 JWT 문자열 그대로임에 주의.
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
