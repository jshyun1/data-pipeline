package com.company.pipeline.alert;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.user.AppUser;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * 알림 규칙 관리 + 조치(확인/스누즈) API (U9/U18, 설계서 5-2 ⑤⑥).
 * 규칙 관리 화면(U21)·확인/스누즈 UI(U18)가 이 API 를 쓴다.
 */
@RestController
@com.company.pipeline.authz.RequirePermission(system = com.company.pipeline.authz.SystemCode.ADMIN)
public class AlertAdminController {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(AlertAdminController.class);

    private final JdbcTemplate jdbc;

    public AlertAdminController(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    // ---------------------------------------------------------------- 규칙 관리

    @GetMapping("/api/admin/alert-rules")
    public ApiResponse<List<Map<String, Object>>> listRules() {
        return ApiResponse.success(jdbc.queryForList("""
                SELECT r.id, r.rule_type_code, rt.label AS type_label, rt.category, r.builtin_key, r.name,
                       r.enabled, r.severity,
                       -- jsonb 를 그대로 내리면 드라이버가 PGobject({type,value,null})로 감싸서
                       -- 화면이 조건 값을 못 읽는다. text 로 캐스팅해 순수 JSON 문자열로 보낸다.
                       r.params_json::text AS params_json,
                       r.scope_json::text AS scope_json, r.renotify_seconds,
                       r.for_seconds, r.clear_seconds, r.mandatory,
                       r.schedule_enabled, r.schedule_time, r.schedule_last_fired_on,
                       r.schedule_run_count, r.schedule_last_run_at,
                       r.last_evaluated_at, r.last_eval_error,
                       -- 목록 화면의 «누가·언제» 컬럼. 컬럼은 처음부터 있었는데 내려주지 않아
                       -- 화면에서 볼 수 없었다.
                       r.created_by, r.updated_by, r.created_at, r.updated_at
                FROM alert_rule r JOIN alert_rule_type rt ON r.rule_type_code = rt.code
                WHERE r.deleted_at IS NULL ORDER BY rt.eval_priority, r.name"""));
    }

    /**
     * 이 대상(job/파이프라인)을 감시하고 있는 규칙들.
     *
     * <p>실행 현황 화면이 "이 작업에 어떤 알림이 걸려 있나"를 보여주는 데 쓴다. 예전에는
     * DAG마다 따로 «이상 감지 설정»을 두었는데, 알림 규칙과 판정 기준이 두 벌이 되어
     * 어느 쪽이 실제로 울리는지 알 수 없었다. 규칙 하나로 합치고 여기서 읽기만 한다.
     *
     * <p>«사용 중»(enabled)인 규칙만 돌려준다. 꺼둔 규칙은 울리지 않으므로 이 대상을 감시하는
     * 것이 아닌데도 목록에 섞여, 「미사용 · 심각도 CRITICAL」 같은 항목이 감시 중인 것처럼
     * 보였다. 규칙을 켜고 끄는 것은 설정 화면의 몫이고, 여기는 «지금 감시 중인 것»만 본다.
     *
     * @param target CDC(=pipeline_definition.id) 또는 ETL(=etl_job.id)
     * @param ids    쉼표로 구분한 대상 id. ETL이면 그 job의 스텝(적재 체인)까지 함께 본다.
     */
    @GetMapping("/api/admin/alert-rules/watching")
    public ApiResponse<List<Map<String, Object>>> rulesWatching(
            @RequestParam(defaultValue = "ETL") String target,
            @RequestParam(required = false) String ids) {
        Set<Long> targetIds = parseIds(ids);
        if (targetIds.isEmpty()) {
            return ApiResponse.success(List.of());
        }
        // 규칙 유형마다 감시 대상이 다르다. CDC 쪽 규칙에 ETL job id를 맞춰보면 엉뚱한 규칙이 걸린다.
        List<String> types = "CDC".equalsIgnoreCase(target)
                ? List.of("CDC_LAG", "DATA_FRESHNESS", "CONNECTOR_FAILED")
                : List.of("JOB_FAILURE", "JOB_NOT_RUN", "WORKFLOW_FAILURE", "WORKFLOW_NOT_COMPLETED");
        // idKind=CHAIN 규칙은 etl_job이 아니라 etl_job_step을 가리킨다.
        Set<Long> chainIds = new HashSet<>();
        if (!"CDC".equalsIgnoreCase(target)) {
            jdbc.queryForList("""
                    SELECT s.id FROM etl_job_step s
                    WHERE s.deleted_at IS NULL AND s.job_id IN (%s)"""
                            .formatted(placeholders(targetIds)), targetIds.toArray())
                    .forEach(row -> chainIds.add(((Number) row.get("id")).longValue()));
        }

        List<Map<String, Object>> rows = jdbc.queryForList("""
                SELECT r.id, r.rule_type_code, rt.label AS type_label, rt.category, r.name,
                       r.enabled, r.severity, r.scope_json::text AS scope_json,
                       r.schedule_enabled, r.schedule_time, r.last_evaluated_at, r.last_eval_error
                FROM alert_rule r JOIN alert_rule_type rt ON r.rule_type_code = rt.code
                WHERE r.deleted_at IS NULL AND r.enabled AND r.rule_type_code IN (%s)
                ORDER BY rt.eval_priority, r.name""".formatted(placeholders(types)), types.toArray());

        List<Map<String, Object>> matched = new java.util.ArrayList<>();
        for (Map<String, Object> row : rows) {
            if (scopeCovers(String.valueOf(row.get("scope_json")), targetIds, chainIds)) {
                matched.add(row);
            }
        }
        return ApiResponse.success(matched);
    }


    /**
     * scope_json 의 {@code groupPgIds}(NiFi 프로세스 그룹) → 그 아래 감시 단위 id 로 펼친다.
     *
     * <p>AlertEngine.expandGroupPgIds 와 같은 규칙이다 - 여기가 다르면 «실제로 울리는 범위»와
     * «화면이 감시 중이라고 보여주는 범위»가 어긋난다.
     */
    private Set<Long> expandGroupPgIds(com.fasterxml.jackson.databind.JsonNode scope, String idKind) {
        if (!scope.has("groupPgIds") || !scope.get("groupPgIds").isArray()
                || scope.get("groupPgIds").isEmpty()) {
            return Set.of();
        }
        List<String> pgIds = new java.util.ArrayList<>();
        scope.get("groupPgIds").forEach(x -> pgIds.add(x.asText()));
        String leafSql = "CHAIN".equals(idKind)
                ? "SELECT s.id FROM etl_job_step s JOIN etl_job j ON j.id = s.job_id"
                  + " WHERE s.deleted_at IS NULL AND j.deleted_at IS NULL AND s.target_table IS NOT NULL"
                  + " AND j.nifi_pg_id IN (SELECT process_group_id FROM d)"
                : "SELECT id FROM etl_job"
                  + " WHERE deleted_at IS NULL AND nifi_pg_id IN (SELECT process_group_id FROM d)";
        String sql = "WITH RECURSIVE d AS ("
                + " SELECT process_group_id FROM nifi_process_group_metadata"
                + " WHERE process_group_id IN (" + placeholders(pgIds) + ")"
                + " UNION ALL"
                + " SELECT m.process_group_id FROM nifi_process_group_metadata m"
                + " JOIN d ON m.parent_group_id = d.process_group_id) " + leafSql;
        try {
            Set<Long> out = new HashSet<>();
            jdbc.queryForList(sql, pgIds.toArray())
                    .forEach(row -> out.add(((Number) row.values().iterator().next()).longValue()));
            return out;
        } catch (Exception ex) {
            return Set.of();
        }
    }


    /**
     * scope_json 의 {@code groupConnIds}(소스 연결정보) → 그 원천의 파이프라인 id 로 펼친다.
     *
     * <p>CDC 규칙의 «그룹째 감시». ETL 의 groupPgIds 와 같은 이유로 저장은 연결정보 id 로 해두고
     * 펼치는 것은 평가 시점에 한다 - 그 원천에 파이프라인이 추가돼도 자동으로 감시된다.
     */
    private Set<Long> expandGroupConnIds(com.fasterxml.jackson.databind.JsonNode scope) {
        if (!scope.has("groupConnIds") || !scope.get("groupConnIds").isArray()
                || scope.get("groupConnIds").isEmpty()) {
            return Set.of();
        }
        List<Long> connIds = new java.util.ArrayList<>();
        scope.get("groupConnIds").forEach(x -> connIds.add(x.asLong()));
        String sql = "SELECT id FROM pipeline_definition WHERE source_connection_id IN ("
                + placeholders(connIds) + ")";
        try {
            Set<Long> out = new HashSet<>();
            jdbc.queryForList(sql, connIds.toArray())
                    .forEach(row -> out.add(((Number) row.values().iterator().next()).longValue()));
            return out;
        } catch (Exception ex) {
            return Set.of();
        }
    }

    private static String placeholders(java.util.Collection<?> values) {
        return values.stream().map(v -> "?").collect(java.util.stream.Collectors.joining(","));
    }

    private static Set<Long> parseIds(String raw) {
        Set<Long> out = new java.util.LinkedHashSet<>();
        if (raw == null || raw.isBlank()) {
            return out;
        }
        for (String part : raw.split(",")) {
            try {
                out.add(Long.parseLong(part.trim()));
            } catch (NumberFormatException ignored) {
                // 화면이 보낸 값이 깨져도 나머지 id는 살린다.
            }
        }
        return out;
    }

    /** scope_json이 이 대상들을 포함하는지. AlertEngine.ScopeFilter와 같은 규칙이다. */
    private boolean scopeCovers(String scopeJson, Set<Long> targetIds, Set<Long> chainIds) {
        if (scopeJson == null || scopeJson.isBlank() || "null".equals(scopeJson)) {
            return true;
        }
        try {
            com.fasterxml.jackson.databind.JsonNode node =
                    new com.fasterxml.jackson.databind.ObjectMapper().readTree(scopeJson);
            String kind = node.path("kind").asText("ALL");
            if ("ALL".equals(kind)) {
                return true;
            }
            Set<Long> scopeIds = new HashSet<>();
            if (node.has("ids") && node.get("ids").isArray()) {
                node.get("ids").forEach(x -> scopeIds.add(x.asLong()));
            }
            String idKind = node.path("idKind").asText("JOB");
            // 그룹째 고른 것은 AlertEngine 과 같은 규칙으로 «지금» 하위 대상까지 펼쳐서 본다.
            // 안 그러면 그룹으로 감시 중인데 속성창에는 «감시 중인 규칙 없음»으로 보인다.
            scopeIds.addAll(expandGroupPgIds(node, idKind));
            scopeIds.addAll(expandGroupConnIds(node));
            Set<Long> compare = "CHAIN".equals(idKind) ? chainIds : targetIds;
            if ("INCLUDE".equals(kind)) {
                // 지정이 비어 있으면 사실상 전체다(미설정 방어). AlertEngine과 같은 판정.
                return scopeIds.isEmpty() || scopeIds.stream().anyMatch(compare::contains);
            }
            if ("EXCLUDE".equals(kind)) {
                return compare.stream().anyMatch(id -> !scopeIds.contains(id));
            }
            return true;
        } catch (Exception ex) {
            return true;
        }
    }

    /**
     * 규칙 감시 범위 선택 후보. category=ETL → etl_job 목록(id,name), CDC → pipeline_definition 목록.
     * 규칙 추가/수정 폼의 «감시 대상 선택»이 이 목록에서 고른다.
     */
    @GetMapping("/api/admin/alert-scope-targets")
    public ApiResponse<List<Map<String, Object>>> scopeTargets(@RequestParam(defaultValue = "ETL") String category) {
        if ("CDC".equalsIgnoreCase(category)) {
            return ApiResponse.success(jdbc.queryForList(
                    "SELECT id, name FROM pipeline_definition ORDER BY name"));
        }
        // ETL_CHAIN: 프로세스 그룹이 아니라 «적재 테이블 하나»를 감시 단위로 고른다.
        // 그룹(DZ) 하나가 테이블 5개를 적재하므로 그룹 단위로는 "COM001M만 감시"가 불가능했다.
        // 체인의 대표는 최종 적재 스텝(target_table 보유)이고, 그 앞의 trigger/extract/truncate 는
        // etl_job_link 를 거슬러 올라가 같은 체인으로 묶인다(AlertEngine.chainOfProcessor).
        if ("ETL_CHAIN".equalsIgnoreCase(category)) {
            return ApiResponse.success(jdbc.queryForList("""
                    SELECT s.id,
                           j.job_name || ' / ' || s.target_table
                           -- 같은 그룹에 같은 테이블을 적재하는 스텝이 둘 이상이면(Template 등)
                           -- 이름만으로는 드롭다운에서 구분이 안 되니 스텝명을 덧붙인다.
                           || CASE WHEN count(*) OVER (PARTITION BY j.job_name, s.target_table) > 1
                                   THEN ' (' || s.step_name || ')' ELSE '' END AS name
                    FROM etl_job_step s JOIN etl_job j ON j.id = s.job_id
                    WHERE s.deleted_at IS NULL AND j.deleted_at IS NULL AND s.target_table IS NOT NULL
                    ORDER BY j.job_name, s.target_table"""));
        }
        return ApiResponse.success(jdbc.queryForList(
                "SELECT id, job_name AS name FROM etl_job WHERE deleted_at IS NULL ORDER BY job_name"));
    }

    /**
     * 감시 범위 선택용 «트리».
     *
     * <p>대상이 수백 개가 되면 평면 목록에서는 고르기 어렵다. ETL 관리 화면과 같은 계층으로
     * 보여주고 그 안에서 고르게 한다. 계층은 NiFi 프로세스 그룹(etl_job.parent_pg_id)에서 온다.
     *
     * <p>노드의 {@code id} 가 null 이면 «묶음»일 뿐 선택할 수 없다. 규칙이 감시하는 단위와
     * 화면에 보이는 계층이 다르기 때문이다 - 예컨대 ETL_CHAIN 은 그룹이 아니라 그 아래
     * 적재 스텝(테이블) 하나가 감시 단위다.
     */
    @GetMapping("/api/admin/alert-scope-tree")
    public ApiResponse<List<Map<String, Object>>> scopeTree(@RequestParam(defaultValue = "ETL") String category) {
        if ("CDC".equalsIgnoreCase(category)) {
            // CDC 는 NiFi 그룹 계층이 없다. 소스 연결정보로 묶어 준다(같은 원천끼리 모임).
            // 연결정보 노드도 «그룹째» 고를 수 있다 - 그 원천의 파이프라인 전부를 감시한다.
            List<Map<String, Object>> rows = jdbc.queryForList("""
                    SELECT p.id, p.name, p.source_connection_id AS conn_id,
                           COALESCE(c.name, '(연결정보 없음)') AS group_name
                    FROM pipeline_definition p
                    LEFT JOIN pipeline_connection c ON c.id = p.source_connection_id
                    ORDER BY group_name, p.name""");
            return ApiResponse.success(connGroupTree(rows));
        }

        if ("WORKFLOW".equalsIgnoreCase(category)) {
            // 워크플로우도 NiFi 그룹에 속한다(nifi_group_pg_id) - ETL 과 같은 계층에 얹어
            // 그룹째 감시가 그대로 된다. 게시 안 한 것은 Airflow 에 DAG 가 없어 제외한다.
            List<Map<String, Object>> groups = jdbc.queryForList("""
                    SELECT process_group_id AS pg, process_group_name AS name, parent_group_id AS parent_pg
                    FROM nifi_process_group_metadata ORDER BY process_group_name""");
            List<Map<String, Object>> leaves = jdbc.queryForList("""
                    SELECT id, nifi_group_pg_id AS pg, name
                    FROM etl_workflow
                    WHERE deleted_at IS NULL AND published_at IS NOT NULL
                    ORDER BY name""");
            return ApiResponse.success(pgTree(groups, leaves));
        }

        // 계층은 NiFi 프로세스 그룹 메타(nifi_process_group_metadata)에서 온다 - ETL>관리 화면의
        // 트리와 같은 출처다. 예전에는 etl_job.parent_pg_id 로 이었는데, 상위 그룹(DZ·DW·ETL Root)은
        // etl_job 에 없어서 부모를 못 찾고 전부 최상위로 흩어졌다(같은 이름 job 이 여러 번 보였다).
        List<Map<String, Object>> groups = jdbc.queryForList("""
                SELECT process_group_id AS pg, process_group_name AS name, parent_group_id AS parent_pg
                FROM nifi_process_group_metadata ORDER BY process_group_name""");

        boolean chain = "ETL_CHAIN".equalsIgnoreCase(category);
        List<Map<String, Object>> leaves = chain
                ? jdbc.queryForList("""
                        SELECT s.id, j.nifi_pg_id AS pg, s.target_table AS name
                        FROM etl_job_step s JOIN etl_job j ON j.id = s.job_id
                        WHERE s.deleted_at IS NULL AND j.deleted_at IS NULL AND s.target_table IS NOT NULL
                        ORDER BY s.target_table""")
                : jdbc.queryForList("""
                        SELECT id, nifi_pg_id AS pg, job_name AS name
                        FROM etl_job WHERE deleted_at IS NULL ORDER BY job_name""");
        return ApiResponse.success(pgTree(groups, leaves));
    }

    /**
     * NiFi 프로세스 그룹 계층으로 만든 감시 대상 트리.
     *
     * <p>그룹 노드는 {@code groupPgId} 로 «그룹째» 고를 수 있고, 잎(테이블/잡)은 {@code id} 로
     * 하나만 고른다. 그룹을 고르면 그 아래 전부가 감시 대상이 된다 - 나중에 테이블이 추가돼도
     * 규칙을 다시 저장할 필요가 없도록, 저장은 그룹 id 로 하고 펼치는 것은 평가 시점에 한다.
     *
     * <p>고를 것이 하나도 없는 가지는 빼서 목록을 짧게 유지한다.
     */
    private List<Map<String, Object>> pgTree(List<Map<String, Object>> groups,
                                             List<Map<String, Object>> leaves) {
        Map<String, List<Map<String, Object>>> leavesByPg = new java.util.LinkedHashMap<>();
        for (Map<String, Object> l : leaves) {
            leavesByPg.computeIfAbsent(String.valueOf(l.get("pg")), k -> new java.util.ArrayList<>())
                    .add(node(l.get("id"), String.valueOf(l.get("name")), List.of()));
        }

        Map<String, Map<String, Object>> nodeByPg = new java.util.LinkedHashMap<>();
        for (Map<String, Object> g : groups) {
            String pg = String.valueOf(g.get("pg"));
            Map<String, Object> n = node(null, String.valueOf(g.get("name")),
                    leavesByPg.getOrDefault(pg, List.of()));
            n.put("groupPgId", pg);   // 그룹째 고를 때 쓰는 키
            nodeByPg.put(pg, n);
        }

        List<Map<String, Object>> roots = new java.util.ArrayList<>();
        for (Map<String, Object> g : groups) {
            Map<String, Object> self = nodeByPg.get(String.valueOf(g.get("pg")));
            Object parentPg = g.get("parent_pg");
            Map<String, Object> parent = parentPg == null ? null : nodeByPg.get(String.valueOf(parentPg));
            if (parent != null && parent != self) {
                childrenOf(parent).add(self);
            } else {
                roots.add(self);
            }
        }
        // 그룹 밖(메타에 없는 pg)에 달린 잎은 최상위로 올린다 - 목록에서 사라지지 않게.
        leavesByPg.forEach((pg, ls) -> {
            if (!nodeByPg.containsKey(pg)) {
                roots.addAll(ls);
            }
        });
        roots.removeIf(n -> !hasSelectable(n));
        return roots;
    }

    /** 이 가지에 고를 것(잎 또는 하위 잎)이 하나라도 있나. 없으면 트리에서 뺀다. */
    private boolean hasSelectable(Map<String, Object> n) {
        if (n.get("id") != null) {
            return true;
        }
        List<Map<String, Object>> kids = childrenOf(n);
        kids.removeIf(k -> !hasSelectable(k));
        return !kids.isEmpty();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> childrenOf(Map<String, Object> n) {
        return (List<Map<String, Object>>) n.get("children");
    }


    /**
     * CDC 감시 대상 트리. 소스 연결정보로 묶고, 그 묶음도 «그룹째» 고를 수 있게 한다.
     *
     * <p>연결정보가 없는 파이프라인은 고를 수 있는 묶음이 아니다(무엇을 감시할지 특정 못 함) -
     * 잎만 고르게 둔다.
     */
    private List<Map<String, Object>> connGroupTree(List<Map<String, Object>> rows) {
        java.util.LinkedHashMap<String, Map<String, Object>> byGroup = new java.util.LinkedHashMap<>();
        for (Map<String, Object> r : rows) {
            String gname = String.valueOf(r.get("group_name"));
            Object connId = r.get("conn_id");
            Map<String, Object> g = byGroup.computeIfAbsent(gname, k -> {
                Map<String, Object> n = node(null, gname, List.of());
                if (connId != null) {
                    n.put("groupConnId", connId);   // 연결정보째 고를 때 쓰는 키
                }
                return n;
            });
            childrenOf(g).add(node(r.get("id"), String.valueOf(r.get("name")), List.of()));
        }
        return new java.util.ArrayList<>(byGroup.values());
    }

    /** 한 컬럼 값으로 묶은 2단 트리. 묶음 노드는 선택 불가(id=null). */
    private List<Map<String, Object>> groupRows(List<Map<String, Object>> rows, String groupKey) {
        java.util.LinkedHashMap<String, List<Map<String, Object>>> byGroup = new java.util.LinkedHashMap<>();
        for (Map<String, Object> r : rows) {
            byGroup.computeIfAbsent(String.valueOf(r.get(groupKey)), k -> new java.util.ArrayList<>())
                    .add(node(r.get("id"), String.valueOf(r.get("name")), List.of()));
        }
        List<Map<String, Object>> out = new java.util.ArrayList<>();
        byGroup.forEach((name, children) -> out.add(node(null, name, children)));
        return out;
    }

    /**
     * etl_job 을 parent_pg_id 로 이어 그룹 계층을 만들고, 스텝이 있으면 잎으로 붙인다.
     *
     * <p>스텝을 붙이는 경우(ETL_CHAIN) 그룹 자체는 선택 대상이 아니다. 부모를 못 찾은 그룹은
     * 최상위로 올린다 - 미러가 아직 상위 그룹을 못 읽었어도 목록에서 사라지지 않게.
     */
    private List<Map<String, Object>> jobTree(List<Map<String, Object>> jobs, List<Map<String, Object>> steps) {
        boolean withSteps = !steps.isEmpty();
        Map<Object, List<Map<String, Object>>> stepsByJob = new java.util.LinkedHashMap<>();
        for (Map<String, Object> st : steps) {
            String label = String.valueOf(st.get("target_table"));
            stepsByJob.computeIfAbsent(st.get("job_id"), k -> new java.util.ArrayList<>())
                    .add(node(st.get("id"), label, List.of()));
        }

        Map<String, Map<String, Object>> nodeByPg = new java.util.LinkedHashMap<>();
        for (Map<String, Object> j : jobs) {
            List<Map<String, Object>> children = new java.util.ArrayList<>(
                    stepsByJob.getOrDefault(j.get("id"), List.of()));
            // 스텝을 붙이는 화면에서는 그룹을 고를 수 없다(감시 단위가 스텝이므로).
            Object selfId = withSteps ? null : j.get("id");
            nodeByPg.put(String.valueOf(j.get("nifi_pg_id")),
                    node(selfId, String.valueOf(j.get("job_name")), children));
        }

        List<Map<String, Object>> roots = new java.util.ArrayList<>();
        for (Map<String, Object> j : jobs) {
            Map<String, Object> self = nodeByPg.get(String.valueOf(j.get("nifi_pg_id")));
            Map<String, Object> parent = j.get("parent_pg_id") == null
                    ? null : nodeByPg.get(String.valueOf(j.get("parent_pg_id")));
            if (parent != null && parent != self) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> siblings = (List<Map<String, Object>>) parent.get("children");
                siblings.add(self);
            } else {
                roots.add(self);
            }
        }
        return roots;
    }

    private Map<String, Object> node(Object id, String name, List<Map<String, Object>> children) {
        Map<String, Object> n = new java.util.LinkedHashMap<>();
        n.put("id", id);
        n.put("name", name);
        n.put("children", new java.util.ArrayList<>(children));
        return n;
    }

    /**
     * 규칙 하나의 «평가 이력». 화면의 [로그] 버튼이 이걸 띄운다.
     *
     * <p>규칙이 항상 성공하는 것은 아니다 - 신호를 못 읽거나 평가가 예외로 끝날 수 있는데,
     * 예전에는 그런 일이 로그 한 줄로만 남아 화면에서는 아무 일도 없었던 것처럼 보였다.
     */
    @GetMapping("/api/admin/alert-rules/{id}/eval-logs")
    public ApiResponse<List<Map<String, Object>>> ruleEvalLogs(
            @PathVariable long id,
            @RequestParam(defaultValue = "100") int limit) {
        int capped = Math.max(1, Math.min(limit, 500));
        return ApiResponse.success(jdbc.queryForList("""
                SELECT id, occurred_at, result, matched_count, duration_ms, message
                FROM alert_rule_eval_log
                WHERE rule_id = ?
                ORDER BY occurred_at DESC, id DESC
                LIMIT ?""", id, capped));
    }

    /**
     * 규칙 유형 카탈로그 + 유형별 조건 파라미터 스키마.
     *
     * <p>화면이 "이 유형은 어떤 조건을 입력받아야 하는가"를 알아야 조건 입력 폼을 그릴 수 있다.
     * 스키마를 프론트에 하드코딩하면 유형이 하나 늘 때마다 두 곳을 고쳐야 하고, 곧 서로 어긋난다.
     */
    @GetMapping("/api/admin/alert-rule-types")
    public ApiResponse<List<Map<String, Object>>> listRuleTypes() {
        List<Map<String, Object>> types = jdbc.queryForList("""
                SELECT code, label, category, kpi_axis, mandatory, default_severity, min_severity,
                       description, eval_priority
                FROM alert_rule_type ORDER BY eval_priority, label""");
        for (Map<String, Object> t : types) {
            t.put("paramSpec", PARAM_SPEC.getOrDefault((String) t.get("code"), List.of()));
        }
        return ApiResponse.success(types);
    }

    /**
     * 유형별 조건 파라미터 정의. {key, label, unit, type, min, max, defaultValue}.
     *
     * <p>params_json 의 키와 1:1로 맞춘다 — 평가 엔진이 읽는 키와 화면이 쓰는 키가 다르면
     * 화면에서 저장한 값이 조용히 무시된다.
     */
    private static final Map<String, List<Map<String, Object>>> PARAM_SPEC = Map.of(
            "SERVER_MEMORY", List.of(
                    param("threshold", "발화 임계", "%", 1, 99, 80),
                    param("clear", "해제 임계", "%", 1, 99, 72)),
            "SERVER_DISK", List.of(
                    param("threshold", "발화 임계", "%", 1, 99, 80),
                    param("clear", "해제 임계", "%", 1, 99, 72)),
            "DATA_FRESHNESS", List.of(
                    param("staleness_minutes", "적재 정체 시간", "분", 1, 1440, 30)),
            "CDC_LAG", List.of(
                    param("threshold", "미처리 임계", "건", 1, 100000000, 50000),
                    param("clear", "해제 임계", "건", 0, 100000000, 10000)),
            "WORKFLOW_NOT_COMPLETED", List.of(
                    param("grace_minutes", "성공 없이 지날 수 있는 시간", "분", 10, 20160, 1440)),
            "JOB_NOT_RUN", List.of(
                    param("days", "미실행 허용 기간", "일", 1, 365, 7)),
            "CONNECTOR_FAILED", List.of(),
            "SERVICE_UNREACHABLE", List.of(),
            "JOB_FAILURE", List.of(),
            "COLLECTOR_DOWN", List.of());

    private static Map<String, Object> param(String key, String label, String unit,
                                             long min, long max, long defaultValue) {
        return Map.of("key", key, "label", label, "unit", unit, "type", "INT",
                "min", min, "max", max, "defaultValue", defaultValue);
    }

    // scheduleEnabled/scheduleTime("HH:mm") = 일별 점검. null 이면 종전 값 유지(부분 수정 허용).
    public record UpdateRuleRequest(Boolean enabled, String severity, String paramsJson,
                                    Integer forSeconds, Integer clearSeconds, String name,
                                    String scopeJson, Integer renotifySeconds,
                                    Boolean scheduleEnabled, String scheduleTime) {}

    public record CreateRuleRequest(String ruleTypeCode, String name, String severity, String paramsJson,
                                    Integer forSeconds, Integer clearSeconds,
                                    String scopeJson, Integer renotifySeconds,
                                    Boolean scheduleEnabled, String scheduleTime) {}

    /**
     * 규칙 추가 (PDF 9쪽 "[+ 규칙 추가]"). 같은 유형으로 임계값만 다른 규칙을 여러 개 두는 것을 허용한다 —
     * "메모리 80% 경고 / 90% 위험"처럼 한 지표에 두 단계를 거는 게 실제 운영 방식이다.
     */
    @PostMapping("/api/admin/alert-rules")
    public ApiResponse<Map<String, Object>> createRule(@RequestBody CreateRuleRequest req,
                                                       @AuthenticationPrincipal AppUser user) {
        if (req.ruleTypeCode() == null || req.ruleTypeCode().isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "규칙 유형은 필수입니다.");
        }
        if (req.name() == null || req.name().isBlank()) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "규칙명은 필수입니다.");
        }
        Integer known = jdbc.queryForObject(
                "SELECT count(*) FROM alert_rule_type WHERE code = ?", Integer.class, req.ruleTypeCode());
        if (known == null || known == 0) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "알 수 없는 규칙 유형입니다: " + req.ruleTypeCode());
        }
        String by = user != null ? user.getUserId() : "admin";
        try {
            Long id = jdbc.queryForObject("""
                    INSERT INTO alert_rule
                        (rule_type_code, name, severity, params_json, scope_json, for_seconds, clear_seconds,
                         renotify_seconds, enabled, mandatory, created_by, updated_by,
                         schedule_enabled, schedule_time, schedule_last_fired_on,
                         schedule_run_count, schedule_last_run_at)
                    VALUES (?, ?,
                            COALESCE(?, (SELECT default_severity FROM alert_rule_type WHERE code = ?)),
                            COALESCE(?::jsonb, '{}'::jsonb),
                            COALESCE(?::jsonb, '{"kind": "ALL"}'::jsonb),
                            COALESCE(?, 120), COALESCE(?, 300), COALESCE(?, 1800), TRUE, FALSE, ?, ?,
                            COALESCE(?, FALSE), ?::time,
                            -- 이미 지난 시각으로 스케줄을 만들면 저장하자마자 발화한다.
                            -- 오늘 몫은 끝난 것으로 찍어 두고 내일부터 돈다. schedule_last_run_at
                            -- 이 NULL 이라 2회차 조건(간격 경과)도 오늘은 성립하지 않는다.
                            CASE WHEN ?::time IS NOT NULL AND ?::time <= localtime
                                 THEN current_date END,
                            0, NULL)
                    RETURNING id
                    """, Long.class,
                    req.ruleTypeCode(), req.name(), req.severity(), req.ruleTypeCode(),
                    req.paramsJson(), req.scopeJson(), req.forSeconds(), req.clearSeconds(),
                    req.renotifySeconds(), by, by,
                    req.scheduleEnabled(), req.scheduleTime(), req.scheduleTime(), req.scheduleTime());
            return ApiResponse.success(Map.of("id", id == null ? 0L : id));
        } catch (DataAccessException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "규칙 값이 올바르지 않습니다(조건·심각도 확인).");
        }
    }

    /**
     * 규칙 삭제. 운영자가 자기 환경에 맞게 규칙 구성을 정할 수 있어야 해서 필수 규칙도 삭제를 허용한다
     * (mandatory 는 기본값 표시용으로만 남는다).
     */
    // ------------------------------------------------------------ 규칙별 수신자

    /**
     * 이 규칙의 알림을 «누가» 받는지. 수신자 전원을 내려주고 각자 켬/끔 상태를 함께 준다.
     *
     * <p>행이 하나도 없는 규칙은 종전대로 «전원» 받는다(=응답의 enabled 가 모두 true).
     * 여기서는 누가 받는지만 정한다 — 어떤 Job 을 감시할지는 규칙의 «감시 범위»가 정한다.
     */
    @GetMapping("/api/admin/alert-rules/{id}/recipients")
    public ApiResponse<List<Map<String, Object>>> ruleRecipients(@PathVariable long id) {
        return ApiResponse.success(jdbc.queryForList("""
                SELECT r.id AS recipient_id, r.display_name, r.email, r.phone, r.enabled AS recipient_enabled,
                       -- 규칙에 명시 목록이 없으면 전원 수신이 기본이라 true 로 채워 내린다.
                       COALESCE(rr.enabled, NOT EXISTS (
                           SELECT 1 FROM notification_recipient_rule x WHERE x.rule_id = ?
                       )) AS enabled
                FROM notification_recipient r
                LEFT JOIN notification_recipient_rule rr ON rr.recipient_id = r.id AND rr.rule_id = ?
                WHERE r.deleted_at IS NULL
                ORDER BY r.display_name
                """, id, id));
    }

    public record RuleRecipientRequest(Long recipientId, Boolean enabled) {}

    /**
     * 규칙별 수신자 «전체 교체». 화면이 켬/끔 목록을 통째로 저장하므로 교체가 맞다.
     *
     * <p>끈 수신자도 enabled=false 로 «남긴다». 행을 지워 버리면 "명시 목록이 없다 = 전원 수신"과
     * 구분되지 않아, 전원을 끄면 오히려 전원에게 가는 뒤집힌 결과가 된다.
     */
    @PutMapping("/api/admin/alert-rules/{id}/recipients")
    public ApiResponse<Void> replaceRuleRecipients(@PathVariable long id,
                                                   @RequestBody List<RuleRecipientRequest> recipients) {
        jdbc.update("DELETE FROM notification_recipient_rule WHERE rule_id = ?", id);
        if (recipients == null) {
            return ApiResponse.success(null);
        }
        for (RuleRecipientRequest r : recipients) {
            if (r == null || r.recipientId() == null) {
                continue;
            }
            jdbc.update("""
                    INSERT INTO notification_recipient_rule (recipient_id, rule_id, enabled)
                    VALUES (?, ?, COALESCE(?, TRUE))
                    ON CONFLICT (recipient_id, rule_id) DO UPDATE SET
                        enabled = EXCLUDED.enabled, updated_at = now()
                    """, r.recipientId(), id, r.enabled());
        }
        return ApiResponse.success(null);
    }

    @DeleteMapping("/api/admin/alert-rules/{id}")
    public ApiResponse<Void> deleteRule(@PathVariable long id) {
        int n = jdbc.update("UPDATE alert_rule SET deleted_at=now() WHERE id=? AND deleted_at IS NULL", id);
        if (n == 0) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "규칙을 찾을 수 없습니다: " + id);
        }
        return ApiResponse.success(null);
    }

    @PutMapping("/api/admin/alert-rules/{id}")
    public ApiResponse<Void> updateRule(@PathVariable long id, @RequestBody UpdateRuleRequest req,
                                        @AuthenticationPrincipal AppUser user) {
        String by = user != null ? user.getUserId() : "admin";
        try {
            int n = jdbc.update("""
                    UPDATE alert_rule SET
                        name             = COALESCE(?, name),
                        enabled          = COALESCE(?, enabled),
                        severity         = COALESCE(?, severity),
                        params_json      = COALESCE(?::jsonb, params_json),
                        scope_json       = COALESCE(?::jsonb, scope_json),
                        for_seconds      = COALESCE(?, for_seconds),
                        clear_seconds    = COALESCE(?, clear_seconds),
                        renotify_seconds = COALESCE(?, renotify_seconds),
                        schedule_enabled = COALESCE(?, schedule_enabled),
                        -- 맨 물음표에 캐스팅이 없으면, 값을 안 보낸 부분 수정(예: 사용여부 토글만)에서
                        -- NULL의 타입을 정하지 못해 «could not determine data type» 으로 통째로 실패한다.
                        schedule_time    = CASE WHEN ?::time IS NULL THEN schedule_time ELSE ?::time END,
                        -- 시각을 바꿨는데 그게 오늘 이미 지났으면 오늘 몫은 끝난 것으로 본다
                        -- (저장하자마자 발화하는 것을 막는다). 시각이 아직 안 지났으면 빗장을 푼다.
                        schedule_last_fired_on = CASE
                            WHEN ?::time IS NULL THEN schedule_last_fired_on
                            WHEN ?::time <= localtime THEN current_date
                            ELSE NULL END,
                        -- 시각을 바꾸면 오늘 돈 회차는 무효다. 되돌려 두지 않으면
                        -- "오늘 이미 3회 다 돌았다"로 남아 새 시각이 오늘 안 돈다.
                        schedule_run_count  = CASE WHEN ?::time IS NULL THEN schedule_run_count ELSE 0 END,
                        schedule_last_run_at = CASE WHEN ?::time IS NULL THEN schedule_last_run_at ELSE NULL END,
                        updated_by = ?, updated_at = now()
                    WHERE id = ? AND deleted_at IS NULL
                    """, req.name(), req.enabled(), req.severity(), req.paramsJson(),
                    req.scopeJson(), req.forSeconds(), req.clearSeconds(), req.renotifySeconds(),
                    req.scheduleEnabled(), req.scheduleTime(), req.scheduleTime(),
                    req.scheduleTime(), req.scheduleTime(),
                    req.scheduleTime(), req.scheduleTime(), by, id);
            if (n == 0) {
                throw new BusinessException(ErrorCode.VALIDATION_ERROR, "규칙을 찾을 수 없습니다: " + id);
            }
        } catch (DataAccessException ex) {
            // 원인을 삼키면 화면에는 «값이 올바르지 않습니다»만 남아 무엇이 문제인지 알 수 없다.
            log.warn("알림 규칙 수정 실패 - id={} : {}", id, ex.getMostSpecificCause().getMessage());
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "규칙 값이 올바르지 않습니다(params_json 등 확인).");
        }
        return ApiResponse.success(null);
    }

    // ------------------------------------------------------------- 확인/스누즈

    public record AckRequest(String comment) {}

    @PostMapping("/api/alerts/{id}/ack")
    public ApiResponse<Void> ack(@PathVariable long id, @RequestBody(required = false) AckRequest req,
                                 @AuthenticationPrincipal AppUser user) {
        String actor = user != null ? user.getUserId() : "admin";
        String comment = req != null ? req.comment() : null;
        int n = jdbc.update("UPDATE alert_instance SET ack_by=?, ack_at=now(), ack_comment=?, updated_at=now(), "
                + "version=version+1 WHERE id=? AND closed_at IS NULL", actor, comment, id);
        requireFound(n, id);
        event(id, "ACKED", actor, comment);
        return ApiResponse.success(null);
    }

    @PostMapping("/api/alerts/{id}/unack")
    public ApiResponse<Void> unack(@PathVariable long id, @AuthenticationPrincipal AppUser user) {
        String actor = user != null ? user.getUserId() : "admin";
        int n = jdbc.update("UPDATE alert_instance SET ack_by=NULL, ack_at=NULL, ack_comment=NULL, "
                + "updated_at=now(), version=version+1 WHERE id=? AND closed_at IS NULL", id);
        requireFound(n, id);
        event(id, "UNACKED", actor, null);
        return ApiResponse.success(null);
    }

    private void requireFound(int n, long id) {
        if (n == 0) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "열린 알림을 찾을 수 없습니다: " + id);
        }
    }

    private void event(long instanceId, String type, String actor, String comment) {
        jdbc.update("INSERT INTO alert_instance_event (instance_id, event_type, actor, comment, occurred_at) "
                + "VALUES (?, ?, ?, ?, now())", instanceId, type, actor, comment);
    }
}
