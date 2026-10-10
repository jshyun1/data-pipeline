package com.company.pipeline.llm.context;

import com.company.pipeline.jobcatalog.EtlJobLink;
import com.company.pipeline.jobcatalog.EtlJobRun;
import com.company.pipeline.jobcatalog.EtlJobStep;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * ETL 관리 속성창 AI 어시스턴트가 «정보 요약»에 쓰는 job 사실(2026-10-10).
 *
 * <p>예전에는 화면이 이 값을 직접 계산해 고정 보고서(한 줄 요약 · 구성 · 지금 상태 · 등록 및 변경)로 보여 줬다. 이제는 워크플로우 속성창처럼
 * 탭을 열면 GPT 가 요약을 쓰므로, 같은 정보가 답에 그대로 나오도록 서버가 같은 규칙으로 계산해 대상 정보에 붙인다
 * (화면 ConsoleFramePage 의 sourceTargetText · firstStartProcessor · lastTerminalProcessor · processorFlowDescription ·
 * aiRunSummaryText · aiDurationSummaryText 와 같은 규칙).
 *
 * <p>프로세서 속성(propsJson)에서는 정해진 키(DB 종류 · 조회 SQL · 스키마 · 테이블 · 입력 폴더)만 읽는다 - 비밀번호 같은 값이 섞이지 않게.
 */
final class EtlJobFacts {

    static final int RECENT_DAYS = 5;
    private static final int MAX_FLOW_STEPS = 6;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern SQL_TABLE_CLAUSE = Pattern.compile(
            "\\b(?:from|join)\\s+(.+?)(?=\\s+\\b(?:inner|left|right|full|cross|join|where|on|group|order|having|union|limit|offset|fetch)\\b|$)",
            Pattern.CASE_INSENSITIVE);

    private EtlJobFacts() {
    }

    /**
     * 소스: 캔버스 위치 순으로 처음 «읽는» 단계 - SELECT 문의 FROM 테이블, 테이블 조회 프로세서의 테이블, 파일 목록의 입력 폴더.
     * 예전 화면 규칙(연결이 안 들어오는 첫 단계)은 시작 단계가 GenerateFlowFile 같은 방아쇠면 소스를 못 찾았다(DZ_COM001M 의
     * 01_START_INITIAL_LOAD, 2026-10-10 확인) - 그래서 읽는 단계를 먼저 찾고, 없을 때만 그 규칙으로 돌아간다.
     * «DB 종류 / 테이블», 파일 적재면 «파일 / 입력 폴더».
     */
    static String source(List<EtlJobStep> steps, List<EtlJobLink> links) {
        for (EtlJobStep step : byPosition(steps)) {
            Optional<String> read = readSource(step);
            if (read.isPresent()) {
                return read.get();
            }
        }
        EtlJobStep step = firstStart(steps, links).orElse(null);
        if (step == null) {
            return "-";
        }
        Map<String, String> props = props(step);
        return join(pick(props, "Database Type", "db-type"), pick(props, "Table Name", "table-name"));
    }

    private static Optional<String> readSource(EtlJobStep step) {
        if (typeIncludes(step, "Put") || typeIncludes(step, "QueryRecord")) {
            return Optional.empty();   // 쓰는 단계 · 흐름 안의 레코드를 다시 고르는 단계(FROM FLOWFILE)
        }
        Map<String, String> props = props(step);
        if (typeIncludes(step, "ListFile") || typeIncludes(step, "GetFile")) {
            return Optional.of("파일 / " + Optional.ofNullable(pick(props, "Input Directory", "input-directory")).orElse("-"));
        }
        String databaseType = pick(props, "Database Type", "db-type");
        String sql = Optional.ofNullable(pick(props, "SQL select query")).orElse(step.getSqlText());
        List<String> tables = sqlSourceTables(sql);
        if (!tables.isEmpty()) {
            return Optional.of(join(databaseType, String.join(", ", tables)));
        }
        if (typeIncludes(step, "QueryDatabaseTable") || typeIncludes(step, "GenerateTableFetch")) {
            String table = pick(props, "Table Name", "table-name");
            return table == null ? Optional.empty() : Optional.of(join(databaseType, table));
        }
        return Optional.empty();
    }

    /** 타깃: 연결이 나가지 않는 마지막 프로세서(위치 순) 기준. «DB 종류 / 스키마 / 테이블». */
    static String target(List<EtlJobStep> steps, List<EtlJobLink> links) {
        EtlJobStep step = lastTerminal(steps, links).orElse(null);
        if (step == null) {
            return "-";
        }
        Map<String, String> props = props(step);
        return join(pick(props, "Database Type", "db-type"),
                pick(props, "SCHEMA NAME", "Schema Name", "put-db-record-schema-name"),
                Optional.ofNullable(pick(props, "TABLE NAME", "Table Name", "put-db-record-table-name"))
                        .orElse(step.getTargetTable()));
    }

    /** 처리 흐름: 프로세서 이름을 캔버스 위치 순(왼쪽 → 오른쪽, 위 → 아래)으로. */
    static String flow(List<EtlJobStep> steps) {
        List<String> names = byPosition(steps).stream()
                .map(step -> step.getStepName() != null && !step.getStepName().isBlank()
                        ? step.getStepName() : shortType(step.getStepType()))
                .filter(name -> name != null && !name.isBlank() && !"-".equals(name))
                .toList();
        if (names.isEmpty()) {
            return "(프로세서 구성이 아직 동기화되지 않았다)";
        }
        String shown = String.join(" → ", names.subList(0, Math.min(MAX_FLOW_STEPS, names.size())));
        return names.size() > MAX_FLOW_STEPS ? shown + " → … (전체 " + names.size() + "개)" : shown;
    }

    /**
     * 최근 {@value #RECENT_DAYS}일 실행 요약(오늘 포함). 화면과 같은 기준: 성공 = SUCCESS · COMPLETED · DONE 이고 실패 단계 0,
     * 실패 = FAIL 이 들어가거나 실패 단계가 있음. 연속 성공은 가장 최근부터 센다.
     *
     * @param runs 최근 실행(새것부터, 화면 실행 이력과 같은 최근 20건)
     */
    static String recentRuns(List<EtlJobRun> runs, LocalDate today) {
        LocalDateTime from = today.minusDays(RECENT_DAYS - 1L).atStartOfDay();
        List<EtlJobRun> recent = runs.stream()
                .filter(run -> run.getStartedAt() != null && !run.getStartedAt().isBefore(from))
                .toList();
        if (recent.isEmpty()) {
            return "최근 " + RECENT_DAYS + "일 실행 기록 없음";
        }
        long success = recent.stream().filter(run -> succeeded(run) && !failed(run)).count();
        long failure = recent.stream().filter(EtlJobFacts::failed).count();
        int streak = 0;
        for (EtlJobRun run : runs) {
            if (!succeeded(run) || failed(run)) {
                break;
            }
            streak++;
        }
        return "최근 " + RECENT_DAYS + "일 " + recent.size() + "회 실행(성공 " + success + " · 실패 " + failure + ")"
                + (streak > 1 ? ", 최근 " + streak + "회 연속 성공" : "")
                + (runs.size() >= 20 ? " - 최근 기록 20건 기준" : "");
    }

    /** 가장 최근에 끝난 실행의 소요 시간. */
    static String latestDuration(List<EtlJobRun> runs) {
        return runs.stream()
                .filter(run -> run.getStartedAt() != null && run.getEndedAt() != null)
                .findFirst()
                .map(run -> durationText(Duration.between(run.getStartedAt(), run.getEndedAt())) + " ("
                        + ContextText.time(run.getStartedAt()) + " 시작)")
                .orElse("확인되지 않음(끝난 실행 기록 없음)");
    }

    static String durationText(Duration duration) {
        long seconds = Math.max(0, duration.getSeconds());
        if (seconds < 60) {
            return seconds + "초";
        }
        long minutes = seconds / 60;
        if (minutes < 60) {
            return minutes + "분" + (seconds % 60 > 0 ? " " + seconds % 60 + "초" : "");
        }
        return minutes / 60 + "시간" + (minutes % 60 > 0 ? " " + minutes % 60 + "분" : "");
    }

    private static boolean succeeded(EtlJobRun run) {
        String status = Objects.toString(run.getStatus(), "").toUpperCase(Locale.ROOT);
        return status.contains("SUCCESS") || status.contains("COMPLETED") || status.equals("DONE");
    }

    private static boolean failed(EtlJobRun run) {
        return Objects.toString(run.getStatus(), "").toUpperCase(Locale.ROOT).contains("FAIL") || run.getFailedStepCount() > 0;
    }

    private static Optional<EtlJobStep> firstStart(List<EtlJobStep> steps, List<EtlJobLink> links) {
        Set<String> incoming = endpoints(steps, links, EtlJobLink::getToComponentId);
        return byPosition(steps.stream().filter(step -> !incoming.contains(step.getNifiProcessorId())).toList()).stream()
                .findFirst()
                .or(() -> byPosition(steps).stream().findFirst());
    }

    private static Optional<EtlJobStep> lastTerminal(List<EtlJobStep> steps, List<EtlJobLink> links) {
        Set<String> outgoing = endpoints(steps, links, EtlJobLink::getFromComponentId);
        List<EtlJobStep> terminals = byPosition(steps.stream().filter(step -> !outgoing.contains(step.getNifiProcessorId())).toList());
        if (!terminals.isEmpty()) {
            return Optional.of(terminals.get(terminals.size() - 1));
        }
        List<EtlJobStep> all = byPosition(steps);
        return all.isEmpty() ? Optional.empty() : Optional.of(all.get(all.size() - 1));
    }

    private static Set<String> endpoints(List<EtlJobStep> steps, List<EtlJobLink> links,
                                         java.util.function.Function<EtlJobLink, String> side) {
        Set<String> processorIds = steps.stream().map(EtlJobStep::getNifiProcessorId).collect(Collectors.toSet());
        return links.stream().map(side).filter(Objects::nonNull).filter(processorIds::contains).collect(Collectors.toSet());
    }

    private static List<EtlJobStep> byPosition(List<EtlJobStep> steps) {
        return steps.stream()
                .sorted(Comparator.comparing((EtlJobStep step) -> step.getXPos() == null ? Double.MAX_VALUE : step.getXPos())
                        .thenComparing(step -> step.getYPos() == null ? Double.MAX_VALUE : step.getYPos()))
                .toList();
    }

    private static Map<String, String> props(EtlJobStep step) {
        if (step.getPropsJson() == null || step.getPropsJson().isBlank()) {
            return Map.of();
        }
        try {
            JsonNode node = JSON.readTree(step.getPropsJson());
            if (node == null || !node.isObject()) {
                return Map.of();
            }
            Map<String, String> out = new java.util.LinkedHashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                out.put(field.getKey(), field.getValue().isNull() ? "" : field.getValue().asText());
            }
            return out;
        } catch (Exception ex) {
            return Map.of();
        }
    }

    /** 정확한 키 → 대소문자만 다른 키 순으로 처음 값이 있는 것. */
    private static String pick(Map<String, String> props, String... keys) {
        for (String key : keys) {
            String exact = props.get(key);
            if (exact != null && !exact.isBlank()) {
                return exact.trim();
            }
            for (Map.Entry<String, String> entry : props.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(key) && entry.getValue() != null && !entry.getValue().isBlank()) {
                    return entry.getValue().trim();
                }
            }
        }
        return null;
    }

    /**
     * SELECT(· WITH) 문의 FROM · JOIN 뒤 테이블 이름(따옴표만 떼고 스키마는 남긴다, 하위 질의 · FLOWFILE 제외).
     * DELETE FROM · TRUNCATE 같은 문은 읽는 문이 아니라 빈 목록이다.
     */
    static List<String> sqlSourceTables(String sql) {
        if (sql == null || sql.isBlank()) {
            return List.of();
        }
        String normalized = sql.replaceAll("(?s)/\\*.*?\\*/", " ")
                .replaceAll("(?m)--.*$", " ")
                .replaceAll("\\s+", " ")
                .trim();
        String head = normalized.toLowerCase(Locale.ROOT);
        if (!head.startsWith("select") && !head.startsWith("with") && !head.startsWith("(")) {
            return List.of();
        }
        Set<String> tables = new LinkedHashSet<>();
        Matcher matcher = SQL_TABLE_CLAUSE.matcher(normalized);
        while (matcher.find()) {
            for (String part : matcher.group(1).split(",")) {
                String token = part.trim().split("\\s+")[0];
                String cleaned = token.replaceAll("[`\"\\[\\]]", "").trim();
                if (cleaned.isEmpty() || cleaned.startsWith("(")) {
                    continue;
                }
                if (!cleaned.equalsIgnoreCase("FLOWFILE")) {
                    tables.add(cleaned);
                }
            }
        }
        return new ArrayList<>(tables);
    }

    private static boolean typeIncludes(EtlJobStep step, String type) {
        return Objects.toString(step.getStepType(), "").toLowerCase(Locale.ROOT).contains(type.toLowerCase(Locale.ROOT));
    }

    private static String shortType(String type) {
        if (type == null || type.isBlank()) {
            return "-";
        }
        return type.substring(type.lastIndexOf('.') + 1);
    }

    private static String join(String... parts) {
        String joined = java.util.Arrays.stream(parts)
                .filter(part -> part != null && !part.isBlank())
                .collect(Collectors.joining(" / "));
        return joined.isEmpty() ? "-" : joined;
    }
}
