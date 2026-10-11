package com.company.pipeline.llm.context;

import com.company.pipeline.airflowdashboard.AirflowDagRunClient.DagRun;
import com.company.pipeline.airflowdashboard.AirflowDagRunClient.TaskInstance;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 워크플로우 실행 한 건(차수)을 글로 옮기는 규칙(2026-10-11, {@link WorkflowRunContextCollector}).
 * 화면(워크플로우 로그 · logV2Data.ts)과 같은 판정을 쓴다 - task 를 노드 단위로 묶는 법, 노드 상태 접기, 차수 세기.
 * Airflow · DB 를 부르지 않아 따로 시험할 수 있다.
 */
final class WorkflowRunFacts {

    static final ZoneId ZONE = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final Set<String> ACTIVE = Set.of("running", "deferred", "up_for_reschedule", "queued", "scheduled",
            "up_for_retry", "restarting");

    private WorkflowRunFacts() {
    }

    /** 노드 하나(태스크 그룹)의 실행 결과. steps 는 Airflow task 그대로다. */
    record NodeRun(String nodeKey, List<TaskInstance> steps, String state, OffsetDateTime startedAt,
                   OffsetDateTime endedAt, int retries) {

        /** 실패한 단계 이름(open_run · start_pg · await …). */
        List<String> failedSteps() {
            return steps.stream().filter(step -> "failed".equals(step.state())).map(step -> stepName(step.taskId())).toList();
        }

        /** 점이 없는 task(workflow_done 등) - 노드가 아니라 워크플로우 끝 표시다. */
        boolean isEnd() {
            return steps.stream().noneMatch(step -> step.taskId() != null && step.taskId().contains("."));
        }

        /** 실행 뒤 게시로 빠진 노드(Airflow 가 removed 로 남긴다 - 끝나지 않은 차수에 새 DAG 구조가 덮인 경우). */
        boolean isRemoved() {
            return !steps.isEmpty() && steps.stream().allMatch(step -> "removed".equals(step.state()));
        }
    }

    /** task 를 노드 단위로 묶고, 처음 시작한 시각 순으로 세운다(시작 전 노드는 Airflow 가 준 순서대로 뒤에). */
    static List<NodeRun> nodes(List<TaskInstance> tasks) {
        Map<String, List<TaskInstance>> byNode = new LinkedHashMap<>();
        for (TaskInstance task : tasks) {
            byNode.computeIfAbsent(nodeKeyOf(task.taskId()), key -> new ArrayList<>()).add(task);
        }
        List<NodeRun> out = new ArrayList<>();
        byNode.forEach((key, steps) -> out.add(new NodeRun(key, steps, rollUp(steps),
                steps.stream().map(TaskInstance::startDate).filter(Objects::nonNull).min(Comparator.naturalOrder()).orElse(null),
                steps.stream().map(TaskInstance::endDate).filter(Objects::nonNull).max(Comparator.naturalOrder()).orElse(null),
                steps.stream().mapToInt(step -> Math.max(0, (step.tryNumber() == null ? 1 : step.tryNumber()) - 1)).sum())));
        List<NodeRun> started = new ArrayList<>(out.stream().filter(node -> node.startedAt() != null)
                .sorted(Comparator.comparing(NodeRun::startedAt)).toList());
        started.addAll(out.stream().filter(node -> node.startedAt() == null).toList());
        return started;
    }

    /** 단계 상태를 노드 상태로 접는다. 화면 rollUpState 와 같다 - 하나라도 실패면 실패, 도는 단계가 있으면 실행 중. */
    static String rollUp(List<TaskInstance> steps) {
        List<String> states = steps.stream().map(TaskInstance::state).toList();
        if (states.contains("failed")) {
            return "failed";
        }
        // 아직 차례가 안 온 단계는 상태가 null 이다 - Set.of 의 contains(null) 은 NPE 라 먼저 거른다.
        if (states.stream().anyMatch(state -> state != null && ACTIVE.contains(state))) {
            return "running";
        }
        if (states.contains("upstream_failed")) {
            return "upstream_failed";
        }
        if (!states.isEmpty() && states.stream().allMatch("success"::equals)) {
            return "success";
        }
        if (states.stream().anyMatch(Objects::isNull) && states.stream().anyMatch(Objects::nonNull)) {
            return "running";
        }
        return states.stream().filter(Objects::nonNull).findFirst().orElse(null);
    }

    static String nodeKeyOf(String taskId) {
        int dot = taskId == null ? -1 : taskId.lastIndexOf('.');
        return dot > 0 ? taskId.substring(0, dot) : taskId;
    }

    static String stepName(String taskId) {
        int dot = taskId == null ? -1 : taskId.lastIndexOf('.');
        return dot >= 0 ? taskId.substring(dot + 1) : taskId;
    }

    /** 사람이 읽는 상태. Airflow 상태값 그대로 보내면 모델이 «upstream_failed» 같은 말을 그대로 옮긴다. */
    static String stateLabel(String state) {
        if (state == null) {
            return "실행 안 됨";
        }
        return switch (state) {
            case "success" -> "성공";
            case "failed" -> "실패";
            case "running" -> "실행 중";
            case "queued", "scheduled" -> "대기";
            case "upstream_failed" -> "선행 실패로 미실행";
            case "skipped" -> "건너뜀";
            case "removed" -> "이 차수 뒤 설계에서 빠짐";
            case "up_for_retry" -> "재시도 대기";
            default -> state;
        };
    }

    static String stamp(OffsetDateTime at) {
        return at == null ? "-" : at.atZoneSameInstant(ZONE).format(STAMP);
    }

    /** 시각만(HH:mm:ss). 노드 줄은 날짜가 실행과 같아 시각만 적는다. */
    static String clock(OffsetDateTime at) {
        return at == null ? "-" : at.atZoneSameInstant(ZONE).format(CLOCK);
    }

    /** «1분 24초» 꼴. 끝나지 않았으면 now 까지(«경과»라고 붙이는 것은 부르는 쪽). */
    static String duration(OffsetDateTime start, OffsetDateTime end) {
        if (start == null || end == null) {
            return "-";
        }
        long seconds = Math.max(0, Duration.between(start, end).getSeconds());
        long hours = seconds / 3600;
        long minutes = seconds % 3600 / 60;
        long rest = seconds % 60;
        if (hours > 0) {
            return hours + "시간 " + minutes + "분 " + rest + "초";
        }
        return minutes > 0 ? minutes + "분 " + rest + "초" : rest + "초";
    }

    /**
     * 그날(KST) 이 워크플로우의 몇 번째 실행인지 - 화면 «N차»와 같은 규칙(시작 순, 그날 실행만). 목록에 없으면 0.
     * 대기 중이라 시작 시각이 없는 실행은 세지 않는다.
     */
    static int ordinal(List<DagRun> runs, String runId, OffsetDateTime startedAt) {
        if (startedAt == null) {
            return 0;
        }
        var day = startedAt.atZoneSameInstant(ZONE).toLocalDate();
        List<DagRun> sameDay = runs.stream()
                .filter(run -> run.startDate() != null && run.startDate().atZoneSameInstant(ZONE).toLocalDate().equals(day))
                .sorted(Comparator.comparing(DagRun::startDate))
                .toList();
        for (int i = 0; i < sameDay.size(); i++) {
            if (runId.equals(sameDay.get(i).dagRunId())) {
                return i + 1;
            }
        }
        return 0;
    }

    /** 실행 유형. 상위 워크플로우가 부른 실행은 run id 가 {@code manual__{상위 run id}__{노드}} 다(DAG 생성기 규칙). */
    static String runTypeLabel(String runType, String runId) {
        if (runId != null && runId.startsWith("manual__") && runId.lastIndexOf("__") > "manual__".length()) {
            return "상위 워크플로우 호출";
        }
        if (runType == null) {
            return "-";
        }
        return switch (runType) {
            case "manual" -> "수동 실행";
            case "scheduled" -> "스케줄";
            case "asset_triggered", "dataset_triggered" -> "선행 워크플로우 완료";
            case "backfill" -> "백필";
            default -> runType;
        };
    }

    /** 하위 워크플로우 노드가 띄운 실행의 id(DAG 생성기 trigger_run_id 규칙). */
    static String childRunId(String parentRunId, String nodeKey) {
        return "manual__" + parentRunId + "__" + nodeKey;
    }
}
