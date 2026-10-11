package com.company.pipeline.llm.context;

import static org.assertj.core.api.Assertions.assertThat;

import com.company.pipeline.airflowdashboard.AirflowDagRunClient.DagRun;
import com.company.pipeline.airflowdashboard.AirflowDagRunClient.TaskInstance;
import com.company.pipeline.llm.context.WorkflowRunFacts.NodeRun;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkflowRunFactsTest {

    private static final OffsetDateTime T0 = OffsetDateTime.of(2026, 10, 11, 9, 58, 56, 0, ZoneOffset.UTC);   // KST 18:58:56

    private static TaskInstance task(String id, String state, int startSec, Integer endSec, int tryNumber) {
        return new TaskInstance(id, state, tryNumber, T0.plusSeconds(startSec), endSec == null ? null : T0.plusSeconds(endSec));
    }

    private static TaskInstance notStarted(String id) {
        return new TaskInstance(id, null, 0, null, null);
    }

    @Test
    void groupsTasksIntoNodesInStartOrder() {
        List<NodeRun> nodes = WorkflowRunFacts.nodes(List.of(
                task("job02.open_run", "success", 5, 6, 1),
                task("job02.await", "success", 7, 20, 1),
                task("job01.open_run", "success", 0, 1, 1),
                task("job01.await", "failed", 2, 30, 2),
                task("sub03.run", "success", 31, 90, 1),
                notStarted("workflow_done")));

        assertThat(nodes).extracting(NodeRun::nodeKey).containsExactly("job01", "job02", "sub03", "workflow_done");
        NodeRun job01 = nodes.get(0);
        assertThat(job01.state()).isEqualTo("failed");
        assertThat(job01.failedSteps()).containsExactly("await");
        assertThat(job01.retries()).isEqualTo(1);
        assertThat(nodes.get(1).state()).isEqualTo("success");
        assertThat(nodes.get(3).isEnd()).isTrue();
        assertThat(nodes.get(2).isEnd()).isFalse();
    }

    @Test
    void rollUpMatchesScreenRule() {
        // 끝난 단계와 아직 차례가 안 온 단계가 섞이면 «실행 중»(화면 rollUpState 와 같다).
        assertThat(WorkflowRunFacts.rollUp(List.of(task("a.await", "success", 0, 1, 1), notStarted("a.stop_pg"))))
                .isEqualTo("running");
        assertThat(WorkflowRunFacts.rollUp(List.of(task("a.run", "deferred", 0, null, 1)))).isEqualTo("running");
        assertThat(WorkflowRunFacts.rollUp(List.of(new TaskInstance("a.open_run", "upstream_failed", 0, null, null))))
                .isEqualTo("upstream_failed");
        assertThat(WorkflowRunFacts.rollUp(List.of(new TaskInstance("a.open_run", "removed", 0, null, null),
                new TaskInstance("a.await", "removed", 0, null, null)))).isEqualTo("removed");
    }

    @Test
    void ordinalCountsSameKstDayByStart() {
        List<DagRun> runs = List.of(
                new DagRun("r6", "success", T0, T0.plusMinutes(1), null),
                new DagRun("r5", "success", T0.minusMinutes(39), T0.minusMinutes(37), null),
                new DagRun("prev-day", "success", T0.minusHours(12), T0.minusHours(11), null),   // KST 10-11 06:58 - 같은 날
                new DagRun("older", "success", T0.minusHours(20), T0.minusHours(19), null));     // KST 10-10 22:58 - 전날

        assertThat(WorkflowRunFacts.ordinal(runs, "r6", T0)).isEqualTo(3);
        assertThat(WorkflowRunFacts.ordinal(runs, "prev-day", T0.minusHours(12))).isEqualTo(1);
        assertThat(WorkflowRunFacts.ordinal(runs, "missing", T0)).isZero();
        assertThat(WorkflowRunFacts.ordinal(runs, "r6", null)).isZero();
    }

    @Test
    void runTypeAndChildRunIdFollowDagGeneratorRule() {
        String parent = "manual__2026-10-11T09:58:56+00:00";
        String child = WorkflowRunFacts.childRunId(parent, "sub03");

        assertThat(child).isEqualTo("manual__manual__2026-10-11T09:58:56+00:00__sub03");
        assertThat(WorkflowRunFacts.runTypeLabel("manual", child)).isEqualTo("상위 워크플로우 호출");
        assertThat(WorkflowRunFacts.runTypeLabel("manual", parent)).isEqualTo("수동 실행");
        assertThat(WorkflowRunFacts.runTypeLabel("scheduled", "scheduled__2026-10-11T01:30:00+00:00")).isEqualTo("스케줄");
        assertThat(WorkflowRunFacts.runTypeLabel("asset_triggered", "asset_triggered__x")).isEqualTo("선행 워크플로우 완료");
    }

    @Test
    void textHelpers() {
        assertThat(WorkflowRunFacts.duration(T0, T0.plusSeconds(84))).isEqualTo("1분 24초");
        assertThat(WorkflowRunFacts.duration(T0, T0.plusSeconds(3725))).isEqualTo("1시간 2분 5초");
        assertThat(WorkflowRunFacts.duration(T0, null)).isEqualTo("-");
        assertThat(WorkflowRunFacts.stamp(T0)).isEqualTo("2026-10-11 18:58:56");
        assertThat(WorkflowRunFacts.clock(T0)).isEqualTo("18:58:56");
        assertThat(WorkflowRunFacts.clock(null)).isEqualTo("-");
        assertThat(WorkflowRunFacts.stateLabel("upstream_failed")).isEqualTo("선행 실패로 미실행");
    }
}
