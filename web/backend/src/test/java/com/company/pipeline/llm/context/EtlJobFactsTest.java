package com.company.pipeline.llm.context;

import static org.assertj.core.api.Assertions.assertThat;

import com.company.pipeline.jobcatalog.EtlJobLink;
import com.company.pipeline.jobcatalog.EtlJobRun;
import com.company.pipeline.jobcatalog.EtlJobStep;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class EtlJobFactsTest {

    private static final LocalDate TODAY = LocalDate.of(2026, 10, 10);

    private static EtlJobStep step(String id, String name, String type, double x, String sql, String props) {
        EtlJobStep step = new EtlJobStep(1L, id, name, type);
        step.applySnapshot(1L, name, type, null, null, sql, null, null, null, null, x, 0.0, null, null, props);
        return step;
    }

    private static EtlJobRun run(LocalDateTime startedAt, int failedSteps) {
        EtlJobRun run = new EtlJobRun(1L, startedAt);
        run.observeActivity(startedAt.plusMinutes(3), 0);
        run.close(failedSteps);
        return run;
    }

    @Test
    void 소스는_첫_조회_SQL_테이블_타깃은_마지막_프로세서의_스키마_테이블() {
        // 타깃은 화면 예전 보고서(sourceTargetText)와 같은 규칙: 연결이 안 나가는 마지막 단계.
        List<EtlJobStep> steps = List.of(
                step("p1", "01_SELECT", "org.apache.nifi.processors.standard.ExecuteSQLRecord", 0,
                        null, "{\"SQL select query\":\"select a.* from ora.TB_SRC a join \\\"TB_CODE\\\" c on a.k = c.k\"}"),
                step("p2", "02_CONVERT", "org.apache.nifi.processors.standard.QueryRecord", 400, null, "{}"),
                step("p3", "03_INSERT", "org.apache.nifi.processors.standard.PutDatabaseRecord", 800, null,
                        "{\"Database Type\":\"PostgreSQL\",\"put-db-record-schema-name\":\"public\",\"put-db-record-table-name\":\"tb_dst\",\"Password\":\"secret\"}"));
        List<EtlJobLink> links = List.of(new EtlJobLink(1L, "c1", "p1", "p2"), new EtlJobLink(1L, "c2", "p2", "p3"));

        assertThat(EtlJobFacts.source(steps, links)).isEqualTo("ora.TB_SRC, TB_CODE");
        assertThat(EtlJobFacts.target(steps, links)).isEqualTo("PostgreSQL / public / tb_dst").doesNotContain("secret");
        assertThat(EtlJobFacts.flow(steps)).isEqualTo("01_SELECT → 02_CONVERT → 03_INSERT");
    }

    @Test
    void 시작_단계가_방아쇠여도_읽는_단계를_소스로_찾는다() {
        // DZ_COM001M 모양(2026-10-10): 01_START(GenerateFlowFile) → 02_TRUNCATE(지우기) → 04_SELECT → 04-1(QueryRecord) → 05_INSERT.
        // 예전 규칙(연결이 안 들어오는 첫 단계)은 01_START 를 골라 소스가 «-» 였다.
        List<EtlJobStep> steps = List.of(
                step("s1", "01_START_INITIAL_LOAD", "org.apache.nifi.processors.standard.GenerateFlowFile", 0, null, "{}"),
                step("s2", "02_TRUNCATE", "org.apache.nifi.processors.standard.ExecuteSQL", 100,
                        "DELETE FROM public.dz_com001m", "{}"),
                step("s3", "04_SELECT", "org.apache.nifi.processors.standard.ExecuteSQLRecord", 200, null,
                        "{\"SQL select query\":\"SELECT * FROM CSB.TB_COM001M\"}"),
                step("s4", "04-1_add_columns", "org.apache.nifi.processors.standard.QueryRecord", 300,
                        "select *, 1 as x from FLOWFILE", "{}"),
                step("s5", "05_INSERT", "org.apache.nifi.processors.standard.PutDatabaseRecord", 400, null,
                        "{\"Database Type\":\"PostgreSQL\",\"put-db-record-table-name\":\"dz_com001m\"}"));
        List<EtlJobLink> links = List.of(new EtlJobLink(1L, "c1", "s1", "s2"), new EtlJobLink(1L, "c2", "s2", "s3"),
                new EtlJobLink(1L, "c3", "s3", "s4"), new EtlJobLink(1L, "c4", "s4", "s5"));

        assertThat(EtlJobFacts.source(steps, links)).isEqualTo("CSB.TB_COM001M");
        assertThat(EtlJobFacts.target(steps, links)).isEqualTo("PostgreSQL / dz_com001m");
        assertThat(EtlJobFacts.sqlSourceTables("DELETE FROM public.dz_com001m")).isEmpty();
        assertThat(EtlJobFacts.sqlSourceTables("select *, 1 as x from FLOWFILE")).isEmpty();
    }

    @Test
    void 파일_적재는_입력_폴더를_소스로() {
        List<EtlJobStep> steps = List.of(step("f1", "ListFile", "org.apache.nifi.processors.standard.ListFile", 0, null,
                "{\"Input Directory\":\"/data/in\"}"));

        assertThat(EtlJobFacts.source(steps, List.of())).isEqualTo("파일 / /data/in");
    }

    @Test
    void 최근_5일_실행은_성공_실패와_연속_성공을_센다() {
        List<EtlJobRun> runs = List.of(   // 새것부터
                run(TODAY.atTime(9, 0), 0),
                run(TODAY.minusDays(1).atTime(9, 0), 0),
                run(TODAY.minusDays(2).atTime(9, 0), 1),
                run(TODAY.minusDays(6).atTime(9, 0), 0));   // 5일 밖

        assertThat(EtlJobFacts.recentRuns(runs, TODAY)).isEqualTo("최근 5일 3회 실행(성공 2 · 실패 1), 최근 2회 연속 성공");
        assertThat(EtlJobFacts.latestDuration(runs)).startsWith("3분");
        assertThat(EtlJobFacts.recentRuns(List.of(), TODAY)).isEqualTo("최근 5일 실행 기록 없음");
    }

    @Test
    void 소요_시간_표기() {
        assertThat(EtlJobFacts.durationText(Duration.ofSeconds(42))).isEqualTo("42초");
        assertThat(EtlJobFacts.durationText(Duration.ofSeconds(185))).isEqualTo("3분 5초");
        assertThat(EtlJobFacts.durationText(Duration.ofMinutes(125))).isEqualTo("2시간 5분");
    }
}
