package com.company.pipeline.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.heartbeat.HeartbeatService;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import com.company.pipeline.connection.DbType;
import com.company.pipeline.rollup.RollupService;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * 처리 건수(delta) 계산. offset 은 "레코드 수"가 아니라 "위치"라, retention 으로 앞부분이
 * 삭제된 토픽에서는 committed 차이가 실제로 읽은 레코드 수보다 커진다(2026-08-25 실측).
 */
@ExtendWith(MockitoExtension.class)
class KafkaPipelineMetricSchedulerTest {

    private static final Long PIPELINE_ID = 14L;

    @Mock
    private PipelineDefinitionRepository pipelineDefinitionRepository;
    @Mock
    private PipelineMetricSnapshotService pipelineMetricSnapshotService;
    @Mock
    private PipelineMetricSnapshotRepository pipelineMetricSnapshotRepository;
    @Mock
    private PipelineDailyLoadMetricService dailyLoadMetricService;
    @Mock
    private HeartbeatService heartbeat;
    @Mock
    private RollupService rollupService;

    private KafkaPipelineMetricScheduler scheduler() {
        return new KafkaPipelineMetricScheduler(pipelineDefinitionRepository, pipelineMetricSnapshotService,
                pipelineMetricSnapshotRepository, dailyLoadMetricService, heartbeat, rollupService);
    }

    private static PipelineMetricSnapshot snapshot(Long committed, Long earliest) {
        PipelineMetricSnapshot s = new PipelineMetricSnapshot();
        s.setPipelineId(PIPELINE_ID);
        s.setCommittedOffset(committed);
        s.setEarliestOffset(earliest);
        return s;
    }

    /** 직전/현재 스냅샷을 주고 스케줄러를 한 번 돌린 뒤, 롤업에 기록된 delta 를 돌려준다. */
    private long runAndCaptureDelta(PipelineMetricSnapshot previous, PipelineMetricSnapshot current) {
        PipelineDefinition pipeline = new PipelineDefinition("platform-test2-tb_imp018m", "TABLE_CDC",
                1L, 2L, DbType.ORACLE, DbType.POSTGRESQL, "CSB", "TB_IMP018M", "public", "tb_imp018m",
                "oracle-cdc.CSB.TB_IMP018M", false, null, "test");
        pipeline.setId(PIPELINE_ID);
        when(pipelineDefinitionRepository.findByStatusIn(any())).thenReturn(List.of(pipeline));
        when(pipelineMetricSnapshotRepository.findTopByPipelineIdOrderByCollectedAtDesc(PIPELINE_ID))
                .thenReturn(Optional.ofNullable(previous));
        when(pipelineMetricSnapshotService.recordSnapshot(PIPELINE_ID)).thenReturn(current);

        scheduler().checkDeployedPipelines();

        ArgumentCaptor<Long> delta = ArgumentCaptor.forClass(Long.class);
        verify(rollupService).recordObservation(anyString(), anyString(), anyString(), anyString(),
                eq(PIPELINE_ID), delta.capture());
        return delta.getValue();
    }

    @Test
    void 평상시_retention_영향이_없으면_committed_증가분_그대로_센다() {
        long delta = runAndCaptureDelta(snapshot(1_000L, 0L), snapshot(1_500L, 0L));

        assertThat(delta).isEqualTo(500L);
        verify(dailyLoadMetricService).incrementLoadedCount(anyString(), anyString(), anyString(), anyString(),
                eq(500L));
    }

    @Test
    void retention_으로_앞부분이_삭제됐으면_삭제구간은_처리로_세지_않는다() {
        // 컨슈머 그룹이 처음 붙어 committed=0, 그런데 토픽 앞 10만 건은 이미 삭제된 상태.
        // 컨슈머는 100000 부터 읽어 200000 까지 진행한다 - 실제로 읽은 건 10만 건뿐이다.
        long delta = runAndCaptureDelta(snapshot(0L, 100_000L), snapshot(200_000L, 100_000L));

        assertThat(delta).isEqualTo(100_000L);
    }

    @Test
    void 보정_전이라면_20만으로_세던_상황이다() {
        // earliest 를 모르던 시절(기존 행은 NULL)에는 하한이 0 이라 보정 전과 같은 값이 된다.
        // 하위호환 확인 - NULL 이어도 예외 없이 동작해야 한다.
        long delta = runAndCaptureDelta(snapshot(0L, null), snapshot(200_000L, 100_000L));

        assertThat(delta).isEqualTo(200_000L);
    }

    @Test
    void 커넥터_재생성으로_offset_이_되감기면_음수_대신_0_으로_센다() {
        long delta = runAndCaptureDelta(snapshot(200_000L, 0L), snapshot(0L, 0L));

        assertThat(delta).isZero();
        // 0건이어도 관측 자체는 기록한다("0건 관측"과 "미관측"의 구분).
        verify(dailyLoadMetricService, never())
                .incrementLoadedCount(anyString(), anyString(), anyString(), anyString(), anyLong());
    }

    @Test
    void 직전_스냅샷이_없으면_기준점만_잡고_0_으로_센다() {
        long delta = runAndCaptureDelta(null, snapshot(200_000L, 100_000L));

        assertThat(delta).isZero();
    }
}
