package com.company.pipeline.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.jobcatalog.JobLookup;
import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.dto.NifiBulletinBoardResponse;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * bulletin 수집이 NiFi 재시작을 넘어서도 계속 동작하는지.
 *
 * <p>회귀 방지 대상: bulletin id는 NiFi 프로세스 안에서만 단조 증가하고 재시작하면
 * 1부터 다시 시작한다. 예전 구현은 max(bulletin_id) 이후만 요청해서(after 파라미터)
 * 재시작 뒤의 새 실패를 통째로 놓쳤고, 실제로 DZ 5개 테이블이 비워진 사고가 ETL
 * 로그에 한 줄도 안 남았다(2026-07-29).
 */
@ExtendWith(MockitoExtension.class)
class NifiBulletinCollectionTest {

    @Mock
    private JobLookup jobLookup;
    @Mock
    private NifiClient nifiClient;
    @Mock
    private NifiCounterSnapshotRepository snapshotRepository;
    @Mock
    private PipelineDailyLoadMetricService dailyLoadMetricService;
    @Mock
    private NifiExecutionLogEntryRepository executionLogRepository;

    private NifiPipelineMetricScheduler scheduler() {
        return new NifiPipelineMetricScheduler(nifiClient, snapshotRepository, dailyLoadMetricService,
                executionLogRepository, jobLookup);
    }

    private static NifiBulletinBoardResponse board(NifiBulletinBoardResponse.Bulletin... bulletins) {
        List<NifiBulletinBoardResponse.BulletinEntity> entities = List.of(bulletins).stream()
                .map(b -> new NifiBulletinBoardResponse.BulletinEntity(b.id(), b.groupId(), b.sourceId(), b))
                .toList();
        return new NifiBulletinBoardResponse(new NifiBulletinBoardResponse.BulletinBoard(entities));
    }

    private static NifiBulletinBoardResponse.Bulletin error(long id, String source, String message) {
        return new NifiBulletinBoardResponse.Bulletin(id, "LOG", "grp-1", "proc-" + source, source,
                "ERROR", message, "09:04:17 KST");
    }

    @Test
    void collectBulletins_alwaysReadsWholeBoard_soRestartedIdsAreNotFilteredOut() {
        // 커서를 쓰면 재시작 뒤 작아진 id가 NiFi 쪽에서 걸러진다. after=0으로만 부를 것.
        when(nifiClient.getBulletins(anyLong())).thenReturn(board());

        scheduler().collectBulletins();

        verify(nifiClient).getBulletins(0L);
    }

    @Test
    void collectBulletins_lowIdAfterNifiRestart_isStillRecorded() {
        // 어제 id=5까지 저장해둔 상태에서 NiFi가 재시작해 새 실패가 id=1로 올라온 상황.
        // 시간 범위 기준 중복 판정이라 "예전 id=5" 때문에 걸러지면 안 된다.
        when(nifiClient.getBulletins(0L)).thenReturn(
                board(error(1L, "extract-tb-COM003M", "ORA-12170: Cannot connect")));
        when(executionLogRepository.existsByBulletinIdAndOccurredAtAfter(eq(1L), any(LocalDateTime.class)))
                .thenReturn(false);

        scheduler().collectBulletins();

        ArgumentCaptor<NifiExecutionLogEntry> captor = ArgumentCaptor.forClass(NifiExecutionLogEntry.class);
        verify(executionLogRepository).save(captor.capture());
        NifiExecutionLogEntry saved = captor.getValue();
        assertThat(saved.getStatus()).isEqualTo(NifiExecutionLogEntry.STATUS_FAILED);
        assertThat(saved.getProcessorName()).isEqualTo("extract-tb-COM003M");
        assertThat(saved.getMessage()).contains("ORA-12170");
    }

    @Test
    void collectBulletins_sameBulletinWithinWindow_isNotSavedTwice() {
        // 30초마다 같은 링버퍼를 다시 읽으므로, 같은 세션의 중복은 걸러져야 한다.
        when(nifiClient.getBulletins(0L)).thenReturn(
                board(error(7L, "load-dz-POP003L", "Failed to put records")));
        when(executionLogRepository.existsByBulletinIdAndOccurredAtAfter(eq(7L), any(LocalDateTime.class)))
                .thenReturn(true);

        scheduler().collectBulletins();

        verify(executionLogRepository, never()).save(any());
    }

    @Test
    void collectBulletins_infoLevel_isIgnored() {
        var info = new NifiBulletinBoardResponse.Bulletin(3L, "LOG", "grp-1", "proc-x", "x",
                "INFO", "그냥 알림", "09:04:17 KST");
        when(nifiClient.getBulletins(0L)).thenReturn(board(info));

        scheduler().collectBulletins();

        verify(executionLogRepository, never()).save(any());
    }
}
