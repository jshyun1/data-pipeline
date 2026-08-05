package com.company.pipeline.jobcatalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.dto.NifiFlowResponse;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ConnectionComponent;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ConnectionEndpoint;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ConnectionEntity;
import com.company.pipeline.nifi.dto.NifiFlowResponse.Flow;
import com.company.pipeline.nifi.dto.NifiFlowResponse.Position;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessGroupComponent;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessGroupEntity;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessGroupFlowEntity;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessorComponent;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessorConfig;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessorEntity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class NifiJobMirrorServiceTest {

    @Mock
    private NifiClient nifiClient;
    @Mock
    private EtlJobRepository jobRepository;
    @Mock
    private EtlJobStepRepository stepRepository;
    @Mock
    private EtlJobLinkRepository linkRepository;
    @Mock
    private EtlJobParamRepository paramRepository;
    @Mock
    private EtlJobSnapshotRepository snapshotRepository;
    @Mock
    private JobLookup jobLookup;

    private NifiJobMirrorService service;

    @BeforeEach
    void setUp() {
        service = new NifiJobMirrorService(nifiClient, jobRepository, stepRepository,
                linkRepository, paramRepository, snapshotRepository, jobLookup);
        when(jobRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(stepRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(linkRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(stepRepository.findByJobIdAndDeletedAtIsNull(any())).thenReturn(List.of());
        when(linkRepository.findByJobIdAndDeletedAtIsNull(any())).thenReturn(List.of());
        when(paramRepository.findByJobIdAndDeletedAtIsNull(any())).thenReturn(List.of());
        when(snapshotRepository.findFirstByJobIdOrderByCapturedAtDesc(any())).thenReturn(Optional.empty());
        when(jobRepository.findByDeletedAtIsNullOrderByJobNameAsc()).thenReturn(List.of());
    }

    @Test
    void PutDatabaseRecord_설정에서_적재대상과_UPSERT키를_컬럼으로_뽑는다() {
        givenCanvasWithSingleJob();

        service.sync();

        ArgumentCaptor<EtlJobStep> captor = ArgumentCaptor.forClass(EtlJobStep.class);
        verify(stepRepository, org.mockito.Mockito.atLeastOnce()).save(captor.capture());

        EtlJobStep upsert = captor.getAllValues().stream()
                .filter(step -> "PutDatabaseRecord".equals(step.getStepType()))
                .findFirst()
                .orElseThrow();
        assertThat(upsert.getTargetTable()).isEqualTo("dz_com001m");
        assertThat(upsert.getStatementType()).isEqualTo("UPSERT");
        assertThat(upsert.getUpdateKeys()).isEqualTo("#{DZ_UPSERT_KEYS_COM001M}");
        assertThat(upsert.getPropsJson()).contains("put-db-record-table-name");

        EtlJobStep extract = captor.getAllValues().stream()
                .filter(step -> "ExecuteSQL".equals(step.getStepType()))
                .findFirst()
                .orElseThrow();
        assertThat(extract.getSqlText()).isEqualTo("SELECT * FROM TB_COM001M");
        assertThat(extract.getDbcpServiceId()).isEqualTo("oracle-pool");
    }

    @Test
    void 연결선은_관계이름까지_담는다() {
        givenCanvasWithSingleJob();

        service.sync();

        ArgumentCaptor<EtlJobLink> captor = ArgumentCaptor.forClass(EtlJobLink.class);
        verify(linkRepository, org.mockito.Mockito.atLeastOnce()).save(captor.capture());
        assertThat(captor.getAllValues())
                .anySatisfy(link -> assertThat(link.getRelationships()).isEqualTo("failure,retry"));
    }

    @Test
    void 잡_하나가_사라지면_지우지_않고_삭제표시만_한다() {
        givenEmptyCanvas();
        EtlJob missing = new EtlJob("gone-1", "사라진잡");
        when(jobRepository.findByDeletedAtIsNullOrderByJobNameAsc()).thenReturn(new ArrayList<>(List.of(missing)));

        NifiJobMirrorService.SyncResult result = service.sync();

        assertThat(result.deleted()).isEqualTo(1);
        assertThat(missing.getDeletedAt()).isNotNull();
        verify(jobRepository, never()).delete(any());
    }

    @Test
    void 잡이_한꺼번에_사라지면_삭제표시를_보류한다() {
        givenEmptyCanvas();
        List<EtlJob> many = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            many.add(new EtlJob("gone-" + i, "사라진잡" + i));
        }
        when(jobRepository.findByDeletedAtIsNullOrderByJobNameAsc()).thenReturn(many);

        NifiJobMirrorService.SyncResult result = service.sync();

        assertThat(result.deleted()).isZero();
        assertThat(many).allSatisfy(job -> assertThat(job.getDeletedAt()).isNull());
    }

    @Test
    void 구조가_그대로면_스냅샷을_새로_쌓지_않는다() {
        givenCanvasWithSingleJob();
        service.sync();

        ArgumentCaptor<EtlJobSnapshot> captor = ArgumentCaptor.forClass(EtlJobSnapshot.class);
        verify(snapshotRepository).save(captor.capture());
        EtlJobSnapshot first = captor.getValue();

        when(snapshotRepository.findFirstByJobIdOrderByCapturedAtDesc(any()))
                .thenReturn(Optional.of(first));

        NifiJobMirrorService.SyncResult second = service.sync();

        assertThat(second.snapshots()).isZero();
    }

    @Test
    void Airflow_DAG_이름은_그룹id_앞8자로_만든다() {
        assertThat(EtlJob.airflowDagId("c68c6a27-019f-1000-705c-a58368837909"))
                .isEqualTo("nifi_pipeline_c68c6a27_control");
    }

    // ---------------------------------------------------------------- fixtures

    private void givenEmptyCanvas() {
        when(nifiClient.getFlow("root")).thenReturn(new NifiFlowResponse(
                new ProcessGroupFlowEntity("root", null, null, new Flow(List.of(), List.of(), List.of()))));
    }

    /** DZ_UPSERT 레인 하나(extract -> upsert, upsert -> 오류로그)를 흉내낸 캔버스. */
    private void givenCanvasWithSingleJob() {
        ProcessGroupComponent groupComponent = new ProcessGroupComponent(
                "pg-1", "DZ_UPSERT", "변경적재", new Position(184.0, 288.0),
                0, 3, 0, 0, null);
        when(nifiClient.getFlow("root")).thenReturn(new NifiFlowResponse(
                new ProcessGroupFlowEntity("root", null, null,
                        new Flow(List.of(new ProcessGroupEntity("pg-1", groupComponent)),
                                List.of(), List.of()))));

        ProcessorEntity extract = new ProcessorEntity("p-extract", new ProcessorComponent(
                "p-extract", "extract-chg-COM001M",
                "org.apache.nifi.processors.standard.ExecuteSQL",
                new Position(1050.0, 320.0), "STOPPED", "VALID",
                new ProcessorConfig(Map.of(
                        "SQL select query", "SELECT * FROM TB_COM001M",
                        "Database Connection Pooling Service", "oracle-pool"),
                        "TIMER_DRIVEN", "0 sec")));

        ProcessorEntity upsert = new ProcessorEntity("p-upsert", new ProcessorComponent(
                "p-upsert", "upsert-dz-COM001M",
                "org.apache.nifi.processors.standard.PutDatabaseRecord",
                new Position(1400.0, 320.0), "STOPPED", "VALID",
                new ProcessorConfig(Map.of(
                        "put-db-record-table-name", "dz_com001m",
                        "put-db-record-statement-type", "UPSERT",
                        "put-db-record-update-keys", "#{DZ_UPSERT_KEYS_COM001M}",
                        "put-db-record-dcbp-service", "pg-pool"),
                        "TIMER_DRIVEN", "0 sec")));

        ConnectionEntity failureLink = new ConnectionEntity("c-1", new ConnectionComponent(
                "c-1",
                new ConnectionEndpoint("p-upsert", "upsert-dz-COM001M", "PROCESSOR"),
                new ConnectionEndpoint("p-log", "log-error-COM001M", "PROCESSOR"),
                List.of("failure", "retry")));

        when(nifiClient.getFlow("pg-1")).thenReturn(new NifiFlowResponse(
                new ProcessGroupFlowEntity("pg-1", "root", null,
                        new Flow(List.of(), List.of(extract, upsert), List.of(failureLink)))));
        when(nifiClient.getFlow(anyString())).thenAnswer(invocation -> {
            String groupId = invocation.getArgument(0);
            if ("root".equals(groupId)) {
                return new NifiFlowResponse(new ProcessGroupFlowEntity("root", null, null,
                        new Flow(List.of(new ProcessGroupEntity("pg-1", groupComponent)),
                                List.of(), List.of())));
            }
            if ("pg-1".equals(groupId)) {
                return new NifiFlowResponse(new ProcessGroupFlowEntity("pg-1", "root", null,
                        new Flow(List.of(), List.of(extract, upsert), List.of(failureLink))));
            }
            return new NifiFlowResponse(new ProcessGroupFlowEntity(groupId, "root", null,
                    new Flow(List.of(), List.of(), List.of())));
        });
        when(jobRepository.findByNifiPgId("pg-1")).thenReturn(Optional.empty());
    }
}
