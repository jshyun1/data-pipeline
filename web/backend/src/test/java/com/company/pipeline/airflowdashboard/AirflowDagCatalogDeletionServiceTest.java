package com.company.pipeline.airflowdashboard;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.pipeline.PipelineService;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AirflowDagCatalogDeletionServiceTest {

    private final AirflowDagCatalogRepository repository = mock(AirflowDagCatalogRepository.class);
    private final AirflowDagRunClient airflowClient = mock(AirflowDagRunClient.class);
    private final PipelineService pipelineService = mock(PipelineService.class);
    private final NifiClient nifiClient = mock(NifiClient.class);
    private final AirflowDagCatalog catalog = mock(AirflowDagCatalog.class);
    private AirflowDagCatalogDeletionService service;

    @BeforeEach
    void setUp() {
        service = new AirflowDagCatalogDeletionService(repository, airflowClient, pipelineService, nifiClient);
    }

    @Test
    void deletesCdcSourceAndDag() {
        String dagId = "kafka_pipeline_17_control";
        when(repository.findByDagId(dagId)).thenReturn(Optional.of(catalog));

        service.delete(dagId);

        verify(pipelineService).delete(17L);
        verify(airflowClient).deleteDag(dagId);
        verify(repository).delete(catalog);
    }

    @Test
    void deletesEtlSourceAndDag() {
        String dagId = "nifi_pipeline_abcd1234_control";
        when(repository.findByDagId(dagId)).thenReturn(Optional.of(catalog));

        service.delete(dagId);

        verify(nifiClient).deleteRootProcessGroupByIdPrefix("abcd1234");
        verify(airflowClient).deleteDag(dagId);
        verify(repository).delete(catalog);
    }
}
