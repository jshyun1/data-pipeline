package com.company.pipeline.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.pipeline.dto.PipelineBatchCreateRequest;
import com.company.pipeline.pipeline.dto.PipelineCreateRequest;
import com.company.pipeline.pipeline.dto.PipelineResponse;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PipelineBatchCreateServiceTest {
    @Mock PipelineService pipelineService;
    @Mock PipelineDeployService deployService;

    @Test
    void create_deploysAllDefinitions() {
        var first = response(1L, "one"); var second = response(2L, "two");
        var service = new PipelineBatchCreateService(pipelineService, deployService);
        var request = new PipelineBatchCreateRequest(List.of(request("one"), request("two")));
        when(pipelineService.createBatchDefinitions(request.pipelines())).thenReturn(List.of(first, second));
        when(deployService.deploy(1L)).thenReturn(first); when(deployService.deploy(2L)).thenReturn(second);

        assertThat(service.create(request).createdCount()).isEqualTo(2);
    }

    @Test
    void create_compensatesEveryDefinitionInReverseOrderWhenDeployFails() {
        var first = response(1L, "one"); var second = response(2L, "two");
        var service = new PipelineBatchCreateService(pipelineService, deployService);
        var request = new PipelineBatchCreateRequest(List.of(request("one"), request("two")));
        when(pipelineService.createBatchDefinitions(request.pipelines())).thenReturn(List.of(first, second));
        when(deployService.deploy(1L)).thenReturn(first);
        when(deployService.deploy(2L)).thenThrow(new RuntimeException("boom"));

        assertThatThrownBy(() -> service.create(request)).hasMessageContaining("모두 정리");
        InOrder order = inOrder(pipelineService);
        order.verify(pipelineService).delete(2L);
        order.verify(pipelineService).delete(1L);
    }

    private PipelineCreateRequest request(String name) {
        return new PipelineCreateRequest(name, 1L, 2L, "s", "t", "d", "t", "topic", false, null);
    }

    private PipelineResponse response(Long id, String name) {
        return new PipelineResponse(id, name, "TABLE_CDC", 1L, 2L, null, null, "s", "t", "d", "t",
                "topic", PipelineStatus.READY, "INITIAL", false, null, List.of(), null, null);
    }
}
