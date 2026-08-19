package com.company.pipeline.connection;

import com.company.pipeline.connection.dto.ConnectionReferenceResponse;
import com.company.pipeline.connection.dto.ConnectionUsageResponse;
import com.company.pipeline.jobcatalog.EtlJobRepository;
import com.company.pipeline.jobcatalog.EtlJobStepRepository;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineDefinitionRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(readOnly = true)
public class ConnectionUsageService {
    private final ConnectionRepository connectionRepository;
    private final PipelineDefinitionRepository pipelineRepository;
    private final EtlJobStepRepository etlJobStepRepository;
    private final EtlJobRepository etlJobRepository;

    public ConnectionUsageService(ConnectionRepository connectionRepository,
            PipelineDefinitionRepository pipelineRepository,
            EtlJobStepRepository etlJobStepRepository,
            EtlJobRepository etlJobRepository) {
        this.connectionRepository = connectionRepository;
        this.pipelineRepository = pipelineRepository;
        this.etlJobStepRepository = etlJobStepRepository;
        this.etlJobRepository = etlJobRepository;
    }

    public List<ConnectionUsageResponse> list() {
        return connectionRepository.findAll().stream().map(this::usage).toList();
    }

    public ConnectionUsageResponse get(Long connectionId) {
        PipelineConnection connection = connectionRepository.findById(connectionId)
                .orElseThrow(() -> new ConnectionNotFoundException(connectionId));
        return usage(connection);
    }

    private ConnectionUsageResponse usage(PipelineConnection connection) {
        List<ConnectionReferenceResponse> references = new ArrayList<>();
        List<PipelineDefinition> pipelines = pipelineRepository
                .findBySourceConnectionIdOrTargetConnectionId(connection.getId(), connection.getId());
        long sourceCount = 0;
        long targetCount = 0;
        for (PipelineDefinition pipeline : pipelines) {
            if (Objects.equals(pipeline.getSourceConnectionId(), connection.getId())) {
                sourceCount++;
                references.add(new ConnectionReferenceResponse(
                        "CDC", pipeline.getId(), pipeline.getName(), "SOURCE", pipeline.getStatus().name()));
            }
            if (Objects.equals(pipeline.getTargetConnectionId(), connection.getId())) {
                targetCount++;
                references.add(new ConnectionReferenceResponse(
                        "CDC", pipeline.getId(), pipeline.getName(), "TARGET", pipeline.getStatus().name()));
            }
        }
        List<Long> etlJobIds = connection.getNifiControllerServiceId() == null
                ? List.of()
                : etlJobStepRepository.findByDbcpServiceIdAndDeletedAtIsNull(
                        connection.getNifiControllerServiceId()).stream()
                        .map(step -> step.getJobId()).distinct().toList();
        etlJobRepository.findAllById(etlJobIds).forEach(job -> references.add(
                new ConnectionReferenceResponse("ETL", job.getId(), job.getJobName(), "DBCP", null)));
        return new ConnectionUsageResponse(connection.getId(), sourceCount, targetCount,
                etlJobIds.size(), references.isEmpty(), List.copyOf(references));
    }
}
