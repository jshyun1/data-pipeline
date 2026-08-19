package com.company.pipeline.airflowdashboard;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.pipeline.PipelineService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.stereotype.Service;

@Service
public class AirflowDagCatalogDeletionService {

    private static final Pattern CDC_DAG = Pattern.compile("^kafka_pipeline_(\\d+)_control$");
    private static final Pattern ETL_DAG = Pattern.compile("^nifi_pipeline_([a-fA-F0-9]{8})_control$");

    private final AirflowDagCatalogRepository repository;
    private final AirflowDagRunClient airflowClient;
    private final PipelineService pipelineService;
    private final NifiClient nifiClient;

    public AirflowDagCatalogDeletionService(
            AirflowDagCatalogRepository repository,
            AirflowDagRunClient airflowClient,
            PipelineService pipelineService,
            NifiClient nifiClient) {
        this.repository = repository;
        this.airflowClient = airflowClient;
        this.pipelineService = pipelineService;
        this.nifiClient = nifiClient;
    }

    public void delete(String dagId) {
        AirflowDagCatalog catalog = repository.findByDagId(dagId)
                .orElseThrow(() -> new BusinessException(
                        ErrorCode.VALIDATION_ERROR, "DAG를 찾을 수 없습니다: " + dagId));
        Matcher cdc = CDC_DAG.matcher(dagId);
        Matcher etl = ETL_DAG.matcher(dagId);
        if (cdc.matches()) {
            pipelineService.delete(Long.parseLong(cdc.group(1)));
        } else if (etl.matches()) {
            nifiClient.deleteRootProcessGroupByIdPrefix(etl.group(1));
        } else {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "삭제할 수 없는 DAG 형식입니다: " + dagId);
        }
        airflowClient.deleteDag(dagId);
        repository.delete(catalog);
    }
}
