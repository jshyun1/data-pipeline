package com.company.pipeline.airflowdashboard;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.NifiProcessGroupTreeService;
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
    private final NifiProcessGroupTreeService processGroupTreeService;

    public AirflowDagCatalogDeletionService(
            AirflowDagCatalogRepository repository,
            AirflowDagRunClient airflowClient,
            PipelineService pipelineService,
            NifiClient nifiClient,
            NifiProcessGroupTreeService processGroupTreeService) {
        this.repository = repository;
        this.airflowClient = airflowClient;
        this.pipelineService = pipelineService;
        this.nifiClient = nifiClient;
        this.processGroupTreeService = processGroupTreeService;
    }

    public void delete(String dagId) {
        AirflowDagCatalog catalog = repository.findByDagId(dagId)
                .orElseThrow(() -> new BusinessException(
                        ErrorCode.VALIDATION_ERROR, "DAG를 찾을 수 없습니다: " + dagId));
        // 워크플로우 DAG는 "job들의 조합"이지 NiFi 그룹이 아니다. 여기서 지우면 조합만
        // 없애려다 실제 NiFi 자산을 건드리게 되므로, 캔버스의 게시 취소로 유도한다.
        if (dagId.startsWith("etl_wf_")) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR,
                    "워크플로우 DAG는 여기서 삭제할 수 없습니다. 워크플로우 > 설계에서 게시를 내려주세요.");
        }
        Matcher cdc = CDC_DAG.matcher(dagId);
        Matcher etl = ETL_DAG.matcher(dagId);
        if (cdc.matches()) {
            pipelineService.delete(Long.parseLong(cdc.group(1)));
        } else if (etl.matches()) {
            nifiClient.deleteRootProcessGroupByIdPrefix(etl.group(1));
            processGroupTreeService.refreshAfterMutation();
        } else {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, "삭제할 수 없는 DAG 형식입니다: " + dagId);
        }
        airflowClient.deleteDag(dagId);
        repository.delete(catalog);
    }
}
