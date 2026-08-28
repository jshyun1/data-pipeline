package com.company.pipeline.monitoring;

import com.company.pipeline.nifi.NifiClient;
import com.company.pipeline.nifi.dto.NifiFlowResponse;
import com.company.pipeline.nifi.dto.NifiFlowResponse.LabelEntity;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessGroupEntity;
import com.company.pipeline.nifi.dto.NifiFlowResponse.ProcessorEntity;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 이전 버전에서 NiFi 원본 캔버스에 만들던 JOB 상태 라벨을 정리한다.
 *
 * <p>상태와 마지막 실행 정보는 이제 포털 상세 패널 상단에 표시하므로, NiFi 캔버스에는
 * 별도 라벨을 남기지 않는다.
 */
@Component
public class NifiCanvasStatusOverlayService {

    private static final Logger log = LoggerFactory.getLogger(NifiCanvasStatusOverlayService.class);
    private static final String ROOT_GROUP_ID = "root";

    private final NifiClient nifiClient;
    private final NifiCanvasStatusLabelRepository labelRepository;

    public NifiCanvasStatusOverlayService(
            NifiClient nifiClient,
            NifiCanvasStatusLabelRepository labelRepository) {
        this.nifiClient = nifiClient;
        this.labelRepository = labelRepository;
    }

    @Scheduled(fixedRateString = "${pipeline.nifi.canvas-status.interval-millis:60000}",
            initialDelayString = "${pipeline.nifi.canvas-status.initial-delay-millis:90000}")
    @Transactional
    public void syncCanvasStatusLabels() {
        try {
            syncGroup(ROOT_GROUP_ID);
        } catch (RuntimeException ex) {
            log.warn("NiFi 캔버스 상태 라벨 동기화 실패(다음 주기에 재시도): {}", ex.getMessage());
        }
    }

    private void syncGroup(String parentGroupId) {
        NifiFlowResponse parentFlow = nifiClient.getFlow(parentGroupId);
        var flow = flowOf(parentFlow);
        if (flow == null || flow.processGroups() == null || flow.processGroups().isEmpty()) {
            return;
        }

        Map<String, LabelEntity> labelsById = labelsById(flow.labels());
        for (ProcessGroupEntity child : flow.processGroups()) {
            var component = child.component();
            String childGroupId = component != null && component.id() != null ? component.id() : child.id();
            if (childGroupId == null) {
                continue;
            }

            NifiFlowResponse childFlow = nifiClient.getFlow(childGroupId);
            var childContents = flowOf(childFlow);
            List<ProcessorEntity> directProcessors =
                    childContents == null || childContents.processors() == null
                            ? List.of() : childContents.processors();
            if (!directProcessors.isEmpty()) {
                deleteExistingLabel(childGroupId, labelsById);
            }

            syncGroup(childGroupId);
        }
    }

    private void deleteExistingLabel(String groupId, Map<String, LabelEntity> labelsById) {
        Optional<NifiCanvasStatusLabel> mapping = labelRepository.findById(groupId);
        LabelEntity existingLabel = mapping.map(NifiCanvasStatusLabel::getLabelId).map(labelsById::get).orElse(null);
        if (existingLabel == null) {
            mapping.ifPresent(labelRepository::delete);
            return;
        }
        long version = existingLabel.revision() == null || existingLabel.revision().version() == null
                ? 0L : existingLabel.revision().version();
        try {
            nifiClient.deleteLabel(existingLabel.id(), version);
            mapping.ifPresent(labelRepository::delete);
        } catch (RuntimeException ex) {
            log.warn("NiFi 캔버스 상태 라벨 삭제 실패 groupId={} labelId={} (다음 주기에 재시도): {}",
                    groupId, existingLabel.id(), ex.getMessage());
        }
    }

    private Map<String, LabelEntity> labelsById(List<LabelEntity> labels) {
        Map<String, LabelEntity> result = new HashMap<>();
        if (labels != null) {
            labels.forEach(label -> result.put(label.id(), label));
        }
        return result;
    }

    private NifiFlowResponse.Flow flowOf(NifiFlowResponse response) {
        return response == null || response.processGroupFlow() == null ? null : response.processGroupFlow().flow();
    }
}
