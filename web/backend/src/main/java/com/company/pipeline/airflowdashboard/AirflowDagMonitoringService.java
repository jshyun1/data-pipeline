package com.company.pipeline.airflowdashboard;

import com.company.pipeline.airflowdashboard.AirflowDagAlert.RuleType;
import com.company.pipeline.airflowdashboard.AirflowDagAlert.Status;
import com.company.pipeline.airflowdashboard.AirflowDagAlertEvaluator.Detection;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AirflowDagMonitoringService {

    private static final List<Status> ACTIVE = List.of(Status.OPEN, Status.ACKNOWLEDGED);

    private final AirflowDagCatalogRepository catalogRepository;
    private final AirflowDagAlertRepository alertRepository;

    public AirflowDagMonitoringService(
            AirflowDagCatalogRepository catalogRepository,
            AirflowDagAlertRepository alertRepository) {
        this.catalogRepository = catalogRepository;
        this.alertRepository = alertRepository;
    }

    public AirflowDagCatalog getCatalog(String dagId) {
        return catalogRepository.findByDagId(dagId)
                .orElseThrow(() -> new BusinessException(ErrorCode.PIPELINE_NOT_FOUND, "DAG를 찾을 수 없습니다: " + dagId));
    }

    @Transactional
    public AirflowDagCatalog updateSettings(
            String dagId,
            boolean enabled,
            int failureThreshold,
            int staleDays,
            BigDecimal durationMultiplier,
            Integer slaMinutes) {
        AirflowDagCatalog catalog = getCatalog(dagId);
        catalog.updateMonitoring(enabled, failureThreshold, staleDays, durationMultiplier, slaMinutes);
        if (!enabled) {
            LocalDateTime now = LocalDateTime.now();
            alertRepository.findByDagIdAndStatusIn(dagId, ACTIVE).forEach(alert -> alert.resolve(now));
        }
        return catalog;
    }

    public List<AirflowDagAlert> activeAlerts() {
        return alertRepository.findByStatusInOrderByLastDetectedAtDesc(ACTIVE);
    }

    public List<AirflowDagAlert> alertHistory() {
        return alertRepository.findAllByOrderByDetectedAtDesc();
    }

    @Transactional
    public AirflowDagAlert acknowledge(Long id) {
        AirflowDagAlert alert = alertRepository.findById(id)
                .orElseThrow(() -> new BusinessException(ErrorCode.PIPELINE_NOT_FOUND, "조치 항목을 찾을 수 없습니다: " + id));
        alert.acknowledge(LocalDateTime.now());
        return alert;
    }

    @Transactional
    public void synchronize(String dagId, Map<RuleType, Detection> detections, LocalDateTime now) {
        for (RuleType ruleType : RuleType.values()) {
            Detection detection = detections.get(ruleType);
            var active = alertRepository.findByDagIdAndRuleTypeAndStatusIn(dagId, ruleType, ACTIVE);
            if (detection != null) {
                AirflowDagAlert alert = active.orElseGet(() -> new AirflowDagAlert(
                        dagId, ruleType, detection.severity(), detection.message(), now));
                if (active.isPresent()) alert.detect(detection.severity(), detection.message(), now);
                alertRepository.save(alert);
            } else {
                active.ifPresent(alert -> alert.resolve(now));
            }
        }
    }
}
