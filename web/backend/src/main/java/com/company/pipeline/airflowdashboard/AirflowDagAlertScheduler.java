package com.company.pipeline.airflowdashboard;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class AirflowDagAlertScheduler {

    private static final Logger log = LoggerFactory.getLogger(AirflowDagAlertScheduler.class);

    private final AirflowDagCatalogRepository catalogRepository;
    private final AirflowDagRunClient runClient;
    private final AirflowDagAlertEvaluator evaluator;
    private final AirflowDagMonitoringService monitoringService;

    public AirflowDagAlertScheduler(
            AirflowDagCatalogRepository catalogRepository,
            AirflowDagRunClient runClient,
            AirflowDagAlertEvaluator evaluator,
            AirflowDagMonitoringService monitoringService) {
        this.catalogRepository = catalogRepository;
        this.runClient = runClient;
        this.evaluator = evaluator;
        this.monitoringService = monitoringService;
    }

    @Scheduled(
            fixedDelayString = "${airflow.alert.interval-millis:300000}",
            initialDelayString = "${airflow.alert.initial-delay-millis:60000}")
    public void evaluate() {
        OffsetDateTime now = OffsetDateTime.now();
        for (AirflowDagCatalog catalog : catalogRepository.findByEnabledTrueAndMonitoringEnabledTrue()) {
            try {
                var detections = evaluator.evaluate(catalog, runClient.getDagRuns(catalog.getDagId()), now);
                monitoringService.synchronize(catalog.getDagId(), detections, LocalDateTime.now());
            } catch (Exception ex) {
                log.warn("Airflow DAG 감지 실패, 다음 주기에 재시도(dagId={}): {}", catalog.getDagId(), ex.getMessage());
            }
        }
    }
}
