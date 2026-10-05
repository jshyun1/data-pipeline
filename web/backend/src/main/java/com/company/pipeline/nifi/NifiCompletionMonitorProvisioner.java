package com.company.pipeline.nifi;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * NiFi 에 «job 완료 감시»(Cerebro NAR 의 JobCompletionReportingTask)가 등록 · 실행돼 있게 한다(2026-10-05).
 *
 * <p>NAR 은 NiFi 이미지에 들어가지만, Reporting Task 는 누군가 «추가 → 속성 입력 → 시작»을 해야 돈다. 환경마다(로컬 · MSA)
 * 손으로 하지 않게 백엔드가 주기적으로 확인해 없으면 만들어 시작한다. 서비스 토큰도 백엔드 설정값을 그대로 넣는다.
 *
 * <ul>
 *   <li>NAR 이 없는 NiFi(이미지를 아직 안 바꾼 환경)면 아무것도 하지 않는다 - 그 경우 워크플로우는 예전처럼 추측 판정으로 돈다.</li>
 *   <li>이미 있으면 건드리지 않는다. 관리자가 일부러 멈춰 둔 것을 다시 켜지 않는다.</li>
 * </ul>
 */
@Component
public class NifiCompletionMonitorProvisioner {

    private static final Logger log = LoggerFactory.getLogger(NifiCompletionMonitorProvisioner.class);
    static final String TASK_TYPE = "com.company.cerebro.nifi.JobCompletionReportingTask";
    static final String TASK_NAME = "Cerebro job 완료 감시";

    private final NifiClient nifiClient;

    @Value("${nifi.completion-monitor.enabled:true}")
    private boolean enabled;

    /** NiFi 컨테이너에서 닿는 백엔드 주소. */
    @Value("${nifi.completion-monitor.api-url:http://pipeline-api:8081}")
    private String apiUrl;

    @Value("${authz.service-token:}")
    private String serviceToken;

    public NifiCompletionMonitorProvisioner(NifiClient nifiClient) {
        this.nifiClient = nifiClient;
    }

    @Scheduled(initialDelay = 60_000L, fixedDelay = 300_000L)
    public void scheduledEnsure() {
        // 5분마다 목록 두 번 조회뿐이라 가볍다. 한 번 등록했어도 계속 본다 - NiFi 볼륨을 새로 만들면 등록이 사라진다.
        if (!enabled) {
            return;
        }
        try {
            ensure();
        } catch (RuntimeException ex) {
            log.warn("NiFi job 완료 감시 등록 확인 실패(다음 주기에 다시): {}", ex.getMessage());
        }
    }

    /** @return 이번에 새로 만들었으면 true */
    public boolean ensure() {
        for (JsonNode task : nifiClient.listReportingTasks().path("reportingTasks")) {
            if (TASK_TYPE.equals(task.path("component").path("type").asText())) {
                return false;
            }
        }
        JsonNode bundle = null;
        for (JsonNode type : nifiClient.reportingTaskTypes().path("reportingTaskTypes")) {
            if (TASK_TYPE.equals(type.path("type").asText())) {
                bundle = type.path("bundle");
                break;
            }
        }
        if (bundle == null) {
            log.debug("NiFi 에 Cerebro NAR(job 완료 감시)이 없어 등록하지 않습니다 - 이미지를 다시 빌드하면 붙습니다.");
            return false;
        }
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("Cerebro API URL", apiUrl);
        if (StringUtils.hasText(serviceToken)) {
            properties.put("Service Token", serviceToken);
        }
        JsonNode created = nifiClient.createReportingTask(TASK_TYPE,
                Map.of("group", bundle.path("group").asText(), "artifact", bundle.path("artifact").asText(),
                        "version", bundle.path("version").asText()),
                TASK_NAME, properties, "2 sec");
        nifiClient.startReportingTask(created.path("id").asText(), created.path("revision").path("version").asLong(0));
        log.info("NiFi job 완료 감시를 등록하고 시작했습니다 (id={})", created.path("id").asText());
        return true;
    }
}
