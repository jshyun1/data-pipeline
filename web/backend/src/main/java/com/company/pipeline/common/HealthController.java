package com.company.pipeline.common;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 컨테이너 헬스체크 전용 경량 엔드포인트 (U3). 인증 없이(permitAll) 200 을 반환한다.
 *
 * <p>기존엔 업무 API(/api/connections)를 healthcheck 로 썼는데, 인가가 활성화되면 그 순간
 * 컨테이너가 영구 unhealthy 가 된다. 헬스는 항상 열려 있어야 하므로 별도 경로로 분리한다.
 */
@RestController
@RequestMapping("/api/health")
public class HealthController {

    @GetMapping
    public Map<String, String> health() {
        return Map.of("status", "UP");
    }
}
