package com.company.pipeline.infra;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

/**
 * 인프라 구역(서버 리소스)에서 볼 대상.
 *
 * <p>procPath는 CPU/메모리/부하를 읽는 procfs 경로다. 컨테이너 안에서도 /proc/stat과
 * /proc/meminfo는 네임스페이스로 가려지지 않고 호스트 값을 그대로 보여주므로, 이 앱이
 * 컨테이너로 떠 있어도 화면에 나오는 값은 "서버(호스트) 기준"이다.
 *
 * <p>diskPaths는 사용률을 볼 마운트 경로다. 기본값 "/"는 컨테이너 루트(= docker 데이터가
 * 올라간 호스트 파일시스템)라서, 이 파이프라인이 쌓는 데이터가 실제로 채우는 디스크를 본다.
 */
@ConfigurationProperties(prefix = "infra")
public record InfraProperties(String procPath, List<String> diskPaths) {

    public InfraProperties {
        procPath = StringUtils.hasText(procPath) ? procPath : "/proc";
        diskPaths = diskPaths == null || diskPaths.isEmpty() ? List.of("/") : List.copyOf(diskPaths);
    }
}
