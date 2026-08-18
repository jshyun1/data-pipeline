package com.company.pipeline.infra;

import java.util.List;
import java.util.Map;
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
 *
 * <p>volumePaths는 "용도별" 분해에 쓰는 {@code 라벨=경로} 목록이다(원본 문서 5-3 #12).
 * named volume 은 전부 같은 파일시스템이라 df 로 안 갈라지므로 디렉터리 크기를 직접 걷는다.
 * compose 에서 읽기전용으로 개별 바인드한 경로만 넣는다.
 */
@ConfigurationProperties(prefix = "infra")
public record InfraProperties(String procPath, List<String> diskPaths, List<String> volumePaths) {

    public InfraProperties {
        procPath = StringUtils.hasText(procPath) ? procPath : "/proc";
        diskPaths = diskPaths == null || diskPaths.isEmpty() ? List.of("/") : List.copyOf(diskPaths);
        volumePaths = volumePaths == null ? List.of() : List.copyOf(volumePaths);
    }

    public Map<String, String> volumePathMap() {
        return DiskBreakdownService.parse(volumePaths);
    }
}
