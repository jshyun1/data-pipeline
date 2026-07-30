package com.company.pipeline.infra.dto;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 서버(호스트) 리소스 현황. 읽을 수 없는 항목은 null로 내려서 화면이 "측정 불가"로
 * 표시할 수 있게 한다(예: procfs가 없는 개발 환경).
 */
public record HostResourceResponse(
        LocalDateTime collectedAt,
        CpuUsage cpu,
        MemoryUsage memory,
        List<DiskUsage> disks,
        SystemInfo system
) {

    /** usedPercent는 직전 관측과의 tick 차이로 계산한 구간 평균 사용률. */
    public record CpuUsage(double usedPercent, int cores) {
    }

    public record MemoryUsage(long totalBytes, long usedBytes, long availableBytes, double usedPercent) {
    }

    public record DiskUsage(String mount, long totalBytes, long usedBytes, long availableBytes, double usedPercent) {
    }

    public record SystemInfo(
            long uptimeSeconds, double load1, double load5, double load15,
            int runningProcesses, int totalProcesses) {
    }
}
