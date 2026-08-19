package com.company.pipeline.infra;

import static org.assertj.core.api.Assertions.assertThat;

import com.company.pipeline.infra.HostResourceService.CpuTicks;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HostResourceServiceTest {

    private static final List<String> STAT_LINES = List.of(
            "cpu  100 0 50 800 50 0 0 0 0 0",
            "cpu0 50 0 25 400 25 0 0 0 0 0",
            "cpu1 50 0 25 400 25 0 0 0 0 0",
            "intr 12345",
            "ctxt 6789");

    @Test
    void parseCpuTicks_readsAggregateLineAndCountsCores() {
        CpuTicks ticks = HostResourceService.parseCpuTicks(STAT_LINES);

        assertThat(ticks).isNotNull();
        assertThat(ticks.total()).isEqualTo(1000L);
        // idle(800) + iowait(50): 디스크 대기는 CPU가 놀고 있는 것으로 본다.
        assertThat(ticks.idle()).isEqualTo(850L);
        assertThat(ticks.cores()).isEqualTo(2);
    }

    @Test
    void parseCpuTicks_withoutAggregateLine_returnsNull() {
        assertThat(HostResourceService.parseCpuTicks(List.of("intr 1", "ctxt 2"))).isNull();
    }

    @Test
    void usedPercent_usesTickDeltaBetweenSamples() {
        CpuTicks previous = new CpuTicks(1000L, 850L, 2);
        CpuTicks current = new CpuTicks(1200L, 900L, 2);

        // 구간 tick 200 중 유휴 50 -> 75% 사용.
        assertThat(HostResourceService.usedPercent(previous, current)).isEqualTo(75.0);
    }

    @Test
    void usedPercent_counterReset_doesNotReturnNegative() {
        CpuTicks previous = new CpuTicks(1000L, 850L, 2);
        CpuTicks current = new CpuTicks(500L, 400L, 2);

        assertThat(HostResourceService.usedPercent(previous, current)).isEqualTo(0.0);
    }

    @Test
    void parseMemory_usesMemAvailableSoPageCacheIsNotCountedAsUsed() {
        var memory = HostResourceService.parseMemory(List.of(
                "MemTotal:       11215500 kB",
                "MemFree:          192284 kB",
                "MemAvailable:    2455176 kB",
                "Buffers:            5000 kB"));

        assertThat(memory).isNotNull();
        assertThat(memory.totalBytes()).isEqualTo(11_215_500L * 1024);
        assertThat(memory.availableBytes()).isEqualTo(2_455_176L * 1024);
        assertThat(memory.usedBytes()).isEqualTo((11_215_500L - 2_455_176L) * 1024);
        assertThat(memory.usedPercent()).isEqualTo(78.1);
    }

    @Test
    void parseMemory_withoutMemTotal_returnsNull() {
        assertThat(HostResourceService.parseMemory(List.of("MemFree: 100 kB"))).isNull();
    }

    @Test
    void parseSystem_readsLoadAverageAndProcessCounts() {
        var system = HostResourceService.parseSystem("123456.78 987654.32", "0.78 1.04 0.98 3/1387 13494");

        assertThat(system.uptimeSeconds()).isEqualTo(123_456L);
        assertThat(system.load1()).isEqualTo(0.78);
        assertThat(system.load5()).isEqualTo(1.04);
        assertThat(system.load15()).isEqualTo(0.98);
        assertThat(system.runningProcesses()).isEqualTo(3);
        assertThat(system.totalProcesses()).isEqualTo(1387);
    }

    @Test
    void collect_readsFromConfiguredProcPath(@TempDir Path procDir) throws IOException {
        Files.write(procDir.resolve("stat"), STAT_LINES);
        Files.write(procDir.resolve("meminfo"), List.of("MemTotal: 2000 kB", "MemAvailable: 500 kB"));
        Files.write(procDir.resolve("uptime"), List.of("3600.00 7200.00"));
        Files.write(procDir.resolve("loadavg"), List.of("0.10 0.20 0.30 1/100 200"));
        var service = new HostResourceService(new InfraProperties(procDir.toString(), List.of("/"), List.of()));

        var response = service.collect();

        assertThat(response.cpu()).isNotNull();
        assertThat(response.cpu().cores()).isEqualTo(2);
        assertThat(response.memory().usedPercent()).isEqualTo(75.0);
        assertThat(response.system().uptimeSeconds()).isEqualTo(3600L);
        assertThat(response.disks()).isNotEmpty();
        assertThat(response.disks().getFirst().totalBytes()).isPositive();
    }

    @Test
    void collect_withoutProcfs_returnsNullSectionsInsteadOfFailing(@TempDir Path emptyDir) {
        var service = new HostResourceService(new InfraProperties(emptyDir.toString(), List.of("/"), List.of()));

        var response = service.collect();

        assertThat(response.cpu()).isNull();
        assertThat(response.memory()).isNull();
        assertThat(response.system()).isNull();
        assertThat(response.collectedAt()).isNotNull();
    }

    @Test
    void properties_fillDefaultsWhenUnset() {
        var properties = new InfraProperties(null, null, null);

        assertThat(properties.procPath()).isEqualTo("/proc");
        assertThat(properties.diskPaths()).containsExactly("/");
    }
}
