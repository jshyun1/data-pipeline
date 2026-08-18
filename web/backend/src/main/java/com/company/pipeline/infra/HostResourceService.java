package com.company.pipeline.infra;

import com.company.pipeline.infra.dto.HostResourceResponse;
import com.company.pipeline.infra.dto.HostResourceResponse.CpuUsage;
import com.company.pipeline.infra.dto.HostResourceResponse.DiskUsage;
import com.company.pipeline.infra.dto.HostResourceResponse.MemoryUsage;
import com.company.pipeline.infra.dto.HostResourceResponse.SystemInfo;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

/**
 * 대시보드 인프라 구역의 서버 리소스(CPU/메모리/디스크) 조회.
 *
 * <p>CPU 사용률은 순간값을 알 수 없고 누적 tick의 차이로만 구할 수 있어서 직전 관측을
 * 기억해 두고 그 사이 구간의 평균을 낸다. 그래서 대시보드가 처음 뜬 직후 한 번은
 * 기준점을 만들기 위해 아주 짧게(200ms) 두 번 읽는다.
 */
@Service
@EnableConfigurationProperties(InfraProperties.class)
public class HostResourceService {

    private static final Logger log = LoggerFactory.getLogger(HostResourceService.class);

    /** 이보다 짧은 간격의 두 샘플은 tick 차이가 너무 작아 값이 튀므로 직전 계산값을 재사용한다. */
    private static final long MIN_SAMPLE_GAP_MILLIS = 500L;
    private static final long BOOTSTRAP_SAMPLE_GAP_MILLIS = 200L;
    private static final long KIB = 1024L;

    private final Path procPath;
    private final List<String> diskPaths;

    private CpuTicks previousTicks;
    private long previousTicksAt;
    private double lastCpuUsedPercent;

    public HostResourceService(InfraProperties properties) {
        this.procPath = Path.of(properties.procPath());
        this.diskPaths = properties.diskPaths();
    }

    public HostResourceResponse collect() {
        return new HostResourceResponse(LocalDateTime.now(), readCpu(), readMemory(), readDisks(), readSystem());
    }

    private synchronized CpuUsage readCpu() {
        List<String> statLines = readLines("stat");
        CpuTicks ticks = statLines == null ? null : parseCpuTicks(statLines);
        if (ticks == null) {
            return null;
        }

        long now = System.currentTimeMillis();
        if (previousTicks == null) {
            sleepQuietly(BOOTSTRAP_SAMPLE_GAP_MILLIS);
            List<String> secondLines = readLines("stat");
            CpuTicks second = secondLines == null ? null : parseCpuTicks(secondLines);
            if (second != null) {
                lastCpuUsedPercent = usedPercent(ticks, second);
                ticks = second;
                now = System.currentTimeMillis();
            }
        } else if (now - previousTicksAt >= MIN_SAMPLE_GAP_MILLIS) {
            lastCpuUsedPercent = usedPercent(previousTicks, ticks);
        }
        previousTicks = ticks;
        previousTicksAt = now;
        return new CpuUsage(round1(lastCpuUsedPercent), ticks.cores());
    }

    private MemoryUsage readMemory() {
        List<String> lines = readLines("meminfo");
        return lines == null ? null : parseMemory(lines);
    }

    private SystemInfo readSystem() {
        List<String> uptimeLines = readLines("uptime");
        List<String> loadLines = readLines("loadavg");
        if (uptimeLines == null || uptimeLines.isEmpty() || loadLines == null || loadLines.isEmpty()) {
            return null;
        }
        return parseSystem(uptimeLines.get(0), loadLines.get(0));
    }

    private List<DiskUsage> readDisks() {
        List<DiskUsage> disks = new ArrayList<>();
        for (String mount : diskPaths) {
            try {
                FileStore store = Files.getFileStore(Path.of(mount));
                long total = store.getTotalSpace();
                long available = store.getUsableSpace();
                long used = Math.max(0L, total - available);
                disks.add(new DiskUsage(mount, total, used, available, round1(percent(used, total))));
            } catch (IOException | RuntimeException ex) {
                log.debug("디스크 사용량 조회 실패 - {}: {}", mount, ex.getMessage());
            }
        }
        return disks;
    }

    /**
     * 메모리 "용도별" 분해 — 디스크 용도별(원본 5-3 #12)과 같은 질문에 답한다:
     * 82%가 무엇으로 차 있는가.
     *
     * <p>사용률 숫자 하나만으로는 조치를 정할 수 없다. 버퍼·캐시가 대부분이면 커널이 알아서
     * 회수하므로 기다리면 되고, 프로세스가 실제로 쥐고 있으면 뭔가를 내려야 한다. 이 둘은
     * 같은 82%라도 완전히 다른 상황이다.
     *
     * <p>스왑을 함께 내리는 이유: 이 환경은 메모리 압박으로 호스트가 두 번 다운된 이력이 있고,
     * 그때 선행 신호가 스왑 증가였다.
     */
    public List<Map<String, Object>> memoryBreakdown() {
        List<String> lines = readLines("meminfo");
        if (lines == null) {
            return List.of();
        }
        Map<String, Long> kb = new java.util.HashMap<>();
        for (String line : lines) {
            int colon = line.indexOf(':');
            if (colon <= 0) {
                continue;
            }
            String key = line.substring(0, colon).trim();
            String rest = line.substring(colon + 1).trim().replace(" kB", "");
            try {
                kb.put(key, Long.parseLong(rest.trim()));
            } catch (NumberFormatException ignored) {
                // 숫자가 아닌 항목(HugePages 단위 없는 줄 등)은 건너뛴다.
            }
        }
        Long total = kb.get("MemTotal");
        if (total == null || total <= 0) {
            return List.of();
        }
        long available = kb.getOrDefault("MemAvailable", 0L);
        long free = kb.getOrDefault("MemFree", 0L);
        long buffers = kb.getOrDefault("Buffers", 0L);
        long cached = kb.getOrDefault("Cached", 0L);
        long sreclaim = kb.getOrDefault("SReclaimable", 0L);
        long shmem = kb.getOrDefault("Shmem", 0L);
        // 커널이 회수할 수 있는 몫. Shmem 은 캐시로 잡히지만 회수되지 않아 뺀다.
        long reclaimable = Math.max(0, buffers + cached + sreclaim - shmem);
        // "실제로 쥐고 있는" 몫 = 전체 - 회수 가능한 여유(MemAvailable).
        long inUse = Math.max(0, total - available);

        List<Map<String, Object>> out = new java.util.ArrayList<>();
        out.add(entry("프로세스 사용", inUse * 1024, "회수되지 않는 실사용분 (전체 - 사용 가능)"));
        out.add(entry("버퍼·캐시", reclaimable * 1024, "압박 시 커널이 회수한다 — 즉시 위험은 아니다"));
        out.add(entry("여유", free * 1024, "아직 아무도 쓰지 않는 몫"));

        long swapTotal = kb.getOrDefault("SwapTotal", 0L);
        if (swapTotal > 0) {
            long swapUsed = Math.max(0, swapTotal - kb.getOrDefault("SwapFree", 0L));
            out.add(entry("스왑 사용", swapUsed * 1024,
                    String.format("스왑 전체 %.1fGB 중 — 증가 중이면 메모리 압박 신호",
                            swapTotal / 1024.0 / 1024.0)));
        }
        return out;
    }

    private static Map<String, Object> entry(String label, long bytes, String note) {
        Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("label", label);
        m.put("usedBytes", bytes);
        m.put("note", note);
        return m;
    }

    private List<String> readLines(String fileName) {
        Path file = procPath.resolve(fileName);
        try {
            return Files.readAllLines(file);
        } catch (IOException | RuntimeException ex) {
            log.debug("{} 조회 실패: {}", file, ex.getMessage());
            return null;
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * /proc/stat의 집계 라인("cpu ...")에서 전체/유휴 tick을, "cpuN ..." 라인 개수에서
     * 코어 수를 얻는다. iowait은 유휴로 본다(디스크를 기다리는 동안은 CPU가 놀고 있다).
     */
    static CpuTicks parseCpuTicks(List<String> statLines) {
        long total = 0L;
        long idle = 0L;
        int cores = 0;
        boolean aggregateFound = false;
        for (String line : statLines) {
            if (!line.startsWith("cpu")) {
                continue;
            }
            String[] parts = line.trim().split("\\s+");
            if ("cpu".equals(parts[0])) {
                for (int index = 1; index < parts.length; index++) {
                    long value = parseLong(parts[index]);
                    total += value;
                    if (index == 4 || index == 5) {
                        idle += value;
                    }
                }
                aggregateFound = true;
            } else {
                cores++;
            }
        }
        return aggregateFound && total > 0 ? new CpuTicks(total, idle, cores) : null;
    }

    static double usedPercent(CpuTicks previous, CpuTicks current) {
        long totalDelta = current.total() - previous.total();
        long idleDelta = current.idle() - previous.idle();
        if (totalDelta <= 0L) {
            return 0.0;
        }
        double used = (double) (totalDelta - idleDelta) / totalDelta * 100.0;
        return Math.min(100.0, Math.max(0.0, used));
    }

    /**
     * MemAvailable은 "지금 당장 새 프로세스에 줄 수 있는 양"이라 캐시/버퍼를 이미 감안한
     * 값이다. MemFree로 계산하면 캐시까지 사용중으로 잡혀 상시 90%대로 보인다.
     */
    static MemoryUsage parseMemory(List<String> meminfoLines) {
        long totalKib = 0L;
        long availableKib = 0L;
        for (String line : meminfoLines) {
            if (line.startsWith("MemTotal:")) {
                totalKib = parseFirstNumber(line);
            } else if (line.startsWith("MemAvailable:")) {
                availableKib = parseFirstNumber(line);
            }
        }
        if (totalKib <= 0L) {
            return null;
        }
        long total = totalKib * KIB;
        long available = availableKib * KIB;
        long used = Math.max(0L, total - available);
        return new MemoryUsage(total, used, available, round1(percent(used, total)));
    }

    /** loadavg 형식: "0.78 1.04 0.98 1/1387 13494" (실행중/전체 프로세스, 마지막 pid). */
    static SystemInfo parseSystem(String uptimeLine, String loadavgLine) {
        String[] uptimeParts = uptimeLine.trim().split("\\s+");
        long uptimeSeconds = (long) parseDouble(uptimeParts[0]);

        String[] loadParts = loadavgLine.trim().split("\\s+");
        double load1 = loadParts.length > 0 ? parseDouble(loadParts[0]) : 0.0;
        double load5 = loadParts.length > 1 ? parseDouble(loadParts[1]) : 0.0;
        double load15 = loadParts.length > 2 ? parseDouble(loadParts[2]) : 0.0;
        int running = 0;
        int totalProcesses = 0;
        if (loadParts.length > 3) {
            String[] processParts = loadParts[3].split("/");
            running = (int) parseLong(processParts[0]);
            totalProcesses = processParts.length > 1 ? (int) parseLong(processParts[1]) : 0;
        }
        return new SystemInfo(uptimeSeconds, load1, load5, load15, running, totalProcesses);
    }

    private static double percent(long part, long total) {
        return total <= 0L ? 0.0 : (double) part / total * 100.0;
    }

    private static double round1(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static long parseFirstNumber(String line) {
        String[] parts = line.trim().split("\\s+");
        return parts.length > 1 ? parseLong(parts[1]) : 0L;
    }

    private static long parseLong(String value) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException ex) {
            return 0L;
        }
    }

    private static double parseDouble(String value) {
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException ex) {
            return 0.0;
        }
    }

    record CpuTicks(long total, long idle, int cores) {
    }
}
