package com.company.pipeline.infra;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

/**
 * 대시보드 CPU 카드의 «상세» - 서버(호스트)에서 도는 프로세스별 사용률(2026-10-12 요청).
 *
 * <p>CPU 사용률은 순간값이 없다. {@code /proc/<pid>/stat} 의 누적 tick 을 짧은 간격으로 두 번 읽어
 * 그 사이 쓴 몫을 센다(top 과 같은 방식). 화면에서 «상세»를 눌렀을 때만 읽는다 - 주기적으로 전체를 훑지 않는다.
 *
 * <p><b>어느 /proc 을 읽나</b>: 컨테이너의 {@code /proc} 은 {@code stat · meminfo} 는 호스트 값을 주지만
 * 프로세스 목록은 그 컨테이너 것만 보인다. 그래서 compose 에서 호스트 {@code /proc} 을 읽기 전용으로 걸어 둔
 * {@code hostProcPath}(기본 {@code /host/proc})를 먼저 보고, 없으면 평소 {@code procPath} 로 물러난다.
 * 물러난 경우 {@code hostScope=false} 로 내려 화면이 «이 서버 전체가 아니라 백엔드 컨테이너 안»임을 알린다.
 */
@Service
@EnableConfigurationProperties(InfraProperties.class)
public class HostProcessService {

    private static final Logger log = LoggerFactory.getLogger(HostProcessService.class);
    /** 두 번 읽는 사이. 너무 짧으면 tick 차이가 0 이라 값이 튀고, 길면 화면이 기다린다. */
    private static final long SAMPLE_GAP_MILLIS = 300L;
    private static final long PAGE_SIZE = 4096L;
    /** 커널이 1초에 세는 tick. 리눅스 x86_64 기본값이다(sysconf 를 JVM 에서 못 읽는다). */
    private static final double TICKS_PER_SECOND = 100.0;
    private static final int MAX_LIMIT = 200;

    /**
     * @param pid        프로세스 번호
     * @param name       이름({@code /proc/<pid>/stat} 의 comm)
     * @param command    실행 명령(길면 자른다). 권한이 없으면 빈 문자열
     * @param state      R(실행) · S(대기) 같은 한 글자 상태
     * @param cpuPercent 두 샘플 사이의 CPU 사용률(코어 1개 = 100%)
     * @param memoryBytes 실제 메모리(RSS)
     * @param memoryPercent 전체 메모리 대비
     * @param threads    스레드 수
     */
    public record HostProcess(long pid, String name, String command, String state,
                              double cpuPercent, long memoryBytes, double memoryPercent, int threads) {
    }

    /**
     * @param hostScope 호스트 전체를 본 것인지(false 면 백엔드 컨테이너 안만 보인 것)
     * @param total     읽은 프로세스 수(자른 뒤 목록 길이와 다를 수 있다)
     */
    public record Snapshot(boolean hostScope, int total, int cores, long memoryTotalBytes, List<HostProcess> items) {
    }

    private final Path procPath;
    private final Path hostProcPath;

    public HostProcessService(InfraProperties properties) {
        this.procPath = Path.of(properties.procPath());
        this.hostProcPath = Path.of(properties.hostProcPath());
    }

    public Snapshot collect(int limit) {
        int size = Math.max(1, Math.min(limit, MAX_LIMIT));
        boolean hostScope = Files.isDirectory(hostProcPath.resolve("1"));
        Path root = hostScope ? hostProcPath : procPath;

        Map<Long, long[]> first = readCpuTicks(root);
        long firstAt = System.nanoTime();
        try {
            Thread.sleep(SAMPLE_GAP_MILLIS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        Map<Long, long[]> second = readCpuTicks(root);
        double elapsedSeconds = (System.nanoTime() - firstAt) / 1_000_000_000.0;

        long memoryTotal = readMemoryTotalBytes(root);
        List<HostProcess> items = new ArrayList<>();
        for (Map.Entry<Long, long[]> entry : second.entrySet()) {
            long pid = entry.getKey();
            long[] now = entry.getValue();
            long[] before = first.get(pid);
            // 두 번째에만 있는 프로세스(그 사이 새로 뜬 것)는 비교할 앞 값이 없어 0 으로 둔다.
            long usedTicks = before == null ? 0 : Math.max(0, now[0] - before[0]);
            double cpuPercent = elapsedSeconds <= 0 ? 0 : usedTicks / TICKS_PER_SECOND / elapsedSeconds * 100.0;
            long memoryBytes = now[1] * PAGE_SIZE;
            items.add(new HostProcess(
                    pid,
                    readName(root, pid),
                    readCommand(root, pid),
                    readState(root, pid),
                    round(cpuPercent),
                    memoryBytes,
                    memoryTotal <= 0 ? 0 : round(memoryBytes * 100.0 / memoryTotal),
                    (int) now[2]));
        }
        items.sort(Comparator.comparingDouble(HostProcess::cpuPercent).reversed()
                .thenComparing(Comparator.comparingLong(HostProcess::memoryBytes).reversed()));
        int total = items.size();
        return new Snapshot(hostScope, total, Runtime.getRuntime().availableProcessors(), memoryTotal,
                items.size() > size ? List.copyOf(items.subList(0, size)) : List.copyOf(items));
    }

    /** pid → [쓴 tick(사용자+커널), RSS 페이지 수, 스레드 수] */
    private Map<Long, long[]> readCpuTicks(Path root) {
        Map<Long, long[]> result = new HashMap<>();
        try (Stream<Path> pids = Files.list(root)) {
            pids.forEach(dir -> {
                String name = dir.getFileName().toString();
                if (!name.chars().allMatch(Character::isDigit)) {
                    return;
                }
                long[] values = parseStat(dir.resolve("stat"));
                if (values != null) {
                    result.put(Long.parseLong(name), values);
                }
            });
        } catch (IOException ex) {
            log.debug("프로세스 목록을 읽지 못했습니다 root={} - {}", root, ex.getMessage());
        }
        return result;
    }

    /**
     * {@code /proc/<pid>/stat} 한 줄에서 필요한 값만 꺼낸다.
     * 이름(2번째 칸)에 공백·괄호가 들어갈 수 있어 마지막 «)» 뒤부터 센다.
     */
    private long[] parseStat(Path statFile) {
        String line;
        try {
            line = Files.readString(statFile).trim();
        } catch (IOException | RuntimeException ex) {
            // 읽는 사이에 끝난 프로세스 · 권한 없는 프로세스. 조용히 건너뛴다.
            return null;
        }
        int close = line.lastIndexOf(')');
        if (close < 0 || close + 2 >= line.length()) {
            return null;
        }
        String[] rest = line.substring(close + 2).split("\\s+");
        // rest[0] 이 stat 의 3번째 칸(state)이다 - utime 14 · stime 15 · threads 20 · rss 24.
        if (rest.length < 22) {
            return null;
        }
        try {
            long utime = Long.parseLong(rest[11]);
            long stime = Long.parseLong(rest[12]);
            long threads = Long.parseLong(rest[17]);
            long rssPages = Long.parseLong(rest[21]);
            return new long[] { utime + stime, rssPages, threads };
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private String readName(Path root, long pid) {
        try {
            String line = Files.readString(root.resolve(String.valueOf(pid)).resolve("stat"));
            int open = line.indexOf('(');
            int close = line.lastIndexOf(')');
            return open >= 0 && close > open ? line.substring(open + 1, close) : "-";
        } catch (IOException | RuntimeException ex) {
            return "-";
        }
    }

    private String readState(Path root, long pid) {
        try {
            String line = Files.readString(root.resolve(String.valueOf(pid)).resolve("stat"));
            int close = line.lastIndexOf(')');
            return close > 0 && close + 2 < line.length() ? line.substring(close + 2, close + 3) : "-";
        } catch (IOException | RuntimeException ex) {
            return "-";
        }
    }

    /** 실행 명령. 인자는 \0 으로 구분돼 있어 공백으로 바꾸고, 길면 자른다. */
    private String readCommand(Path root, long pid) {
        try {
            byte[] raw = Files.readAllBytes(root.resolve(String.valueOf(pid)).resolve("cmdline"));
            String text = new String(raw).replace('\0', ' ').trim();
            return text.length() > 200 ? text.substring(0, 200) + "…" : text;
        } catch (IOException | RuntimeException ex) {
            return "";
        }
    }

    private long readMemoryTotalBytes(Path root) {
        try {
            for (String line : Files.readAllLines(root.resolve("meminfo"))) {
                if (line.startsWith("MemTotal:")) {
                    String[] parts = line.split("\\s+");
                    return Long.parseLong(parts[1]) * 1024L;
                }
            }
        } catch (IOException | RuntimeException ex) {
            log.debug("MemTotal 을 읽지 못했습니다 - {}", ex.getMessage());
        }
        return 0L;
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }
}
