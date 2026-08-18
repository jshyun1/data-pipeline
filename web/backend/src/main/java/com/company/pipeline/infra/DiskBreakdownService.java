package com.company.pipeline.infra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * 디스크 용도별 사용량 — 원본 문서 5-3 "#12 디스크 용도별 분리".
 *
 * <p>루트 파일시스템 단일 값(74.2GB / 1007GB)만으로는 "무엇이 채우고 있는지"를 알 수 없다.
 * Kafka 로그와 NiFi 리포지터리를 분리해서 보여준다.
 *
 * <p>도커 named volume 은 전부 같은 파일시스템에 올라가므로 df 로는 구분되지 않는다.
 * 그래서 디렉터리 크기를 직접 걷는다(du 등가). 비싸므로 5분 주기로 백그라운드에서만 계산하고
 * 화면 요청은 캐시된 값만 읽는다.
 *
 * <p>크기는 반드시 <b>블록 기준</b>(du)으로 잰다. 파일 길이를 더하면 Kafka 인덱스처럼 사전할당된
 * 희소 파일에서 실제 점유량의 수십 배가 나온다(실측: 논리 1,795MB / 실제 58MB). 디스크가 얼마나
 * 찼는지 보려는 화면에서 그건 틀린 값이다. du 를 쓸 수 없는 환경에서만 파일 길이 합으로 후퇴하고,
 * 그 사실을 measuredBy 로 화면에 알린다.
 *
 * <p>관측 대상은 compose 에서 읽기전용으로 개별 바인드한 경로만이다.
 * {@code /var/lib/docker} 나 docker.sock 은 루트 권한 등가라서 마운트하지 않는다 —
 * 그래서 "컨테이너·이미지" 항목은 제공하지 않는다.
 */
@Service
public class DiskBreakdownService {

    private static final Logger log = LoggerFactory.getLogger(DiskBreakdownService.class);

    /** 한 경로를 걷는 동안 방문할 파일 수 상한. 넘으면 하한값(isLowerBound)으로 표시한다. */
    private static final long MAX_VISITS = 400_000L;

    private final InfraProperties properties;
    private final AtomicReference<Snapshot> cache = new AtomicReference<>(Snapshot.empty());

    public DiskBreakdownService(InfraProperties properties) {
        this.properties = properties;
    }

    /** measuredBy: BLOCKS(du, 정확) / APPARENT(파일 길이 합, 희소 파일에서 과대) / NONE. */
    public record Entry(String label, String path, long usedBytes, boolean isLowerBound,
                        String measuredBy, String error) {}

    public record Snapshot(String collectedAt, List<Entry> entries) {
        static Snapshot empty() {
            return new Snapshot(null, List.of());
        }
    }

    public Snapshot current() {
        return cache.get();
    }

    /**
     * 기동 직후 한 번(20초 뒤), 이후 5분 주기. 화면이 처음 열릴 때 빈 값을 보지 않게 초기 지연을 짧게 둔다.
     */
    @Scheduled(fixedRate = 300_000, initialDelay = 20_000)
    public void refresh() {
        Map<String, String> targets = properties.volumePathMap();
        if (targets.isEmpty()) {
            return;
        }
        List<Entry> entries = new ArrayList<>();
        for (Map.Entry<String, String> t : targets.entrySet()) {
            entries.add(measure(t.getKey(), t.getValue()));
        }
        cache.set(new Snapshot(Instant.now().toString(), entries));
    }

    private Entry measure(String label, String rawPath) {
        Path root = Path.of(rawPath);
        if (!Files.isDirectory(root)) {
            // 마운트를 안 걸어둔 환경(로컬 실행 등)에서는 조용히 "관측 불가"로 남긴다.
            return new Entry(label, rawPath, 0L, false, "NONE", "NOT_MOUNTED");
        }
        Long blocks = duBytes(root);
        if (blocks != null) {
            return new Entry(label, rawPath, blocks, false, "BLOCKS", null);
        }
        return walkApparent(label, rawPath, root);
    }

    /**
     * {@code du -s -B1} 로 블록 기준 점유량을 읽는다. 실패하면 null 을 돌려 호출부가 후퇴하게 한다.
     * 걷는 비용이 크므로 30초 상한을 둔다 — 5분 주기이니 한 번 놓쳐도 다음 주기에 다시 잰다.
     */
    private Long duBytes(Path root) {
        Process p = null;
        try {
            p = new ProcessBuilder("du", "-s", "-B1", root.toString())
                    .redirectErrorStream(false)
                    .start();
            String first;
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                first = r.readLine();
            }
            if (!p.waitFor(30, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return null;
            }
            if (first == null) {
                return null;
            }
            // "1234567\t/observed/kafka" 형태. 권한 없는 하위가 있어도 exit code 1 로 부분합이 나온다.
            String head = first.split("\\s+")[0];
            return Long.parseLong(head.trim());
        } catch (Exception e) {
            log.debug("du 실패 path={} - 파일 길이 합으로 후퇴한다", root, e);
            return null;
        } finally {
            if (p != null && p.isAlive()) {
                p.destroyForcibly();
            }
        }
    }

    /** du 를 못 쓸 때만 쓰는 후퇴 경로. 희소 파일에서 과대 집계되므로 APPARENT 로 표시한다. */
    private Entry walkApparent(String label, String rawPath, Path root) {
        Counter counter = new Counter();
        try {
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    if (attrs.isRegularFile()) {
                        counter.bytes += attrs.size();
                    }
                    return ++counter.visits >= MAX_VISITS ? FileVisitResult.TERMINATE : FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    // 읽기 권한이 없는 항목 하나 때문에 전체 집계를 버리지 않는다.
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            log.debug("디스크 용도별 집계 실패 label={} path={}", label, rawPath, e);
            return new Entry(label, rawPath, 0L, false, "NONE", "READ_FAILED");
        }
        return new Entry(label, rawPath, counter.bytes, counter.visits >= MAX_VISITS, "APPARENT", null);
    }

    private static final class Counter {
        private long bytes;
        private long visits;
    }

    /** 라벨 → 경로. compose 에서 읽기전용으로 바인드한 것만 온다. */
    static Map<String, String> parse(List<String> raw) {
        Map<String, String> out = new LinkedHashMap<>();
        if (raw == null) {
            return out;
        }
        for (String item : raw) {
            if (item == null) {
                continue;
            }
            int sep = item.indexOf('=');
            if (sep <= 0 || sep == item.length() - 1) {
                continue;
            }
            out.put(item.substring(0, sep).trim(), item.substring(sep + 1).trim());
        }
        return out;
    }
}
