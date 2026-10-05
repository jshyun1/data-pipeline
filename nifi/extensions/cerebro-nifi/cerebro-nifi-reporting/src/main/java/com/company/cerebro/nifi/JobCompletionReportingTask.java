package com.company.cerebro.nifi;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.ReportingContext;

/**
 * ETL job 완료 · 실패 감시(2026-10-05). NiFi 안에서 job 그룹의 상태를 직접 보고, 실행 한 번이 끝나면 Cerebro 백엔드의
 * 실행 원장에 «완료(적재 건수)» 또는 «실패(ERROR 알림)»를 알린다.
 *
 * <p>예전에는 워크플로우(Airflow)가 15초마다 NiFi 를 REST 로 조회하고 활동을 못 보면 180초를 기다리는 추측으로 완료를 판정했다.
 * 템플릿 끝에 알림 프로세서를 붙이는 방식도 있었지만, 이미 만든 job 에는 소급되지 않았다. 이 감시는 캔버스를 건드리지 않고 모든 job 에 적용된다.
 *
 * <p>무엇을 감시하나: 백엔드가 알려 주는 «job 그룹 목록»(GET /api/etl/job-runs/watched-groups)만. 판정 로직은 {@link CompletionDetector}.
 *
 * <p>안정성 원칙 - NiFi 와 같은 프로세스에서 돈다:
 * <ul>
 *   <li>판정({@link #onTrigger})은 메모리의 상태만 읽고 끝난다. 백엔드 호출은 별도 스레드 하나가 대기열에서 꺼내 보낸다 - 백엔드가 느려도 NiFi 타이머 스레드를 붙잡지 않는다.</li>
 *   <li>모든 호출에 짧은 시간 제한, 대기열 상한({@link #QUEUE_LIMIT}). 넘치면 오래된 것부터 버린다(원장은 백엔드 관측기 · Airflow 가 뒤에서 받친다).</li>
 *   <li>어떤 예외도 밖으로 던지지 않는다 - 경고만 남기고 다음 주기에 다시 본다.</li>
 * </ul>
 */
@Tags({"cerebro", "etl", "job", "completion", "monitor"})
@CapabilityDescription("Cerebro ETL job 그룹의 실행 완료 · 실패를 감지해 Cerebro 백엔드 실행 원장에 알린다.")
@DefaultSchedule(period = "2 sec")
public class JobCompletionReportingTask extends AbstractReportingTask {

    static final PropertyDescriptor API_URL = new PropertyDescriptor.Builder()
            .name("Cerebro API URL")
            .description("Cerebro 백엔드(pipeline-api) 주소. NiFi 컨테이너에서 닿는 주소여야 한다.")
            .required(true)
            .defaultValue("http://pipeline-api:8081")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    static final PropertyDescriptor SERVICE_TOKEN = new PropertyDescriptor.Builder()
            .name("Service Token")
            .description("백엔드 서비스 토큰(PIPELINE_SERVICE_TOKEN). X-Service-Token 헤더로 보낸다.")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor SETTLE_SECONDS = new PropertyDescriptor.Builder()
            .name("Settle Seconds")
            .description("활동을 본 뒤 이만큼 조용하면 실행이 끝난 것으로 본다.")
            .required(true)
            .defaultValue("3")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor REFRESH_SECONDS = new PropertyDescriptor.Builder()
            .name("Watched Groups Refresh Seconds")
            .description("감시할 job 그룹 목록을 백엔드에서 다시 받아 오는 간격.")
            .required(true)
            .defaultValue("60")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final int QUEUE_LIMIT = 1_000;
    /**
     * 재전송 횟수 · 간격: 2 · 4 · 8 · 16 · 32 · 60 · 60초 → 약 3분. 백엔드 재배포(기동 60~90초) 동안 끝난 실행도 놓치지 않게
     * 그보다 길게 잡는다(처음엔 5번 · 약 30초였다가 시험 중 재배포 시간보다 짧은 것을 보고 늘렸다).
     */
    private static final int MAX_ATTEMPTS = 8;
    private static final long MAX_BACKOFF_MILLIS = 60_000L;
    private static final String LOAD_COUNTER_SUFFIX = "updates performed";
    private static final Pattern UUID_IN_JSON = Pattern.compile("\"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\"");

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(2))
            .build();
    private final ExecutorService sender = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "cerebro-job-completion-sender");
        thread.setDaemon(true);
        return thread;
    });
    private final Deque<Outgoing> outbox = new ArrayDeque<>();
    private volatile boolean sending;

    private CompletionDetector detector;
    private long detectorSettleMillis;
    private volatile Set<String> watched = Set.of();
    private volatile boolean watchedLoaded;
    private long watchedFetchedAt;

    private record Outgoing(String path, String body, int attempt, long notBefore) {
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(API_URL, SERVICE_TOKEN, SETTLE_SECONDS, REFRESH_SECONDS);
    }

    @OnStopped
    public void onStopped() {
        synchronized (outbox) {
            outbox.clear();
        }
        detector = null;
        watchedLoaded = false;
        watchedFetchedAt = 0;
    }

    @Override
    public void onTrigger(ReportingContext context) {
        try {
            String api = trimSlash(context.getProperty(API_URL).getValue());
            String token = context.getProperty(SERVICE_TOKEN).getValue();
            long settle = context.getProperty(SETTLE_SECONDS).asLong() * 1000L;
            long refresh = context.getProperty(REFRESH_SECONDS).asLong() * 1000L;
            long now = System.currentTimeMillis();
            if (detector == null || detectorSettleMillis != settle) {
                detector = new CompletionDetector(settle);
                detectorSettleMillis = settle;
            }
            if (now - watchedFetchedAt >= refresh) {
                watchedFetchedAt = now;
                refreshWatched(api, token);
            }
            if (!watchedLoaded) {
                return;   // 감시 목록을 아직 못 받았다 - 엉뚱한 그룹을 보고하지 않는다
            }

            Map<String, CompletionDetector.Snapshot> snapshots = new HashMap<>();
            collect(context.getEventAccess().getControllerStatus(), snapshots);
            // 실행 중인 그룹의 ERROR 알림을 판정 전에 모은다. 긴 실행은 알림 보관(최근 몇 분)에서 앞쪽 알림이 밀려나므로 끝날 때만 보면 놓친다.
            for (String groupId : snapshots.keySet()) {
                if (detector.inRun(groupId)) {
                    errorsSince(context, groupId, detector.runStartedAt(groupId)).forEach(error -> detector.addError(groupId, error));
                }
            }
            List<CompletionDetector.Completion> done = detector.tick(snapshots, now);
            detector.startedThisTick.forEach(line -> getLogger().info("job 그룹 실행 시작 감지: {}", line));
            for (CompletionDetector.Completion completion : done) {
                // 끝난 순간에 한 번 더 훑는다 - 판정 간격보다 짧게 끝난 job 은 실행 중에 한 번도 «실행 중»으로 안 잡혔다(시험 중 발견).
                Set<String> errors = new java.util.LinkedHashSet<>(completion.errors());
                errors.addAll(errorsSince(context, completion.groupId(), completion.startedAtMillis()));
                enqueue(completion, errors.stream().limit(CompletionDetector.MAX_ERRORS).toList());
            }
            flush(api, token);
        } catch (RuntimeException ex) {
            getLogger().warn("job 완료 감시 중 오류(다음 주기에 다시 봅니다): {}", ex.getMessage());
        }
    }

    /** 감시 대상 그룹만 골라 상태를 모은다. 그룹 상태는 하위 그룹까지 합친 값이다. */
    private void collect(ProcessGroupStatus group, Map<String, CompletionDetector.Snapshot> out) {
        if (group == null) {
            return;
        }
        if (watched.contains(group.getId())) {
            out.put(group.getId(), snapshot(group));
        }
        Collection<ProcessGroupStatus> children = group.getProcessGroupStatus();
        if (children != null) {
            children.forEach(child -> collect(child, out));
        }
    }

    /** 그룹의 ERROR 알림 중 {@code sinceMillis} 1초 전부터의 것(알림 시각은 초 단위로 잘리기도 한다). */
    private static List<String> errorsSince(ReportingContext context, String groupId, long sinceMillis) {
        List<String> out = new java.util.ArrayList<>();
        for (Bulletin bulletin : context.getBulletinRepository().findBulletinsForGroupBySource(groupId)) {
            if ("ERROR".equalsIgnoreCase(bulletin.getLevel()) && bulletin.getTimestamp() != null
                    && bulletin.getTimestamp().getTime() >= sinceMillis - 1_000L) {
                out.add(bulletin.getSourceName() + ": " + bulletin.getMessage());
            }
        }
        return out;
    }

    private static CompletionDetector.Snapshot snapshot(ProcessGroupStatus group) {
        long[] load = {0L};
        boolean[] hasLoad = {false};
        long[] invocations = {0L};
        walkProcessors(group, processor -> {
            invocations[0] += processor.getInvocations();
            Map<String, Long> counters = processor.getCounters();
            if (counters != null) {
                counters.forEach((name, value) -> {
                    if (name != null && name.endsWith(LOAD_COUNTER_SUFFIX) && value != null) {
                        load[0] += value;
                        hasLoad[0] = true;
                    }
                });
            }
        });
        int active = group.getActiveThreadCount() == null ? 0 : group.getActiveThreadCount();
        int queued = group.getQueuedCount() == null ? 0 : group.getQueuedCount();
        return new CompletionDetector.Snapshot(active, queued, hasLoad[0] ? load[0] : null, invocations[0]);
    }

    private static void walkProcessors(ProcessGroupStatus group, java.util.function.Consumer<ProcessorStatus> visit) {
        if (group.getProcessorStatus() != null) {
            group.getProcessorStatus().forEach(visit);
        }
        if (group.getProcessGroupStatus() != null) {
            group.getProcessGroupStatus().forEach(child -> walkProcessors(child, visit));
        }
    }

    private void refreshWatched(String api, String token) {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(api + "/api/etl/job-runs/watched-groups"))
                    .timeout(Duration.ofSeconds(3)).GET();
            if (token != null && !token.isBlank()) {
                builder.header("X-Service-Token", token);
            }
            HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() / 100 != 2) {
                getLogger().warn("감시 대상 job 목록을 받지 못했습니다(HTTP {}) - 이전 목록으로 계속합니다", response.statusCode());
                return;
            }
            Set<String> ids = new HashSet<>();
            Matcher matcher = UUID_IN_JSON.matcher(response.body());
            while (matcher.find()) {
                ids.add(matcher.group(1));
            }
            watched = Set.copyOf(ids);
            watchedLoaded = true;
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            getLogger().warn("감시 대상 job 목록 조회 실패 - 이전 목록으로 계속합니다: {}", ex.getMessage());
        }
    }

    private void enqueue(CompletionDetector.Completion completion, List<String> errors) {
        boolean failed = !errors.isEmpty();
        String path = "/api/etl/job-runs/by-pg/" + completion.groupId() + (failed ? "/fail" : "/complete");
        // 시작 · 끝 시각(epoch ms)도 보낸다 - 원장의 소요시간이 백엔드 관측(15초 간격)이 아니라 실제 실행 시각을 따르게.
        String times = ",\"startedAt\":" + completion.startedAtMillis() + ",\"endedAt\":" + completion.endedAtMillis();
        String body = failed
                ? "{\"error\":" + json(truncate(String.join("\n", errors), 1_000)) + times + "}"
                : "{\"rows\":" + (completion.rows() == null ? "null" : completion.rows()) + times + "}";
        synchronized (outbox) {
            if (outbox.size() >= QUEUE_LIMIT) {
                outbox.pollFirst();
            }
            outbox.addLast(new Outgoing(path, body, 0, 0L));
        }
        getLogger().info("job 그룹 {} 실행 {} 감지(rows={}, {}초, 적재 카운터 {}→{})", completion.groupId(), failed ? "실패" : "완료",
                completion.rows(), (completion.endedAtMillis() - completion.startedAtMillis()) / 1000,
                completion.baselineLoad(), completion.finalLoad());
    }

    /** 대기열을 별도 스레드에서 보낸다. 이미 보내는 중이면 그 스레드가 이어서 처리한다. */
    private void flush(String api, String token) {
        synchronized (outbox) {
            if (sending || outbox.isEmpty()) {
                return;
            }
            sending = true;
        }
        sender.execute(() -> {
            try {
                for (;;) {
                    Outgoing next;
                    synchronized (outbox) {
                        next = outbox.peekFirst();
                        if (next == null || next.notBefore() > System.currentTimeMillis()) {
                            return;   // 비었거나, 맨 앞은 아직 재시도 시각이 아니다 - 다음 주기에 다시
                        }
                        outbox.pollFirst();
                    }
                    if (!post(api, token, next)) {
                        if (next.attempt() + 1 < MAX_ATTEMPTS) {
                            long backoff = Math.min(MAX_BACKOFF_MILLIS, 2_000L << next.attempt());
                            synchronized (outbox) {
                                outbox.addFirst(new Outgoing(next.path(), next.body(), next.attempt() + 1,
                                        System.currentTimeMillis() + backoff));
                            }
                        } else {
                            getLogger().warn("job 완료 알림을 {}번 보내지 못해 버립니다: {}", MAX_ATTEMPTS, next.path());
                        }
                        return;
                    }
                }
            } finally {
                synchronized (outbox) {
                    sending = false;
                }
            }
        });
    }

    /** @return 보냈거나(2xx) 다시 보내도 소용없으면(4xx) true, 다시 보내 볼 만하면 false */
    private boolean post(String api, String token, Outgoing item) {
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(api + item.path()))
                    .timeout(Duration.ofSeconds(3))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(item.body(), StandardCharsets.UTF_8));
            if (token != null && !token.isBlank()) {
                builder.header("X-Service-Token", token);
            }
            int status = http.send(builder.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
            if (status / 100 == 4) {
                getLogger().warn("job 완료 알림이 거절됐습니다(HTTP {}): {}", status, item.path());
                return true;
            }
            return status / 100 == 2;
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }

    static String json(String text) {
        if (text == null) {
            return "null";
        }
        StringBuilder out = new StringBuilder("\"");
        for (char c : text.toCharArray()) {
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.append('"').toString();
    }

    private static String truncate(String text, int max) {
        return text == null || text.length() <= max ? text : text.substring(0, max);
    }

    private static String trimSlash(String url) {
        String out = url == null ? "" : url.trim();
        while (out.endsWith("/")) {
            out = out.substring(0, out.length() - 1);
        }
        return out;
    }
}
