package com.company.pipeline.jobcatalog;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 프로세서/그룹 -> 잡 해석기.
 *
 * <p>이력을 남기는 쪽(15초·30초 주기)에서 매번 DB를 조회하지 않도록 캐시한다. 캔버스에서
 * 프로세서를 추가하면 캐시에 없으므로 그때 한 번 조회하고, 그래도 없으면(아직 미러가
 * 안 돌았거나 지워진 프로세서) 빈 값으로 캐시해 매 주기 재조회하지 않는다.
 *
 * <p>미러가 5분마다 갱신되므로 새 프로세서는 최대 5분 뒤부터 잡에 귀속된다. 그 사이 이력은
 * job_id가 비어 있고, 나중에 소급해서 채울 수 있다(V18 소급 UPDATE와 같은 방식).
 */
@Component
public class JobLookup {

    /** 값이 없음을 캐시하기 위한 표식 - ConcurrentHashMap은 null을 담지 못한다. */
    private static final Long UNKNOWN = -1L;

    private final EtlJobStepRepository stepRepository;
    private final EtlJobRepository jobRepository;

    private final Map<String, Long> byProcessorId = new ConcurrentHashMap<>();
    private final Map<String, Long> byGroupId = new ConcurrentHashMap<>();

    public JobLookup(EtlJobStepRepository stepRepository, EtlJobRepository jobRepository) {
        this.stepRepository = stepRepository;
        this.jobRepository = jobRepository;
    }

    /**
     * 프로세서가 속한 잡. 하위 그룹에 있는 프로세서도 최상위 잡으로 해석된다.
     * 모르면 groupId로 한 번 더 시도한다(미러가 아직 이 프로세서를 못 본 경우).
     */
    @Transactional(readOnly = true)
    public Optional<Long> resolveJobId(String processorId, String groupId) {
        Long cached = processorId == null ? null : byProcessorId.get(processorId);
        if (cached != null && !UNKNOWN.equals(cached)) {
            return Optional.of(cached);
        }
        if (cached == null && processorId != null) {
            Long resolved = stepRepository.findByNifiProcessorId(processorId)
                    .map(EtlJobStep::getJobId)
                    .orElse(null);
            byProcessorId.put(processorId, resolved == null ? UNKNOWN : resolved);
            if (resolved != null) {
                return Optional.of(resolved);
            }
        }
        return resolveByGroupId(groupId);
    }

    /** 그룹 id가 최상위 잡 그룹일 때만 맞는다(하위 그룹은 카탈로그에 없다). */
    @Transactional(readOnly = true)
    public Optional<Long> resolveByGroupId(String groupId) {
        if (groupId == null) {
            return Optional.empty();
        }
        Long cached = byGroupId.get(groupId);
        if (cached != null) {
            return UNKNOWN.equals(cached) ? Optional.empty() : Optional.of(cached);
        }
        Long resolved = jobRepository.findByNifiPgId(groupId).map(EtlJob::getId).orElse(null);
        byGroupId.put(groupId, resolved == null ? UNKNOWN : resolved);
        return Optional.ofNullable(resolved);
    }

    /** 미러 동기화로 구성이 바뀐 뒤 호출해 캐시를 비운다. */
    public void invalidate() {
        byProcessorId.clear();
        byGroupId.clear();
    }
}
