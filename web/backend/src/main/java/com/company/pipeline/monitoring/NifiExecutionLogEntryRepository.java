package com.company.pipeline.monitoring;

import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface NifiExecutionLogEntryRepository extends JpaRepository<NifiExecutionLogEntry, Long> {

    List<NifiExecutionLogEntry> findByOccurredAtBetweenOrderByOccurredAtDesc(LocalDateTime from, LocalDateTime to);

    /**
     * 지금까지 적재한 bulletin 중 가장 큰 id. 다음 조회를 이 값 이후로만 요청해서
     * 같은 bulletin을 두 번 넣지 않는다. pipeline-api가 재기동돼도 DB에서 다시
     * 읽어오므로 메모리에만 들고 있을 때와 달리 재기동 직후 중복이 생기지 않는다.
     */
    @Query("SELECT MAX(e.bulletinId) FROM NifiExecutionLogEntry e WHERE e.bulletinId IS NOT NULL")
    Long findMaxBulletinId();

    boolean existsByBulletinId(Long bulletinId);
}
