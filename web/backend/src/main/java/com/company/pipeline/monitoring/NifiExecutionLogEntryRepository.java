package com.company.pipeline.monitoring;

import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NifiExecutionLogEntryRepository extends JpaRepository<NifiExecutionLogEntry, Long> {

    List<NifiExecutionLogEntry> findByOccurredAtBetweenOrderByOccurredAtDesc(LocalDateTime from, LocalDateTime to);

    /**
     * 최근 {@code since} 이후에 같은 bulletin id를 이미 넣었는지.
     *
     * <p>bulletin id는 NiFi 프로세스 안에서만 단조 증가하고 재시작하면 1부터 다시
     * 시작한다. 그래서 id만으로 중복 판정을 하면 재시작 이후의 새 실패가 "이미 본
     * 것"으로 걸러진다 - 실제로 이것 때문에 DZ 테이블이 통째로 비워진 사고가 ETL
     * 로그에 한 줄도 안 남았다(2026-07-29).
     *
     * <p>시간 범위를 같이 보면 두 경우가 다 맞는다. 같은 NiFi 세션에서 30초마다
     * 다시 긁어온 같은 bulletin은 범위 안에 있으니 걸러지고, 재시작 뒤 재사용된
     * id는 예전 행이 범위 밖이라 새 행으로 남는다. pipeline-api가 재기동돼도
     * DB를 보므로 직전 몇 분치가 중복되지 않는다.
     */
    boolean existsByBulletinIdAndOccurredAtAfter(Long bulletinId, LocalDateTime since);
}
