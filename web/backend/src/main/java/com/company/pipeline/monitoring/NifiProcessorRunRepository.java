package com.company.pipeline.monitoring;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NifiProcessorRunRepository extends JpaRepository<NifiProcessorRun, Long> {

    /** 아직 안 닫힌 구간들. 프로세서당 하나뿐이도록 DB에 부분 UNIQUE 인덱스가 걸려 있다. */
    List<NifiProcessorRun> findByEndedAtIsNull();

    Optional<NifiProcessorRun> findByProcessorIdAndEndedAtIsNull(String processorId);

    /**
     * 화면 조회용. 진행 중인 구간도 보여야 하므로 started_at 기준으로 자른다
     * (ended_at 기준으로 자르면 아직 안 끝난 실행이 목록에서 빠진다).
     */
    List<NifiProcessorRun> findByStartedAtBetweenOrderByStartedAtDesc(LocalDateTime from, LocalDateTime to);

    Optional<NifiProcessorRun> findTopByGroupIdOrderByStartedAtDesc(String groupId);
}
