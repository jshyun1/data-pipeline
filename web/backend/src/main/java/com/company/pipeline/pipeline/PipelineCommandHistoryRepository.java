package com.company.pipeline.pipeline;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PipelineCommandHistoryRepository extends JpaRepository<PipelineCommandHistory, Long> {

    List<PipelineCommandHistory> findByPipelineIdOrderByRequestedAtDesc(Long pipelineId);

    /**
     * 화면 «이력» 탭이 쓰는 조회. 최근 것부터 {@code pageable} 만큼만 준다.
     *
     * <p>{@code includeAuto=false} 면 자동 감시 기록({@code RUNTIME_MONITOR}, 30초 주기라 수천 건이다)을 뺀다 -
     * 사람이 한 제어(시작 · 중지 · 배포 · 건너뛰기)가 그 사이에 묻히면 이력을 보는 뜻이 없다.
     */
    @Query("""
            SELECT h FROM PipelineCommandHistory h
             WHERE h.pipelineId = :pipelineId
               AND (:includeAuto = true OR h.command <> 'RUNTIME_MONITOR')
             ORDER BY h.requestedAt DESC
            """)
    List<PipelineCommandHistory> findRecent(@Param("pipelineId") Long pipelineId,
                                            @Param("includeAuto") boolean includeAuto,
                                            Pageable pageable);

    List<PipelineCommandHistory> findTop10ByResultOrderByRequestedAtDesc(String result);

    List<PipelineCommandHistory> findTop10ByCommandOrderByRequestedAtDesc(String command);

    Optional<PipelineCommandHistory> findFirstByPipelineIdOrderByRequestedAtDesc(Long pipelineId);

    List<PipelineCommandHistory> findByPipelineIdInAndRequestedAtBetweenOrderByRequestedAtDesc(
            List<Long> pipelineIds, LocalDateTime from, LocalDateTime to);

    long countByRequestedAtGreaterThanEqual(LocalDateTime since);

    long countByResultAndRequestedAtGreaterThanEqual(String result, LocalDateTime since);
}
