package com.company.pipeline.pipeline;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PipelineCommandHistoryRepository extends JpaRepository<PipelineCommandHistory, Long> {

    List<PipelineCommandHistory> findByPipelineIdOrderByRequestedAtDesc(Long pipelineId);

    List<PipelineCommandHistory> findTop10ByResultOrderByRequestedAtDesc(String result);

    List<PipelineCommandHistory> findTop10ByCommandOrderByRequestedAtDesc(String command);

    Optional<PipelineCommandHistory> findFirstByPipelineIdOrderByRequestedAtDesc(Long pipelineId);

    List<PipelineCommandHistory> findByPipelineIdInAndRequestedAtBetweenOrderByRequestedAtDesc(
            List<Long> pipelineIds, LocalDateTime from, LocalDateTime to);

    long countByRequestedAtGreaterThanEqual(LocalDateTime since);

    long countByResultAndRequestedAtGreaterThanEqual(String result, LocalDateTime since);
}
