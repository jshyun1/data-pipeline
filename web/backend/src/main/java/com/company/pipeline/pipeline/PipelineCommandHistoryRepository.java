package com.company.pipeline.pipeline;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PipelineCommandHistoryRepository extends JpaRepository<PipelineCommandHistory, Long> {

    List<PipelineCommandHistory> findByPipelineIdOrderByRequestedAtDesc(Long pipelineId);

    List<PipelineCommandHistory> findTop10ByResultOrderByRequestedAtDesc(String result);

    List<PipelineCommandHistory> findTop10ByCommandOrderByRequestedAtDesc(String command);
}
