package com.company.pipeline.pipeline;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PipelineGroupRepository extends JpaRepository<PipelineGroup, Long> {

    List<PipelineGroup> findAllByOrderByNameAsc();

    List<PipelineGroup> findByParentId(Long parentId);

    boolean existsByParentIdAndName(Long parentId, String name);

    boolean existsByParentIdIsNullAndName(String name);
}
