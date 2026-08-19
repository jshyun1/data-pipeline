package com.company.pipeline.monitoring;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DlqReplayRequestRepository extends JpaRepository<DlqReplayRequest, Long> {
    boolean existsByDlqTopicAndDlqPartitionAndDlqOffset(String topic, Integer partition, Long offset);
    List<DlqReplayRequest> findTop100ByOrderByRequestedAtDesc();
}
