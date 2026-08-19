package com.company.pipeline.airflowdashboard;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AirflowDagAlertRepository extends JpaRepository<AirflowDagAlert, Long> {

    Optional<AirflowDagAlert> findByDagIdAndRuleTypeAndStatusIn(
            String dagId,
            AirflowDagAlert.RuleType ruleType,
            Collection<AirflowDagAlert.Status> statuses);

    List<AirflowDagAlert> findByStatusInOrderByLastDetectedAtDesc(Collection<AirflowDagAlert.Status> statuses);

    List<AirflowDagAlert> findByDagIdAndStatusIn(
            String dagId,
            Collection<AirflowDagAlert.Status> statuses);

    List<AirflowDagAlert> findAllByOrderByDetectedAtDesc();
}
