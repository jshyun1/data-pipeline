package com.company.pipeline.nifi;

import java.time.OffsetDateTime;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface NifiProcessorEditLockRepository extends JpaRepository<NifiProcessorEditLock, String> {

    @Modifying
    @Query("delete from NifiProcessorEditLock l where l.expiresAt <= :now")
    int deleteExpired(@Param("now") OffsetDateTime now);

    @Modifying
    @Query("delete from NifiProcessorEditLock l where l.processorId = :processorId and l.ownerToken = :ownerToken")
    int deleteByProcessorIdAndOwnerToken(
            @Param("processorId") String processorId,
            @Param("ownerToken") String ownerToken);
}
