package com.company.pipeline.nifi.dto;

import com.company.pipeline.nifi.NifiProcessorEditLock;
import java.time.OffsetDateTime;

public record NifiProcessorEditLockResponse(
        String processorId,
        String processorName,
        boolean editable,
        boolean heldByMe,
        String lockedByUserId,
        String lockedByUserNm,
        OffsetDateTime acquiredAt,
        OffsetDateTime expiresAt
) {

    public static NifiProcessorEditLockResponse from(NifiProcessorEditLock lock, String ownerToken) {
        boolean heldByMe = lock.isOwnedBy(ownerToken);
        return new NifiProcessorEditLockResponse(
                lock.getProcessorId(),
                lock.getProcessorName(),
                heldByMe,
                heldByMe,
                lock.getLockedByUserId(),
                lock.getLockedByUserNm(),
                lock.getAcquiredAt(),
                lock.getExpiresAt());
    }
}
