package com.company.pipeline.nifi;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "nifi_processor_edit_lock")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class NifiProcessorEditLock {

    @Id
    @Column(name = "processor_id", length = 64)
    private String processorId;

    @Column(name = "processor_name", length = 255)
    private String processorName;

    @Column(name = "owner_token", nullable = false, length = 64)
    private String ownerToken;

    @Column(name = "locked_by_user_id", nullable = false, length = 255)
    private String lockedByUserId;

    @Column(name = "locked_by_user_nm", nullable = false, length = 255)
    private String lockedByUserNm;

    @Column(name = "acquired_at", nullable = false)
    private OffsetDateTime acquiredAt;

    @Column(name = "expires_at", nullable = false)
    private OffsetDateTime expiresAt;

    public NifiProcessorEditLock(String processorId, String processorName, String ownerToken,
            String lockedByUserId, String lockedByUserNm, OffsetDateTime now, OffsetDateTime expiresAt) {
        this.processorId = processorId;
        assign(processorName, ownerToken, lockedByUserId, lockedByUserNm, now, expiresAt);
    }

    public void assign(String processorName, String ownerToken, String lockedByUserId, String lockedByUserNm,
            OffsetDateTime now, OffsetDateTime expiresAt) {
        this.processorName = processorName;
        this.ownerToken = ownerToken;
        this.lockedByUserId = lockedByUserId;
        this.lockedByUserNm = lockedByUserNm;
        this.acquiredAt = now;
        this.expiresAt = expiresAt;
    }

    public boolean isOwnedBy(String ownerToken) {
        return this.ownerToken.equals(ownerToken);
    }

    public boolean isExpired(OffsetDateTime now) {
        return !expiresAt.isAfter(now);
    }
}
