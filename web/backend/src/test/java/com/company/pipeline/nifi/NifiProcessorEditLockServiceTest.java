package com.company.pipeline.nifi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.company.pipeline.nifi.dto.NifiProcessorEditLockRequest;
import com.company.pipeline.user.AppUser;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NifiProcessorEditLockServiceTest {

    private static final String PROCESSOR_ID = "9e2dd726-019f-1000-338b-b3f02d4d9673";
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-08-23T03:00:00Z"), ZoneId.of("Asia/Seoul"));

    @Mock
    private NifiProcessorEditLockRepository repository;

    @Mock
    private AppUser user;

    private NifiProcessorEditLockService service() {
        lenient().when(user.getUserId()).thenReturn("jsh");
        lenient().when(user.getUserNm()).thenReturn("JSH");
        return new NifiProcessorEditLockService(repository, CLOCK);
    }

    @Test
    void acquireCreatesEditableLockWhenProcessorIsFree() {
        when(repository.findById(PROCESSOR_ID)).thenReturn(Optional.empty());

        var response = service().acquire(PROCESSOR_ID, request("owner-a"), user);

        assertThat(response.editable()).isTrue();
        assertThat(response.heldByMe()).isTrue();
        assertThat(response.lockedByUserId()).isEqualTo("jsh");

        ArgumentCaptor<NifiProcessorEditLock> captor = ArgumentCaptor.forClass(NifiProcessorEditLock.class);
        verify(repository).save(captor.capture());
        assertThat(captor.getValue().getProcessorId()).isEqualTo(PROCESSOR_ID);
        assertThat(captor.getValue().getOwnerToken()).isEqualTo("owner-a");
    }

    @Test
    void acquireReturnsReadOnlyWhenAnotherOwnerHasLiveLock() {
        OffsetDateTime now = OffsetDateTime.now(CLOCK);
        var existing = new NifiProcessorEditLock(PROCESSOR_ID, "load", "owner-a",
                "alice", "Alice", now.minusSeconds(5), now.plusSeconds(30));
        when(repository.findById(PROCESSOR_ID)).thenReturn(Optional.of(existing));

        var response = service().acquire(PROCESSOR_ID, request("owner-b"), user);

        assertThat(response.editable()).isFalse();
        assertThat(response.heldByMe()).isFalse();
        assertThat(response.lockedByUserId()).isEqualTo("alice");
        assertThat(existing.getOwnerToken()).isEqualTo("owner-a");
    }

    @Test
    void acquireTakesOverExpiredLock() {
        OffsetDateTime now = OffsetDateTime.now(CLOCK);
        var existing = new NifiProcessorEditLock(PROCESSOR_ID, "load", "owner-a",
                "alice", "Alice", now.minusMinutes(2), now.minusSeconds(1));
        when(repository.findById(PROCESSOR_ID)).thenReturn(Optional.of(existing));

        var response = service().acquire(PROCESSOR_ID, request("owner-b"), user);

        assertThat(response.editable()).isTrue();
        assertThat(response.heldByMe()).isTrue();
        assertThat(response.lockedByUserId()).isEqualTo("jsh");
        assertThat(existing.getOwnerToken()).isEqualTo("owner-b");
        assertThat(existing.getExpiresAt()).isEqualTo(now.plusSeconds(NifiProcessorEditLockService.LOCK_TTL_SECONDS));
    }

    private static NifiProcessorEditLockRequest request(String ownerToken) {
        return new NifiProcessorEditLockRequest(ownerToken, "load");
    }
}
