package com.company.pipeline.nifi;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.nifi.dto.NifiProcessorEditLockRequest;
import com.company.pipeline.nifi.dto.NifiProcessorEditLockResponse;
import com.company.pipeline.user.AppUser;
import java.time.Clock;
import java.time.OffsetDateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

@Service
public class NifiProcessorEditLockService {

    static final long LOCK_TTL_SECONDS = 45;

    private final NifiProcessorEditLockRepository repository;
    private final Clock clock;

    @Autowired
    public NifiProcessorEditLockService(NifiProcessorEditLockRepository repository) {
        this(repository, Clock.systemDefaultZone());
    }

    NifiProcessorEditLockService(NifiProcessorEditLockRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    @Transactional
    public NifiProcessorEditLockResponse acquire(String processorId, NifiProcessorEditLockRequest request, AppUser user) {
        requireUser(user);
        OffsetDateTime now = now();
        repository.deleteExpired(now);

        String ownerToken = request.ownerToken().trim();
        String processorName = trimToNull(request.processorName());
        NifiProcessorEditLock lock = repository.findById(processorId).orElse(null);
        if (lock == null) {
            lock = new NifiProcessorEditLock(processorId, processorName, ownerToken,
                    user.getUserId(), displayName(user), now, expiresAt(now));
            repository.save(lock);
            return NifiProcessorEditLockResponse.from(lock, ownerToken);
        }
        if (lock.isExpired(now) || lock.isOwnedBy(ownerToken)) {
            lock.assign(processorName != null ? processorName : lock.getProcessorName(), ownerToken,
                    user.getUserId(), displayName(user), now, expiresAt(now));
        }
        return NifiProcessorEditLockResponse.from(lock, ownerToken);
    }

    @Transactional
    public NifiProcessorEditLockResponse heartbeat(String processorId, NifiProcessorEditLockRequest request, AppUser user) {
        requireUser(user);
        OffsetDateTime now = now();
        repository.deleteExpired(now);

        String ownerToken = request.ownerToken().trim();
        NifiProcessorEditLock lock = repository.findById(processorId).orElse(null);
        if (lock == null) {
            return acquire(processorId, request, user);
        }
        if (lock.isOwnedBy(ownerToken)) {
            lock.assign(trimToNull(request.processorName()) != null ? request.processorName().trim() : lock.getProcessorName(),
                    ownerToken, user.getUserId(), displayName(user), now, expiresAt(now));
        }
        return NifiProcessorEditLockResponse.from(lock, ownerToken);
    }

    @Transactional
    public void release(String processorId, NifiProcessorEditLockRequest request, AppUser user) {
        requireUser(user);
        repository.deleteByProcessorIdAndOwnerToken(processorId, request.ownerToken().trim());
    }

    private OffsetDateTime now() {
        return OffsetDateTime.now(clock);
    }

    private OffsetDateTime expiresAt(OffsetDateTime now) {
        return now.plusSeconds(LOCK_TTL_SECONDS);
    }

    private static void requireUser(AppUser user) {
        if (user == null) {
            throw new BusinessException(ErrorCode.UNAUTHORIZED, ErrorCode.UNAUTHORIZED.getDefaultMessage());
        }
    }

    private static String displayName(AppUser user) {
        return StringUtils.hasText(user.getUserNm()) ? user.getUserNm() : user.getUserId();
    }

    private static String trimToNull(String value) {
        return StringUtils.hasText(value) ? value.trim() : null;
    }
}
