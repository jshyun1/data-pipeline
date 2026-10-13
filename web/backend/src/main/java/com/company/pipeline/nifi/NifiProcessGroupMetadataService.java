package com.company.pipeline.nifi;

import com.company.pipeline.user.AppUser;
import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

@Service
public class NifiProcessGroupMetadataService {

    public static final String LEGACY_ACTOR = "admin";

    private final NifiProcessGroupMetadataRepository repository;

    public NifiProcessGroupMetadataService(NifiProcessGroupMetadataRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public NifiProcessGroupMetadata recordCreated(String processGroupId, String processGroupName,
                                                  String parentGroupId, String comments, AppUser user) {
        String actor = actor(user);
        NifiProcessGroupMetadata metadata = repository.findById(processGroupId)
                .orElseGet(() -> new NifiProcessGroupMetadata(
                        processGroupId, displayName(processGroupName, processGroupId), parentGroupId, comments, actor));
        metadata.applySnapshot(displayName(processGroupName, processGroupId), parentGroupId, comments, actor);
        return repository.save(metadata);
    }

    @Transactional
    public NifiProcessGroupMetadata recordWizardCreated(String processGroupId, String processGroupName,
                                                       String parentGroupId, String comments,
                                                       String wizardPayload, AppUser user) {
        String actor = actor(user);
        NifiProcessGroupMetadata metadata = repository.findById(processGroupId)
                .orElseGet(() -> new NifiProcessGroupMetadata(
                        processGroupId, displayName(processGroupName, processGroupId), parentGroupId, comments, actor));
        metadata.applyWizardSnapshot(displayName(processGroupName, processGroupId), parentGroupId, comments,
                wizardPayload, actor);
        return repository.save(metadata);
    }

    @Transactional
    public NifiProcessGroupMetadata updateWizardSnapshot(String processGroupId, String processGroupName,
                                                         String parentGroupId, String comments,
                                                         String wizardPayload, AppUser user) {
        String actor = actor(user);
        NifiProcessGroupMetadata metadata = repository.findById(processGroupId)
                .orElseThrow(() -> new NifiClientException("생성마법사 정보를 찾지 못했습니다.", null));
        if (!metadata.isWizardCreated()) {
            throw new NifiClientException("생성마법사로 만든 그룹만 편집할 수 있습니다.", null);
        }
        metadata.applyWizardSnapshot(displayName(processGroupName, processGroupId), parentGroupId, comments,
                wizardPayload, actor);
        return repository.save(metadata);
    }

    @Transactional
    public NifiProcessGroupMetadata ensureDiscovered(String processGroupId, String processGroupName,
                                                     String parentGroupId, String comments) {
        return repository.findById(processGroupId)
                .map(metadata -> {
                    String nextName = displayName(processGroupName, processGroupId);
                    if (changed(metadata.getProcessGroupName(), nextName)
                            || changed(metadata.getParentGroupId(), parentGroupId)
                            || changed(metadata.getComments(), comments)) {
                        metadata.applySnapshot(nextName, parentGroupId, comments, metadata.getUpdatedBy());
                        return repository.save(metadata);
                    }
                    return metadata;
                })
                .orElseGet(() -> repository.save(new NifiProcessGroupMetadata(
                        processGroupId, displayName(processGroupName, processGroupId), parentGroupId, comments,
                        LEGACY_ACTOR)));
    }

    @Transactional(readOnly = true)
    public Optional<NifiProcessGroupMetadata> findById(String processGroupId) {
        return repository.findById(processGroupId);
    }

    private static String actor(AppUser user) {
        return user == null || !StringUtils.hasText(user.getUserId()) ? "anonymous" : user.getUserId();
    }

    private static String displayName(String name, String fallback) {
        return StringUtils.hasText(name) ? name : fallback;
    }

    private static boolean changed(String current, String next) {
        return current == null ? next != null : !current.equals(next);
    }
}
