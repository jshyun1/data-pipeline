package com.company.pipeline.nifi.dto;

import com.fasterxml.jackson.databind.JsonNode;

public record NifiEtlWizardResponse(
        String processGroupId,
        String processGroupName,
        String parentGroupId,
        String comments,
        boolean wizardCreated,
        JsonNode wizardSnapshot
) {
}
