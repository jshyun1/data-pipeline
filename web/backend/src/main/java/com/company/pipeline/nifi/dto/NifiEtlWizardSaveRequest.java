package com.company.pipeline.nifi.dto;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;

public record NifiEtlWizardSaveRequest(
        @NotBlank
        String etlType,

        @Valid
        NifiInitialDbToDbCreateRequest initialDbToDb,

        @Valid
        NifiFileLoadCreateRequest fileLoad,

        @Valid
        NifiHttpLoadCreateRequest httpLoad,

        @Valid
        NifiUnstructuredLoadCreateRequest unstructuredLoad,

        JsonNode wizardSnapshot,

        String unstructuredInputDirectory
) {
}
