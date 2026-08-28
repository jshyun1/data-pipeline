package com.company.pipeline.nifi.dto;

import java.util.List;

public record NifiFileUploadResponse(
        String folderName,
        String serverDirectory,
        String nifiInputDirectory,
        List<String> storedFiles,
        List<String> columns
) {
}
