package com.company.pipeline.connection.dto;

public record ColumnMetadataResponse(String name, String dataType, boolean primaryKey, boolean maskable) {
}
