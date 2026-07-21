package com.company.pipeline.common;

public record ApiResponse<T>(boolean success, T data, ErrorPayload error) {

    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(true, data, null);
    }

    public static <T> ApiResponse<T> error(ErrorCode code, String message) {
        return new ApiResponse<>(false, null, new ErrorPayload(code.name(), message));
    }
}
