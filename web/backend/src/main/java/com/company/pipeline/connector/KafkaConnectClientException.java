package com.company.pipeline.connector;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;

public class KafkaConnectClientException extends BusinessException {

    public KafkaConnectClientException(String message) {
        super(ErrorCode.KAFKA_CONNECT_ERROR, message);
    }

    public KafkaConnectClientException(String message, Throwable cause) {
        super(ErrorCode.KAFKA_CONNECT_ERROR, message);
        initCause(cause);
    }
}
