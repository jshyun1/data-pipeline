package com.company.pipeline.monitoring;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;

public class KafkaTopicOffsetReaderException extends BusinessException {

    public KafkaTopicOffsetReaderException(String message, Throwable cause) {
        super(ErrorCode.INTERNAL_ERROR, message);
        initCause(cause);
    }
}
