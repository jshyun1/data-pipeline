package com.company.pipeline.nifi;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;

public class NifiClientException extends BusinessException {

    public NifiClientException(String message, Throwable cause) {
        super(ErrorCode.NIFI_ERROR, message, cause);
    }
}
