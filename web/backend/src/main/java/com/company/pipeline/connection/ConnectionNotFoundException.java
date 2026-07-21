package com.company.pipeline.connection;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;

public class ConnectionNotFoundException extends BusinessException {

    public ConnectionNotFoundException(Long id) {
        super(ErrorCode.CONNECTION_NOT_FOUND, "연결정보를 찾을 수 없습니다. id=" + id);
    }
}
