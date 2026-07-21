package com.company.pipeline.pipeline;

import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;

public class PipelineNotFoundException extends BusinessException {

    public PipelineNotFoundException(Long id) {
        super(ErrorCode.PIPELINE_NOT_FOUND, "파이프라인을 찾을 수 없습니다. id=" + id);
    }
}
