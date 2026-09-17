package com.company.pipeline.llm;

public enum LlmTargetType {
    GENERAL,
    NIFI_GROUP,
    ETL_JOB,
    CDC_PIPELINE,
    WORKFLOW_NODE,
    WORKFLOW_TASK
}
