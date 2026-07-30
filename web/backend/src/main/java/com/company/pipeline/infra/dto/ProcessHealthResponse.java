package com.company.pipeline.infra.dto;

import java.time.LocalDateTime;
import java.util.List;

/** 파이프라인 축(CDC/NiFi/Airflow)별로 "떠 있어야 하는" 프로세스들의 현황. */
public record ProcessHealthResponse(LocalDateTime collectedAt, List<ProcessGroup> groups) {

    public record ProcessGroup(String key, String label, ProcessStatus status, List<ProcessItem> processes) {

        public static ProcessGroup of(String key, String label, List<ProcessItem> processes) {
            ProcessStatus status = processes.stream()
                    .map(ProcessItem::status)
                    .reduce(ProcessStatus.UP, ProcessStatus::worst);
            return new ProcessGroup(key, label, status, processes);
        }
    }

    /** detail은 화면에 그대로 보여줄 한 줄 설명(버전, heartbeat 시각, 실패 개수 등). */
    public record ProcessItem(String name, ProcessStatus status, String detail) {
    }
}
