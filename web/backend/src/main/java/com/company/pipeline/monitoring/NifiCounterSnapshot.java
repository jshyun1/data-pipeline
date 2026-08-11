package com.company.pipeline.monitoring;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** NiFi PutDatabaseRecord 프로세서별 "* updates performed" 카운터의 마지막 확인 값. V10 마이그레이션. */
@Entity
@Table(name = "nifi_counter_snapshot")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class NifiCounterSnapshot {

    @Id
    @Column(name = "processor_id", length = 100)
    private String processorId;

    @Column(name = "processor_name", length = 200)
    private String processorName;

    @Column(name = "last_value", nullable = false)
    private Long lastValue;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public NifiCounterSnapshot(String processorId, String processorName, Long lastValue) {
        this.processorId = processorId;
        this.processorName = processorName;
        this.lastValue = lastValue;
        this.updatedAt = LocalDateTime.now();
    }
}
