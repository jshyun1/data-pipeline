package com.company.pipeline.nifi;

import org.springframework.data.jpa.repository.JpaRepository;

public interface NifiProcessGroupMetadataRepository extends JpaRepository<NifiProcessGroupMetadata, String> {
}
