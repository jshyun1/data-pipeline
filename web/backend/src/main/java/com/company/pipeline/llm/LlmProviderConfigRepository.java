package com.company.pipeline.llm;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LlmProviderConfigRepository extends JpaRepository<LlmProviderConfig, Long> {

    List<LlmProviderConfig> findAllByOrderByIdAsc();

    Optional<LlmProviderConfig> findByActiveTrue();

    boolean existsByName(String name);

    boolean existsByNameAndIdNot(String name, Long id);
}
