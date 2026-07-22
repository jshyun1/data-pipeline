package com.company.pipeline.user;

import org.springframework.data.jpa.repository.JpaRepository;

public interface AppUserRepository extends JpaRepository<AppUser, String> {
    boolean existsByUserId(String userId);
}
