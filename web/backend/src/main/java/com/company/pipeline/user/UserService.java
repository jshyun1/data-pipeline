package com.company.pipeline.user;

import com.company.pipeline.user.dto.UserResponse;
import java.time.LocalDateTime;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class UserService {

    private final AppUserRepository userRepository;

    public UserService(AppUserRepository userRepository) {
        this.userRepository = userRepository;
    }

    /**
     * 통합 계정 테이블(ST_USER)로 인증된 사용자의 앱측 프로필을 반환한다. app_user에 행이
     * 없으면(첫 로그인) 자동 프로비저닝한다 - app_user는 인증이 아니라 본부/직함/관리자여부
     * 같은 "앱 프로필·권한"을 담는 저장소로만 쓴다.
     *
     * Keycloak 제거 이후 realm role 같은 외부 권한 소스가 없어져서, admin_yn은 최초 생성
     * 시 기본값(N)만 주고 이후로는 여기서 건드리지 않는다 - 필요하면 DB에서 수동으로 부여.
     */
    @Transactional
    public UserResponse provisionAndGet(String userId, String userNm, String email) {
        AppUser user = userRepository.findById(userId).orElseGet(() -> {
            AppUser created = new AppUser();
            created.setUserId(userId);
            created.setUserNm(userNm != null ? userNm : userId);
            created.setEmail(email);
            created.setUseYn("Y"); // 계정 테이블이 이미 인증했으므로 활성 상태로 생성
            return created;
        });

        if (email != null) {
            user.setEmail(email);
        }
        user.setLastLoginDt(LocalDateTime.now());
        user.setUpdDt(LocalDateTime.now());
        userRepository.save(user);
        return UserResponse.from(user);
    }
}
