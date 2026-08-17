package com.company.pipeline.settings;

import com.company.pipeline.common.ApiResponse;
import com.company.pipeline.common.BusinessException;
import com.company.pipeline.common.ErrorCode;
import com.company.pipeline.settings.SettingService.SettingRow;
import com.company.pipeline.user.AppUser;
import java.util.List;
import java.util.Map;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 운영 설정(임계값·주기·보존) 관리 API (설계서 5-2 ⑧, U2).
 *
 * <p>재정의(override)만 저장한다 - 삭제는 "기본값으로 초기화"다. 값 변경은 SettingService 의
 * 30초 TTL 캐시 때문에 최대 2 평가주기 안에 판정에 반영된다.
 */
@RestController
@RequestMapping("/api/admin/settings")
public class SettingsController {

    private final SettingService settingService;

    public SettingsController(SettingService settingService) {
        this.settingService = settingService;
    }

    /** 전 카탈로그 키를 실효값·기본값·재정의여부·범위와 함께 반환. */
    @GetMapping
    public ApiResponse<List<SettingRow>> list() {
        return ApiResponse.success(settingService.listAll());
    }

    /** 벌크 저장: {key: value} 맵. 타입/범위 위반이 하나라도 있으면 아무것도 저장하지 않고 400. */
    @PutMapping
    public ApiResponse<List<SettingRow>> update(
            @RequestBody Map<String, String> changes,
            @AuthenticationPrincipal AppUser user) {
        String updatedBy = user != null ? user.getUserId() : "admin";
        try {
            settingService.applyAll(changes, updatedBy);
        } catch (IllegalArgumentException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, ex.getMessage());
        }
        return ApiResponse.success(settingService.listAll());
    }

    /** 재정의 제거(기본값으로 초기화). */
    @DeleteMapping("/{key}")
    public ApiResponse<Void> reset(@PathVariable String key) {
        try {
            settingService.reset(key);
        } catch (IllegalArgumentException ex) {
            throw new BusinessException(ErrorCode.VALIDATION_ERROR, ex.getMessage());
        }
        return ApiResponse.success(null);
    }
}
