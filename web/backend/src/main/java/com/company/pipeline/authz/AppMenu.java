package com.company.pipeline.authz;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 메뉴 카탈로그. V47(app_menu) 매핑. 화면 라우트와 1:1인 고정 목록이라 마이그레이션이 심고
 * 관리자는 노출/숨김만 바꾼다. {@code required_bits} = "이 메뉴를 보려면 해당 시스템에 최소
 * 이만큼 필요"(설계서 §6.1). 그룹 노드는 {@code menu_url} 이 NULL.
 */
@Entity
@Table(name = "app_menu")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class AppMenu {

    @Id
    @Column(name = "menu_id", length = 40)
    private String menuId;

    @Column(name = "parent_id", length = 40)
    private String parentId;

    @Column(name = "menu_nm", nullable = false, length = 100)
    private String menuNm;

    @Column(name = "menu_url", length = 200)
    private String menuUrl;

    @Column(name = "icon", length = 50)
    private String icon;

    @Column(name = "system_code", nullable = false, length = 20)
    private String systemCode;

    @Column(name = "required_bits", nullable = false)
    private int requiredBits = 1;

    @Column(name = "sort_ord", nullable = false)
    private int sortOrd = 0;

    @Column(name = "use_yn", nullable = false, length = 1)
    private String useYn = "Y";

    public boolean isGroup() {
        return menuUrl == null || menuUrl.isBlank();
    }
}
