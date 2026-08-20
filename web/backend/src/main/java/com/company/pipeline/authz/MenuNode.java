package com.company.pipeline.authz;

import java.util.List;

/** 사용자에게 보일 메뉴 트리 노드(설계서 §7.4, 프론트 동적 메뉴용). */
public record MenuNode(
        String menuId,
        String parentId,
        String menuNm,
        String menuUrl,
        String icon,
        String systemCode,
        int sortOrd,
        List<MenuNode> children) {
}
