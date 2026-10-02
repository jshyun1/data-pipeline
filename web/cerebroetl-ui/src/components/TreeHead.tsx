import { useState } from "react";
import { Button } from "antd";
import { FolderOutlined, ReloadOutlined } from "@ant-design/icons";

/**
 * 트리 패널 머리줄 - ETL 관리 화면 트리(ConsoleFramePage)와 <b>같은 클래스</b>를 쓴다(2026-10-01 요청).
 * 클래스가 같으니 글자 · 색 · 단추 자리가 그 화면과 그대로 같다.
 *
 * <p>왼쪽은 «📁 트리 검색», 오른쪽은 새로고침 · 전체 접기.
 * «전체 접기»를 누르면 전부 접히고 단추가 «전체 열기»로 바뀐다. 다시 누르면 전부 열리고 «전체 접기»로 돌아간다(2026-10-02 요청).
 * 새로고침을 안 넘기면(이미 화면에 같은 단추가 있는 곳) 그 단추는 그리지 않는다.
 */
export function TreeHead({ onRefresh, refreshing = false, onCollapseAll, onExpandAll, refreshTitle = "다시 불러옵니다", extraAction }: {
  onRefresh?: () => void;
  refreshing?: boolean;
  onCollapseAll: () => void;
  onExpandAll: () => void;
  refreshTitle?: string;
  /** 화면마다 다른 단추(설계 팔레트의 ETL 동기화 등)를 새로고침 자리에 대신 넣을 때. */
  extraAction?: React.ReactNode;
}) {
  // 처음엔 «전체 접기». 한 칸씩 손으로 여닫는 것은 따로 세지 않는다 - 단추는 «마지막에 누른 것의 반대»를 보여 준다.
  const [allCollapsed, setAllCollapsed] = useState(false);
  const toggleAll = () => {
    if (allCollapsed) onExpandAll();
    else onCollapseAll();
    setAllCollapsed(!allCollapsed);
  };
  return (
    <div className="nifi-tree-search-heading">
      <span className="nifi-tree-section-title">
        <FolderOutlined />
        트리 검색
      </span>
      <div className="nifi-tree-heading-actions">
        {extraAction}
        {onRefresh && (
          <Button size="small" className="nifi-tree-refresh-button" icon={<ReloadOutlined />} loading={refreshing}
                  title={refreshTitle} aria-label={refreshTitle} onClick={onRefresh} />
        )}
        <Button size="small" className="nifi-tree-collapse-button" onClick={toggleAll}>
          {allCollapsed ? "전체 열기" : "전체 접기"}
        </Button>
      </div>
    </div>
  );
}
