import { Card, Empty } from "antd";

export function PermissionsPage() {
  return (
    <div style={{ padding: 16 }}>
      <h2 style={{ marginTop: 0 }}>권한 관리</h2>
      <Card>
        <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="준비 중입니다." />
      </Card>
    </div>
  );
}
