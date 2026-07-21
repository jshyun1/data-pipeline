import { Layout, Menu } from "antd";
import { ApiOutlined, NodeIndexOutlined } from "@ant-design/icons";
import { Link, Outlet, useLocation } from "react-router-dom";

const { Header, Sider, Content } = Layout;

export function AppLayout() {
  const location = useLocation();

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Header style={{ display: "flex", alignItems: "center" }}>
        <span style={{ color: "white", fontSize: 18, fontWeight: 600 }}>
          Kafka Pipeline Console
        </span>
      </Header>
      <Layout>
        <Sider width={200} theme="light">
          <Menu
            mode="inline"
            selectedKeys={[location.pathname]}
            style={{ height: "100%" }}
            items={[
              {
                key: "/connections",
                icon: <ApiOutlined />,
                label: <Link to="/connections">연결정보</Link>,
              },
              {
                key: "/pipelines",
                icon: <NodeIndexOutlined />,
                label: <Link to="/pipelines">파이프라인</Link>,
              },
            ]}
          />
        </Sider>
        <Content style={{ padding: 24 }}>
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
}
