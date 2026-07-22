import { useMutation } from "@tanstack/react-query";
import { Button, Form, Input, Space, Typography, message } from "antd";
import { ApartmentOutlined, PlusOutlined } from "@ant-design/icons";
import { useNavigate } from "react-router-dom";
import { createNifiProcessGroup } from "../api/platform";

interface ProcessGroupCreateForm {
  name: string;
}

export function EtlCreatePage() {
  const [form] = Form.useForm<ProcessGroupCreateForm>();
  const navigate = useNavigate();

  const createMutation = useMutation({
    mutationFn: (values: ProcessGroupCreateForm) => createNifiProcessGroup(values.name.trim()),
    onSuccess: (createdGroup) => {
      const processGroupId = createdGroup.id;
      message.success("Processor Group을 생성했습니다.");
      form.resetFields();

      if (processGroupId) {
        navigate(`/etl/manage?processGroupId=${encodeURIComponent(processGroupId)}`);
      } else {
        navigate("/etl/manage");
      }
    },
    onError: (error) => {
      message.error(error instanceof Error ? error.message : "Processor Group 생성에 실패했습니다.");
    },
  });

  return (
    <div className="etl-create-page">
      <div className="etl-create-shell">
        <section className="etl-create-panel" aria-label="Processor Group 생성">
          <div className="etl-create-heading">
            <span className="etl-create-icon">
              <ApartmentOutlined />
            </span>
            <div>
              <Typography.Title level={3}>Processor Group</Typography.Title>
              <Typography.Text type="secondary">NiFi root canvas에 새 ETL 작업 공간을 생성합니다.</Typography.Text>
            </div>
          </div>

          <Form
            form={form}
            layout="vertical"
            requiredMark={false}
            onFinish={(values) => createMutation.mutate(values)}
            initialValues={{ name: "" }}
          >
            <Form.Item
              label="Group 이름"
              name="name"
              normalize={(value: string) => value?.trimStart()}
              rules={[
                { required: true, message: "Group 이름을 입력하세요." },
                { max: 128, message: "Group 이름은 128자 이내로 입력하세요." },
              ]}
            >
              <Input size="large" placeholder="예: customer-cdc-etl" autoFocus />
            </Form.Item>

            <Space>
              <Button
                type="primary"
                htmlType="submit"
                icon={<PlusOutlined />}
                loading={createMutation.isPending}
              >
                생성
              </Button>
              <Button onClick={() => navigate("/etl/manage")}>관리로 이동</Button>
            </Space>
          </Form>
        </section>
      </div>
    </div>
  );
}
