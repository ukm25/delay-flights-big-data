'use client';

import React from 'react';
import { Button, Typography } from 'antd';
import { PlayCircleOutlined, CodeOutlined } from '@ant-design/icons';
import { useRouter } from 'next/navigation';

const { Title, Text } = Typography;

export default function Home() {
  const router = useRouter();

  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 24 }}>
        <div style={{ textAlign: 'center', marginBottom: 24 }}>
          <CodeOutlined style={{ fontSize: 48, color: '#1677ff', marginBottom: 16 }} />
          <Title level={1} style={{ margin: 0 }}>Aviation Big Data Platform</Title>
          <Text type="secondary" style={{ fontSize: 16 }}>
            Powered by PySpark and Ant Design
          </Text>
        </div>

        <Button
          type="primary"
          size="large"
          icon={<PlayCircleOutlined />}
          onClick={() => router.push('/pipeline')}
          style={{ height: 64, fontSize: 20, padding: '0 48px', borderRadius: 32 }}
        >
          Execute Real-Time Pipeline
        </Button>
      </div>
    </div>
  );
}
