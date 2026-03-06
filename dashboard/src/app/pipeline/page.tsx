'use client';

import React, { useEffect, useState, useRef } from 'react';
import { Card, Typography, Spin, Badge, Button, Layout, Space, Tag, Row, Col, Statistic, Divider } from 'antd';
import {
    CodeOutlined, SyncOutlined, HomeOutlined,
    BarChartOutlined, RiseOutlined, DotChartOutlined, FunctionOutlined
} from '@ant-design/icons';
import { useRouter } from 'next/navigation';
import Highcharts from 'highcharts';
import HighchartsReact from 'highcharts-react-official';

const { Header, Content } = Layout;
const { Title } = Typography;

interface DashboardData {
    analysis: {
        delay_causes: Record<string, number>;
        top_origin_delays: Array<{ ORIGIN: string; ORIGIN_CITY_NAME: string; Total_Flights: number; Avg_Dep_Delay_Minutes: number }>;
        monthly_delays: Array<{ Flight_Month: number; Total_Flights: number; Avg_Arr_Delay_Minutes: number }>;
    };
    machine_learning: {
        accuracy: number;
        feature_importance: Array<{ feature: string; impact: number }>;
    };
}

const MONTH_NAMES: Record<number, string> = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
};

// Dark theme shared across all charts
const HC_DARK: Highcharts.Options = {
    chart: { backgroundColor: 'transparent', style: { fontFamily: 'inherit' } },
    title: { text: '' },
    credits: { enabled: false },
    colors: ['#1677ff', '#52c41a', '#faad14', '#ff4d4f', '#722ed1', '#13c2c2', '#eb2f96'],
    legend: { itemStyle: { color: '#aaa' }, itemHoverStyle: { color: '#fff' } },
    xAxis: { labels: { style: { color: '#aaa' } }, lineColor: '#303030', tickColor: '#303030' },
    yAxis: { labels: { style: { color: '#aaa' } }, gridLineColor: '#1f1f1f', title: { style: { color: '#aaa' } } },
    tooltip: { backgroundColor: '#1f1f1f', style: { color: '#fff' }, borderColor: '#303030' },
    plotOptions: { series: { animation: { duration: 800 } } }
};

const mergeHC = (opts: Highcharts.Options): Highcharts.Options => Highcharts.merge(HC_DARK, opts);

export default function PipelinePage() {
    const router = useRouter();
    const [logs, setLogs] = useState<string[]>([]);
    const [status, setStatus] = useState<'connecting' | 'running' | 'completed' | 'error'>('connecting');
    const [chartData, setChartData] = useState<DashboardData | null>(null);
    const logsEndRef = useRef<HTMLDivElement>(null);

    useEffect(() => { logsEndRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [logs]);

    useEffect(() => {
        let eventSource: EventSource;

        const startPipeline = () => {
            setStatus('running');
            setLogs(['[SYSTEM] Initializing connection to PySpark Engine via Docker...']);

            eventSource = new EventSource('/api/run');
            eventSource.onmessage = async (event) => {
                if (event.data === '[DONE]') {
                    setStatus('completed');
                    eventSource.close();
                } else if (event.data === '[CHART_READY]') {
                    try {
                        const res = await fetch('/dashboard_data.json?t=' + Date.now());
                        const data: DashboardData = await res.json();
                        setChartData(data);
                    } catch (e) { console.error('Failed to fetch chart data:', e); }
                } else {
                    try {
                        const parsed = JSON.parse(event.data);
                        setLogs(prev => [...prev, parsed]);
                    } catch { setLogs(prev => [...prev, event.data]); }
                }
            };
            eventSource.onerror = () => {
                setLogs(prev => [...prev, '[SYSTEM ERROR] Connection lost.']);
                setStatus('error');
                eventSource.close();
            };
        };

        startPipeline();
        return () => { if (eventSource) eventSource.close(); };
    }, []);

    const renderLog = (log: string, i: number) => {
        let color = 'rgba(255,255,255,0.65)';
        let fontWeight = 'normal';
        if (log.includes('[ERROR]') || log.includes('Exception')) color = '#ff4d4f';
        else if (log.includes('[SUCCESS]') || log.includes('[DONE]')) { color = '#52c41a'; fontWeight = 'bold'; }
        else if (log.includes('[PHASE')) { color = '#1677ff'; fontWeight = 'bold'; }
        else if (log.includes('[Task')) color = '#13c2c2';
        else if (log.includes('-->') || log.includes('- Impact of')) color = '#faad14';
        else if (log.includes('---')) color = '#722ed1';
        return <div key={i} style={{ color, fontWeight, whiteSpace: 'pre-wrap', wordBreak: 'break-all', marginBottom: 4 }}>{log}</div>;
    };

    const getStatusBadge = () => {
        if (status === 'running') return <Badge status="processing" text={<span style={{ color: '#1677ff' }}>Running</span>} />;
        if (status === 'completed') return <Badge status="success" text={<span style={{ color: '#52c41a' }}>Completed</span>} />;
        if (status === 'error') return <Badge status="error" text={<span style={{ color: '#ff4d4f' }}>Error</span>} />;
        return <Badge status="warning" text={<span style={{ color: '#faad14' }}>Connecting...</span>} />;
    };

    // ──── Highcharts options ────
    const pieOptions = chartData ? mergeHC({
        chart: { type: 'pie', backgroundColor: 'transparent', style: { fontFamily: 'inherit' } },
        plotOptions: {
            pie: {
                innerSize: '55%',
                dataLabels: { enabled: true, color: '#ccc', format: '<b>{point.name}</b>: {point.percentage:.1f}%' },
                showInLegend: true,
                borderWidth: 0,
            }
        },
        series: [{
            type: 'pie',
            name: 'Delay (min)',
            data: [
                { name: 'Late Aircraft', y: chartData.analysis.delay_causes.Late_Aircraft_Error || 0 },
                { name: 'Carrier', y: chartData.analysis.delay_causes.Carrier_Error || 0 },
                { name: 'NAS', y: chartData.analysis.delay_causes.NAS_Error || 0 },
                { name: 'Weather', y: chartData.analysis.delay_causes.Weather_Error || 0 },
                { name: 'Security', y: chartData.analysis.delay_causes.Security_Error || 0 },
            ]
        }],
        tooltip: { backgroundColor: '#1f1f1f', borderColor: '#303030', style: { color: '#fff' }, pointFormat: '<b>{point.y:,.0f} min</b>' }
    }) : {};

    const originsSorted = chartData?.analysis.top_origin_delays
        .slice()
        .sort((a, b) => b.Avg_Dep_Delay_Minutes - a.Avg_Dep_Delay_Minutes) || [];

    const columnOptions = chartData ? mergeHC({
        chart: { type: 'column', backgroundColor: 'transparent', style: { fontFamily: 'inherit' } },
        xAxis: { categories: originsSorted.map(r => r.ORIGIN), labels: { style: { color: '#aaa' } }, lineColor: '#303030', tickColor: '#303030' },
        yAxis: { title: { text: 'Avg Delay (min)', style: { color: '#aaa' } }, gridLineColor: '#1f1f1f', labels: { style: { color: '#aaa' } } },
        series: [{
            type: 'column',
            name: 'Avg Dep Delay',
            data: originsSorted.map(r => ({ y: r.Avg_Dep_Delay_Minutes })),
            color: { linearGradient: { x1: 0, x2: 0, y1: 0, y2: 1 }, stops: [[0, '#1677ff'], [1, '#0a2a5c']] },
            borderRadius: 4,
        }],
        tooltip: { valueSuffix: ' min' },
        legend: { enabled: false },
    }) : {};

    const monthlyData = chartData?.analysis.monthly_delays
        .slice()
        .sort((a, b) => a.Flight_Month - b.Flight_Month) || [];

    const lineOptions = chartData ? mergeHC({
        chart: { type: 'spline', backgroundColor: 'transparent', style: { fontFamily: 'inherit' } },
        xAxis: { categories: monthlyData.map(r => MONTH_NAMES[r.Flight_Month]), labels: { style: { color: '#aaa' } }, lineColor: '#303030', tickColor: '#303030' },
        yAxis: { title: { text: 'Avg Delay (min)', style: { color: '#aaa' } }, gridLineColor: '#1f1f1f', labels: { style: { color: '#aaa' } } },
        series: [{
            type: 'spline',
            name: 'Avg Arr Delay',
            data: monthlyData.map(r => r.Avg_Arr_Delay_Minutes),
            color: '#52c41a',
            lineWidth: 3,
            marker: { fillColor: '#141414', lineWidth: 2, lineColor: '#52c41a', radius: 5 },
        }],
        tooltip: { valueSuffix: ' min/flight' },
    }) : {};

    const featureSorted = chartData?.machine_learning.feature_importance
        .slice()
        .sort((a, b) => a.impact - b.impact) || [];

    const barOptions = chartData ? mergeHC({
        chart: { type: 'bar', backgroundColor: 'transparent', style: { fontFamily: 'inherit' } },
        xAxis: { categories: featureSorted.map(f => f.feature), labels: { style: { color: '#aaa' } }, lineColor: '#303030', tickColor: '#303030' },
        yAxis: { title: { text: 'Impact (%)', style: { color: '#aaa' } }, gridLineColor: '#1f1f1f', labels: { style: { color: '#aaa' } }, max: 100 },
        series: [{
            type: 'bar',
            name: 'Impact',
            data: featureSorted.map((f, i) => ({ y: Math.round(f.impact * 100) / 100, color: i === featureSorted.length - 1 ? '#faad14' : '#1677ff' })),
            borderRadius: 4,
        }],
        tooltip: { valueSuffix: '%' },
        legend: { enabled: false },
    }) : {};

    const CARD_STYLE = { background: '#1a1a1a', borderColor: '#303030' };
    const CARD_HEADER = { borderBottomColor: '#303030', background: '#141414' };

    return (
        <Layout style={{ minHeight: '100vh', background: '#141414' }}>
            <Header style={{ background: '#141414', borderBottom: '1px solid #303030', display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0 24px', position: 'sticky', top: 0, zIndex: 10 }}>
                <Space>
                    <CodeOutlined style={{ fontSize: 22, color: '#1677ff' }} />
                    <Title level={4} style={{ margin: 0 }}>Execution Terminal</Title>
                </Space>
                <Space size="large">
                    <Tag style={{ border: '1px solid #303030', background: '#1f1f1f' }}>
                        <span style={{ fontFamily: 'monospace' }}>final_project.py</span>
                    </Tag>
                    <div style={{ background: '#1f1f1f', padding: '4px 16px', borderRadius: 16, border: '1px solid #303030' }}>
                        {getStatusBadge()}
                    </div>
                    {status === 'completed' && (
                        <Button type="primary" ghost icon={<HomeOutlined />} onClick={() => router.push('/')}>Return Home</Button>
                    )}
                </Space>
            </Header>

            <Content style={{ padding: 24 }}>
                {/* Terminal */}
                <Card
                    title={<Space><SyncOutlined spin={status === 'running'} style={{ color: '#1677ff' }} /> Live PySpark Output</Space>}
                    style={{ background: '#000', borderColor: '#303030', marginBottom: 24 }}
                    styles={{
                        header: { borderBottomColor: '#303030', background: '#141414' },
                        body: { overflowY: 'auto', maxHeight: chartData ? 260 : 'calc(100vh - 130px)', padding: 20, fontFamily: "Consolas, Monaco, monospace", fontSize: 13 }
                    }}
                >
                    {logs.length === 0 && <Spin description="Waiting for PySpark Context..." size="large" style={{ display: 'block', marginTop: 40 }} />}
                    {logs.map((log, i) => renderLog(log, i))}
                    {status === 'running' && <span style={{ display: 'inline-block', width: 8, height: 14, background: '#1677ff', animation: 'blink 1s step-end infinite' }} />}
                    <div ref={logsEndRef} />
                </Card>

                {/* Charts */}
                {chartData && (
                    <>
                        <Divider style={{ borderColor: '#303030' }}>
                            <Space>
                                <BarChartOutlined style={{ color: '#1677ff' }} />
                                <Title level={4} style={{ margin: 0, color: '#e6f4ff' }}>Analytics Dashboard</Title>
                            </Space>
                        </Divider>

                        {/* Stats */}
                        <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
                            <Col xs={24} sm={8}>
                                <Card style={CARD_STYLE} styles={{ body: { padding: 20 } }}>
                                    <Statistic title={<span style={{ color: '#8c8c8c' }}>ML Accuracy (Decision Tree)</span>}
                                        value={chartData.machine_learning.accuracy} precision={2} suffix="%" valueStyle={{ color: '#52c41a', fontSize: 30 }} prefix={<FunctionOutlined />} />
                                </Card>
                            </Col>
                            <Col xs={24} sm={8}>
                                <Card style={CARD_STYLE} styles={{ body: { padding: 20 } }}>
                                    <Statistic title={<span style={{ color: '#8c8c8c' }}>Worst Delay Airport</span>}
                                        value={originsSorted[0]?.ORIGIN ?? '-'} suffix={`${originsSorted[0]?.Avg_Dep_Delay_Minutes ?? ''}m avg`}
                                        valueStyle={{ color: '#ff4d4f', fontSize: 30 }} prefix={<RiseOutlined />} />
                                </Card>
                            </Col>
                            <Col xs={24} sm={8}>
                                <Card style={CARD_STYLE} styles={{ body: { padding: 20 } }}>
                                    <Statistic title={<span style={{ color: '#8c8c8c' }}>Top Delay Factor</span>}
                                        value={featureSorted.at(-1)?.feature ?? '-'} suffix={`${(featureSorted.at(-1)?.impact ?? 0).toFixed(1)}%`}
                                        valueStyle={{ color: '#faad14', fontSize: 30 }} prefix={<DotChartOutlined />} />
                                </Card>
                            </Col>
                        </Row>

                        <Row gutter={[16, 16]}>
                            {/* Pie */}
                            <Col xs={24} lg={12}>
                                <Card title="🥧 Delay Causes Breakdown" style={CARD_STYLE} styles={{ header: CARD_HEADER }}>
                                    <HighchartsReact highcharts={Highcharts} options={pieOptions} />
                                </Card>
                            </Col>
                            {/* Feature Importance Bar */}
                            <Col xs={24} lg={12}>
                                <Card title="🤖 ML Feature Importance" style={CARD_STYLE} styles={{ header: CARD_HEADER }}>
                                    <HighchartsReact highcharts={Highcharts} options={barOptions} />
                                </Card>
                            </Col>
                            {/* Top Airports Column */}
                            <Col xs={24}>
                                <Card title="🛫 Top 10 Origin Airports — Worst Departure Delays" style={CARD_STYLE} styles={{ header: CARD_HEADER }}>
                                    <HighchartsReact highcharts={Highcharts} options={columnOptions} />
                                </Card>
                            </Col>
                            {/* Monthly Line */}
                            <Col xs={24}>
                                <Card title="📈 Average Arrival Delay by Month" style={CARD_STYLE} styles={{ header: CARD_HEADER }}>
                                    <HighchartsReact highcharts={Highcharts} options={lineOptions} />
                                </Card>
                            </Col>
                        </Row>
                    </>
                )}
            </Content>

            <style dangerouslySetInnerHTML={{ __html: `@keyframes blink { 0%,100% { opacity:1; } 50% { opacity:0; } }` }} />
        </Layout>
    );
}
