'use client';

import React, { useEffect, useState, useRef } from 'react';
import { Card, Typography, Spin, Badge, Button, Layout, Space, Tag, Row, Col, Statistic, Divider, Steps } from 'antd';
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

// Light theme for screenshots
const HC_LIGHT: Highcharts.Options = {
    chart: { backgroundColor: '#ffffff', style: { fontFamily: 'inherit' } },
    title: { text: '' },
    credits: { enabled: false },
    colors: ['#1677ff', '#52c41a', '#faad14', '#ff4d4f', '#722ed1', '#13c2c2', '#eb2f96'],
    legend: { itemStyle: { color: '#333' }, itemHoverStyle: { color: '#000' } },
    xAxis: { labels: { style: { color: '#333' } }, lineColor: '#ccc', tickColor: '#ccc' },
    yAxis: { labels: { style: { color: '#333' } }, gridLineColor: '#e0e0e0', title: { style: { color: '#333' } } },
    tooltip: { backgroundColor: '#ffffff', style: { color: '#333' }, borderColor: '#ccc' },
    plotOptions: { series: { animation: { duration: 800 } } }
};

const mergeHC = (opts: Highcharts.Options): Highcharts.Options => Highcharts.merge(HC_LIGHT, opts);

export default function PipelinePage() {
    const router = useRouter();
    const [logs, setLogs] = useState<string[]>([]);
    const [status, setStatus] = useState<'connecting' | 'running' | 'completed' | 'error'>('connecting');
    const [chartData, setChartData] = useState<DashboardData | null>(null);
    const logsContainerRef = useRef<HTMLDivElement>(null);

    interface PipelineStep {
        title: string;
        status: 'wait' | 'process' | 'finish' | 'error';
        duration: string;
    }

    const [pipelineSteps, setPipelineSteps] = useState<PipelineStep[]>([
        { title: 'Engine Initialization', status: 'wait', duration: '' },
        { title: 'HDFS Ingestion', status: 'wait', duration: '' },
        { title: 'Data Cleaning', status: 'wait', duration: '' },
        { title: 'Aggregation', status: 'wait', duration: '' },
        { title: 'Machine Learning', status: 'wait', duration: '' },
    ]);

    useEffect(() => {
        const el = logsContainerRef.current;
        if (el) el.scrollTop = el.scrollHeight;
    }, [logs]);

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

                        // Parse timing information
                        if (typeof parsed === 'string') {
                            const timeMatch = parsed.match(/--> \[Time\] Step (\d) \(.*?\) took: ([\d.]+)s/);
                            if (timeMatch) {
                                const stepIdx = parseInt(timeMatch[1]) - 1;
                                const duration = timeMatch[2];
                                setPipelineSteps(prev => {
                                    const next = [...prev];
                                    if (next[stepIdx]) {
                                        next[stepIdx] = { ...next[stepIdx], status: 'finish', duration: `${duration}s` };
                                        // Set next step to processing if exists
                                        if (next[stepIdx + 1]) {
                                            next[stepIdx + 1] = { ...next[stepIdx + 1], status: 'process' };
                                        }
                                    }
                                    return next;
                                });
                            }

                            // Update processing status based on phase markers
                            if (parsed.includes('[PHASE 1]')) setPipelineSteps(prev => prev.map((s, i) => i === 0 ? { ...s, status: 'process' } : s));
                            if (parsed.includes('[PHASE 2]')) setPipelineSteps(prev => prev.map((s, i) => i === 1 ? { ...s, status: 'process' } : s));
                            if (parsed.includes('[PHASE 3]')) setPipelineSteps(prev => prev.map((s, i) => i === 2 ? { ...s, status: 'process' } : s));
                            if (parsed.includes('[PHASE 4]')) setPipelineSteps(prev => prev.map((s, i) => i === 3 ? { ...s, status: 'process' } : s));
                            if (parsed.includes('[PHASE 5]')) setPipelineSteps(prev => prev.map((s, i) => i === 4 ? { ...s, status: 'process' } : s));
                        }
                    } catch { 
                        const logText = event.data;
                        setLogs(prev => [...prev, logText]);
                        
                        // Same parsing for plain text logs
                        const timeMatch = logText.match(/--> \[Time\] Step (\d) \(.*?\) took: ([\d.]+)s/);
                        if (timeMatch) {
                            const stepIdx = parseInt(timeMatch[1]) - 1; // Step numbers in logs are 1-based (1-5)
                            const duration = timeMatch[2];
                            setPipelineSteps(prev => {
                                const next = [...prev];
                                if (next[stepIdx]) {
                                    next[stepIdx] = { ...next[stepIdx], status: 'finish', duration: `${duration}s` };
                                    if (next[stepIdx + 1]) next[stepIdx + 1] = { ...next[stepIdx + 1], status: 'process' };
                                }
                                return next;
                            });
                        }
                        if (logText.includes('[PHASE 1]')) setPipelineSteps(prev => prev.map((s, i) => i === 0 ? { ...s, status: 'process' } : s));
                        if (logText.includes('[PHASE 2]')) setPipelineSteps(prev => prev.map((s, i) => i === 1 ? { ...s, status: 'process' } : s));
                        if (logText.includes('[PHASE 3]')) setPipelineSteps(prev => prev.map((s, i) => i === 2 ? { ...s, status: 'process' } : s));
                        if (logText.includes('[PHASE 4]')) setPipelineSteps(prev => prev.map((s, i) => i === 3 ? { ...s, status: 'process' } : s));
                        if (logText.includes('[PHASE 5]')) setPipelineSteps(prev => prev.map((s, i) => i === 4 ? { ...s, status: 'process' } : s));
                    }
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
        let color = '#333';
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
        chart: { type: 'pie', backgroundColor: '#ffffff', style: { fontFamily: 'inherit' } },
        plotOptions: {
            pie: {
                innerSize: '55%',
                dataLabels: { enabled: true, color: '#333', format: '<b>{point.name}</b>: {point.percentage:.1f}%' },
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
        tooltip: { backgroundColor: '#ffffff', borderColor: '#ccc', style: { color: '#333' }, pointFormat: '<b>{point.y:,.0f} min</b>' }
    }) : {};

    const originsSorted = chartData?.analysis.top_origin_delays
        .slice()
        .sort((a, b) => b.Avg_Dep_Delay_Minutes - a.Avg_Dep_Delay_Minutes) || [];

    const columnOptions = chartData ? mergeHC({
        chart: { type: 'column', backgroundColor: '#ffffff', style: { fontFamily: 'inherit' } },
        xAxis: { categories: originsSorted.map(r => r.ORIGIN), labels: { style: { color: '#333' } }, lineColor: '#ccc', tickColor: '#ccc' },
        yAxis: { title: { text: 'Avg Delay (min)', style: { color: '#333' } }, gridLineColor: '#e0e0e0', labels: { style: { color: '#333' } } },
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
        chart: { type: 'spline', backgroundColor: '#ffffff', style: { fontFamily: 'inherit' } },
        xAxis: { categories: monthlyData.map(r => MONTH_NAMES[r.Flight_Month]), labels: { style: { color: '#333' } }, lineColor: '#ccc', tickColor: '#ccc' },
        yAxis: { title: { text: 'Avg Delay (min)', style: { color: '#333' } }, gridLineColor: '#e0e0e0', labels: { style: { color: '#333' } } },
        series: [{
            type: 'spline',
            name: 'Avg Arr Delay',
            data: monthlyData.map(r => r.Avg_Arr_Delay_Minutes),
            color: '#52c41a',
            lineWidth: 3,
            marker: { fillColor: '#ffffff', lineWidth: 2, lineColor: '#52c41a', radius: 5 },
        }],
        tooltip: { valueSuffix: ' min/flight' },
    }) : {};

    const featureSorted = chartData?.machine_learning.feature_importance
        .slice()
        .sort((a, b) => a.impact - b.impact) || [];

    const barOptions = chartData ? mergeHC({
        chart: { type: 'bar', backgroundColor: '#ffffff', style: { fontFamily: 'inherit' } },
        xAxis: { categories: featureSorted.map(f => f.feature), labels: { style: { color: '#333' } }, lineColor: '#ccc', tickColor: '#ccc' },
        yAxis: { title: { text: 'Impact (%)', style: { color: '#333' } }, gridLineColor: '#e0e0e0', labels: { style: { color: '#333' } }, max: 100 },
        series: [{
            type: 'bar',
            name: 'Impact',
            data: featureSorted.map((f, i) => ({ y: Math.round(f.impact * 100) / 100, color: i === featureSorted.length - 1 ? '#faad14' : '#1677ff' })),
            borderRadius: 4,
        }],
        tooltip: { valueSuffix: '%' },
        legend: { enabled: false },
    }) : {};

    const CARD_STYLE = { background: '#ffffff', borderColor: '#d9d9d9' };
    const CARD_HEADER = { borderBottomColor: '#d9d9d9', background: '#fafafa' };

    return (
        <Layout style={{ minHeight: '100vh', background: '#f0f2f5' }}>
            <Header style={{ background: '#ffffff', borderBottom: '1px solid #d9d9d9', display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0 24px', position: 'sticky', top: 0, zIndex: 10 }}>
                <Space>
                    <CodeOutlined style={{ fontSize: 22, color: '#1677ff' }} />
                    <Title level={4} style={{ margin: 0, color: '#000' }}>Execution Terminal</Title>
                </Space>
                <Space size="large">
                    <div style={{ background: '#f0f0f0', height: 32, display: 'flex', alignItems: 'center', padding: '0 16px', borderRadius: 16, border: '1px solid #d9d9d9' }}>
                        {getStatusBadge()}
                    </div>
                    {status === 'completed' && (
                        <Button type="primary" ghost icon={<HomeOutlined />} onClick={() => router.push('/')}>Return Home</Button>
                    )}
                </Space>
            </Header>

            <Content style={{ padding: 24 }}>
                {/* Pipeline Wizard */}
                <Card style={{ ...CARD_STYLE, marginBottom: 24, borderRadius: 8 }} styles={{ body: { padding: '24px 40px' } }}>
                    <Steps
                        current={pipelineSteps.reduce((acc, s, i) => s.status !== 'wait' ? i : acc, 0)}
                        status={status === 'error' ? 'error' : status === 'completed' ? 'finish' : 'process'}
                        items={pipelineSteps.map((step, idx) => ({
                            title: <span style={{ color: step.status === 'wait' ? 'rgba(0,0,0,0.45)' : '#000' }}>{step.title}</span>,
                            subTitle: step.duration ? (
                                <Tag color="green" style={{ marginLeft: 8, borderRadius: 12, border: 'none' }}>{step.duration}</Tag>
                            ) : null,
                            status: step.status
                        }))}
                    />
                </Card>

                {/* Terminal */}
                <Card
                    title={<Space><SyncOutlined spin={status === 'running'} style={{ color: '#1677ff' }} /> Live PySpark Output</Space>}
                    style={{ background: '#ffffff', borderColor: '#d9d9d9', marginBottom: 24 }}
                    styles={{
                        header: { borderBottomColor: '#d9d9d9', background: '#fafafa' },
                        body: { overflowY: 'auto', maxHeight: chartData ? 390 : 'calc(100vh - 130px)', padding: 20, fontFamily: "Consolas, Monaco, monospace", fontSize: 13 }
                    }}
                >
                    <div ref={logsContainerRef} style={{ overflowY: 'auto', maxHeight: chartData ? 360 : 'calc(100vh - 160px)' }}>
                        {logs.length === 0 && <Spin description="Waiting for PySpark Context..." size="large" style={{ display: 'block', marginTop: 40 }} />}
                        {logs.map((log, i) => renderLog(log, i))}
                        {status === 'running' && <span style={{ display: 'inline-block', width: 8, height: 14, background: '#1677ff', animation: 'blink 1s step-end infinite' }} />}
                    </div>
                </Card>

                {/* Charts */}
                {chartData && (
                    <>
                        <Divider style={{ borderColor: '#d9d9d9' }}>
                            <Space>
                                <Title level={4} style={{ margin: 0, color: '#e6f4ff' }}>Analytics Dashboard</Title>
                            </Space>
                        </Divider>

                        {/* Stats */}
                        <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
                            <Col xs={24} sm={8}>
                                <Card style={CARD_STYLE} styles={{ body: { padding: 20 } }}>
                                    <Statistic title={<span style={{ color: '#8c8c8c' }}>ML Accuracy (Decision Tree)</span>}
                                        value={chartData.machine_learning.accuracy} precision={2} suffix="%" styles={{ content: { color: '#52c41a', fontSize: 30 } }} prefix={<FunctionOutlined />} />
                                </Card>
                            </Col>
                            <Col xs={24} sm={8}>
                                <Card style={CARD_STYLE} styles={{ body: { padding: 20 } }}>
                                    <Statistic title={<span style={{ color: '#8c8c8c' }}>Worst Delay Airport</span>}
                                        value={originsSorted[0]?.ORIGIN ?? '-'} suffix={`${originsSorted[0]?.Avg_Dep_Delay_Minutes ?? ''}m avg`}
                                        styles={{ content: { color: '#ff4d4f', fontSize: 30 } }} prefix={<RiseOutlined />} />
                                </Card>
                            </Col>
                            <Col xs={24} sm={8}>
                                <Card style={CARD_STYLE} styles={{ body: { padding: 20 } }}>
                                    <Statistic title={<span style={{ color: '#8c8c8c' }}>Top Delay Factor</span>}
                                        value={featureSorted.at(-1)?.feature ?? '-'} suffix={`${(featureSorted.at(-1)?.impact ?? 0).toFixed(1)}%`}
                                        styles={{ content: { color: '#faad14', fontSize: 30 } }} prefix={<DotChartOutlined />} />
                                </Card>
                            </Col>
                        </Row>

                        <Row gutter={[16, 16]}>
                            {/* Pie */}
                            <Col xs={24} lg={12}>
                                <Card title={<Title level={4} style={{ margin: 0, color: '#1677ff' }}>Delay Causes Breakdown</Title>} style={CARD_STYLE} styles={{ header: CARD_HEADER }}>
                                    <HighchartsReact highcharts={Highcharts} options={pieOptions} />
                                </Card>
                            </Col>
                            {/* Feature Importance Bar */}
                            <Col xs={24} lg={12}>
                                <Card title={<Title level={4} style={{ margin: 0, color: '#faad14' }}>ML Feature Importance</Title>} style={CARD_STYLE} styles={{ header: CARD_HEADER }}>
                                    <HighchartsReact highcharts={Highcharts} options={barOptions} />
                                </Card>
                            </Col>
                            {/* Top Airports Column */}
                            <Col xs={24}>
                                <Card title={<Title level={4} style={{ margin: 0, color: '#4096ff' }}>Top 10 Origin Airports — Worst Departure Delays</Title>} style={CARD_STYLE} styles={{ header: CARD_HEADER }}>
                                    <HighchartsReact highcharts={Highcharts} options={columnOptions} />
                                </Card>
                            </Col>
                            {/* Monthly Line */}
                            <Col xs={24}>
                                <Card title={<Title level={4} style={{ margin: 0, color: '#52c41a' }}>Average Arrival Delay by Month</Title>} style={CARD_STYLE} styles={{ header: CARD_HEADER }}>
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
