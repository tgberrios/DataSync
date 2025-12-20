import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import {
  Container,
  Header,
  Section,
  SectionTitle,
  Grid,
  Value,
  ErrorMessage,
  Button,
} from "../components/shared/BaseComponents";
import { dashboardApi, configApi } from "../services/api";
import type {
  DashboardStats,
  BatchConfig,
  CurrentlyProcessing,
} from "../services/api";
import { extractApiError } from "../utils/errorHandler";
import styled from "styled-components";
import { theme } from "../theme/theme";

const ProgressBar = styled.div<{ $progress: number }>`
  width: 100%;
  height: 24px;
  background-color: #e0e0e0;
  margin: 10px 0;
  border-radius: 12px;
  overflow: hidden;
  box-shadow: inset 0 2px 4px rgba(0, 0, 0, 0.1);
  position: relative;

  &::after {
    content: "";
    display: block;
    width: ${(props) => props.$progress}%;
    height: 100%;
    background: linear-gradient(
      90deg,
      ${theme.colors.primary.main} 0%,
      ${theme.colors.primary.light} 50%,
      ${theme.colors.primary.dark} 100%
    );
    border-radius: 12px;
    animation: progressBar 0.6s ease-out;
    box-shadow: 0 0 10px rgba(13, 27, 42, 0.4);
    position: relative;

    &::after {
      content: "";
      position: absolute;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      background: linear-gradient(
        90deg,
        transparent,
        rgba(255, 255, 255, 0.2),
        transparent
      );
      animation: shimmer 2s infinite;
    }
  }
`;

const ChartContainer = styled.div`
  width: 100%;
  margin-top: 20px;
  padding: 25px;
  background: ${theme.colors.background.main};
  border-radius: ${theme.borderRadius.lg};
  box-shadow: ${theme.shadows.md};
  color: ${theme.colors.text.primary};
  overflow: visible;
  position: relative;
  animation: fadeIn 0.6s cubic-bezier(0.4, 0, 0.2, 1);
  transition: box-shadow 0.3s ease, transform 0.3s ease;
  
  &:hover {
    box-shadow: ${theme.shadows.lg};
    transform: translateY(-2px);
  }
  
  @keyframes fadeIn {
    from {
      opacity: 0;
      transform: translateY(15px) scale(0.98);
    }
    to {
      opacity: 1;
      transform: translateY(0) scale(1);
    }
  }
`;

const ChartTitle = styled.div`
  font-weight: bold;
  margin-bottom: 20px;
  color: ${theme.colors.text.primary};
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 1px;
  display: flex;
  gap: 24px;
  flex-wrap: wrap;
  animation: fadeInDown 0.5s ease-out;
  
  @keyframes fadeInDown {
    from {
      opacity: 0;
      transform: translateY(-5px);
    }
    to {
      opacity: 1;
      transform: translateY(0);
    }
  }
`;

const SVGChart = styled.svg`
  width: 100%;
  height: 100%;
  display: block;
  min-height: 400px;
`;

const ChartArea = styled.div`
  width: 100%;
  height: 500px;
  min-height: 400px;
  position: relative;
  background: ${theme.colors.background.main};
  border-radius: ${theme.borderRadius.md};
  padding: 20px;
  overflow: visible;
`;

const LegendItem = styled.label<{ $lineStyle: string; $active: boolean }>`
  display: inline-flex;
  align-items: center;
  gap: 8px;
  font-size: 10px;
  color: ${(props) => (props.$active ? theme.colors.text.primary : theme.colors.text.light)};
  transition: all 0.3s ease;
  padding: 6px 10px;
  border-radius: ${theme.borderRadius.sm};
  cursor: pointer;
  user-select: none;
  border: 1px solid ${(props) => (props.$active ? theme.colors.border.medium : "transparent")};
  background: ${(props) => (props.$active ? theme.colors.background.secondary : "transparent")};
  
  &:hover {
    background: ${theme.colors.background.secondary};
    color: ${theme.colors.text.primary};
    transform: translateY(-1px);
    border-color: ${theme.colors.border.medium};
  }
`;

const Checkbox = styled.input`
  width: 14px;
  height: 14px;
  cursor: pointer;
  accent-color: ${theme.colors.status.info.text};
  
  &:checked {
    accent-color: ${theme.colors.status.info.text};
  }
`;

const ShowAllButton = styled.button`
  margin-left: auto;
  padding: 6px 12px;
  font-size: 10px;
  background: ${theme.colors.background.secondary};
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.sm};
  color: ${theme.colors.text.primary};
  cursor: pointer;
  transition: all 0.3s ease;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  font-weight: 500;
  
  &:hover {
    background: ${theme.colors.primary.main};
    color: ${theme.colors.text.white};
    border-color: ${theme.colors.primary.main};
    transform: translateY(-1px);
  }
  
  &:active {
    transform: translateY(0);
  }
`;

const LegendLine = styled.span<{ $color: string; $active: boolean }>`
  display: inline-block;
  width: 24px;
  height: 0;
  opacity: ${(props) => (props.$active ? 1 : 0.4)};
  transition: opacity 0.3s ease;
  border-top: 2px solid;
  border-color: ${(props) => props.$color};
`;

const GridLine = styled.line`
  stroke: ${theme.colors.border.light};
  stroke-width: 1;
  opacity: 0.2;
`;

const AxisLabel = styled.text`
  font-size: 10px;
  fill: ${theme.colors.text.secondary};
  font-family: system-ui, -apple-system, sans-serif;
`;

const AxisLine = styled.line`
  stroke: ${theme.colors.border.medium};
  stroke-width: 1.5;
  opacity: 0.6;
`;

const InProgressBadge = styled.span`
  background: linear-gradient(
    90deg,
    ${theme.colors.primary.main} 0%,
    ${theme.colors.primary.light} 50%,
    ${theme.colors.primary.dark} 100%
  );
  color: white;
  padding: 2px 8px;
  border-radius: 12px;
  font-size: 0.8em;
  font-weight: bold;
  display: inline-block;
  box-shadow: 0 0 8px rgba(13, 27, 42, 0.3);
  transition: all 0.3s ease;
  
  &:hover {
    box-shadow: 0 0 12px rgba(13, 27, 42, 0.5);
    transform: translateY(-1px);
  }
`;

const ChartPath = styled.path`
  transition: d 0.6s cubic-bezier(0.25, 0.46, 0.45, 0.94);
  will-change: d;
  opacity: 1;
  vector-effect: non-scaling-stroke;
`;

const ChartAreaPath = styled.path`
  transition: d 0.6s cubic-bezier(0.25, 0.46, 0.45, 0.94);
  will-change: d;
  opacity: 1;
`;


const LineChart = ({
  datasets,
  labels,
}: {
  datasets: Array<{ data: number[]; symbol: string; name: string }>;
  labels: string[];
}) => {
  const chartRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 800, height: 460 });

  useEffect(() => {
    const updateDimensions = () => {
      if (chartRef.current) {
        const rect = chartRef.current.getBoundingClientRect();
        setDimensions({
          width: Math.max(rect.width, 800),
          height: Math.max(rect.height, 460),
        });
      }
    };

    updateDimensions();
    const resizeObserver = new ResizeObserver(updateDimensions);
    if (chartRef.current) {
      resizeObserver.observe(chartRef.current);
    }
    window.addEventListener("resize", updateDimensions);
    return () => {
      resizeObserver.disconnect();
      window.removeEventListener("resize", updateDimensions);
    };
  }, []);


  if (datasets.length === 0 || datasets[0].data.length === 0) {
    return (
      <ChartArea ref={chartRef}>
        <div style={{ textAlign: "center", padding: "100px 0", color: theme.colors.text.secondary }}>
          No data available
        </div>
      </ChartArea>
    );
  }

  const padding = { top: 20, right: 40, bottom: 40, left: 60 };
  const chartWidth = dimensions.width - padding.left - padding.right;
  const chartHeight = dimensions.height - padding.top - padding.bottom;

  const allValues = datasets.flatMap((d) => d.data);
  const min = Math.min(...allValues);
  const max = Math.max(...allValues);
  const range = max - min || 1;

  // Mapeo de colores por nombre de métrica - coherentes con la paleta del tema
  const colorMap: Record<string, string> = {
    "CPU Usage (%)": "#1976d2", // Azul principal
    "Memory (%)": "#0d47a1", // Azul oscuro
    "Network (IOPS)": "#1565c0", // Azul medio
    "Throughput (RPS)": "#424242", // Gris oscuro
  };
  // Todas las líneas con el mismo estilo y grosor
  const lineWidth = 2;

  const normalizeY = (value: number) => {
    return chartHeight - ((value - min) / range) * chartHeight;
  };

  const getX = (index: number) => {
    return (index / (labels.length - 1)) * chartWidth;
  };

  const createPath = (data: number[]): string => {
    if (data.length === 0) return "";
    if (data.length === 1) {
      const x = getX(0);
      const y = normalizeY(data[0]);
      return `M ${x} ${y}`;
    }

    const points = data.map((value, index) => ({
      x: getX(index),
      y: normalizeY(value),
    }));

    let path = `M ${points[0].x} ${points[0].y}`;

    for (let i = 0; i < points.length - 1; i++) {
      const current = points[i];
      const next = points[i + 1];
      const afterNext = points[i + 2];

      if (afterNext) {
        const cp1x = current.x + (next.x - current.x) / 2;
        const cp1y = current.y;
        const cp2x = next.x - (afterNext.x - next.x) / 2;
        const cp2y = next.y;
        path += ` C ${cp1x} ${cp1y}, ${cp2x} ${cp2y}, ${next.x} ${next.y}`;
      } else {
        const cp1x = current.x + (next.x - current.x) / 2;
        const cp1y = current.y;
        const cp2x = next.x;
        const cp2y = next.y;
        path += ` C ${cp1x} ${cp1y}, ${cp2x} ${cp2y}, ${next.x} ${next.y}`;
      }
    }

    return path;
  };

  const yAxisLabels: number[] = [];
  const yAxisCount = 8;
  for (let i = 0; i <= yAxisCount; i++) {
    const value = max - (range / yAxisCount) * i;
    yAxisLabels.push(value);
  }

  const xAxisLabels: string[] = [];
  const xAxisCount = 6;
  for (let i = 0; i <= xAxisCount; i++) {
    const idx = Math.floor((i / xAxisCount) * (labels.length - 1));
    xAxisLabels.push(labels[idx] || "");
  }

  return (
    <ChartArea ref={chartRef}>
      <SVGChart viewBox={`0 0 ${dimensions.width} ${dimensions.height}`} preserveAspectRatio="none">
        <defs>
          {datasets.map((dataset, idx) => {
            const color = colorMap[dataset.name] || theme.colors.status.info.text;
            return (
              <linearGradient key={idx} id={`gradient-${idx}`} x1="0%" y1="0%" x2="0%" y2="100%">
                <stop offset="0%" stopColor={color} stopOpacity="0.15" />
                <stop offset="100%" stopColor={color} stopOpacity="0" />
              </linearGradient>
            );
          })}
        </defs>
        <g transform={`translate(${padding.left}, ${padding.top})`}>
          {yAxisLabels.map((value, i) => {
            const y = normalizeY(value);
            return (
              <g key={`y-axis-${i}`}>
                <GridLine x1={0} y1={y} x2={chartWidth} y2={y} />
                <AxisLabel x={-10} y={y + 4} textAnchor="end">
                  {value.toFixed(1)}
                </AxisLabel>
              </g>
            );
          })}

          {xAxisLabels.map((label, i) => {
            const x = (i / xAxisCount) * chartWidth;
            return (
              <g key={`x-axis-${i}`}>
                <AxisLabel x={x} y={chartHeight + 20} textAnchor="middle">
                  {label.length > 5 ? label.substring(0, 5) : label}
                </AxisLabel>
              </g>
            );
          })}

          <AxisLine x1={0} y1={0} x2={0} y2={chartHeight} />
          <AxisLine x1={0} y1={chartHeight} x2={chartWidth} y2={chartHeight} />

          {datasets.map((dataset, idx) => {
            const path = createPath(dataset.data);
            const areaPath = path + ` L ${getX(dataset.data.length - 1)} ${chartHeight} L ${getX(0)} ${chartHeight} Z`;
            const color = colorMap[dataset.name] || theme.colors.status.info.text;
            // Key estable basada en el nombre para mantener la identidad de la línea
            const pathKey = `${dataset.name}-${idx}`;

            return (
              <g key={pathKey}>
                <ChartAreaPath
                  as="path"
                  d={areaPath}
                  fill={`url(#gradient-${idx})`}
                />
                <ChartPath
                  as="path"
                  d={path}
                  fill="none"
                  stroke={color}
                  strokeWidth={lineWidth}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </g>
            );
          })}
        </g>
      </SVGChart>
    </ChartArea>
  );
};

const Dashboard = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [batchConfig, setBatchConfig] = useState<BatchConfig>({
    key: "chunk_size",
    value: "25000",
    description: "Tamaño de lote para procesamiento de datos",
    updated_at: new Date().toISOString(),
  });
  const [currentlyProcessing, setCurrentlyProcessing] =
    useState<CurrentlyProcessing[]>([]);
  const [processingPagination, setProcessingPagination] = useState<{
    page: number;
    limit: number;
    total: number;
    totalPages: number;
    hasNext: boolean;
    hasPrev: boolean;
  } | null>(null);
  const [processingPage, setProcessingPage] = useState(1);
  const [isProcessingExpanded, setIsProcessingExpanded] = useState(false);
  const isMountedRef = useRef(true);
  const isInitialLoadRef = useRef(true);
  const [resourceHistory, setResourceHistory] = useState<{
    timestamp: string[];
    cpuUsage: number[];
    memoryPercentage: number[];
    network: number[];
    throughput: number[];
  }>({
    timestamp: [],
    cpuUsage: [],
    memoryPercentage: [],
    network: [],
    throughput: [],
  });
  const [visibleLines, setVisibleLines] = useState<{
    cpu: boolean;
    memory: boolean;
    network: boolean;
    throughput: boolean;
  }>({
    cpu: true,
    memory: true,
    network: true,
    throughput: true,
  });

  const [stats, setStats] = useState<DashboardStats>({
    syncStatus: {
      progress: 75,
      listeningChanges: 6,
      pending: 0,
      fullLoadActive: 0,
      fullLoadInactive: 0,
      noData: 3,
      skip: 0,
      errors: 0,
      inProgress: 0,
      currentProcess: "dbo.test_performance (NO_DATA)",
    },
    systemResources: {
      cpuUsage: "0.0",
      memoryUsed: "12.90",
      memoryTotal: "30.54",
      memoryPercentage: "42.2",
      rss: "12.90",
      virtual: "19.35",
    },
    dbHealth: {
      activeConnections: "1/100",
      connectionPercentage: "0.0",
      responseTime: "< 1ms",
      bufferHitRate: "0.0",
      cacheHitRate: "0.0",
      status: "Healthy",
      uptimeSeconds: 0,
      activeQueries: 0,
      waitingQueries: 0,
      avgQueryDuration: 0,
      databaseSizeBytes: 0,
      queryEfficiencyScore: 0,
      longRunningQueries: 0,
      blockingQueries: 0,
      totalQueries24h: 0,
    },
    batchConfig: {
      key: "chunk_size",
      value: "25000",
      description: "Tamaño de lote para procesamiento de datos",
      updated_at: new Date().toISOString(),
    },
  });

  /**
   * Formatea un número con separadores de miles
   *
   * @param {number} num - Número a formatear
   * @returns {string} Número formateado con comas
   */
  const formatNumberWithCommas = useCallback((num: number): string => {
    return num.toLocaleString("en-US");
  }, []);

  /**
   * Calcula el porcentaje de progreso basado en tablas escuchando cambios vs activas
   *
   * @returns {number} Porcentaje de progreso (0-100)
   */
  const progressPercentage = useMemo(() => {
    if (
      !stats.syncStatus.fullLoadActive ||
      stats.syncStatus.fullLoadActive === 0
    ) {
      return 0;
    }
    const listening = stats.syncStatus.listeningChanges || 0;
    const active = stats.syncStatus.fullLoadActive || 0;
    return Math.min(100, Math.round((listening / active) * 100));
  }, [
    stats.syncStatus.listeningChanges,
    stats.syncStatus.fullLoadActive,
  ]);

  const fetchSystemLogsHistory = useCallback(async () => {
    try {
      if (!isMountedRef.current) return;
      const logs = await dashboardApi.getSystemLogs(60);
      if (isMountedRef.current && logs.length > 0) {
        setResourceHistory({
          timestamp: logs.map((log) => log.timestamp),
          cpuUsage: logs.map((log) => log.cpuUsage),
          memoryPercentage: logs.map((log) => log.memoryPercentage),
          network: logs.map((log) => log.network),
          throughput: logs.map((log) => log.throughput),
        });
      }
    } catch (err) {
      if (isMountedRef.current) {
        console.error("Error fetching system logs history:", err);
      }
    }
  }, []);

  const fetchStats = useCallback(async () => {
    try {
      if (!isMountedRef.current) return;
      if (isInitialLoadRef.current) {
        setLoading(true);
        setError(null);
        await fetchSystemLogsHistory();
      }
      const [dashboardData, batchData] = await Promise.all([
        dashboardApi.getDashboardStats(),
        configApi.getBatchConfig(),
      ]);
      if (isMountedRef.current) {
        setStats(dashboardData);
        setBatchConfig(batchData);
        if (isInitialLoadRef.current) {
          isInitialLoadRef.current = false;
        }
        setError(null);
      }
    } catch (err) {
      if (isMountedRef.current) {
        if (isInitialLoadRef.current) {
          setError(extractApiError(err));
        } else {
          console.error("Error fetching dashboard stats:", err);
        }
      }
    } finally {
      if (isMountedRef.current) {
        setLoading(false);
      }
    }
  }, [fetchSystemLogsHistory]);

  const fetchSyncStats = useCallback(async () => {
    try {
      if (!isMountedRef.current) return;
      const dashboardData = await dashboardApi.getDashboardStats();
      if (isMountedRef.current) {
        setStats((prev) => ({
          ...prev,
          syncStatus: dashboardData.syncStatus,
        }));
      }
    } catch (err) {
      if (isMountedRef.current) {
        console.error("Error fetching sync stats:", err);
      }
    }
  }, []);

  const fetchSystemResources = useCallback(async () => {
    try {
      if (!isMountedRef.current) return;
      const dashboardData = await dashboardApi.getDashboardStats();
      if (isMountedRef.current) {
        setStats((prev) => ({
          ...prev,
          systemResources: dashboardData.systemResources,
        }));
        const now = new Date();
        const timeLabel = `${now.getHours().toString().padStart(2, "0")}:${now.getMinutes().toString().padStart(2, "0")}:${now.getSeconds().toString().padStart(2, "0")}`;
        const networkValue = dashboardData.metricsCards?.currentIops || 0;
        const throughputValue = dashboardData.metricsCards?.currentThroughput?.avgRps || 0;
        
        setResourceHistory((prev) => {
          const maxPoints = 60;
          const newHistory = {
            timestamp: [...prev.timestamp, timeLabel].slice(-maxPoints),
            cpuUsage: [
              ...prev.cpuUsage,
              parseFloat(dashboardData.systemResources.cpuUsage) || 0,
            ].slice(-maxPoints),
            memoryPercentage: [
              ...prev.memoryPercentage,
              parseFloat(dashboardData.systemResources.memoryPercentage) || 0,
            ].slice(-maxPoints),
            network: [
              ...prev.network,
              networkValue,
            ].slice(-maxPoints),
            throughput: [
              ...prev.throughput,
              throughputValue,
            ].slice(-maxPoints),
          };
          return newHistory;
        });
      }
    } catch (err) {
      if (isMountedRef.current) {
        console.error("Error fetching system resources:", err);
      }
    }
  }, []);

  /**
   * Obtiene la tabla que se está procesando actualmente
   *
   * @returns {Promise<void>}
   */
  const fetchCurrentlyProcessing = useCallback(async () => {
    try {
      if (!isMountedRef.current) return;
      const response = await dashboardApi.getCurrentlyProcessing(processingPage);
      if (isMountedRef.current) {
        setCurrentlyProcessing(response.data);
        setProcessingPagination(response.pagination);
      }
    } catch (err) {
      if (isMountedRef.current) {
        console.error("Error fetching currently processing tables:", err);
      }
    }
  }, [processingPage]);

  /**
   * Maneja el reintento de carga de datos después de un error
   *
   * @returns {Promise<void>}
   */
  const handleRetry = useCallback(async () => {
    setError(null);
    await fetchStats();
  }, [fetchStats]);

  useEffect(() => {
    isMountedRef.current = true;
    isInitialLoadRef.current = true;
    fetchStats();
    fetchCurrentlyProcessing();

    const syncStatsInterval = setInterval(fetchSyncStats, 500);
    const processingInterval = setInterval(fetchCurrentlyProcessing, 500);
    const systemResourcesInterval = setInterval(fetchSystemResources, 10000);

    return () => {
      isMountedRef.current = false;
      clearInterval(syncStatsInterval);
      clearInterval(processingInterval);
      clearInterval(systemResourcesInterval);
    };
  }, [fetchStats, fetchSyncStats, fetchCurrentlyProcessing, fetchSystemResources, processingPage]);

  if (loading) {
    return (
      <Container>
        <div style={{ textAlign: "center", padding: "20px" }}>
          Loading dashboard data...
        </div>
      </Container>
    );
  }

  if (error) {
    return (
      <Container>
        <ErrorMessage>
          <div style={{ fontWeight: "bold", marginBottom: "10px" }}>
            Error al cargar datos:
          </div>
          <div>{error}</div>
          <Button $variant="primary" onClick={handleRetry} style={{ marginTop: "10px" }}>
            Reintentar
          </Button>
        </ErrorMessage>
      </Container>
    );
  }

  return (
    <Container>
      <Header>DataSync Real-Time Dashboard</Header>

      <Section>
        <SectionTitle>■ SYNCHRONIZATION STATUS</SectionTitle>
        <ProgressBar $progress={progressPercentage} />
        <Grid>
          <Value>Listening Changes: {stats.syncStatus.listeningChanges || 0}</Value>
          <Value>Pending: {stats.syncStatus.pending || 0}</Value>
          <Value>Active: {stats.syncStatus.fullLoadActive || 0}</Value>
          <Value>Inactive: {stats.syncStatus.fullLoadInactive || 0}</Value>
          <Value>No Data: {stats.syncStatus.noData || 0}</Value>
          <Value>Skip: {stats.syncStatus.skip || 0}</Value>
          <Value>Errors: {stats.syncStatus.errors || 0}</Value>
          <Value>In Progress: {stats.syncStatus.inProgress || 0}</Value>
          <Value>Total Tables: {(stats.syncStatus.listeningChanges || 0) + (stats.syncStatus.pending || 0) + (stats.syncStatus.fullLoadActive || 0) + (stats.syncStatus.fullLoadInactive || 0) + (stats.syncStatus.noData || 0) + (stats.syncStatus.skip || 0) + (stats.syncStatus.inProgress || 0)}</Value>
        </Grid>

        <div style={{ marginTop: "20px" }}>
          <SectionTitle style={{ fontSize: "1em", marginBottom: "10px" }}>
            ■ DATA PROGRESS METRICS
          </SectionTitle>
          <Grid>
            <Value>
              <div style={{ fontWeight: "bold", marginBottom: "5px" }}>
                Progress Percentage
              </div>
              <div style={{ fontSize: "1.2em", color: "#333" }}>
                {progressPercentage}%
              </div>
              <div style={{ fontSize: "0.8em", color: "#666" }}>
                Overall completion
              </div>
            </Value>
          </Grid>
        </div>

        <div style={{ marginTop: "20px" }}>
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              cursor: "pointer",
              marginBottom: "10px",
            }}
            onClick={() => setIsProcessingExpanded(!isProcessingExpanded)}
          >
            <SectionTitle style={{ fontSize: "1em", marginBottom: "0" }}>
              ► Currently Processing & Recent Activity
            </SectionTitle>
            <div style={{ fontSize: "0.9em", color: "#666" }}>
              {isProcessingExpanded ? "▼" : "▶"} {processingPagination?.total || currentlyProcessing.length} {(processingPagination?.total || currentlyProcessing.length) === 1 ? "table" : "tables"}
            </div>
          </div>
          {currentlyProcessing.length === 0 ? (
            <Value
              style={{
                background:
                  "linear-gradient(135deg, #ffffff 0%, #f0f7ff 100%)",
                borderLeft: "4px solid #0d1b2a",
                padding: "15px",
              }}
            >
              No active processing detected
            </Value>
          ) : (
            <>
              {currentlyProcessing[0] && (
                <Value
                  style={{
                    background:
                      "linear-gradient(135deg, #ffffff 0%, #f0f7ff 100%)",
                    borderLeft: "4px solid #0d1b2a",
                    padding: "12px 15px",
                    marginBottom: isProcessingExpanded ? "10px" : "0",
                  }}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <div>
                      <strong>{currentlyProcessing[0].schema_name}.{currentlyProcessing[0].table_name}</strong> [{currentlyProcessing[0].db_engine}]
                      <div style={{ fontSize: "0.9em", color: "#666", marginTop: "4px", display: "flex", alignItems: "center", gap: "8px" }}>
                        {formatNumberWithCommas(currentlyProcessing[0].total_records)} records
                        {currentlyProcessing[0].status === "IN_PROGRESS" && (
                          <InProgressBadge>
                            IN_PROGRESS
                          </InProgressBadge>
                        )}
                        {currentlyProcessing[0].status !== "IN_PROGRESS" && (
                          <span>{currentlyProcessing[0].status}</span>
                        )}
                        {currentlyProcessing[0].processed_at && (
                          <span>
                            • {new Date(currentlyProcessing[0].processed_at).toLocaleString()}
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                </Value>
              )}
              {isProcessingExpanded && (
                <>
                  <div style={{ display: "flex", flexDirection: "column", gap: "10px" }}>
                    {currentlyProcessing.slice(1).map((item, index) => (
                      <Value
                        key={index + 1}
                        style={{
                          background:
                            "linear-gradient(135deg, #ffffff 0%, #f0f7ff 100%)",
                          borderLeft: "4px solid #0d1b2a",
                          padding: "12px 15px",
                        }}
                      >
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                          <div>
                            <strong>{item.schema_name}.{item.table_name}</strong> [{item.db_engine}]
                            <div style={{ fontSize: "0.9em", color: "#666", marginTop: "4px", display: "flex", alignItems: "center", gap: "8px" }}>
                              {formatNumberWithCommas(item.total_records)} records
                              {item.status === "IN_PROGRESS" && (
                                <InProgressBadge>
                                  IN_PROGRESS
                                </InProgressBadge>
                              )}
                              {item.status !== "IN_PROGRESS" && (
                                <span>{item.status}</span>
                              )}
                              {item.processed_at && (
                                <span>
                                  • {new Date(item.processed_at).toLocaleString()}
                                </span>
                              )}
                            </div>
                          </div>
                        </div>
                      </Value>
                    ))}
                  </div>
                  {processingPagination && processingPagination.totalPages > 1 && (
                    <div style={{ display: "flex", justifyContent: "center", alignItems: "center", gap: "10px", marginTop: "15px" }}>
                      <Button
                        $variant="secondary"
                        onClick={() => setProcessingPage(Math.max(1, processingPage - 1))}
                        disabled={!processingPagination.hasPrev}
                        style={{ padding: "8px 14px", fontSize: "0.9em" }}
                      >
                        {'[<]'} Prev
                      </Button>
                      <div style={{ fontSize: "0.9em", color: "#666" }}>
                        Page {processingPagination.page} of {processingPagination.totalPages}
                      </div>
                      <Button
                        $variant="secondary"
                        onClick={() => setProcessingPage(Math.min(processingPagination.totalPages, processingPage + 1))}
                        disabled={!processingPagination.hasNext}
                        style={{ padding: "8px 14px", fontSize: "0.9em" }}
                      >
                        Next {'[>]'}
                      </Button>
                    </div>
                  )}
                </>
              )}
            </>
          )}
        </div>

      </Section>

      <Section>
        <SectionTitle>⚙️ CONFIGURATION</SectionTitle>
        <Grid>
          <Value>Current Batch: {batchConfig.value}</Value>
          <Value>
            Last Updated: {new Date(batchConfig.updated_at).toLocaleString()}
          </Value>
        </Grid>
      </Section>
    </Container>
  );
};

export default Dashboard;
