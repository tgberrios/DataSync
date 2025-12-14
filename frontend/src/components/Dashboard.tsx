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

/**
 * Componente principal del Dashboard que muestra estadísticas en tiempo real
 * del sistema de sincronización de datos.
 *
 * @returns {JSX.Element} Componente Dashboard renderizado
 */
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
    useState<CurrentlyProcessing | null>(null);
  const isMountedRef = useRef(true);
  const isInitialLoadRef = useRef(true);

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
      responseTime: "< 1ms",
      bufferHitRate: "0.0",
      cacheHitRate: "0.0",
      status: "Healthy",
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

  const fetchStats = useCallback(async () => {
    try {
      if (!isMountedRef.current) return;
      if (isInitialLoadRef.current) {
        setLoading(true);
        setError(null);
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
  }, []);

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
      const data = await dashboardApi.getCurrentlyProcessing();
      if (isMountedRef.current) {
        setCurrentlyProcessing(data);
      }
    } catch (err) {
      if (isMountedRef.current) {
        console.error("Error fetching currently processing table:", err);
      }
    }
  }, []);

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
  }, [fetchStats, fetchSyncStats, fetchCurrentlyProcessing, fetchSystemResources]);

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
          <Value>Total Tables: {(stats.syncStatus.listeningChanges || 0) + (stats.syncStatus.pending || 0) + (stats.syncStatus.fullLoadActive || 0) + (stats.syncStatus.fullLoadInactive || 0) + (stats.syncStatus.noData || 0) + (stats.syncStatus.skip || 0)}</Value>
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

        <Value
          style={{
            marginTop: "20px",
            background:
              "linear-gradient(135deg, #ffffff 0%, #f0f7ff 100%)",
            borderLeft: "4px solid #0d1b2a",
            animation: currentlyProcessing ? "pulse 2s infinite" : "none",
          }}
        >
          ► Currently Processing:{" "}
          {currentlyProcessing
            ? `${currentlyProcessing.schema_name}.${currentlyProcessing.table_name} [${currentlyProcessing.db_engine}] (${formatNumberWithCommas(currentlyProcessing.total_records)} records) - ${currentlyProcessing.status}`
            : "No active processing detected"}
        </Value>

        <div style={{ marginTop: "15px" }}>
          <SectionTitle style={{ fontSize: "1em", marginBottom: "10px" }}>
            ■ PK STRATEGY INFORMATION
          </SectionTitle>
          <Grid>
            <Value>
              <div style={{ fontWeight: "bold", marginBottom: "5px" }}>
                Current Table Strategy
              </div>
              <div style={{ fontSize: "1.2em", color: "#333" }}>
                {currentlyProcessing ? "PK" : "N/A"}
              </div>
              <div style={{ fontSize: "0.8em", color: "#666" }}>
                Primary Key based pagination
              </div>
            </Value>
            <Value>
              <div style={{ fontWeight: "bold", marginBottom: "5px" }}>
                Strategy Status
              </div>
              <div style={{ fontSize: "1.2em", color: "#333" }}>
                {currentlyProcessing ? "Active" : "Idle"}
              </div>
              <div style={{ fontSize: "0.8em", color: "#666" }}>
                Cursor-based processing
              </div>
            </Value>
          </Grid>
        </div>
      </Section>

      <Section>
        <SectionTitle>● SYSTEM RESOURCES</SectionTitle>
        <Grid>
          <Value>
            <div style={{ fontWeight: "bold", marginBottom: "5px" }}>
              CPU Usage
            </div>
            <div style={{ fontSize: "1.2em", color: "#333" }}>
              {stats.systemResources.cpuUsage}%
            </div>
            <div style={{ fontSize: "0.8em", color: "#666" }}>(20 cores)</div>
          </Value>
          <Value>
            <div style={{ fontWeight: "bold", marginBottom: "5px" }}>
              Memory
            </div>
            <div style={{ fontSize: "1.2em", color: "#333" }}>
              {stats.systemResources.memoryUsed}/{stats.systemResources.memoryTotal}{" "}
              GB
            </div>
            <div style={{ fontSize: "0.8em", color: "#666" }}>
              ({stats.systemResources.memoryPercentage}%)
            </div>
          </Value>
          <Value>
            <div style={{ fontWeight: "bold", marginBottom: "5px" }}>RSS</div>
            <div style={{ fontSize: "1.2em", color: "#333" }}>
              {stats.systemResources.rss} GB
            </div>
            <div style={{ fontSize: "0.8em", color: "#666" }}>
              Resident Set Size
            </div>
          </Value>
          <Value>
            <div style={{ fontWeight: "bold", marginBottom: "5px" }}>
              Virtual
            </div>
            <div style={{ fontSize: "1.2em", color: "#333" }}>
              {stats.systemResources.virtual} GB
            </div>
            <div style={{ fontSize: "0.8em", color: "#666" }}>
              Virtual Memory
            </div>
          </Value>
        </Grid>
      </Section>

      <Section>
        <SectionTitle>■ DATABASE HEALTH</SectionTitle>
        <Grid>
          <Value>
            Active Connections: {stats.dbHealth.activeConnections}
          </Value>
          <Value>Response Time: {stats.dbHealth.responseTime}</Value>
          <Value>Buffer Hit Rate: {stats.dbHealth.bufferHitRate}%</Value>
          <Value>Cache Hit Rate: {stats.dbHealth.cacheHitRate}%</Value>
          <Value>Status: ✓ {stats.dbHealth.status}</Value>
        </Grid>
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
