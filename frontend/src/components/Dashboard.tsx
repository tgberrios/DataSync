import { useState, useEffect } from 'react';
import styled from 'styled-components';
import { dashboardApi, configApi } from '../services/api';
import type { DashboardStats, BatchConfig } from '../services/api';

const DashboardContainer = styled.div`
  background-color: white;
  color: #333;
  padding: 20px;
  font-family: monospace;
  box-sizing: border-box;
`;

const Header = styled.div`
  border: 2px solid #333;
  padding: 15px;
  text-align: center;
  margin-bottom: 30px;
  font-size: 1.5em;
  font-weight: bold;
  background-color: #f5f5f5;
  border-radius: 4px;
`;

const Section = styled.div`
  margin-bottom: 30px;
  padding: 20px;
  border: 1px solid #eee;
  border-radius: 4px;
  background-color: #fafafa;
`;

const ProgressBar = styled.div<{ progress: number }>`
  width: 100%;
  height: 20px;
  background-color: #ddd;
  margin: 10px 0;
  
  &:after {
    content: '';
    display: block;
    width: ${props => props.progress}%;
    height: 100%;
    background-color: #333;
  }
`;

const Grid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 20px;
  margin-top: 15px;
`;

const SectionTitle = styled.h3`
  margin-bottom: 15px;
  font-size: 1.2em;
  color: #222;
  border-bottom: 2px solid #333;
  padding-bottom: 8px;
`;

const Value = styled.div`
  font-size: 1.1em;
  padding: 8px;
  background-color: #fff;
  border-radius: 3px;
  border: 1px solid #ddd;
`;

const Dashboard = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [batchConfig, setBatchConfig] = useState<BatchConfig>({
    key: 'chunk_size',
    value: '25000',
    description: 'Tamaño de lote para procesamiento de datos',
    updated_at: new Date().toISOString()
  });

  // Función para formatear números con separadores de miles
  const formatNumber = (text: string): string => {
    // Buscar patrones de números en el texto (ej: "3649000 records")
    return text.replace(/(\d+)(\s+records?)/g, (match, number, suffix) => {
      const formattedNumber = parseInt(number).toLocaleString('en-US');
      return `${formattedNumber}${suffix}`;
    });
  };
  const [stats, setStats] = useState<DashboardStats>({
    syncStatus: {
      progress: 75,
      perfectMatch: 9,
      listeningChanges: 6,
      fullLoadActive: 0,
      fullLoadInactive: 0,
      noData: 3,
      errors: 0,
      currentProcess: 'dbo.test_performance (NO_DATA)'
    },
    systemResources: {
      cpuUsage: '0.0',
      memoryUsed: '12.90',
      memoryTotal: '30.54',
      memoryPercentage: '42.2',
      rss: '12.90',
      virtual: '19.35'
    },
    dbHealth: {
      activeConnections: '1/100',
      responseTime: '< 1ms',
      bufferHitRate: '0.0',
      cacheHitRate: '0.0',
      status: 'Healthy'
    },
    // Connection pooling removed - using direct connections now
  });

  useEffect(() => {
    const fetchStats = async () => {
      try {
        setLoading(true);
        setError(null);
        const [dashboardData, batchData] = await Promise.all([
          dashboardApi.getDashboardStats(),
          configApi.getBatchConfig()
        ]);
        setStats(dashboardData);
        setBatchConfig(batchData);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading dashboard data');
      } finally {
        setLoading(false);
      }
    };

    fetchStats();
    // Actualizar cada 30 segundos
    const interval = setInterval(fetchStats, 30000);
    return () => clearInterval(interval);
  }, []);

  return (
    <DashboardContainer>
      {loading && (
        <div style={{ textAlign: 'center', padding: '20px' }}>
          Loading dashboard data...
        </div>
      )}

      {error && (
        <div style={{ 
          color: 'red', 
          padding: '20px', 
          textAlign: 'center',
          border: '1px solid red',
          borderRadius: '4px',
          margin: '20px',
          backgroundColor: '#fff5f5'
        }}>
          <div style={{ fontWeight: 'bold', marginBottom: '10px' }}>Error al cargar datos:</div>
          <div>{error}</div>
          <button 
            onClick={() => {
              setError(null);
              setLoading(true);
              dashboardApi.getDashboardStats()
                .then(data => setStats(data))
                .catch(err => setError(err.message))
                .finally(() => setLoading(false));
            }}
            style={{
              marginTop: '10px',
              padding: '8px 16px',
              border: '1px solid red',
              borderRadius: '4px',
              background: 'white',
              color: 'red',
              cursor: 'pointer'
            }}
          >
            Reintentar
          </button>
        </div>
      )}

      {!loading && !error && (
        <>
          <Header>
            DataSync Real-Time Dashboard
          </Header>

          <Section>
            <SectionTitle>■ SYNCHRONIZATION STATUS</SectionTitle>
            <ProgressBar progress={stats.syncStatus.progress} />
            <Grid>
              <Value>Perfect Match: {stats.syncStatus.perfectMatch}</Value>
              <Value>Listening Changes: {stats.syncStatus.listeningChanges}</Value>
              <Value>Active: {stats.syncStatus.fullLoadActive}</Value>
              <Value>Inactive: {stats.syncStatus.fullLoadInactive}</Value>
              <Value>No Data: {stats.syncStatus.noData}</Value>
              <Value>Errors: {stats.syncStatus.errors}</Value>
            </Grid>
            <Value style={{ marginTop: '20px' }}>► Currently Processing: {formatNumber(stats.syncStatus.currentProcess)}</Value>
          </Section>


          <Section>
            <SectionTitle>● SYSTEM RESOURCES</SectionTitle>
            <Grid>
              <Value>CPU Usage: {stats.systemResources.cpuUsage}</Value>
              <Value>Memory: {stats.systemResources.memoryUsed}</Value>
              <Value>RSS: {stats.systemResources.rss}</Value>
              <Value>Virtual: {stats.systemResources.virtual}</Value>
            </Grid>
          </Section>

          <Section>
            <SectionTitle>■ DATABASE HEALTH</SectionTitle>
            <Grid>
              <Value>Active Connections: {stats.dbHealth.activeConnections}</Value>
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
              <Value>Last Updated: {new Date(batchConfig.updated_at).toLocaleString()}</Value>
            </Grid>
          </Section>

          {/* Connection pooling section removed - using direct connections now */}
        </>
      )}
    </DashboardContainer>
  );
};

export default Dashboard;
