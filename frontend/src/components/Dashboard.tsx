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

          {stats.activeTransfersProgress && stats.activeTransfersProgress.length > 0 && (
            <Section>
              <SectionTitle>📊 ACTIVE TRANSFERS PROGRESS</SectionTitle>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                {stats.activeTransfersProgress.map((transfer, index) => (
                  <div key={index} style={{ 
                    padding: '10px', 
                    backgroundColor: '#f9f9f9', 
                    borderRadius: '5px',
                    border: '1px solid #ddd'
                  }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '5px' }}>
                      <strong>{transfer.schemaName}.{transfer.tableName} [{transfer.dbEngine}]</strong>
                      <span style={{ 
                        color: transfer.progressPercentage >= 90 ? '#4CAF50' : 
                               transfer.progressPercentage >= 50 ? '#FF9800' : '#2196F3',
                        fontWeight: 'bold'
                      }}>
                        {transfer.progressPercentage.toFixed(1)}%
                      </span>
                    </div>
                    <div style={{ 
                      width: '100%', 
                      height: '20px', 
                      backgroundColor: '#e0e0e0', 
                      borderRadius: '10px',
                      overflow: 'hidden'
                    }}>
                      <div style={{
                        width: `${Math.min(transfer.progressPercentage, 100)}%`,
                        height: '100%',
                        backgroundColor: transfer.progressPercentage >= 90 ? '#4CAF50' : 
                                       transfer.progressPercentage >= 50 ? '#FF9800' : '#2196F3',
                        transition: 'width 0.3s ease'
                      }} />
                    </div>
                    <div style={{ 
                      fontSize: '0.9em', 
                      color: '#666', 
                      marginTop: '5px',
                      display: 'flex',
                      justifyContent: 'space-between'
                    }}>
                      <span>{transfer.lastOffset.toLocaleString()} / {transfer.tableSize.toLocaleString()} records</span>
                      <span>Status: {transfer.status}</span>
                    </div>
                  </div>
                ))}
              </div>
            </Section>
          )}

          {/* MÉTRICAS CARDS */}
          {stats.metricsCards && (
            <>
              <Section>
                <SectionTitle>■ PERFORMANCE METRICS</SectionTitle>
                <Grid>
                  <Value>
                    <div style={{ marginBottom: '5px', fontWeight: 'bold' }}>Current IO Operations</div>
                    <div style={{ fontSize: '1.2em', color: '#333' }}>
                      {stats.metricsCards.currentIops.toFixed(2)} IOPS
                    </div>
                    <div style={{ fontSize: '0.8em', color: '#666' }}>Average (last hour)</div>
                  </Value>
                  
                  <Value>
                    <div style={{ marginBottom: '5px', fontWeight: 'bold' }}>Current Throughput</div>
                    <div style={{ fontSize: '1.2em', color: '#333' }}>
                      {stats.metricsCards.currentThroughput.avgRps.toFixed(0)} RPS
                    </div>
                    <div style={{ fontSize: '0.8em', color: '#666' }}>
                      {stats.metricsCards.currentThroughput.totalRecords.toLocaleString()} records (last hour)
                    </div>
                  </Value>
                </Grid>
                
                <div style={{ marginTop: '20px' }}>
                  <div style={{ fontWeight: 'bold', marginBottom: '15px', color: '#222', borderBottom: '1px solid #333', paddingBottom: '5px' }}>Top Tables by Throughput</div>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '10px' }}>
                    {stats.metricsCards.topTablesThroughput.slice(0, 5).map((table, index) => (
                      <Value key={index} style={{ 
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center'
                      }}>
                        <div>
                          <div style={{ fontWeight: 'bold', fontSize: '0.9em' }}>{table.tableName}</div>
                          <div style={{ fontSize: '0.8em', color: '#666' }}>[{table.dbEngine}]</div>
                        </div>
                        <div style={{ textAlign: 'right' }}>
                          <div style={{ fontWeight: 'bold', color: '#333' }}>{table.throughputRps.toFixed(0)} RPS</div>
                          <div style={{ fontSize: '0.8em', color: '#666' }}>{table.recordsTransferred.toLocaleString()} records</div>
                        </div>
                      </Value>
                    ))}
                  </div>
                </div>
              </Section>

              <Section>
                <SectionTitle>■ DATA VOLUME METRICS</SectionTitle>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(350px, 1fr))', gap: '15px' }}>
                  {stats.metricsCards.dataVolumeByTable.slice(0, 6).map((table, index) => {
                    const totalMB = table.totalBytes / (1024 * 1024);
                    const totalGB = totalMB / 1024;
                    const displaySize = totalGB >= 1 ? `${totalGB.toFixed(2)} GB` : `${totalMB.toFixed(0)} MB`;
                    
                    return (
                      <Value key={index} style={{ 
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center'
                      }}>
                        <div>
                          <div style={{ fontWeight: 'bold', fontSize: '1em' }}>{table.tableName}</div>
                          <div style={{ fontSize: '0.9em', color: '#666' }}>[{table.dbEngine}] • {table.transferCount} transfers</div>
                        </div>
                        <div style={{ textAlign: 'right' }}>
                          <div style={{ fontWeight: 'bold', color: '#333', fontSize: '1.1em' }}>{displaySize}</div>
                          <div style={{ fontSize: '0.8em', color: '#666' }}>7 days</div>
                        </div>
                      </Value>
                    );
                  })}
                </div>
              </Section>

            </>
          )}

          {/* Connection pooling section removed - using direct connections now */}
        </>
      )}
    </DashboardContainer>
  );
};

export default Dashboard;
