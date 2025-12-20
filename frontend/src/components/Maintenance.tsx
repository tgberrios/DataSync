import { useState, useEffect, useCallback, useRef } from 'react';
import styled from 'styled-components';
import {
  Container,
  Header,
  ErrorMessage,
  LoadingOverlay,
  Grid,
  Value,
  Select,
  FiltersContainer,
} from './shared/BaseComponents';
import { useTableFilters } from '../hooks/useTableFilters';
import { maintenanceApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { theme } from '../theme/theme';
import MaintenanceTreeView from './MaintenanceTreeView';

const MetricsGrid = styled(Grid)`
  margin-bottom: ${theme.spacing.xxl};
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

const MetricCard = styled(Value)`
  padding: ${theme.spacing.md};
  min-height: 80px;
`;

const MetricLabel = styled.div`
  font-size: 0.85em;
  color: ${theme.colors.text.secondary};
  margin-bottom: ${theme.spacing.xs};
  font-weight: 500;
`;

const MetricValue = styled.div`
  font-size: 1.5em;
  font-weight: bold;
  color: ${theme.colors.text.primary};
`;

const TabsContainer = styled.div`
  display: flex;
  gap: ${theme.spacing.sm};
  margin-bottom: ${theme.spacing.lg};
  border-bottom: 2px solid ${theme.colors.border.medium};
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.15s;
  animation-fill-mode: both;
`;

const Tab = styled.button<{ $active: boolean }>`
  padding: 12px 24px;
  border: none;
  background: ${props => props.$active ? theme.colors.primary.main : 'transparent'};
  color: ${props => props.$active ? theme.colors.text.white : theme.colors.text.secondary};
  cursor: pointer;
  font-family: ${theme.fonts.primary};
  font-size: 0.95em;
  font-weight: ${props => props.$active ? 'bold' : 'normal'};
  border-bottom: ${props => props.$active ? `3px solid ${theme.colors.primary.dark}` : '3px solid transparent'};
  transition: all ${theme.transitions.normal};
  margin-bottom: -2px;
  
  &:hover {
    background: ${props => props.$active ? theme.colors.primary.light : theme.colors.background.secondary};
    color: ${props => props.$active ? theme.colors.text.white : theme.colors.text.primary};
  }
`;

const Badge = styled.span<{ $status?: string; $type?: string }>`
  padding: 6px 12px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.8em;
  font-weight: 600;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  transition: all ${theme.transitions.normal};
  border: 2px solid transparent;
  box-shadow: ${theme.shadows.sm};
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  
  ${props => {
    if (props.$status) {
      switch (props.$status) {
        case 'PENDING':
          return `
            background: linear-gradient(135deg, ${theme.colors.status.warning.bg} 0%, ${theme.colors.status.warning.text}15 100%);
            color: ${theme.colors.status.warning.text};
            border-color: ${theme.colors.status.warning.text}40;
          `;
        case 'RUNNING':
          return `
            background: linear-gradient(135deg, #e3f2fd 0%, #1565c015 100%);
            color: #1565c0;
            border-color: #1565c040;
          `;
        case 'COMPLETED':
          return `
            background: linear-gradient(135deg, ${theme.colors.status.success.bg} 0%, ${theme.colors.status.success.text}15 100%);
            color: ${theme.colors.status.success.text};
            border-color: ${theme.colors.status.success.text}40;
          `;
        case 'FAILED':
          return `
            background: linear-gradient(135deg, ${theme.colors.status.error.bg} 0%, ${theme.colors.status.error.text}15 100%);
            color: ${theme.colors.status.error.text};
            border-color: ${theme.colors.status.error.text}40;
          `;
        case 'SKIPPED':
          return `
            background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
            color: ${theme.colors.text.secondary};
            border-color: ${theme.colors.border.medium};
          `;
        default:
          return `
            background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
            color: ${theme.colors.text.secondary};
            border-color: ${theme.colors.border.medium};
          `;
      }
    }
    if (props.$type) {
      return `
        background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
        color: ${theme.colors.text.primary};
        border-color: ${theme.colors.border.medium};
      `;
    }
    return `
      background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
      color: ${theme.colors.text.primary};
      border-color: ${theme.colors.border.medium};
    `;
  }}
  
  &:hover {
    transform: translateY(-2px) scale(1.08);
    box-shadow: ${theme.shadows.lg};
  }
`;

/**
 * Maintenance component
 * Displays database maintenance operations with filtering, tabs, and detailed metrics
 */
const Maintenance = () => {
  const { filters, setFilter } = useTableFilters({
    maintenance_type: '',
    status: '',
    db_engine: ''
  });
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [maintenanceItems, setMaintenanceItems] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [selectedItem, setSelectedItem] = useState<any | null>(null);
  const [activeTab, setActiveTab] = useState<'pending' | 'completed' | 'all'>('pending');
  const isMountedRef = useRef(true);

  const fetchData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      let statusFilter = filters.status as string;
      if (!statusFilter && activeTab !== 'all') {
        statusFilter = activeTab === 'pending' ? 'PENDING' : activeTab === 'completed' ? 'COMPLETED' : '';
      }
      const [itemsData, metricsData] = await Promise.all([
        maintenanceApi.getMaintenanceItems({
          page: 1,
          limit: 1000,
          maintenance_type: filters.maintenance_type as string,
          db_engine: filters.db_engine as string,
          status: statusFilter
        }),
        maintenanceApi.getMetrics()
      ]);
      if (isMountedRef.current) {
        setMaintenanceItems(itemsData.data || []);
        setMetrics(metricsData || {});
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    } finally {
      if (isMountedRef.current) {
        setLoading(false);
      }
    }
  }, [filters.maintenance_type, filters.db_engine, filters.status, activeTab]);

  useEffect(() => {
    isMountedRef.current = true;
    fetchData();
    const interval = setInterval(() => {
      if (isMountedRef.current) {
        fetchData();
      }
    }, 30000);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchData]);

  const handleItemClick = useCallback((item: any) => {
    setSelectedItem((prev: any) => prev?.id === item.id ? null : item);
  }, []);

  const formatBytes = useCallback((mb: number | string | null | undefined) => {
    if (mb === null || mb === undefined) return 'N/A';
    const numMb = Number(mb);
    if (isNaN(numMb)) return 'N/A';
    if (numMb < 1) return `${(numMb * 1024).toFixed(2)} KB`;
    if (numMb < 1024) return `${numMb.toFixed(2)} MB`;
    return `${(numMb / 1024).toFixed(2)} GB`;
  }, []);

  const formatDate = useCallback((date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  }, []);

  const formatDuration = useCallback((seconds: number | string | null | undefined) => {
    if (seconds === null || seconds === undefined) return 'N/A';
    const numSeconds = Number(seconds);
    if (isNaN(numSeconds)) return 'N/A';
    if (numSeconds < 60) return `${numSeconds.toFixed(2)}s`;
    if (numSeconds < 3600) return `${(numSeconds / 60).toFixed(2)}m`;
    return `${(numSeconds / 3600).toFixed(2)}h`;
  }, []);


  const handleFilterChange = useCallback((key: string, value: string) => {
    setFilter(key as any, value);
  }, [setFilter]);

  if (loading && maintenanceItems.length === 0) {
    return (
      <Container>
        <Header>Maintenance</Header>
        <LoadingOverlay>Loading maintenance data...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>Maintenance</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}
      
      <MetricsGrid $columns="repeat(auto-fit, minmax(180px, 1fr))">
        <MetricCard>
          <MetricLabel>Total Pending</MetricLabel>
          <MetricValue>{metrics.total_pending || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Total Completed</MetricLabel>
          <MetricValue>{metrics.total_completed || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Total Failed</MetricLabel>
          <MetricValue>{metrics.total_failed || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Space Reclaimed</MetricLabel>
          <MetricValue>{formatBytes(metrics.total_space_reclaimed_mb)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Avg Impact Score</MetricLabel>
          <MetricValue>{metrics.avg_impact_score ? `${Number(metrics.avg_impact_score).toFixed(1)}` : 'N/A'}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Objects Improved</MetricLabel>
          <MetricValue>{metrics.objects_improved || 0}</MetricValue>
        </MetricCard>
      </MetricsGrid>

      <TabsContainer>
        <Tab $active={activeTab === 'pending'} onClick={() => setActiveTab('pending')}>
          Pending ({metrics.total_pending || 0})
        </Tab>
        <Tab $active={activeTab === 'completed'} onClick={() => setActiveTab('completed')}>
          Completed ({metrics.total_completed || 0})
        </Tab>
        <Tab $active={activeTab === 'all'} onClick={() => setActiveTab('all')}>
          All
        </Tab>
      </TabsContainer>

      <FiltersContainer>
        <Select
          value={filters.maintenance_type as string}
          onChange={(e) => handleFilterChange('maintenance_type', e.target.value)}
        >
          <option value="">All Types</option>
          <option value="VACUUM">VACUUM</option>
          <option value="ANALYZE">ANALYZE</option>
          <option value="REINDEX">REINDEX</option>
          <option value="CLUSTER">CLUSTER</option>
        </Select>
        
        <Select
          value={filters.db_engine as string}
          onChange={(e) => handleFilterChange('db_engine', e.target.value)}
        >
          <option value="">All Engines</option>
          <option value="PostgreSQL">PostgreSQL</option>
          <option value="MariaDB">MariaDB</option>
          <option value="MSSQL">MSSQL</option>
        </Select>
        
        <Select
          value={filters.status as string}
          onChange={(e) => handleFilterChange('status', e.target.value)}
        >
          <option value="">All Status</option>
          <option value="PENDING">PENDING</option>
          <option value="RUNNING">RUNNING</option>
          <option value="COMPLETED">COMPLETED</option>
          <option value="FAILED">FAILED</option>
          <option value="SKIPPED">SKIPPED</option>
        </Select>
      </FiltersContainer>

      <div style={{ display: 'grid', gridTemplateColumns: selectedItem ? '1fr 400px' : '1fr', gap: theme.spacing.lg }}>
        <MaintenanceTreeView 
          items={maintenanceItems} 
          onItemClick={handleItemClick}
        />
        
        {selectedItem && (
          <div style={{
            background: theme.colors.background.secondary,
            border: `1px solid ${theme.colors.border.light}`,
            borderRadius: theme.borderRadius.md,
            padding: theme.spacing.lg,
            position: 'sticky',
            top: theme.spacing.md,
            maxHeight: 'calc(100vh - 200px)',
            overflowY: 'auto'
          }}>
            <h3 style={{ marginTop: 0, marginBottom: theme.spacing.md, color: theme.colors.text.primary }}>
              Maintenance Details
            </h3>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: theme.spacing.md }}>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Type:</strong>
                <div>
                  <Badge $type={selectedItem.maintenance_type}>{selectedItem.maintenance_type}</Badge>
                </div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Engine:</strong>
                <div style={{ color: theme.colors.text.primary }}>{selectedItem.db_engine || 'N/A'}</div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Schema:</strong>
                <div style={{ color: theme.colors.text.primary }}>{selectedItem.schema_name || 'N/A'}</div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Object:</strong>
                <div style={{ color: theme.colors.text.primary }}>{selectedItem.object_name || 'N/A'}</div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Status:</strong>
                <div>
                  <Badge $status={selectedItem.status}>{selectedItem.status}</Badge>
                </div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Priority:</strong>
                <div style={{ color: theme.colors.text.primary }}>{selectedItem.priority || 'N/A'}</div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Impact Score:</strong>
                <div style={{ color: theme.colors.text.primary }}>
                  {selectedItem.impact_score ? Number(selectedItem.impact_score).toFixed(1) : 'N/A'}
                </div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Space Reclaimed:</strong>
                <div style={{ color: theme.colors.text.primary }}>{formatBytes(selectedItem.space_reclaimed_mb)}</div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Duration:</strong>
                <div style={{ color: theme.colors.text.primary }}>{formatDuration(selectedItem.maintenance_duration_seconds)}</div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Last Run:</strong>
                <div style={{ color: theme.colors.text.primary, fontSize: '0.9em' }}>
                  {formatDate(selectedItem.last_maintenance_date)}
                </div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Next Run:</strong>
                <div style={{ color: theme.colors.text.primary, fontSize: '0.9em' }}>
                  {formatDate(selectedItem.next_maintenance_date)}
                </div>
              </div>
              <div style={{ borderTop: `1px solid ${theme.colors.border.light}`, paddingTop: theme.spacing.md, marginTop: theme.spacing.sm }}>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Additional Info:</strong>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: theme.spacing.sm, marginTop: theme.spacing.sm }}>
                  <div>
                    <div style={{ fontSize: '0.85em', color: theme.colors.text.secondary }}>Object Type</div>
                    <div style={{ fontWeight: 600 }}>{selectedItem.object_type || 'N/A'}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.85em', color: theme.colors.text.secondary }}>Auto Execute</div>
                    <div style={{ fontWeight: 600 }}>{selectedItem.auto_execute ? 'Yes' : 'No'}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.85em', color: theme.colors.text.secondary }}>Enabled</div>
                    <div style={{ fontWeight: 600 }}>{selectedItem.enabled ? 'Yes' : 'No'}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.85em', color: theme.colors.text.secondary }}>Maintenance Count</div>
                    <div style={{ fontWeight: 600 }}>{selectedItem.maintenance_count || 0}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.85em', color: theme.colors.text.secondary }}>Performance Improvement</div>
                    <div style={{ fontWeight: 600 }}>
                      {selectedItem.performance_improvement_pct ? `${Number(selectedItem.performance_improvement_pct).toFixed(2)}%` : 'N/A'}
                    </div>
                  </div>
                  {selectedItem.error_details && (
                    <div>
                      <div style={{ fontSize: '0.85em', color: theme.colors.status.error.text }}>Error Details</div>
                      <div style={{ fontWeight: 600, color: theme.colors.status.error.text, fontSize: '0.85em' }}>
                        {selectedItem.error_details}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </Container>
  );
};

export default Maintenance;
