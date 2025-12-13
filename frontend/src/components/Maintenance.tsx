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
  Pagination,
  PageButton,
} from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { maintenanceApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { theme } from '../theme/theme';

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

const FiltersContainer = styled.div`
  display: flex;
  gap: ${theme.spacing.sm};
  margin-bottom: ${theme.spacing.lg};
  flex-wrap: wrap;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const MaintenanceTable = styled.div`
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  overflow: hidden;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.25s;
  animation-fill-mode: both;
`;

const TableHeader = styled.div`
  display: grid;
  grid-template-columns: 120px 100px 150px 120px 120px 100px 100px 100px 1fr 100px;
  background: ${theme.colors.gradient.primary};
  padding: 12px 15px;
  font-weight: bold;
  font-size: 0.85em;
  border-bottom: 2px solid ${theme.colors.border.dark};
  gap: 10px;
`;

const TableRow = styled.div`
  display: grid;
  grid-template-columns: 120px 100px 150px 120px 120px 100px 100px 100px 1fr 100px;
  padding: 12px 15px;
  border-bottom: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  gap: 10px;
  align-items: center;
  font-size: 0.85em;
  
  &:hover {
    background: linear-gradient(90deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
    border-left: 3px solid ${theme.colors.primary.main};
  }
  
  &:last-child {
    border-bottom: none;
  }
`;

const TableCell = styled.div`
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`;

const Badge = styled.span<{ $status?: string; $type?: string }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.8em;
  font-weight: 500;
  display: inline-block;
  transition: all ${theme.transitions.normal};
  
  ${props => {
    if (props.$status) {
      switch (props.$status) {
        case 'PENDING': return `background-color: ${theme.colors.status.warning.bg}; color: ${theme.colors.status.warning.text};`;
        case 'RUNNING': return `background-color: #e3f2fd; color: #1565c0;`;
        case 'COMPLETED': return `background-color: ${theme.colors.status.success.bg}; color: ${theme.colors.status.success.text};`;
        case 'FAILED': return `background-color: ${theme.colors.status.error.bg}; color: ${theme.colors.status.error.text};`;
        case 'SKIPPED': return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.secondary};`;
        default: return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.secondary};`;
      }
    }
    if (props.$type) {
      return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.primary};`;
    }
    return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.secondary};`;
  }}
  
  &:hover {
    transform: scale(1.05);
    box-shadow: ${theme.shadows.sm};
  }
`;

const MaintenanceDetails = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '800px' : '0'};
  opacity: ${props => props.$isOpen ? '1' : '0'};
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  border-top: ${props => props.$isOpen ? `1px solid ${theme.colors.border.light}` : 'none'};
  background-color: ${theme.colors.background.main};
  overflow: hidden;
`;

const DetailGrid = styled.div`
  display: grid;
  grid-template-columns: 200px 1fr;
  padding: ${theme.spacing.md};
  gap: ${theme.spacing.sm};
  font-size: 0.9em;
`;

const DetailLabel = styled.div`
  color: ${theme.colors.text.secondary};
  font-weight: 500;
`;

const DetailValue = styled.div`
  color: ${theme.colors.text.primary};
`;

const MetricsComparison = styled.div`
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: ${theme.spacing.lg};
  padding: ${theme.spacing.md};
  background: ${theme.colors.background.secondary};
  border-radius: ${theme.borderRadius.md};
  margin: ${theme.spacing.md};
`;

const MetricsColumn = styled.div`
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  padding: ${theme.spacing.md};
  background: ${theme.colors.background.main};
`;

const MetricsTitle = styled.div`
  font-weight: bold;
  margin-bottom: ${theme.spacing.sm};
  color: ${theme.colors.text.primary};
  border-bottom: 2px solid ${theme.colors.border.dark};
  padding-bottom: 5px;
`;

/**
 * Maintenance component
 * Displays database maintenance operations with filtering, tabs, and detailed metrics
 */
const Maintenance = () => {
  const { page, limit, setPage } = usePagination(1, 20);
  const { filters, setFilter } = useTableFilters({
    maintenance_type: '',
    status: '',
    db_engine: ''
  });
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [maintenanceItems, setMaintenanceItems] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [openItemId, setOpenItemId] = useState<number | null>(null);
  const [activeTab, setActiveTab] = useState<'pending' | 'completed' | 'all'>('pending');
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 20
  });
  const isMountedRef = useRef(true);

  const fetchData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      const statusFilter = activeTab === 'pending' ? 'PENDING' : activeTab === 'completed' ? 'COMPLETED' : '';
      const [itemsData, metricsData] = await Promise.all([
        maintenanceApi.getMaintenanceItems({
          page,
          limit,
          maintenance_type: filters.maintenance_type as string,
          db_engine: filters.db_engine as string,
          status: statusFilter || (filters.status as string)
        }),
        maintenanceApi.getMetrics()
      ]);
      if (isMountedRef.current) {
        setMaintenanceItems(itemsData.data || []);
        setPagination(itemsData.pagination || {
          total: 0,
          totalPages: 0,
          currentPage: 1,
          limit: 20
        });
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
  }, [page, limit, filters.maintenance_type, filters.db_engine, filters.status, activeTab]);

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

  const toggleItem = useCallback((id: number) => {
    setOpenItemId(prev => prev === id ? null : id);
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

  const formatNumber = useCallback((num: number | null | undefined) => {
    if (num === null || num === undefined) return 'N/A';
    if (num >= 1000000) return `${(num / 1000000).toFixed(2)}M`;
    if (num >= 1000) return `${(num / 1000).toFixed(2)}K`;
    return num.toString();
  }, []);

  const handleFilterChange = useCallback((key: string, value: string) => {
    setFilter(key as any, value);
    setPage(1);
  }, [setFilter, setPage]);

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
        
        {activeTab === 'all' && (
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
        )}
      </FiltersContainer>

      <MaintenanceTable>
        <TableHeader>
          <TableCell>Type</TableCell>
          <TableCell>Engine</TableCell>
          <TableCell>Schema.Table</TableCell>
          <TableCell>Status</TableCell>
          <TableCell>Priority</TableCell>
          <TableCell>Impact</TableCell>
          <TableCell>Space</TableCell>
          <TableCell>Duration</TableCell>
          <TableCell>Last Run</TableCell>
          <TableCell>Next Run</TableCell>
        </TableHeader>
        {maintenanceItems.length === 0 ? (
          <div style={{ padding: '40px', textAlign: 'center', color: theme.colors.text.secondary }}>
            No maintenance items available. Maintenance operations will appear here once detected.
          </div>
        ) : (
          maintenanceItems.map((item) => (
          <div key={item.id}>
            <TableRow onClick={() => toggleItem(item.id)}>
              <TableCell>
                <Badge $type={item.maintenance_type}>{item.maintenance_type}</Badge>
              </TableCell>
              <TableCell>{item.db_engine || 'N/A'}</TableCell>
              <TableCell>{item.schema_name}.{item.object_name}</TableCell>
              <TableCell>
                <Badge $status={item.status}>{item.status}</Badge>
              </TableCell>
              <TableCell>{item.priority || 'N/A'}</TableCell>
              <TableCell>{item.impact_score ? Number(item.impact_score).toFixed(1) : 'N/A'}</TableCell>
              <TableCell>{formatBytes(item.space_reclaimed_mb)}</TableCell>
              <TableCell>{formatDuration(item.maintenance_duration_seconds)}</TableCell>
              <TableCell>{formatDate(item.last_maintenance_date)}</TableCell>
              <TableCell>{formatDate(item.next_maintenance_date)}</TableCell>
            </TableRow>
            <MaintenanceDetails $isOpen={openItemId === item.id}>
              <DetailGrid>
                <DetailLabel>Object Type:</DetailLabel>
                <DetailValue>{item.object_type || 'N/A'}</DetailValue>
                
                <DetailLabel>Auto Execute:</DetailLabel>
                <DetailValue>{item.auto_execute ? 'Yes' : 'No'}</DetailValue>
                
                <DetailLabel>Enabled:</DetailLabel>
                <DetailValue>{item.enabled ? 'Yes' : 'No'}</DetailValue>
                
                <DetailLabel>Maintenance Count:</DetailLabel>
                <DetailValue>{item.maintenance_count || 0}</DetailValue>
                
                <DetailLabel>Performance Improvement:</DetailLabel>
                <DetailValue>{item.performance_improvement_pct ? `${Number(item.performance_improvement_pct).toFixed(2)}%` : 'N/A'}</DetailValue>
                
                <DetailLabel>First Detected:</DetailLabel>
                <DetailValue>{formatDate(item.first_detected_date)}</DetailValue>
                
                <DetailLabel>Last Checked:</DetailLabel>
                <DetailValue>{formatDate(item.last_checked_date)}</DetailValue>
                
                <DetailLabel>Result Message:</DetailLabel>
                <DetailValue>{item.result_message || 'N/A'}</DetailValue>
                
                {item.error_details && (
                  <>
                    <DetailLabel>Error Details:</DetailLabel>
                    <DetailValue style={{ color: theme.colors.status.error.text }}>{item.error_details}</DetailValue>
                  </>
                )}
              </DetailGrid>
              
              {(item.metrics_before || item.metrics_after) && (
                <MetricsComparison>
                  <MetricsColumn>
                    <MetricsTitle>Before</MetricsTitle>
                    {item.fragmentation_before !== null && (
                      <div>Fragmentation: {Number(item.fragmentation_before).toFixed(2)}%</div>
                    )}
                    {item.dead_tuples_before !== null && (
                      <div>Dead Tuples: {formatNumber(item.dead_tuples_before)}</div>
                    )}
                    {item.table_size_before_mb !== null && (
                      <div>Table Size: {formatBytes(item.table_size_before_mb)}</div>
                    )}
                    {item.index_size_before_mb !== null && (
                      <div>Index Size: {formatBytes(item.index_size_before_mb)}</div>
                    )}
                  </MetricsColumn>
                  <MetricsColumn>
                    <MetricsTitle>After</MetricsTitle>
                    {item.fragmentation_after !== null && (
                      <div>Fragmentation: {Number(item.fragmentation_after).toFixed(2)}%</div>
                    )}
                    {item.dead_tuples_after !== null && (
                      <div>Dead Tuples: {formatNumber(item.dead_tuples_after)}</div>
                    )}
                    {item.table_size_after_mb !== null && (
                      <div>Table Size: {formatBytes(item.table_size_after_mb)}</div>
                    )}
                    {item.index_size_after_mb !== null && (
                      <div>Index Size: {formatBytes(item.index_size_after_mb)}</div>
                    )}
                  </MetricsColumn>
                </MetricsComparison>
              )}
            </MaintenanceDetails>
          </div>
          ))
        )}
      </MaintenanceTable>

      {pagination.totalPages > 1 && (
        <Pagination>
          <PageButton
            onClick={() => setPage(Math.max(1, page - 1))}
            disabled={page === 1}
          >
            Previous
          </PageButton>
          <span>
            Page {pagination.currentPage} of {pagination.totalPages} ({pagination.total} total)
          </span>
          <PageButton
            onClick={() => setPage(Math.min(pagination.totalPages, page + 1))}
            disabled={page === pagination.totalPages}
          >
            Next
          </PageButton>
        </Pagination>
      )}
    </Container>
  );
};

export default Maintenance;
