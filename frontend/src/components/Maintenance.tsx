import { useState, useEffect } from 'react';
import styled from 'styled-components';
import { maintenanceApi } from '../services/api';

const MaintenanceContainer = styled.div`
  background-color: white;
  color: #333;
  padding: 20px;
  font-family: monospace;
  animation: fadeIn 0.25s ease-in;
`;

const Header = styled.div`
  border: 2px solid #333;
  padding: 15px;
  text-align: center;
  margin-bottom: 30px;
  font-size: 1.5em;
  font-weight: bold;
  background: linear-gradient(135deg, #f5f5f5 0%, #ffffff 50%, #f5f5f5 100%);
  border-radius: 6px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  position: relative;
  overflow: hidden;
  animation: slideUp 0.3s ease-out;
  
  &::before {
    content: '';
    position: absolute;
    top: 0;
    left: -100%;
    width: 100%;
    height: 100%;
    background: linear-gradient(90deg, transparent, rgba(10, 25, 41, 0.1), transparent);
    animation: shimmer 3s infinite;
  }
`;

const MetricsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 15px;
  margin-bottom: 30px;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

const MetricCard = styled.div`
  border: 1px solid #ddd;
  border-radius: 6px;
  padding: 15px;
  background: linear-gradient(135deg, #fafafa 0%, #ffffff 100%);
  transition: all 0.2s ease;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.03);
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.3);
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
  }
`;

const MetricLabel = styled.div`
  font-size: 0.85em;
  color: #666;
  margin-bottom: 8px;
  font-weight: 500;
`;

const MetricValue = styled.div`
  font-size: 1.5em;
  font-weight: bold;
  color: #0d1b2a;
`;

const TabsContainer = styled.div`
  display: flex;
  gap: 10px;
  margin-bottom: 20px;
  border-bottom: 2px solid #ddd;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.15s;
  animation-fill-mode: both;
`;

const Tab = styled.button<{ $active: boolean }>`
  padding: 12px 24px;
  border: none;
  background: ${props => props.$active ? '#0d1b2a' : 'transparent'};
  color: ${props => props.$active ? 'white' : '#666'};
  cursor: pointer;
  font-family: monospace;
  font-size: 0.95em;
  font-weight: ${props => props.$active ? 'bold' : 'normal'};
  border-bottom: ${props => props.$active ? '3px solid #1e3a5f' : '3px solid transparent'};
  transition: all 0.2s ease;
  margin-bottom: -2px;
  
  &:hover {
    background: ${props => props.$active ? '#1e3a5f' : '#f5f5f5'};
    color: ${props => props.$active ? 'white' : '#333'};
  }
`;

const FiltersContainer = styled.div`
  display: flex;
  gap: 10px;
  margin-bottom: 20px;
  flex-wrap: wrap;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const FilterSelect = styled.select`
  padding: 8px 12px;
  border: 1px solid #ddd;
  border-radius: 6px;
  background: white;
  color: #333;
  font-family: monospace;
  cursor: pointer;
  transition: all 0.2s ease;
  font-size: 0.9em;
  
  &:hover {
    background: #f5f5f5;
    border-color: rgba(10, 25, 41, 0.3);
  }
  
  &:focus {
    outline: none;
    border-color: #0d1b2a;
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
  }
`;

const MaintenanceTable = styled.div`
  border: 1px solid #ddd;
  border-radius: 6px;
  overflow: hidden;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.25s;
  animation-fill-mode: both;
`;

const TableHeader = styled.div`
  display: grid;
  grid-template-columns: 120px 100px 150px 120px 120px 100px 100px 100px 1fr 100px;
  background: linear-gradient(135deg, #f5f5f5 0%, #ffffff 100%);
  padding: 12px 15px;
  font-weight: bold;
  font-size: 0.85em;
  border-bottom: 2px solid #ddd;
  gap: 10px;
`;

const TableRow = styled.div`
  display: grid;
  grid-template-columns: 120px 100px 150px 120px 120px 100px 100px 100px 1fr 100px;
  padding: 12px 15px;
  border-bottom: 1px solid #eee;
  transition: all 0.2s ease;
  cursor: pointer;
  gap: 10px;
  align-items: center;
  font-size: 0.85em;
  
  &:hover {
    background: linear-gradient(90deg, #f0f0f0 0%, #f8f9fa 100%);
    border-left: 3px solid #0d1b2a;
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
  border-radius: 6px;
  font-size: 0.8em;
  font-weight: 500;
  display: inline-block;
  transition: all 0.2s ease;
  
  ${props => {
    if (props.$status) {
      switch (props.$status) {
        case 'PENDING': return 'background-color: #fff3e0; color: #ef6c00;';
        case 'RUNNING': return 'background-color: #e3f2fd; color: #1565c0;';
        case 'COMPLETED': return 'background-color: #e8f5e9; color: #2e7d32;';
        case 'FAILED': return 'background-color: #ffebee; color: #c62828;';
        case 'SKIPPED': return 'background-color: #f5f5f5; color: #757575;';
        default: return 'background-color: #f5f5f5; color: #757575;';
      }
    }
    if (props.$type) {
      return 'background-color: #f0f0f0; color: #333;';
    }
    return 'background-color: #f5f5f5; color: #757575;';
  }}
  
  &:hover {
    transform: scale(1.05);
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
  }
`;

const MaintenanceDetails = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '800px' : '0'};
  opacity: ${props => props.$isOpen ? '1' : '0'};
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  border-top: ${props => props.$isOpen ? '1px solid #eee' : 'none'};
  background-color: white;
  overflow: hidden;
`;

const DetailGrid = styled.div`
  display: grid;
  grid-template-columns: 200px 1fr;
  padding: 15px;
  gap: 10px;
  font-size: 0.9em;
`;

const DetailLabel = styled.div`
  color: #666;
  font-weight: 500;
`;

const DetailValue = styled.div`
  color: #333;
`;

const MetricsComparison = styled.div`
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 20px;
  padding: 15px;
  background: #f8f8f8;
  border-radius: 6px;
  margin: 15px;
`;

const MetricsColumn = styled.div`
  border: 1px solid #ddd;
  border-radius: 6px;
  padding: 15px;
  background: white;
`;

const MetricsTitle = styled.div`
  font-weight: bold;
  margin-bottom: 10px;
  color: #0d1b2a;
  border-bottom: 2px solid #0d1b2a;
  padding-bottom: 5px;
`;

const Pagination = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 10px;
  margin-top: 20px;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.3s;
  animation-fill-mode: both;
`;

const PageButton = styled.button<{ $active?: boolean; $disabled?: boolean }>`
  padding: 8px 16px;
  border: 1px solid #ddd;
  border-radius: 6px;
  background: ${props => props.$active ? '#0d1b2a' : 'white'};
  color: ${props => props.$active ? 'white' : '#333'};
  cursor: ${props => props.$disabled ? 'not-allowed' : 'pointer'};
  font-family: monospace;
  transition: all 0.2s ease;
  opacity: ${props => props.$disabled ? 0.5 : 1};
  
  &:hover:not(:disabled) {
    background: ${props => props.$active ? '#1e3a5f' : '#f5f5f5'};
    border-color: rgba(10, 25, 41, 0.3);
    transform: translateY(-2px);
  }
  
  &:disabled {
    cursor: not-allowed;
  }
`;

const Loading = styled.div`
  text-align: center;
  padding: 40px;
  color: #666;
  font-size: 1.1em;
`;

const Error = styled.div`
  background-color: #ffebee;
  color: #c62828;
  padding: 15px;
  border-radius: 6px;
  margin-bottom: 20px;
  border: 1px solid #ef9a9a;
`;

const Maintenance = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [maintenanceItems, setMaintenanceItems] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [openItemId, setOpenItemId] = useState<number | null>(null);
  const [activeTab, setActiveTab] = useState<'pending' | 'completed' | 'all'>('pending');
  const [filters, setFilters] = useState({
    maintenance_type: '',
    status: '',
    db_engine: ''
  });
  const [page, setPage] = useState(1);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 20
  });

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        const statusFilter = activeTab === 'pending' ? 'PENDING' : activeTab === 'completed' ? 'COMPLETED' : '';
        const [itemsData, metricsData] = await Promise.all([
          maintenanceApi.getMaintenanceItems({
            page,
            limit: 20,
            ...filters,
            status: statusFilter || filters.status
          }),
          maintenanceApi.getMetrics()
        ]);
        setMaintenanceItems(itemsData.data || []);
        setPagination(itemsData.pagination || pagination);
        setMetrics(metricsData || {});
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading maintenance data');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, [page, filters, activeTab]);

  const toggleItem = (id: number) => {
    setOpenItemId(openItemId === id ? null : id);
  };

  const formatBytes = (mb: number | string | null | undefined) => {
    if (mb === null || mb === undefined) return 'N/A';
    const numMb = Number(mb);
    if (isNaN(numMb)) return 'N/A';
    if (numMb < 1) return `${(numMb * 1024).toFixed(2)} KB`;
    if (numMb < 1024) return `${numMb.toFixed(2)} MB`;
    return `${(numMb / 1024).toFixed(2)} GB`;
  };

  const formatDate = (date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  };

  const formatDuration = (seconds: number | string | null | undefined) => {
    if (seconds === null || seconds === undefined) return 'N/A';
    const numSeconds = Number(seconds);
    if (isNaN(numSeconds)) return 'N/A';
    if (numSeconds < 60) return `${numSeconds.toFixed(2)}s`;
    if (numSeconds < 3600) return `${(numSeconds / 60).toFixed(2)}m`;
    return `${(numSeconds / 3600).toFixed(2)}h`;
  };

  if (loading && maintenanceItems.length === 0) {
    return (
      <MaintenanceContainer>
        <Header>Maintenance</Header>
        <Loading>Loading maintenance data...</Loading>
      </MaintenanceContainer>
    );
  }

  return (
    <MaintenanceContainer>
      <Header>Maintenance</Header>
      
      {error && <Error>{error}</Error>}
      
      <MetricsGrid>
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
        <FilterSelect
          value={filters.maintenance_type}
          onChange={(e) => {
            setFilters({ ...filters, maintenance_type: e.target.value });
            setPage(1);
          }}
        >
          <option value="">All Types</option>
          <option value="VACUUM">VACUUM</option>
          <option value="ANALYZE">ANALYZE</option>
          <option value="REINDEX">REINDEX</option>
          <option value="CLUSTER">CLUSTER</option>
        </FilterSelect>
        
        <FilterSelect
          value={filters.db_engine}
          onChange={(e) => {
            setFilters({ ...filters, db_engine: e.target.value });
            setPage(1);
          }}
        >
          <option value="">All Engines</option>
          <option value="PostgreSQL">PostgreSQL</option>
          <option value="MariaDB">MariaDB</option>
          <option value="MSSQL">MSSQL</option>
        </FilterSelect>
        
        {activeTab === 'all' && (
          <FilterSelect
            value={filters.status}
            onChange={(e) => {
              setFilters({ ...filters, status: e.target.value });
              setPage(1);
            }}
          >
            <option value="">All Status</option>
            <option value="PENDING">PENDING</option>
            <option value="RUNNING">RUNNING</option>
            <option value="COMPLETED">COMPLETED</option>
            <option value="FAILED">FAILED</option>
            <option value="SKIPPED">SKIPPED</option>
          </FilterSelect>
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
          <div style={{ padding: '40px', textAlign: 'center', color: '#666' }}>
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
                    <DetailValue style={{ color: '#c62828' }}>{item.error_details}</DetailValue>
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
            onClick={() => setPage(p => Math.max(1, p - 1))}
            disabled={page === 1}
          >
            Previous
          </PageButton>
          <span>
            Page {pagination.currentPage} of {pagination.totalPages} ({pagination.total} total)
          </span>
          <PageButton
            onClick={() => setPage(p => Math.min(pagination.totalPages, p + 1))}
            disabled={page === pagination.totalPages}
          >
            Next
          </PageButton>
        </Pagination>
      )}
    </MaintenanceContainer>
  );
};

const formatNumber = (num: number | null | undefined) => {
  if (num === null || num === undefined) return 'N/A';
  if (num >= 1000000) return `${(num / 1000000).toFixed(2)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(2)}K`;
  return num.toString();
};

export default Maintenance;

