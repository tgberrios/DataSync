import { useState, useEffect } from 'react';
import styled from 'styled-components';
import { governanceCatalogApi } from '../services/api';

const GovernanceContainer = styled.div`
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

const FiltersContainer = styled.div`
  display: flex;
  gap: 10px;
  margin-bottom: 20px;
  flex-wrap: wrap;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.15s;
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

const SearchInput = styled.input`
  flex: 1;
  min-width: 200px;
  padding: 8px 12px;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-family: monospace;
  font-size: 0.9em;
  transition: all 0.2s ease;
  
  &:focus {
    outline: none;
    border-color: #0d1b2a;
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
  }
`;

const Table = styled.div`
  border: 1px solid #ddd;
  border-radius: 6px;
  overflow: hidden;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const TableHeader = styled.div`
  display: grid;
  grid-template-columns: 120px 120px 120px 150px 100px 100px 120px 100px 1fr;
  background: linear-gradient(135deg, #f5f5f5 0%, #ffffff 100%);
  padding: 12px 15px;
  font-weight: bold;
  font-size: 0.8em;
  border-bottom: 2px solid #ddd;
  gap: 10px;
`;

const TableRow = styled.div<{ $expanded?: boolean }>`
  display: grid;
  grid-template-columns: 120px 120px 120px 150px 100px 100px 120px 100px 1fr;
  padding: 12px 15px;
  border-bottom: 1px solid #eee;
  transition: all 0.2s ease;
  cursor: pointer;
  gap: 10px;
  align-items: center;
  font-size: 0.85em;
  background-color: ${props => props.$expanded ? '#f8f9fa' : 'white'};
  
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

const Badge = styled.span<{ $status?: string }>`
  padding: 4px 10px;
  border-radius: 6px;
  font-size: 0.75em;
  font-weight: 500;
  display: inline-block;
  transition: all 0.2s ease;
  
  ${props => {
    switch (props.$status) {
      case 'EXCELLENT':
      case 'HEALTHY':
        return 'background-color: #e8f5e9; color: #2e7d32;';
      case 'WARNING':
        return 'background-color: #fff3e0; color: #ef6c00;';
      case 'CRITICAL':
      case 'EMERGENCY':
        return 'background-color: #ffebee; color: #c62828;';
      case 'REAL_TIME':
      case 'HIGH':
        return 'background-color: #e3f2fd; color: #1565c0;';
      case 'MEDIUM':
        return 'background-color: #f3e5f5; color: #7b1fa2;';
      case 'LOW':
      case 'RARE':
        return 'background-color: #f5f5f5; color: #757575;';
      case 'TABLE':
        return 'background-color: #e3f2fd; color: #1565c0;';
      case 'VIEW':
        return 'background-color: #f3e5f5; color: #7b1fa2;';
      case 'STORED_PROCEDURE':
        return 'background-color: #e8f5e9; color: #2e7d32;';
      default:
        return 'background-color: #f5f5f5; color: #757575;';
    }
  }}
  
  &:hover {
    transform: scale(1.05);
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
  }
`;

const DetailsPanel = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '700px' : '0'};
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

const PerformanceGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 10px;
  padding: 15px;
  background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
  border-radius: 6px;
  border: 1px solid #eee;
  margin: 15px;
`;

const PerformanceMetric = styled.div`
  padding: 10px;
  background: white;
  border-radius: 6px;
  border: 1px solid #ddd;
`;

const PerformanceLabel = styled.div`
  font-size: 0.8em;
  color: #666;
  margin-bottom: 5px;
`;

const PerformanceValue = styled.div`
  font-size: 1.1em;
  font-weight: bold;
  color: #0d1b2a;
`;

const Pagination = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 10px;
  margin-top: 20px;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.25s;
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

const GovernanceCatalogMSSQL = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [items, setItems] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [openItemId, setOpenItemId] = useState<number | null>(null);
  const [filters, setFilters] = useState({
    server_name: '',
    database_name: '',
    object_type: '',
    health_status: '',
    access_frequency: '',
    search: ''
  });
  const [page, setPage] = useState(1);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 20
  });
  const [servers, setServers] = useState<string[]>([]);
  const [databases, setDatabases] = useState<string[]>([]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        const [itemsData, metricsData, serversData] = await Promise.all([
          governanceCatalogApi.getMSSQLItems({
            page,
            limit: 20,
            ...filters
          }),
          governanceCatalogApi.getMSSQLMetrics(),
          governanceCatalogApi.getMSSQLServers()
        ]);
        setItems(itemsData.data || []);
        setPagination(itemsData.pagination || pagination);
        setMetrics(metricsData || {});
        setServers(serversData || []);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading governance catalog');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, [page, filters]);

  useEffect(() => {
    const fetchDatabases = async () => {
      if (filters.server_name) {
        try {
          const databasesData = await governanceCatalogApi.getMSSQLDatabases(filters.server_name);
          setDatabases(databasesData || []);
        } catch (err) {
          console.error('Error loading databases:', err);
        }
      } else {
        setDatabases([]);
      }
    };
    fetchDatabases();
  }, [filters.server_name]);

  const toggleItem = (id: number) => {
    setOpenItemId(openItemId === id ? null : id);
  };

  const formatBytes = (mb: number | string | null | undefined) => {
    if (mb === null || mb === undefined) return 'N/A';
    const num = Number(mb);
    if (isNaN(num)) return 'N/A';
    if (num < 1) return `${(num * 1024).toFixed(2)} KB`;
    return `${num.toFixed(2)} MB`;
  };

  const formatNumber = (value: number | string | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    const num = Number(value);
    if (isNaN(num)) return 'N/A';
    return num.toLocaleString();
  };

  const formatPercentage = (value: number | string | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    const num = Number(value);
    if (isNaN(num)) return 'N/A';
    return `${num.toFixed(2)}%`;
  };

  const formatDate = (date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  };

  const formatTime = (seconds: number | string | null | undefined) => {
    if (seconds === null || seconds === undefined) return 'N/A';
    const num = Number(seconds);
    if (isNaN(num)) return 'N/A';
    if (num < 1) return `${(num * 1000).toFixed(2)} ms`;
    return `${num.toFixed(2)} s`;
  };

  if (loading && items.length === 0) {
    return (
      <GovernanceContainer>
        <Header>Governance Catalog - MSSQL</Header>
        <Loading>Loading governance catalog...</Loading>
      </GovernanceContainer>
    );
  }

  return (
    <GovernanceContainer>
      <Header>Governance Catalog - MSSQL</Header>
      
      {error && <Error>{error}</Error>}
      
      <MetricsGrid>
        <MetricCard>
          <MetricLabel>Total Objects</MetricLabel>
          <MetricValue>{metrics.total_objects || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Total Size</MetricLabel>
          <MetricValue>{formatBytes(metrics.total_size_mb)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Healthy</MetricLabel>
          <MetricValue>{metrics.healthy_count || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Warning</MetricLabel>
          <MetricValue>{metrics.warning_count || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Critical</MetricLabel>
          <MetricValue>{metrics.critical_count || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Unique Servers</MetricLabel>
          <MetricValue>{metrics.unique_servers || 0}</MetricValue>
        </MetricCard>
      </MetricsGrid>

      <FiltersContainer>
        <FilterSelect
          value={filters.server_name}
          onChange={(e) => {
            setFilters({ ...filters, server_name: e.target.value, database_name: '' });
            setPage(1);
          }}
        >
          <option value="">All Servers</option>
          {servers.map(server => (
            <option key={server} value={server}>{server}</option>
          ))}
        </FilterSelect>
        
        <FilterSelect
          value={filters.database_name}
          onChange={(e) => {
            setFilters({ ...filters, database_name: e.target.value });
            setPage(1);
          }}
          disabled={!filters.server_name}
        >
          <option value="">All Databases</option>
          {databases.map(db => (
            <option key={db} value={db}>{db}</option>
          ))}
        </FilterSelect>
        
        <FilterSelect
          value={filters.object_type}
          onChange={(e) => {
            setFilters({ ...filters, object_type: e.target.value });
            setPage(1);
          }}
        >
          <option value="">All Types</option>
          <option value="TABLE">TABLE</option>
          <option value="VIEW">VIEW</option>
          <option value="STORED_PROCEDURE">STORED_PROCEDURE</option>
        </FilterSelect>
        
        <FilterSelect
          value={filters.health_status}
          onChange={(e) => {
            setFilters({ ...filters, health_status: e.target.value });
            setPage(1);
          }}
        >
          <option value="">All Health Status</option>
          <option value="EXCELLENT">Excellent</option>
          <option value="HEALTHY">Healthy</option>
          <option value="WARNING">Warning</option>
          <option value="CRITICAL">Critical</option>
        </FilterSelect>
        
        <FilterSelect
          value={filters.access_frequency}
          onChange={(e) => {
            setFilters({ ...filters, access_frequency: e.target.value });
            setPage(1);
          }}
        >
          <option value="">All Access Frequency</option>
          <option value="REAL_TIME">Real Time</option>
          <option value="HIGH">High</option>
          <option value="MEDIUM">Medium</option>
          <option value="LOW">Low</option>
          <option value="RARE">Rare</option>
        </FilterSelect>
        
        <SearchInput
          type="text"
          placeholder="Search object name..."
          value={filters.search}
          onChange={(e) => {
            setFilters({ ...filters, search: e.target.value });
            setPage(1);
          }}
        />
      </FiltersContainer>

      <Table>
        <TableHeader>
          <TableCell>Server</TableCell>
          <TableCell>Database</TableCell>
          <TableCell>Schema</TableCell>
          <TableCell>Object</TableCell>
          <TableCell>Type</TableCell>
          <TableCell>Rows</TableCell>
          <TableCell>Size</TableCell>
          <TableCell>Health</TableCell>
          <TableCell>Access</TableCell>
        </TableHeader>
        {items.length === 0 ? (
          <div style={{ padding: '40px', textAlign: 'center', color: '#666' }}>
            No governance data available. Data will appear here once collected.
          </div>
        ) : (
          items.map((item) => (
            <div key={item.id}>
              <TableRow $expanded={openItemId === item.id} onClick={() => toggleItem(item.id)}>
                <TableCell>{item.server_name || 'N/A'}</TableCell>
                <TableCell>{item.database_name || 'N/A'}</TableCell>
                <TableCell>{item.schema_name || 'N/A'}</TableCell>
                <TableCell><strong>{item.object_name || item.table_name || 'N/A'}</strong></TableCell>
                <TableCell>
                  <Badge $status={item.object_type}>{item.object_type || 'N/A'}</Badge>
                </TableCell>
                <TableCell>{formatNumber(item.row_count)}</TableCell>
                <TableCell>{formatBytes(item.table_size_mb)}</TableCell>
                <TableCell>
                  <Badge $status={item.health_status}>{item.health_status || 'N/A'}</Badge>
                </TableCell>
                <TableCell>
                  <Badge $status={item.access_frequency}>{item.access_frequency || 'N/A'}</Badge>
                </TableCell>
              </TableRow>
              <DetailsPanel $isOpen={openItemId === item.id}>
                <DetailGrid>
                  <DetailLabel>Object ID:</DetailLabel>
                  <DetailValue>{formatNumber(item.object_id)}</DetailValue>
                  
                  <DetailLabel>Index Name:</DetailLabel>
                  <DetailValue>{item.index_name || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Index ID:</DetailLabel>
                  <DetailValue>{formatNumber(item.index_id)}</DetailValue>
                  
                  <DetailLabel>Fragmentation:</DetailLabel>
                  <DetailValue>{formatPercentage(item.fragmentation_pct)}</DetailValue>
                  
                  <DetailLabel>Page Count:</DetailLabel>
                  <DetailValue>{formatNumber(item.page_count)}</DetailValue>
                  
                  <DetailLabel>Fill Factor:</DetailLabel>
                  <DetailValue>{item.fill_factor ? `${item.fill_factor}%` : 'N/A'}</DetailValue>
                  
                  <DetailLabel>Index Key Columns:</DetailLabel>
                  <DetailValue>{item.index_key_columns || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Index Include Columns:</DetailLabel>
                  <DetailValue>{item.index_include_columns || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Has Missing Index:</DetailLabel>
                  <DetailValue>{item.has_missing_index ? 'Yes' : 'No'}</DetailValue>
                  
                  <DetailLabel>Is Unused:</DetailLabel>
                  <DetailValue>{item.is_unused ? 'Yes' : 'No'}</DetailValue>
                  
                  <DetailLabel>Compatibility Level:</DetailLabel>
                  <DetailValue>{item.compatibility_level || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Recovery Model:</DetailLabel>
                  <DetailValue>{item.recovery_model || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Health Score:</DetailLabel>
                  <DetailValue>{item.health_score ? `${Number(item.health_score).toFixed(2)}` : 'N/A'}</DetailValue>
                  
                  <DetailLabel>Recommendation:</DetailLabel>
                  <DetailValue>{item.recommendation_summary || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Last Full Backup:</DetailLabel>
                  <DetailValue>{formatDate(item.last_full_backup)}</DetailValue>
                  
                  <DetailLabel>Last Diff Backup:</DetailLabel>
                  <DetailValue>{formatDate(item.last_diff_backup)}</DetailValue>
                  
                  <DetailLabel>Last Log Backup:</DetailLabel>
                  <DetailValue>{formatDate(item.last_log_backup)}</DetailValue>
                  
                  <DetailLabel>Snapshot Date:</DetailLabel>
                  <DetailValue>{formatDate(item.snapshot_date)}</DetailValue>
                </DetailGrid>
                
                {(item.user_seeks || item.user_scans || item.user_lookups || item.user_updates || item.execution_count) && (
                  <PerformanceGrid>
                    <PerformanceMetric>
                      <PerformanceLabel>User Seeks</PerformanceLabel>
                      <PerformanceValue>{formatNumber(item.user_seeks)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>User Scans</PerformanceLabel>
                      <PerformanceValue>{formatNumber(item.user_scans)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>User Lookups</PerformanceLabel>
                      <PerformanceValue>{formatNumber(item.user_lookups)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>User Updates</PerformanceLabel>
                      <PerformanceValue>{formatNumber(item.user_updates)}</PerformanceValue>
                    </PerformanceMetric>
                    {item.execution_count && (
                      <PerformanceMetric>
                        <PerformanceLabel>Execution Count</PerformanceLabel>
                        <PerformanceValue>{formatNumber(item.execution_count)}</PerformanceValue>
                      </PerformanceMetric>
                    )}
                    {item.avg_execution_time_seconds && (
                      <PerformanceMetric>
                        <PerformanceLabel>Avg Execution Time</PerformanceLabel>
                        <PerformanceValue>{formatTime(item.avg_execution_time_seconds)}</PerformanceValue>
                      </PerformanceMetric>
                    )}
                    {item.avg_logical_reads && (
                      <PerformanceMetric>
                        <PerformanceLabel>Avg Logical Reads</PerformanceLabel>
                        <PerformanceValue>{formatNumber(item.avg_logical_reads)}</PerformanceValue>
                      </PerformanceMetric>
                    )}
                    {item.avg_physical_reads && (
                      <PerformanceMetric>
                        <PerformanceLabel>Avg Physical Reads</PerformanceLabel>
                        <PerformanceValue>{formatNumber(item.avg_physical_reads)}</PerformanceValue>
                      </PerformanceMetric>
                    )}
                  </PerformanceGrid>
                )}
              </DetailsPanel>
            </div>
          ))
        )}
      </Table>

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
    </GovernanceContainer>
  );
};

export default GovernanceCatalogMSSQL;

