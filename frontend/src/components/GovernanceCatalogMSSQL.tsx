import { useState, useEffect, useCallback, useRef } from 'react';
import styled from 'styled-components';
import {
  Container,
  Header,
  ErrorMessage,
  LoadingOverlay,
  Grid,
  Value,
  Pagination,
  PageButton,
} from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { governanceCatalogApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { sanitizeSearch } from '../utils/validation';
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

const FiltersContainer = styled.div`
  display: flex;
  gap: ${theme.spacing.sm};
  margin-bottom: ${theme.spacing.lg};
  flex-wrap: wrap;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.15s;
  animation-fill-mode: both;
`;

const FilterSelect = styled.select`
  padding: 8px 12px;
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  background: ${theme.colors.background.main};
  color: ${theme.colors.text.primary};
  font-family: ${theme.fonts.primary};
  cursor: pointer;
  transition: all ${theme.transitions.normal};
  font-size: 0.9em;
  
  &:hover {
    background: ${theme.colors.background.secondary};
    border-color: rgba(10, 25, 41, 0.3);
  }
  
  &:focus {
    outline: none;
    border-color: ${theme.colors.primary.main};
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
  }
  
  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
`;

const SearchInput = styled.input`
  flex: 1;
  min-width: 200px;
  padding: 8px 12px;
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  font-family: ${theme.fonts.primary};
  font-size: 0.9em;
  transition: all ${theme.transitions.normal};
  
  &:focus {
    outline: none;
    border-color: ${theme.colors.primary.main};
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
  }
`;

const Table = styled.div`
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  overflow: hidden;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const TableHeader = styled.div`
  display: grid;
  grid-template-columns: 120px 120px 120px 150px 100px 100px 120px 100px 1fr;
  background: ${theme.colors.gradient.primary};
  padding: 12px 15px;
  font-weight: bold;
  font-size: 0.8em;
  border-bottom: 2px solid ${theme.colors.border.dark};
  gap: 10px;
`;

const TableRow = styled.div<{ $expanded?: boolean }>`
  display: grid;
  grid-template-columns: 120px 120px 120px 150px 100px 100px 120px 100px 1fr;
  padding: 12px 15px;
  border-bottom: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  gap: 10px;
  align-items: center;
  font-size: 0.85em;
  background-color: ${props => props.$expanded ? theme.colors.background.secondary : theme.colors.background.main};
  
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

const Badge = styled.span<{ $status?: string }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.75em;
  font-weight: 500;
  display: inline-block;
  transition: all ${theme.transitions.normal};
  
  ${props => {
    switch (props.$status) {
      case 'EXCELLENT':
      case 'HEALTHY':
        return `background-color: ${theme.colors.status.success.bg}; color: ${theme.colors.status.success.text};`;
      case 'WARNING':
        return `background-color: ${theme.colors.status.warning.bg}; color: ${theme.colors.status.warning.text};`;
      case 'CRITICAL':
      case 'EMERGENCY':
        return `background-color: ${theme.colors.status.error.bg}; color: ${theme.colors.status.error.text};`;
      case 'REAL_TIME':
      case 'HIGH':
        return `background-color: ${theme.colors.primary.light}; color: ${theme.colors.primary.dark};`;
      case 'MEDIUM':
        return `background-color: ${theme.colors.status.info.bg}; color: ${theme.colors.status.info.text};`;
      case 'LOW':
      case 'RARE':
        return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.secondary};`;
      case 'TABLE':
        return `background-color: ${theme.colors.primary.light}; color: ${theme.colors.primary.dark};`;
      case 'VIEW':
        return `background-color: ${theme.colors.status.info.bg}; color: ${theme.colors.status.info.text};`;
      case 'STORED_PROCEDURE':
        return `background-color: ${theme.colors.status.success.bg}; color: ${theme.colors.status.success.text};`;
      default:
        return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.secondary};`;
    }
  }}
  
  &:hover {
    transform: scale(1.05);
    box-shadow: ${theme.shadows.sm};
  }
`;

const DetailsPanel = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '700px' : '0'};
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

const PerformanceGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: ${theme.spacing.sm};
  padding: ${theme.spacing.md};
  background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.main} 100%);
  border-radius: ${theme.borderRadius.md};
  border: 1px solid ${theme.colors.border.light};
  margin: ${theme.spacing.md};
`;

const PerformanceMetric = styled.div`
  padding: ${theme.spacing.sm};
  background: ${theme.colors.background.main};
  border-radius: ${theme.borderRadius.md};
  border: 1px solid ${theme.colors.border.medium};
`;

const PerformanceLabel = styled.div`
  font-size: 0.8em;
  color: ${theme.colors.text.secondary};
  margin-bottom: 5px;
`;

const PerformanceValue = styled.div`
  font-size: 1.1em;
  font-weight: bold;
  color: ${theme.colors.text.primary};
`;

/**
 * Governance Catalog component for MSSQL
 * Displays governance metadata for MSSQL objects including tables, views, and stored procedures
 */
const GovernanceCatalogMSSQL = () => {
  const { page, limit, setPage } = usePagination(1, 20);
  const { filters, setFilter } = useTableFilters({
    server_name: '',
    database_name: '',
    object_type: '',
    health_status: '',
    access_frequency: '',
    search: ''
  });
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [items, setItems] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [openItemId, setOpenItemId] = useState<number | null>(null);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 20
  });
  const [servers, setServers] = useState<string[]>([]);
  const [databases, setDatabases] = useState<string[]>([]);
  const isMountedRef = useRef(true);

  const fetchData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(filters.search as string, 100);
      const [itemsData, metricsData, serversData] = await Promise.all([
        governanceCatalogApi.getMSSQLItems({
          page,
          limit,
          server_name: filters.server_name as string,
          database_name: filters.database_name as string,
          object_type: filters.object_type as string,
          health_status: filters.health_status as string,
          access_frequency: filters.access_frequency as string,
          search: sanitizedSearch
        }),
        governanceCatalogApi.getMSSQLMetrics(),
        governanceCatalogApi.getMSSQLServers()
      ]);
      if (isMountedRef.current) {
        setItems(itemsData.data || []);
        setPagination(itemsData.pagination || {
          total: 0,
          totalPages: 0,
          currentPage: 1,
          limit: 20
        });
        setMetrics(metricsData || {});
        setServers(serversData || []);
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
  }, [
    page, 
    limit, 
    filters.server_name, 
    filters.database_name, 
    filters.object_type, 
    filters.health_status, 
    filters.access_frequency, 
    filters.search
  ]);

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

  useEffect(() => {
    const fetchDatabases = async () => {
      if (filters.server_name && isMountedRef.current) {
        try {
          const databasesData = await governanceCatalogApi.getMSSQLDatabases(filters.server_name as string);
          if (isMountedRef.current) {
            setDatabases(databasesData || []);
          }
        } catch (err) {
          if (isMountedRef.current) {
            console.error('Error loading databases:', err);
          }
        }
      } else {
        setDatabases([]);
      }
    };
    fetchDatabases();
  }, [filters.server_name]);

  const toggleItem = useCallback((id: number) => {
    setOpenItemId(prev => prev === id ? null : id);
  }, []);

  const formatBytes = useCallback((mb: number | string | null | undefined) => {
    if (mb === null || mb === undefined) return 'N/A';
    const num = Number(mb);
    if (isNaN(num)) return 'N/A';
    if (num < 1) return `${(num * 1024).toFixed(2)} KB`;
    return `${num.toFixed(2)} MB`;
  }, []);

  const formatNumber = useCallback((value: number | string | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    const num = Number(value);
    if (isNaN(num)) return 'N/A';
    return num.toLocaleString();
  }, []);

  const formatPercentage = useCallback((value: number | string | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    const num = Number(value);
    if (isNaN(num)) return 'N/A';
    return `${num.toFixed(2)}%`;
  }, []);

  const formatDate = useCallback((date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  }, []);

  const formatTime = useCallback((seconds: number | string | null | undefined) => {
    if (seconds === null || seconds === undefined) return 'N/A';
    const num = Number(seconds);
    if (isNaN(num)) return 'N/A';
    if (num < 1) return `${(num * 1000).toFixed(2)} ms`;
    return `${num.toFixed(2)} s`;
  }, []);

  const handleFilterChange = useCallback((key: string, value: string) => {
    setFilter(key as any, value);
    if (key === 'server_name') {
      setFilter('database_name' as any, '');
    }
    setPage(1);
  }, [setFilter, setPage]);

  if (loading && items.length === 0) {
    return (
      <Container>
        <Header>Governance Catalog - MSSQL</Header>
        <LoadingOverlay>Loading governance catalog...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>Governance Catalog - MSSQL</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}
      
      <MetricsGrid $columns="repeat(auto-fit, minmax(180px, 1fr))">
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
          value={filters.server_name as string}
          onChange={(e) => handleFilterChange('server_name', e.target.value)}
        >
          <option value="">All Servers</option>
          {servers.map(server => (
            <option key={server} value={server}>{server}</option>
          ))}
        </FilterSelect>
        
        <FilterSelect
          value={filters.database_name as string}
          onChange={(e) => handleFilterChange('database_name', e.target.value)}
          disabled={!filters.server_name}
        >
          <option value="">All Databases</option>
          {databases.map(db => (
            <option key={db} value={db}>{db}</option>
          ))}
        </FilterSelect>
        
        <FilterSelect
          value={filters.object_type as string}
          onChange={(e) => handleFilterChange('object_type', e.target.value)}
        >
          <option value="">All Types</option>
          <option value="TABLE">TABLE</option>
          <option value="VIEW">VIEW</option>
          <option value="STORED_PROCEDURE">STORED_PROCEDURE</option>
        </FilterSelect>
        
        <FilterSelect
          value={filters.health_status as string}
          onChange={(e) => handleFilterChange('health_status', e.target.value)}
        >
          <option value="">All Health Status</option>
          <option value="EXCELLENT">Excellent</option>
          <option value="HEALTHY">Healthy</option>
          <option value="WARNING">Warning</option>
          <option value="CRITICAL">Critical</option>
        </FilterSelect>
        
        <FilterSelect
          value={filters.access_frequency as string}
          onChange={(e) => handleFilterChange('access_frequency', e.target.value)}
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
          value={filters.search as string}
          onChange={(e) => handleFilterChange('search', e.target.value)}
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
          <div style={{ padding: '40px', textAlign: 'center', color: theme.colors.text.secondary }}>
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

export default GovernanceCatalogMSSQL;
