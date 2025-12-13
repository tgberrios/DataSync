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
import { governanceCatalogOracleApi } from '../services/api';
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
  grid-template-columns: 120px 120px 150px 150px 100px 100px 120px 100px 1fr;
  background: ${theme.colors.gradient.primary};
  padding: 12px 15px;
  font-weight: bold;
  font-size: 0.8em;
  border-bottom: 2px solid ${theme.colors.border.dark};
  gap: 10px;
`;

const TableRow = styled.div<{ $expanded?: boolean }>`
  display: grid;
  grid-template-columns: 120px 120px 150px 150px 100px 100px 120px 100px 1fr;
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
  max-height: ${props => props.$isOpen ? '600px' : '0'};
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

/**
 * Governance Catalog component for Oracle
 * Displays governance metadata for Oracle tables including health status, access frequency, and recommendations
 */
const GovernanceCatalogOracle = () => {
  const { page, limit, setPage } = usePagination(1, 20);
  const { filters, setFilter } = useTableFilters({
    server_name: '',
    schema_name: '',
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
  const [schemas, setSchemas] = useState<string[]>([]);
  const isMountedRef = useRef(true);

  const fetchData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(filters.search as string, 100);
      const [itemsData, metricsData, serversData] = await Promise.all([
        governanceCatalogOracleApi.getOracleItems({
          page,
          limit,
          server_name: filters.server_name as string,
          schema_name: filters.schema_name as string,
          health_status: filters.health_status as string,
          access_frequency: filters.access_frequency as string,
          search: sanitizedSearch
        }),
        governanceCatalogOracleApi.getOracleMetrics(),
        governanceCatalogOracleApi.getOracleServers()
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
    filters.schema_name, 
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
    const fetchSchemas = async () => {
      if (filters.server_name && isMountedRef.current) {
        try {
          const schemasData = await governanceCatalogOracleApi.getOracleSchemas(filters.server_name as string);
          if (isMountedRef.current) {
            setSchemas(schemasData || []);
          }
        } catch (err) {
          if (isMountedRef.current) {
            console.error('Error loading schemas:', err);
          }
        }
      } else {
        setSchemas([]);
      }
    };
    fetchSchemas();
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

  const formatDate = useCallback((date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  }, []);

  const handleFilterChange = useCallback((key: string, value: string) => {
    setFilter(key as any, value);
    if (key === 'server_name') {
      setFilter('schema_name' as any, '');
    }
    setPage(1);
  }, [setFilter, setPage]);

  if (loading && items.length === 0) {
    return (
      <Container>
        <Header>Governance Catalog - Oracle</Header>
        <LoadingOverlay>Loading governance catalog...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>Governance Catalog - Oracle</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}
      
      <MetricsGrid $columns="repeat(auto-fit, minmax(180px, 1fr))">
        <MetricCard>
          <MetricLabel>Total Tables</MetricLabel>
          <MetricValue>{metrics.total_tables || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Total Size</MetricLabel>
          <MetricValue>{formatBytes(metrics.total_size_mb)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Total Rows</MetricLabel>
          <MetricValue>{formatNumber(metrics.total_rows)}</MetricValue>
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
          value={filters.schema_name as string}
          onChange={(e) => handleFilterChange('schema_name', e.target.value)}
          disabled={!filters.server_name}
        >
          <option value="">All Schemas</option>
          {schemas.map(schema => (
            <option key={schema} value={schema}>{schema}</option>
          ))}
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
          placeholder="Search table name..."
          value={filters.search as string}
          onChange={(e) => handleFilterChange('search', e.target.value)}
        />
      </FiltersContainer>

      <Table>
        <TableHeader>
          <TableCell>Server</TableCell>
          <TableCell>Schema</TableCell>
          <TableCell>Table</TableCell>
          <TableCell>Index</TableCell>
          <TableCell>Rows</TableCell>
          <TableCell>Size</TableCell>
          <TableCell>Health</TableCell>
          <TableCell>Access</TableCell>
          <TableCell>Score</TableCell>
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
                <TableCell>{item.schema_name || 'N/A'}</TableCell>
                <TableCell><strong>{item.table_name || 'N/A'}</strong></TableCell>
                <TableCell>{item.index_name || 'N/A'}</TableCell>
                <TableCell>{formatNumber(item.row_count)}</TableCell>
                <TableCell>{formatBytes(item.table_size_mb)}</TableCell>
                <TableCell>
                  <Badge $status={item.health_status}>{item.health_status || 'N/A'}</Badge>
                </TableCell>
                <TableCell>
                  <Badge $status={item.access_frequency}>{item.access_frequency || 'N/A'}</Badge>
                </TableCell>
                <TableCell>{item.health_score ? `${Number(item.health_score).toFixed(1)}` : 'N/A'}</TableCell>
              </TableRow>
              <DetailsPanel $isOpen={openItemId === item.id}>
                <DetailGrid>
                  <DetailLabel>Index Columns:</DetailLabel>
                  <DetailValue>{item.index_columns || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Index Unique:</DetailLabel>
                  <DetailValue>{item.index_unique ? 'Yes' : 'No'}</DetailValue>
                  
                  <DetailLabel>Index Type:</DetailLabel>
                  <DetailValue>{item.index_type || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Table Size:</DetailLabel>
                  <DetailValue>{formatBytes(item.table_size_mb)}</DetailValue>
                  
                  <DetailLabel>Index Size:</DetailLabel>
                  <DetailValue>{formatBytes(item.index_size_mb)}</DetailValue>
                  
                  <DetailLabel>Total Size:</DetailLabel>
                  <DetailValue>{formatBytes(item.total_size_mb)}</DetailValue>
                  
                  <DetailLabel>Tablespace:</DetailLabel>
                  <DetailValue>{item.tablespace_name || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Oracle Version:</DetailLabel>
                  <DetailValue>{item.version || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Block Size:</DetailLabel>
                  <DetailValue>{item.block_size ? `${item.block_size} bytes` : 'N/A'}</DetailValue>
                  
                  <DetailLabel>Num Rows:</DetailLabel>
                  <DetailValue>{formatNumber(item.num_rows)}</DetailValue>
                  
                  <DetailLabel>Blocks:</DetailLabel>
                  <DetailValue>{formatNumber(item.blocks)}</DetailValue>
                  
                  <DetailLabel>Empty Blocks:</DetailLabel>
                  <DetailValue>{formatNumber(item.empty_blocks)}</DetailValue>
                  
                  <DetailLabel>Avg Row Length:</DetailLabel>
                  <DetailValue>{formatNumber(item.avg_row_len)}</DetailValue>
                  
                  <DetailLabel>Chain Count:</DetailLabel>
                  <DetailValue>{formatNumber(item.chain_cnt)}</DetailValue>
                  
                  <DetailLabel>Compression:</DetailLabel>
                  <DetailValue>{item.compression || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Logging:</DetailLabel>
                  <DetailValue>{item.logging || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Partitioned:</DetailLabel>
                  <DetailValue>{item.partitioned || 'N/A'}</DetailValue>
                  
                  <DetailLabel>IOT Type:</DetailLabel>
                  <DetailValue>{item.iot_type || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Temporary:</DetailLabel>
                  <DetailValue>{item.temporary || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Recommendation:</DetailLabel>
                  <DetailValue>{item.recommendation_summary || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Snapshot Date:</DetailLabel>
                  <DetailValue>{formatDate(item.snapshot_date)}</DetailValue>
                </DetailGrid>
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

export default GovernanceCatalogOracle;
