import { useState, useEffect, useCallback, useRef } from 'react';
import styled from 'styled-components';
import { queryPerformanceApi } from '../services/api';
import { Container, Header, Select, FiltersContainer, Input, Pagination, PageButton, LoadingOverlay, ErrorMessage } from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { extractApiError } from '../utils/errorHandler';

const MetricsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
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


const QueryTable = styled.div`
  border: 1px solid #ddd;
  border-radius: 6px;
  overflow: hidden;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const TableHeader = styled.div`
  display: grid;
  grid-template-columns: 80px 100px 120px 100px 1fr 100px 100px 100px;
  background: linear-gradient(135deg, #f5f5f5 0%, #ffffff 100%);
  padding: 12px 15px;
  font-weight: bold;
  font-size: 0.9em;
  border-bottom: 2px solid #ddd;
  gap: 10px;
`;

const TableRow = styled.div`
  display: grid;
  grid-template-columns: 80px 100px 120px 100px 1fr 100px 100px 100px;
  padding: 12px 15px;
  border-bottom: 1px solid #eee;
  transition: all 0.2s ease;
  cursor: pointer;
  gap: 10px;
  align-items: center;
  
  &:hover {
    background: linear-gradient(90deg, #f0f0f0 0%, #f8f9fa 100%);
    border-left: 3px solid #0d1b2a;
  }
  
  &:last-child {
    border-bottom: none;
  }
`;

const TableCell = styled.div`
  font-size: 0.9em;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`;

const QueryTextCell = styled.div`
  font-size: 0.85em;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-family: monospace;
  color: #555;
`;

const Badge = styled.span<{ $tier?: string; $type?: string }>`
  padding: 4px 10px;
  border-radius: 6px;
  font-size: 0.8em;
  font-weight: 500;
  display: inline-block;
  transition: all 0.2s ease;
  
  ${props => {
    if (props.$tier) {
      switch (props.$tier) {
        case 'EXCELLENT': return 'background-color: #e8f5e9; color: #2e7d32;';
        case 'GOOD': return 'background-color: #e3f2fd; color: #1565c0;';
        case 'FAIR': return 'background-color: #fff3e0; color: #ef6c00;';
        case 'POOR': return 'background-color: #ffebee; color: #c62828;';
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

const QueryDetails = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '600px' : '0'};
  opacity: ${props => props.$isOpen ? '1' : '0'};
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  border-top: ${props => props.$isOpen ? '1px solid #eee' : 'none'};
  background-color: white;
  overflow: hidden;
`;

const DetailGrid = styled.div`
  display: grid;
  grid-template-columns: 180px 1fr;
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

const QueryText = styled.pre`
  margin: 0;
  padding: 15px;
  background-color: #f8f8f8;
  border-radius: 6px;
  overflow-x: auto;
  font-size: 0.85em;
  border: 1px solid #eee;
  transition: all 0.2s ease;
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.2);
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  }
`;


/**
 * Componente para monitorear el rendimiento de consultas
 * Muestra métricas de rendimiento, consultas lentas y detalles de ejecución
 */
const QueryPerformance = () => {
  const isMountedRef = useRef(true);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [queries, setQueries] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [openQueryId, setOpenQueryId] = useState<number | null>(null);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 20
  });

  const { page, setPage } = usePagination(1, 20);
  const { filters, setFilter } = useTableFilters({
    performance_tier: '',
    operation_type: '',
    source_type: '',
    search: ''
  });

  /**
   * Obtiene los datos de rendimiento de consultas desde la API
   */
  const fetchData = useCallback(async () => {
    if (!isMountedRef.current) return;
    
    try {
      setLoading(true);
      setError(null);
      const [queriesData, metricsData] = await Promise.all([
        queryPerformanceApi.getQueries({
          page,
          limit: 20,
          performance_tier: filters.performance_tier as string,
          operation_type: filters.operation_type as string,
          source_type: filters.source_type as string,
          search: filters.search as string
        }),
        queryPerformanceApi.getMetrics()
      ]);
      
      if (isMountedRef.current) {
        setQueries(queriesData.data || []);
        setPagination(queriesData.pagination || {
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
  }, [page, filters.performance_tier, filters.operation_type, filters.source_type, filters.search]);

  useEffect(() => {
    isMountedRef.current = true;
    fetchData();
    
    const interval = setInterval(fetchData, 30000);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchData]);

  /**
   * Alterna la expansión de una consulta
   */
  const toggleQuery = useCallback((id: number) => {
    setOpenQueryId(prev => prev === id ? null : id);
  }, []);

  /**
   * Formatea un tiempo en milisegundos a formato legible
   */
  const formatTime = useCallback((ms: number | string | null | undefined) => {
    if (!ms) return 'N/A';
    const numMs = Number(ms);
    if (isNaN(numMs)) return 'N/A';
    if (numMs < 1) return `${(numMs * 1000).toFixed(2)}μs`;
    if (numMs < 1000) return `${numMs.toFixed(2)}ms`;
    return `${(numMs / 1000).toFixed(2)}s`;
  }, []);

  /**
   * Formatea un número a formato legible (K, M)
   */
  const formatNumber = useCallback((num: number | string | null | undefined) => {
    if (num === null || num === undefined) return 'N/A';
    const numVal = Number(num);
    if (isNaN(numVal)) return 'N/A';
    if (numVal >= 1000000) return `${(numVal / 1000000).toFixed(2)}M`;
    if (numVal >= 1000) return `${(numVal / 1000).toFixed(2)}K`;
    return numVal.toString();
  }, []);

  return (
    <Container>
      <Header>Query Performance</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}
      
      <MetricsGrid>
        <MetricCard>
          <MetricLabel>Total Queries</MetricLabel>
          <MetricValue>{formatNumber(metrics.total_queries)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Excellent</MetricLabel>
          <MetricValue>{formatNumber(metrics.excellent_count)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Good</MetricLabel>
          <MetricValue>{formatNumber(metrics.good_count)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Fair</MetricLabel>
          <MetricValue>{formatNumber(metrics.fair_count)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Poor</MetricLabel>
          <MetricValue>{formatNumber(metrics.poor_count)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Long Running</MetricLabel>
          <MetricValue>{formatNumber(metrics.long_running_count)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Blocking</MetricLabel>
          <MetricValue>{formatNumber(metrics.blocking_count)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Avg Efficiency</MetricLabel>
          <MetricValue>{metrics.avg_efficiency ? `${Number(metrics.avg_efficiency).toFixed(1)}%` : 'N/A'}</MetricValue>
        </MetricCard>
      </MetricsGrid>

      <FiltersContainer>
        <Select
          value={filters.performance_tier as string}
          onChange={(e) => {
            setFilter('performance_tier', e.target.value);
            setPage(1);
          }}
        >
          <option value="">All Tiers</option>
          <option value="EXCELLENT">Excellent</option>
          <option value="GOOD">Good</option>
          <option value="FAIR">Fair</option>
          <option value="POOR">Poor</option>
        </Select>
        
        <Select
          value={filters.operation_type as string}
          onChange={(e) => {
            setFilter('operation_type', e.target.value);
            setPage(1);
          }}
        >
          <option value="">All Operations</option>
          <option value="SELECT">SELECT</option>
          <option value="INSERT">INSERT</option>
          <option value="UPDATE">UPDATE</option>
          <option value="DELETE">DELETE</option>
        </Select>
        
        <Select
          value={filters.source_type as string}
          onChange={(e) => {
            setFilter('source_type', e.target.value);
            setPage(1);
          }}
        >
          <option value="">All Sources</option>
          <option value="snapshot">Snapshot</option>
          <option value="activity">Activity</option>
        </Select>
        
        <Input
          type="text"
          placeholder="Search query text..."
          value={filters.search as string}
          onChange={(e) => {
            setFilter('search', e.target.value);
            setPage(1);
          }}
        />
      </FiltersContainer>

      <QueryTable>
        <TableHeader>
          <TableCell>Tier</TableCell>
          <TableCell>Source</TableCell>
          <TableCell>Operation</TableCell>
          <TableCell>Duration</TableCell>
          <TableCell>Query</TableCell>
          <TableCell>Efficiency</TableCell>
          <TableCell>Cache Hit</TableCell>
          <TableCell>Rows</TableCell>
        </TableHeader>
        {queries.length === 0 ? (
          <div style={{ padding: '40px', textAlign: 'center', color: '#666' }}>
            No query performance data available. Data will appear here once queries are captured.
          </div>
        ) : (
          queries.map((query) => (
          <div key={query.id}>
            <TableRow onClick={() => toggleQuery(query.id)}>
              <TableCell>
                <Badge $tier={query.performance_tier}>{query.performance_tier || 'N/A'}</Badge>
              </TableCell>
              <TableCell>
                <Badge $type={query.source_type}>{query.source_type || 'N/A'}</Badge>
              </TableCell>
              <TableCell>{query.operation_type || 'N/A'}</TableCell>
              <TableCell>{formatTime(query.mean_time_ms || query.query_duration_ms)}</TableCell>
              <QueryTextCell title={query.query_text}>
                {query.query_text?.substring(0, 80) || 'N/A'}...
              </QueryTextCell>
              <TableCell>{query.query_efficiency_score ? `${Number(query.query_efficiency_score).toFixed(1)}%` : 'N/A'}</TableCell>
              <TableCell>{query.cache_hit_ratio ? `${Number(query.cache_hit_ratio).toFixed(1)}%` : 'N/A'}</TableCell>
              <TableCell>{formatNumber(query.rows_returned)}</TableCell>
            </TableRow>
            <QueryDetails $isOpen={openQueryId === query.id}>
              <DetailGrid>
                <DetailLabel>Query ID:</DetailLabel>
                <DetailValue>{query.queryid || 'N/A'}</DetailValue>
                
                <DetailLabel>Database:</DetailLabel>
                <DetailValue>{query.dbname || 'N/A'}</DetailValue>
                
                <DetailLabel>Username:</DetailLabel>
                <DetailValue>{query.username || 'N/A'}</DetailValue>
                
                <DetailLabel>Application:</DetailLabel>
                <DetailValue>{query.application_name || 'N/A'}</DetailValue>
                
                <DetailLabel>State:</DetailLabel>
                <DetailValue>{query.state || 'N/A'}</DetailValue>
                
                <DetailLabel>Wait Event:</DetailLabel>
                <DetailValue>{query.wait_event_type || 'N/A'}</DetailValue>
                
                <DetailLabel>Calls:</DetailLabel>
                <DetailValue>{formatNumber(query.calls)}</DetailValue>
                
                <DetailLabel>Total Time:</DetailLabel>
                <DetailValue>{formatTime(query.total_time_ms)}</DetailValue>
                
                <DetailLabel>Mean Time:</DetailLabel>
                <DetailValue>{formatTime(query.mean_time_ms)}</DetailValue>
                
                <DetailLabel>Min Time:</DetailLabel>
                <DetailValue>{formatTime(query.min_time_ms)}</DetailValue>
                
                <DetailLabel>Max Time:</DetailLabel>
                <DetailValue>{formatTime(query.max_time_ms)}</DetailValue>
                
                <DetailLabel>Query Duration:</DetailLabel>
                <DetailValue>{formatTime(query.query_duration_ms)}</DetailValue>
                
                <DetailLabel>Cache Hit Ratio:</DetailLabel>
                <DetailValue>{query.cache_hit_ratio ? `${Number(query.cache_hit_ratio).toFixed(2)}%` : 'N/A'}</DetailValue>
                
                <DetailLabel>IO Efficiency:</DetailLabel>
                <DetailValue>{query.io_efficiency ? Number(query.io_efficiency).toFixed(2) : 'N/A'}</DetailValue>
                
                <DetailLabel>Query Efficiency Score:</DetailLabel>
                <DetailValue>{query.query_efficiency_score ? `${Number(query.query_efficiency_score).toFixed(2)}%` : 'N/A'}</DetailValue>
                
                <DetailLabel>Long Running:</DetailLabel>
                <DetailValue>{query.is_long_running ? 'Yes' : 'No'}</DetailValue>
                
                <DetailLabel>Blocking:</DetailLabel>
                <DetailValue>{query.is_blocking ? 'Yes' : 'No'}</DetailValue>
                
                <DetailLabel>Has Joins:</DetailLabel>
                <DetailValue>{query.has_joins ? 'Yes' : 'No'}</DetailValue>
                
                <DetailLabel>Has Subqueries:</DetailLabel>
                <DetailValue>{query.has_subqueries ? 'Yes' : 'No'}</DetailValue>
                
                <DetailLabel>Has CTE:</DetailLabel>
                <DetailValue>{query.has_cte ? 'Yes' : 'No'}</DetailValue>
                
                <DetailLabel>Has Window Functions:</DetailLabel>
                <DetailValue>{query.has_window_functions ? 'Yes' : 'No'}</DetailValue>
                
                <DetailLabel>Captured At:</DetailLabel>
                <DetailValue>{query.captured_at ? new Date(query.captured_at).toLocaleString() : 'N/A'}</DetailValue>
              </DetailGrid>
              <QueryText>{query.query_text || 'N/A'}</QueryText>
            </QueryDetails>
          </div>
          ))
        )}
      </QueryTable>

      {loading && queries.length === 0 && (
        <LoadingOverlay>Loading query performance data...</LoadingOverlay>
      )}

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

export default QueryPerformance;

