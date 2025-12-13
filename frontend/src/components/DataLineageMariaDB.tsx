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
import { dataLineageApi } from '../services/api';
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

const LineageTable = styled.div`
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  overflow: hidden;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const TableHeader = styled.div`
  display: grid;
  grid-template-columns: 150px 120px 150px 120px 120px 150px 120px 100px 1fr;
  background: ${theme.colors.gradient.primary};
  padding: 12px 15px;
  font-weight: bold;
  font-size: 0.8em;
  border-bottom: 2px solid ${theme.colors.border.dark};
  gap: 10px;
`;

const TableRow = styled.div`
  display: grid;
  grid-template-columns: 150px 120px 150px 120px 120px 150px 120px 100px 1fr;
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

const Badge = styled.span<{ $type?: string; $level?: number }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.75em;
  font-weight: 500;
  display: inline-block;
  transition: all ${theme.transitions.normal};
  
  ${props => {
    if (props.$type) {
      return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.primary};`;
    }
    if (props.$level !== undefined) {
      if (props.$level === 0) return `background-color: ${theme.colors.status.success.bg}; color: ${theme.colors.status.success.text};`;
      if (props.$level === 1) return `background-color: ${theme.colors.status.warning.bg}; color: ${theme.colors.status.warning.text};`;
      if (props.$level === 2) return `background-color: ${theme.colors.status.error.bg}; color: ${theme.colors.status.error.text};`;
      return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.secondary};`;
    }
    return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.secondary};`;
  }}
  
  &:hover {
    transform: scale(1.05);
    box-shadow: ${theme.shadows.sm};
  }
`;

const RelationshipArrow = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  color: ${theme.colors.primary.main};
  font-weight: bold;
  font-size: 1.2em;
`;

const LineageDetails = styled.div<{ $isOpen: boolean }>`
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

const DefinitionText = styled.pre`
  margin: 0;
  padding: ${theme.spacing.md};
  background-color: ${theme.colors.background.secondary};
  border-radius: ${theme.borderRadius.md};
  overflow-x: auto;
  font-size: 0.85em;
  border: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.2);
    box-shadow: ${theme.shadows.sm};
  }
`;

const DataLineageMariaDB = () => {
  const { page, limit, setPage } = usePagination(1, 20);
  const { filters, setFilter } = useTableFilters({
    server_name: '',
    database_name: '',
    object_type: '',
    relationship_type: '',
    search: ''
  });
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lineage, setLineage] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [openEdgeId, setOpenEdgeId] = useState<number | null>(null);
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
      const [lineageData, metricsData, serversData] = await Promise.all([
        dataLineageApi.getMariaDBLineage({
          page,
          limit,
          server_name: filters.server_name as string,
          database_name: filters.database_name as string,
          object_type: filters.object_type as string,
          relationship_type: filters.relationship_type as string,
          search: sanitizedSearch
        }),
        dataLineageApi.getMariaDBMetrics(),
        dataLineageApi.getMariaDBServers()
      ]);
      if (isMountedRef.current) {
        setLineage(lineageData.data || []);
        setPagination(lineageData.pagination || {
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
    filters.relationship_type, 
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
          const databasesData = await dataLineageApi.getMariaDBDatabases(filters.server_name as string);
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

  const toggleEdge = useCallback((id: number) => {
    setOpenEdgeId(prev => prev === id ? null : id);
  }, []);

  const formatDate = useCallback((date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  }, []);

  const formatConfidence = useCallback((score: number | string | null | undefined) => {
    if (score === null || score === undefined) return 'N/A';
    const numScore = Number(score);
    if (isNaN(numScore)) return 'N/A';
    return `${(numScore * 100).toFixed(1)}%`;
  }, []);

  const handleFilterChange = useCallback((key: string, value: string) => {
    setFilter(key as any, value);
    if (key === 'server_name') {
      setFilter('database_name' as any, '');
    }
    setPage(1);
  }, [setFilter, setPage]);

  if (loading && lineage.length === 0) {
    return (
      <Container>
        <Header>Data Lineage - MariaDB</Header>
        <LoadingOverlay>Loading data lineage...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>Data Lineage - MariaDB</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}
      
      <MetricsGrid $columns="repeat(auto-fit, minmax(180px, 1fr))">
        <MetricCard>
          <MetricLabel>Total Relationships</MetricLabel>
          <MetricValue>{metrics.total_relationships || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Unique Objects</MetricLabel>
          <MetricValue>{metrics.unique_objects || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Unique Servers</MetricLabel>
          <MetricValue>{metrics.unique_servers || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>High Confidence</MetricLabel>
          <MetricValue>{metrics.high_confidence || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Avg Confidence</MetricLabel>
          <MetricValue>{metrics.avg_confidence ? `${(Number(metrics.avg_confidence) * 100).toFixed(1)}%` : 'N/A'}</MetricValue>
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
          <option value="TRIGGER">TRIGGER</option>
          <option value="FOREIGN_KEY">FOREIGN_KEY</option>
        </FilterSelect>
        
        <FilterSelect
          value={filters.relationship_type as string}
          onChange={(e) => handleFilterChange('relationship_type', e.target.value)}
        >
          <option value="">All Relationships</option>
          <option value="DEPENDS_ON">DEPENDS_ON</option>
          <option value="REFERENCES">REFERENCES</option>
          <option value="CONTAINS">CONTAINS</option>
          <option value="TRIGGERS">TRIGGERS</option>
        </FilterSelect>
        
        <SearchInput
          type="text"
          placeholder="Search object name..."
          value={filters.search as string}
          onChange={(e) => handleFilterChange('search', e.target.value)}
        />
      </FiltersContainer>

      <LineageTable>
        <TableHeader>
          <TableCell>Source Object</TableCell>
          <TableCell>Source Type</TableCell>
          <TableCell>Relationship</TableCell>
          <TableCell>Target Object</TableCell>
          <TableCell>Target Type</TableCell>
          <TableCell>Server</TableCell>
          <TableCell>Database</TableCell>
          <TableCell>Confidence</TableCell>
          <TableCell>Method</TableCell>
        </TableHeader>
        {lineage.length === 0 ? (
          <div style={{ padding: '40px', textAlign: 'center', color: theme.colors.text.secondary }}>
            No lineage data available. Lineage relationships will appear here once extracted.
          </div>
        ) : (
          lineage.map((edge) => (
            <div key={edge.id}>
              <TableRow onClick={() => toggleEdge(edge.id)}>
                <TableCell>
                  <strong>{edge.object_name || 'N/A'}</strong>
                  {edge.column_name && (
                    <div style={{ fontSize: '0.8em', color: theme.colors.text.secondary }}>.{edge.column_name}</div>
                  )}
                </TableCell>
                <TableCell>
                  <Badge $type={edge.object_type}>{edge.object_type || 'N/A'}</Badge>
                </TableCell>
                <TableCell>
                  <RelationshipArrow>→</RelationshipArrow>
                  <div style={{ fontSize: '0.75em', color: theme.colors.text.secondary, marginTop: '4px' }}>
                    {edge.relationship_type || 'N/A'}
                  </div>
                </TableCell>
                <TableCell>
                  <strong>{edge.target_object_name || 'N/A'}</strong>
                  {edge.target_column_name && (
                    <div style={{ fontSize: '0.8em', color: theme.colors.text.secondary }}>.{edge.target_column_name}</div>
                  )}
                </TableCell>
                <TableCell>
                  <Badge $type={edge.target_object_type}>{edge.target_object_type || 'N/A'}</Badge>
                </TableCell>
                <TableCell>{edge.server_name || 'N/A'}</TableCell>
                <TableCell>{edge.database_name || 'N/A'}</TableCell>
                <TableCell>
                  <Badge $level={edge.confidence_score ? (edge.confidence_score >= 0.8 ? 0 : edge.confidence_score >= 0.5 ? 1 : 2) : 2}>
                    {formatConfidence(edge.confidence_score)}
                  </Badge>
                </TableCell>
                <TableCell>{edge.discovery_method || 'N/A'}</TableCell>
              </TableRow>
              <LineageDetails $isOpen={openEdgeId === edge.id}>
                <DetailGrid>
                  <DetailLabel>Edge Key:</DetailLabel>
                  <DetailValue>{edge.edge_key || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Schema:</DetailLabel>
                  <DetailValue>{edge.schema_name || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Source Column:</DetailLabel>
                  <DetailValue>{edge.column_name || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Target Column:</DetailLabel>
                  <DetailValue>{edge.target_column_name || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Dependency Level:</DetailLabel>
                  <DetailValue>{edge.dependency_level !== null && edge.dependency_level !== undefined ? edge.dependency_level : 'N/A'}</DetailValue>
                  
                  <DetailLabel>Discovered By:</DetailLabel>
                  <DetailValue>{edge.discovered_by || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Consumer Type:</DetailLabel>
                  <DetailValue>{edge.consumer_type || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Consumer Name:</DetailLabel>
                  <DetailValue>{edge.consumer_name || 'N/A'}</DetailValue>
                  
                  <DetailLabel>First Seen:</DetailLabel>
                  <DetailValue>{formatDate(edge.first_seen_at)}</DetailValue>
                  
                  <DetailLabel>Last Seen:</DetailLabel>
                  <DetailValue>{formatDate(edge.last_seen_at)}</DetailValue>
                  
                  <DetailLabel>Created At:</DetailLabel>
                  <DetailValue>{formatDate(edge.created_at)}</DetailValue>
                  
                  <DetailLabel>Updated At:</DetailLabel>
                  <DetailValue>{formatDate(edge.updated_at)}</DetailValue>
                </DetailGrid>
                
                {edge.definition_text && (
                  <>
                    <div style={{ padding: '15px 15px 5px 15px', fontWeight: 'bold', color: theme.colors.text.secondary }}>
                      Definition:
                    </div>
                    <DefinitionText>{edge.definition_text}</DefinitionText>
                  </>
                )}
              </LineageDetails>
            </div>
          ))
        )}
      </LineageTable>

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

export default DataLineageMariaDB;
