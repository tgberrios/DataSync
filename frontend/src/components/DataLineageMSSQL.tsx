import { useState, useEffect } from 'react';
import styled from 'styled-components';
import { dataLineageMSSQLApi } from '../services/api';

const DataLineageContainer = styled.div`
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

const LineageTable = styled.div`
  border: 1px solid #ddd;
  border-radius: 6px;
  overflow: hidden;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const TableHeader = styled.div`
  display: grid;
  grid-template-columns: 150px 120px 150px 120px 120px 120px 120px 100px 1fr;
  background: linear-gradient(135deg, #f5f5f5 0%, #ffffff 100%);
  padding: 12px 15px;
  font-weight: bold;
  font-size: 0.8em;
  border-bottom: 2px solid #ddd;
  gap: 10px;
`;

const TableRow = styled.div`
  display: grid;
  grid-template-columns: 150px 120px 150px 120px 120px 120px 120px 100px 1fr;
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

const Badge = styled.span<{ $type?: string; $level?: number }>`
  padding: 4px 10px;
  border-radius: 6px;
  font-size: 0.75em;
  font-weight: 500;
  display: inline-block;
  transition: all 0.2s ease;
  
  ${props => {
    if (props.$type) {
      return 'background-color: #f0f0f0; color: #333;';
    }
    if (props.$level !== undefined) {
      if (props.$level === 0) return 'background-color: #e8f5e9; color: #2e7d32;';
      if (props.$level === 1) return 'background-color: #e3f2fd; color: #1565c0;';
      if (props.$level === 2) return 'background-color: #fff3e0; color: #ef6c00;';
      return 'background-color: #f5f5f5; color: #757575;';
    }
    return 'background-color: #f5f5f5; color: #757575;';
  }}
  
  &:hover {
    transform: scale(1.05);
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
  }
`;

const RelationshipArrow = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  color: #0d1b2a;
  font-weight: bold;
  font-size: 1.2em;
`;

const LineageDetails = styled.div<{ $isOpen: boolean }>`
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

const DefinitionText = styled.pre`
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

const DataLineageMSSQL = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lineage, setLineage] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [openEdgeId, setOpenEdgeId] = useState<number | null>(null);
  const [filters, setFilters] = useState({
    server_name: '',
    instance_name: '',
    database_name: '',
    object_type: '',
    relationship_type: '',
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
  const [instances, setInstances] = useState<string[]>([]);
  const [databases, setDatabases] = useState<string[]>([]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        const [lineageData, metricsData, serversData] = await Promise.all([
          dataLineageMSSQLApi.getMSSQLLineage({
            page,
            limit: 20,
            ...filters
          }),
          dataLineageMSSQLApi.getMSSQLMetrics(),
          dataLineageMSSQLApi.getMSSQLServers()
        ]);
        setLineage(lineageData.data || []);
        setPagination(lineageData.pagination || pagination);
        setMetrics(metricsData || {});
        setServers(serversData || []);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading data lineage');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, [page, filters]);

  useEffect(() => {
    const fetchInstances = async () => {
      if (filters.server_name) {
        try {
          const instancesData = await dataLineageMSSQLApi.getMSSQLInstances(filters.server_name);
          setInstances(instancesData || []);
        } catch (err) {
          console.error('Error loading instances:', err);
        }
      } else {
        setInstances([]);
      }
    };
    fetchInstances();
  }, [filters.server_name]);

  useEffect(() => {
    const fetchDatabases = async () => {
      if (filters.server_name && filters.instance_name) {
        try {
          const databasesData = await dataLineageMSSQLApi.getMSSQLDatabases(filters.server_name, filters.instance_name);
          setDatabases(databasesData || []);
        } catch (err) {
          console.error('Error loading databases:', err);
        }
      } else {
        setDatabases([]);
      }
    };
    fetchDatabases();
  }, [filters.server_name, filters.instance_name]);

  const toggleEdge = (id: number) => {
    setOpenEdgeId(openEdgeId === id ? null : id);
  };

  const formatDate = (date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  };

  const formatConfidence = (score: number | string | null | undefined) => {
    if (score === null || score === undefined) return 'N/A';
    const numScore = Number(score);
    if (isNaN(numScore)) return 'N/A';
    return `${(numScore * 100).toFixed(1)}%`;
  };

  const formatNumber = (value: number | string | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    const num = Number(value);
    if (isNaN(num)) return 'N/A';
    return num.toLocaleString();
  };

  const formatTime = (ms: number | string | null | undefined) => {
    if (ms === null || ms === undefined) return 'N/A';
    const num = Number(ms);
    if (isNaN(num)) return 'N/A';
    if (num < 1000) return `${num.toFixed(2)} ms`;
    return `${(num / 1000).toFixed(2)} s`;
  };

  if (loading && lineage.length === 0) {
    return (
      <DataLineageContainer>
        <Header>Data Lineage - MSSQL</Header>
        <Loading>Loading data lineage...</Loading>
      </DataLineageContainer>
    );
  }

  return (
    <DataLineageContainer>
      <Header>Data Lineage - MSSQL</Header>
      
      {error && <Error>{error}</Error>}
      
      <MetricsGrid>
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
        <MetricCard>
          <MetricLabel>Total Executions</MetricLabel>
          <MetricValue>{formatNumber(metrics.total_executions)}</MetricValue>
        </MetricCard>
      </MetricsGrid>

      <FiltersContainer>
        <FilterSelect
          value={filters.server_name}
          onChange={(e) => {
            setFilters({ ...filters, server_name: e.target.value, instance_name: '', database_name: '' });
            setPage(1);
          }}
        >
          <option value="">All Servers</option>
          {servers.map(server => (
            <option key={server} value={server}>{server}</option>
          ))}
        </FilterSelect>
        
        <FilterSelect
          value={filters.instance_name}
          onChange={(e) => {
            setFilters({ ...filters, instance_name: e.target.value, database_name: '' });
            setPage(1);
          }}
          disabled={!filters.server_name}
        >
          <option value="">All Instances</option>
          {instances.map(instance => (
            <option key={instance} value={instance}>{instance}</option>
          ))}
        </FilterSelect>
        
        <FilterSelect
          value={filters.database_name}
          onChange={(e) => {
            setFilters({ ...filters, database_name: e.target.value });
            setPage(1);
          }}
          disabled={!filters.server_name || !filters.instance_name}
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
          <option value="FOREIGN_KEY">FOREIGN_KEY</option>
        </FilterSelect>
        
        <FilterSelect
          value={filters.relationship_type}
          onChange={(e) => {
            setFilters({ ...filters, relationship_type: e.target.value });
            setPage(1);
          }}
        >
          <option value="">All Relationships</option>
          <option value="DEPENDS_ON">DEPENDS_ON</option>
          <option value="REFERENCES">REFERENCES</option>
          <option value="CONTAINS">CONTAINS</option>
          <option value="EXECUTES">EXECUTES</option>
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

      <LineageTable>
        <TableHeader>
          <TableCell>Source Object</TableCell>
          <TableCell>Source Type</TableCell>
          <TableCell>Relationship</TableCell>
          <TableCell>Target Object</TableCell>
          <TableCell>Target Type</TableCell>
          <TableCell>Server</TableCell>
          <TableCell>Instance</TableCell>
          <TableCell>Confidence</TableCell>
          <TableCell>Method</TableCell>
        </TableHeader>
        {lineage.length === 0 ? (
          <div style={{ padding: '40px', textAlign: 'center', color: '#666' }}>
            No lineage data available. Lineage relationships will appear here once extracted.
          </div>
        ) : (
          lineage.map((edge) => (
            <div key={edge.id}>
              <TableRow onClick={() => toggleEdge(edge.id)}>
                <TableCell>
                  <strong>{edge.object_name || 'N/A'}</strong>
                  {edge.column_name && (
                    <div style={{ fontSize: '0.8em', color: '#666' }}>.{edge.column_name}</div>
                  )}
                </TableCell>
                <TableCell>
                  <Badge $type={edge.object_type}>{edge.object_type || 'N/A'}</Badge>
                </TableCell>
                <TableCell>
                  <RelationshipArrow>→</RelationshipArrow>
                  <div style={{ fontSize: '0.75em', color: '#666', marginTop: '4px' }}>
                    {edge.relationship_type || 'N/A'}
                  </div>
                </TableCell>
                <TableCell>
                  <strong>{edge.target_object_name || 'N/A'}</strong>
                  {edge.target_column_name && (
                    <div style={{ fontSize: '0.8em', color: '#666' }}>.{edge.target_column_name}</div>
                  )}
                </TableCell>
                <TableCell>
                  <Badge $type={edge.target_object_type}>{edge.target_object_type || 'N/A'}</Badge>
                </TableCell>
                <TableCell>{edge.server_name || 'N/A'}</TableCell>
                <TableCell>{edge.instance_name || 'N/A'}</TableCell>
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
                
                {(edge.execution_count || edge.avg_duration_ms || edge.avg_cpu_ms || edge.avg_logical_reads || edge.avg_physical_reads) && (
                  <PerformanceGrid>
                    <PerformanceMetric>
                      <PerformanceLabel>Execution Count</PerformanceLabel>
                      <PerformanceValue>{formatNumber(edge.execution_count)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>Avg Duration</PerformanceLabel>
                      <PerformanceValue>{formatTime(edge.avg_duration_ms)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>Avg CPU Time</PerformanceLabel>
                      <PerformanceValue>{formatTime(edge.avg_cpu_ms)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>Avg Logical Reads</PerformanceLabel>
                      <PerformanceValue>{formatNumber(edge.avg_logical_reads)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>Avg Physical Reads</PerformanceLabel>
                      <PerformanceValue>{formatNumber(edge.avg_physical_reads)}</PerformanceValue>
                    </PerformanceMetric>
                  </PerformanceGrid>
                )}
                
                {edge.definition_text && (
                  <>
                    <div style={{ padding: '15px 15px 5px 15px', fontWeight: 'bold', color: '#666' }}>
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
    </DataLineageContainer>
  );
};

export default DataLineageMSSQL;

