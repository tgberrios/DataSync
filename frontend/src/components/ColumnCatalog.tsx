import { useState, useEffect } from 'react';
import styled from 'styled-components';
import { columnCatalogApi } from '../services/api';

const ColumnCatalogContainer = styled.div`
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

const ColumnTable = styled.div`
  border: 1px solid #ddd;
  border-radius: 6px;
  overflow: hidden;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const TableHeader = styled.div`
  display: grid;
  grid-template-columns: 150px 150px 150px 100px 120px 100px 100px 100px 100px 100px 1fr;
  background: linear-gradient(135deg, #f5f5f5 0%, #ffffff 100%);
  padding: 12px 15px;
  font-weight: bold;
  font-size: 0.8em;
  border-bottom: 2px solid #ddd;
  gap: 10px;
`;

const TableRow = styled.div`
  display: grid;
  grid-template-columns: 150px 150px 150px 100px 120px 100px 100px 100px 100px 100px 1fr;
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

const Badge = styled.span<{ $type?: string; $level?: string; $flag?: boolean }>`
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
    if (props.$level) {
      switch (props.$level) {
        case 'HIGH': return 'background-color: #ffebee; color: #c62828;';
        case 'MEDIUM': return 'background-color: #fff3e0; color: #ef6c00;';
        case 'LOW': return 'background-color: #e8f5e9; color: #2e7d32;';
        default: return 'background-color: #f5f5f5; color: #757575;';
      }
    }
    if (props.$flag !== undefined) {
      return props.$flag 
        ? 'background-color: #ffebee; color: #c62828;'
        : 'background-color: #e8f5e9; color: #2e7d32;';
    }
    return 'background-color: #f5f5f5; color: #757575;';
  }}
  
  &:hover {
    transform: scale(1.05);
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
  }
`;

const ColumnDetails = styled.div<{ $isOpen: boolean }>`
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

const FlagsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 10px;
  padding: 15px;
  background: #f8f8f8;
  border-radius: 6px;
  margin: 15px;
`;

const FlagItem = styled.div`
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.9em;
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

const ColumnCatalog = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [columns, setColumns] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [openColumnId, setOpenColumnId] = useState<number | null>(null);
  const [filters, setFilters] = useState({
    schema_name: '',
    table_name: '',
    db_engine: '',
    data_type: '',
    sensitivity_level: '',
    contains_pii: '',
    contains_phi: '',
    search: ''
  });
  const [page, setPage] = useState(1);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 20
  });
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        const [columnsData, metricsData, schemasData] = await Promise.all([
          columnCatalogApi.getColumns({
            page,
            limit: 20,
            ...filters
          }),
          columnCatalogApi.getMetrics(),
          columnCatalogApi.getSchemas()
        ]);
        setColumns(columnsData.data || []);
        setPagination(columnsData.pagination || pagination);
        setMetrics(metricsData || {});
        setSchemas(schemasData || []);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading column catalog data');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, [page, filters]);

  useEffect(() => {
    const fetchTables = async () => {
      if (filters.schema_name) {
        try {
          const tablesData = await columnCatalogApi.getTables(filters.schema_name);
          setTables(tablesData || []);
        } catch (err) {
          console.error('Error loading tables:', err);
        }
      } else {
        setTables([]);
      }
    };
    fetchTables();
  }, [filters.schema_name]);

  const toggleColumn = (id: number) => {
    setOpenColumnId(openColumnId === id ? null : id);
  };

  const formatNumber = (num: number | string | null | undefined) => {
    if (num === null || num === undefined) return 'N/A';
    const numVal = Number(num);
    if (isNaN(numVal)) return 'N/A';
    if (numVal >= 1000000) return `${(numVal / 1000000).toFixed(2)}M`;
    if (numVal >= 1000) return `${(numVal / 1000).toFixed(2)}K`;
    return numVal.toString();
  };

  const formatPercentage = (val: number | string | null | undefined) => {
    if (val === null || val === undefined) return 'N/A';
    const numVal = Number(val);
    if (isNaN(numVal)) return 'N/A';
    return `${numVal.toFixed(2)}%`;
  };

  if (loading && columns.length === 0) {
    return (
      <ColumnCatalogContainer>
        <Header>Column Catalog</Header>
        <Loading>Loading column catalog data...</Loading>
      </ColumnCatalogContainer>
    );
  }

  return (
    <ColumnCatalogContainer>
      <Header>Column Catalog</Header>
      
      {error && <Error>{error}</Error>}
      
      <MetricsGrid>
        <MetricCard>
          <MetricLabel>Total Columns</MetricLabel>
          <MetricValue>{formatNumber(metrics.total_columns)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>PII Columns</MetricLabel>
          <MetricValue>{formatNumber(metrics.pii_columns)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>PHI Columns</MetricLabel>
          <MetricValue>{formatNumber(metrics.phi_columns)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>High Sensitivity</MetricLabel>
          <MetricValue>{formatNumber(metrics.high_sensitivity)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Primary Keys</MetricLabel>
          <MetricValue>{formatNumber(metrics.primary_keys)}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Indexed Columns</MetricLabel>
          <MetricValue>{formatNumber(metrics.indexed_columns)}</MetricValue>
        </MetricCard>
      </MetricsGrid>

      <FiltersContainer>
        <FilterSelect
          value={filters.schema_name}
          onChange={(e) => {
            setFilters({ ...filters, schema_name: e.target.value, table_name: '' });
            setPage(1);
          }}
        >
          <option value="">All Schemas</option>
          {schemas.map(schema => (
            <option key={schema} value={schema}>{schema}</option>
          ))}
        </FilterSelect>
        
        <FilterSelect
          value={filters.table_name}
          onChange={(e) => {
            setFilters({ ...filters, table_name: e.target.value });
            setPage(1);
          }}
          disabled={!filters.schema_name}
        >
          <option value="">All Tables</option>
          {tables.map(table => (
            <option key={table} value={table}>{table}</option>
          ))}
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
        
        <FilterSelect
          value={filters.data_type}
          onChange={(e) => {
            setFilters({ ...filters, data_type: e.target.value });
            setPage(1);
          }}
        >
          <option value="">All Types</option>
          <option value="varchar">VARCHAR</option>
          <option value="integer">INTEGER</option>
          <option value="bigint">BIGINT</option>
          <option value="numeric">NUMERIC</option>
          <option value="timestamp">TIMESTAMP</option>
          <option value="boolean">BOOLEAN</option>
        </FilterSelect>
        
        <FilterSelect
          value={filters.sensitivity_level}
          onChange={(e) => {
            setFilters({ ...filters, sensitivity_level: e.target.value });
            setPage(1);
          }}
        >
          <option value="">All Sensitivity</option>
          <option value="HIGH">HIGH</option>
          <option value="MEDIUM">MEDIUM</option>
          <option value="LOW">LOW</option>
        </FilterSelect>
        
        <FilterSelect
          value={filters.contains_pii}
          onChange={(e) => {
            setFilters({ ...filters, contains_pii: e.target.value });
            setPage(1);
          }}
        >
          <option value="">All PII</option>
          <option value="true">Has PII</option>
          <option value="false">No PII</option>
        </FilterSelect>
        
        <SearchInput
          type="text"
          placeholder="Search column name..."
          value={filters.search}
          onChange={(e) => {
            setFilters({ ...filters, search: e.target.value });
            setPage(1);
          }}
        />
      </FiltersContainer>

      <ColumnTable>
        <TableHeader>
          <TableCell>Schema</TableCell>
          <TableCell>Table</TableCell>
          <TableCell>Column</TableCell>
          <TableCell>Engine</TableCell>
          <TableCell>Data Type</TableCell>
          <TableCell>Position</TableCell>
          <TableCell>Nullable</TableCell>
          <TableCell>Sensitivity</TableCell>
          <TableCell>PII</TableCell>
          <TableCell>PHI</TableCell>
          <TableCell>Flags</TableCell>
        </TableHeader>
        {columns.length === 0 ? (
          <div style={{ padding: '40px', textAlign: 'center', color: '#666' }}>
            No column data available. Columns will appear here once cataloged.
          </div>
        ) : (
          columns.map((column) => (
            <div key={column.id}>
              <TableRow onClick={() => toggleColumn(column.id)}>
                <TableCell>{column.schema_name}</TableCell>
                <TableCell>{column.table_name}</TableCell>
                <TableCell>{column.column_name}</TableCell>
                <TableCell>{column.db_engine || 'N/A'}</TableCell>
                <TableCell>
                  <Badge $type={column.data_type}>{column.data_type}</Badge>
                </TableCell>
                <TableCell>{column.ordinal_position || 'N/A'}</TableCell>
                <TableCell>{column.is_nullable ? 'Yes' : 'No'}</TableCell>
                <TableCell>
                  {column.sensitivity_level && (
                    <Badge $level={column.sensitivity_level}>{column.sensitivity_level}</Badge>
                  )}
                </TableCell>
                <TableCell>
                  {column.contains_pii && <Badge $flag={true}>PII</Badge>}
                </TableCell>
                <TableCell>
                  {column.contains_phi && <Badge $flag={true}>PHI</Badge>}
                </TableCell>
                <TableCell>
                  {column.is_primary_key && <Badge $type="PK">PK</Badge>}
                  {column.is_foreign_key && <Badge $type="FK">FK</Badge>}
                  {column.is_unique && <Badge $type="UQ">UQ</Badge>}
                  {column.is_indexed && <Badge $type="IDX">IDX</Badge>}
                </TableCell>
              </TableRow>
              <ColumnDetails $isOpen={openColumnId === column.id}>
                <DetailGrid>
                  <DetailLabel>Ordinal Position:</DetailLabel>
                  <DetailValue>{column.ordinal_position || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Data Type:</DetailLabel>
                  <DetailValue>{column.data_type || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Character Max Length:</DetailLabel>
                  <DetailValue>{column.character_maximum_length || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Numeric Precision:</DetailLabel>
                  <DetailValue>{column.numeric_precision || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Numeric Scale:</DetailLabel>
                  <DetailValue>{column.numeric_scale || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Column Default:</DetailLabel>
                  <DetailValue>{column.column_default || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Data Category:</DetailLabel>
                  <DetailValue>{column.data_category || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Null Count:</DetailLabel>
                  <DetailValue>{formatNumber(column.null_count)}</DetailValue>
                  
                  <DetailLabel>Null Percentage:</DetailLabel>
                  <DetailValue>{formatPercentage(column.null_percentage)}</DetailValue>
                  
                  <DetailLabel>Distinct Count:</DetailLabel>
                  <DetailValue>{formatNumber(column.distinct_count)}</DetailValue>
                  
                  <DetailLabel>Distinct Percentage:</DetailLabel>
                  <DetailValue>{formatPercentage(column.distinct_percentage)}</DetailValue>
                  
                  <DetailLabel>Min Value:</DetailLabel>
                  <DetailValue>{column.min_value || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Max Value:</DetailLabel>
                  <DetailValue>{column.max_value || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Avg Value:</DetailLabel>
                  <DetailValue>{column.avg_value || 'N/A'}</DetailValue>
                  
                  <DetailLabel>First Seen:</DetailLabel>
                  <DetailValue>{column.first_seen_at ? new Date(column.first_seen_at).toLocaleString() : 'N/A'}</DetailValue>
                  
                  <DetailLabel>Last Seen:</DetailLabel>
                  <DetailValue>{column.last_seen_at ? new Date(column.last_seen_at).toLocaleString() : 'N/A'}</DetailValue>
                  
                  <DetailLabel>Last Analyzed:</DetailLabel>
                  <DetailValue>{column.last_analyzed_at ? new Date(column.last_analyzed_at).toLocaleString() : 'N/A'}</DetailValue>
                </DetailGrid>
                
                <FlagsGrid>
                  <FlagItem>
                    <Badge $flag={column.is_primary_key}>Primary Key</Badge>
                  </FlagItem>
                  <FlagItem>
                    <Badge $flag={column.is_foreign_key}>Foreign Key</Badge>
                  </FlagItem>
                  <FlagItem>
                    <Badge $flag={column.is_unique}>Unique</Badge>
                  </FlagItem>
                  <FlagItem>
                    <Badge $flag={column.is_indexed}>Indexed</Badge>
                  </FlagItem>
                  <FlagItem>
                    <Badge $flag={column.is_auto_increment}>Auto Increment</Badge>
                  </FlagItem>
                  <FlagItem>
                    <Badge $flag={column.is_generated}>Generated</Badge>
                  </FlagItem>
                  <FlagItem>
                    <Badge $flag={column.is_nullable}>Nullable</Badge>
                  </FlagItem>
                  <FlagItem>
                    <Badge $flag={column.contains_pii}>Contains PII</Badge>
                  </FlagItem>
                  <FlagItem>
                    <Badge $flag={column.contains_phi}>Contains PHI</Badge>
                  </FlagItem>
                </FlagsGrid>
              </ColumnDetails>
            </div>
          ))
        )}
      </ColumnTable>

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
    </ColumnCatalogContainer>
  );
};

export default ColumnCatalog;

