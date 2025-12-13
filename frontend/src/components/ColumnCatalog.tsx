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
import { columnCatalogApi } from '../services/api';
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

const ColumnTable = styled.div`
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  overflow: hidden;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const TableHeader = styled.div`
  display: grid;
  grid-template-columns: 150px 150px 150px 100px 120px 100px 100px 100px 100px 100px 1fr;
  background: ${theme.colors.gradient.primary};
  padding: 12px 15px;
  font-weight: bold;
  font-size: 0.8em;
  border-bottom: 2px solid ${theme.colors.border.dark};
  gap: 10px;
`;

const TableRow = styled.div`
  display: grid;
  grid-template-columns: 150px 150px 150px 100px 120px 100px 100px 100px 100px 100px 1fr;
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

const Badge = styled.span<{ $type?: string; $level?: string; $flag?: boolean }>`
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
    if (props.$level) {
      switch (props.$level) {
        case 'HIGH': return `background-color: ${theme.colors.status.error.bg}; color: ${theme.colors.status.error.text};`;
        case 'MEDIUM': return `background-color: ${theme.colors.status.warning.bg}; color: ${theme.colors.status.warning.text};`;
        case 'LOW': return `background-color: ${theme.colors.status.success.bg}; color: ${theme.colors.status.success.text};`;
        default: return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.secondary};`;
      }
    }
    if (props.$flag !== undefined) {
      return props.$flag 
        ? `background-color: ${theme.colors.status.error.bg}; color: ${theme.colors.status.error.text};`
        : `background-color: ${theme.colors.status.success.bg}; color: ${theme.colors.status.success.text};`;
    }
    return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.secondary};`;
  }}
  
  &:hover {
    transform: scale(1.05);
    box-shadow: ${theme.shadows.sm};
  }
`;

const ColumnDetails = styled.div<{ $isOpen: boolean }>`
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

const FlagsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: ${theme.spacing.sm};
  padding: ${theme.spacing.md};
  background: ${theme.colors.background.secondary};
  border-radius: ${theme.borderRadius.md};
  margin: ${theme.spacing.md};
`;

const FlagItem = styled.div`
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.9em;
`;

/**
 * Column Catalog component
 * Displays detailed metadata about database columns including data types, sensitivity levels, and PII/PHI flags
 */
const ColumnCatalog = () => {
  const { page, limit, setPage } = usePagination(1, 20);
  const { filters, setFilter } = useTableFilters({
    schema_name: '',
    table_name: '',
    db_engine: '',
    data_type: '',
    sensitivity_level: '',
    contains_pii: '',
    contains_phi: '',
    search: ''
  });
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [columns, setColumns] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [openColumnId, setOpenColumnId] = useState<number | null>(null);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 20
  });
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  const isMountedRef = useRef(true);

  const fetchData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(filters.search as string, 100);
      const [columnsData, metricsData, schemasData] = await Promise.all([
        columnCatalogApi.getColumns({
          page,
          limit,
          schema_name: filters.schema_name as string,
          table_name: filters.table_name as string,
          db_engine: filters.db_engine as string,
          data_type: filters.data_type as string,
          sensitivity_level: filters.sensitivity_level as string,
          contains_pii: filters.contains_pii as string,
          contains_phi: filters.contains_phi as string,
          search: sanitizedSearch
        }),
        columnCatalogApi.getMetrics(),
        columnCatalogApi.getSchemas()
      ]);
      if (isMountedRef.current) {
        setColumns(columnsData.data || []);
        setPagination(columnsData.pagination || {
          total: 0,
          totalPages: 0,
          currentPage: 1,
          limit: 20
        });
        setMetrics(metricsData || {});
        setSchemas(schemasData || []);
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
    filters.schema_name, 
    filters.table_name, 
    filters.db_engine, 
    filters.data_type, 
    filters.sensitivity_level, 
    filters.contains_pii, 
    filters.contains_phi, 
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
    const fetchTables = async () => {
      if (filters.schema_name && isMountedRef.current) {
        try {
          const tablesData = await columnCatalogApi.getTables(filters.schema_name as string);
          if (isMountedRef.current) {
            setTables(tablesData || []);
          }
        } catch (err) {
          if (isMountedRef.current) {
            console.error('Error loading tables:', err);
          }
        }
      } else {
        setTables([]);
      }
    };
    fetchTables();
  }, [filters.schema_name]);

  const toggleColumn = useCallback((id: number) => {
    setOpenColumnId(prev => prev === id ? null : id);
  }, []);

  const formatNumber = useCallback((num: number | string | null | undefined) => {
    if (num === null || num === undefined) return 'N/A';
    const numVal = Number(num);
    if (isNaN(numVal)) return 'N/A';
    if (numVal >= 1000000) return `${(numVal / 1000000).toFixed(2)}M`;
    if (numVal >= 1000) return `${(numVal / 1000).toFixed(2)}K`;
    return numVal.toString();
  }, []);

  const formatPercentage = useCallback((val: number | string | null | undefined) => {
    if (val === null || val === undefined) return 'N/A';
    const numVal = Number(val);
    if (isNaN(numVal)) return 'N/A';
    return `${numVal.toFixed(2)}%`;
  }, []);

  const formatDate = useCallback((date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  }, []);

  const handleFilterChange = useCallback((key: string, value: string) => {
    setFilter(key as any, value);
    if (key === 'schema_name') {
      setFilter('table_name' as any, '');
    }
    setPage(1);
  }, [setFilter, setPage]);

  if (loading && columns.length === 0) {
    return (
      <Container>
        <Header>Column Catalog</Header>
        <LoadingOverlay>Loading column catalog data...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>Column Catalog</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}
      
      <MetricsGrid $columns="repeat(auto-fit, minmax(180px, 1fr))">
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
          value={filters.schema_name as string}
          onChange={(e) => handleFilterChange('schema_name', e.target.value)}
        >
          <option value="">All Schemas</option>
          {schemas.map(schema => (
            <option key={schema} value={schema}>{schema}</option>
          ))}
        </FilterSelect>
        
        <FilterSelect
          value={filters.table_name as string}
          onChange={(e) => handleFilterChange('table_name', e.target.value)}
          disabled={!filters.schema_name}
        >
          <option value="">All Tables</option>
          {tables.map(table => (
            <option key={table} value={table}>{table}</option>
          ))}
        </FilterSelect>
        
        <FilterSelect
          value={filters.db_engine as string}
          onChange={(e) => handleFilterChange('db_engine', e.target.value)}
        >
          <option value="">All Engines</option>
          <option value="PostgreSQL">PostgreSQL</option>
          <option value="MariaDB">MariaDB</option>
          <option value="MSSQL">MSSQL</option>
        </FilterSelect>
        
        <FilterSelect
          value={filters.data_type as string}
          onChange={(e) => handleFilterChange('data_type', e.target.value)}
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
          value={filters.sensitivity_level as string}
          onChange={(e) => handleFilterChange('sensitivity_level', e.target.value)}
        >
          <option value="">All Sensitivity</option>
          <option value="HIGH">HIGH</option>
          <option value="MEDIUM">MEDIUM</option>
          <option value="LOW">LOW</option>
        </FilterSelect>
        
        <FilterSelect
          value={filters.contains_pii as string}
          onChange={(e) => handleFilterChange('contains_pii', e.target.value)}
        >
          <option value="">All PII</option>
          <option value="true">Has PII</option>
          <option value="false">No PII</option>
        </FilterSelect>
        
        <SearchInput
          type="text"
          placeholder="Search column name..."
          value={filters.search as string}
          onChange={(e) => handleFilterChange('search', e.target.value)}
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
          <div style={{ padding: '40px', textAlign: 'center', color: theme.colors.text.secondary }}>
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
                  <DetailValue>{formatDate(column.first_seen_at)}</DetailValue>
                  
                  <DetailLabel>Last Seen:</DetailLabel>
                  <DetailValue>{formatDate(column.last_seen_at)}</DetailValue>
                  
                  <DetailLabel>Last Analyzed:</DetailLabel>
                  <DetailValue>{formatDate(column.last_analyzed_at)}</DetailValue>
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

export default ColumnCatalog;
