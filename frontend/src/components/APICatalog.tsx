import { useState, useEffect, useCallback, useRef } from 'react';
import { format } from 'date-fns';
import AddAPIModal from './AddAPIModal';
import {
  Container,
  Header,
  FiltersContainer,
  Select,
  TableContainer,
  Table,
  Th,
  Td,
  TableRow,
  StatusBadge,
  Pagination,
  PageButton,
  ErrorMessage,
  LoadingOverlay,
  SearchContainer,
  Input,
  Button,
  ActiveBadge,
  ActionButton,
} from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { apiCatalogApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { sanitizeSearch } from '../utils/validation';
import styled from 'styled-components';
import { theme } from '../theme/theme';

const SearchInput = styled(Input)`
  flex: 1;
  font-size: 14px;
`;

const SearchButton = styled(Button)`
  padding: 10px 20px;
  font-weight: bold;
`;

const ClearSearchButton = styled(Button)`
  padding: 10px 15px;
`;

const PaginationInfo = styled.div`
  text-align: center;
  margin-bottom: ${theme.spacing.sm};
  color: ${theme.colors.text.secondary};
  font-size: 0.9em;
  animation: fadeIn 0.25s ease-in;
`;

interface APICatalogEntry {
  id: number;
  api_name: string;
  api_type: string;
  base_url: string;
  endpoint: string;
  http_method: string;
  auth_type: string;
  target_db_engine: string;
  target_schema: string;
  target_table: string;
  status: string;
  active: boolean;
  sync_interval: number;
  last_sync_time: string | null;
  last_sync_status: string | null;
  created_at: string;
  updated_at: string;
}

const APICatalog = () => {
  const { page, limit, setPage } = usePagination(1, 20);
  const { filters, setFilter } = useTableFilters({
    api_type: '',
    target_db_engine: '',
    status: '',
    active: ''
  });
  
  const [searchInput, setSearchInput] = useState('');
  const [search, setSearch] = useState('');
  const [data, setData] = useState<APICatalogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showAddModal, setShowAddModal] = useState(false);
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
      const sanitizedSearch = sanitizeSearch(search, 100);
      const params: any = {
        page,
        limit,
        search: sanitizedSearch
      };
      
      if (filters.api_type) params.api_type = filters.api_type;
      if (filters.target_db_engine) params.target_db_engine = filters.target_db_engine;
      if (filters.status) params.status = filters.status;
      if (filters.active) params.active = filters.active;
      
      const response = await apiCatalogApi.getAPIs(params);
      if (isMountedRef.current) {
        setData(response.data);
        setPagination(response.pagination);
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
    filters.api_type, 
    filters.target_db_engine, 
    filters.status, 
    filters.active, 
    search
  ]);

  useEffect(() => {
    isMountedRef.current = true;
    fetchData();
    return () => {
      isMountedRef.current = false;
    };
  }, [fetchData]);

  const handleSearch = useCallback(() => {
    setSearch(searchInput);
    setPage(1);
  }, [searchInput, setPage]);

  const handleClearSearch = useCallback(() => {
    setSearchInput('');
    setSearch('');
    setPage(1);
  }, [setPage]);

  const handleFilterChange = useCallback((key: string, value: string) => {
    setFilter(key as any, value);
    setPage(1);
  }, [setFilter, setPage]);

  const handleAdd = useCallback(
    async (newEntry: any) => {
      try {
        setLoading(true);
        setError(null);
        await apiCatalogApi.createAPI(newEntry);
        await fetchData();
        setShowAddModal(false);
        alert(`API "${newEntry.api_name}" added successfully.`);
      } catch (err) {
        if (isMountedRef.current) {
          setError(extractApiError(err));
        }
      } finally {
        if (isMountedRef.current) {
          setLoading(false);
        }
      }
    },
    [fetchData]
  );

  const handleToggleActive = useCallback(async (apiName: string, currentActive: boolean) => {
    try {
      await apiCatalogApi.updateActive(apiName, !currentActive);
      fetchData();
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [fetchData]);

  if (loading && data.length === 0) {
    return <LoadingOverlay>Loading API Catalog...</LoadingOverlay>;
  }

  return (
    <Container>
      <Header>API Catalog</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}

      <SearchContainer>
        <SearchInput
          type="text"
          placeholder="Search by API name, endpoint, or target..."
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
          onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
        />
        <SearchButton onClick={handleSearch}>Search</SearchButton>
        {search && (
          <ClearSearchButton onClick={handleClearSearch}>Clear</ClearSearchButton>
        )}
      </SearchContainer>

      <FiltersContainer>
        <Button
          $variant="primary"
          onClick={() => setShowAddModal(true)}
          style={{ marginRight: 'auto' }}
        >
          Add API
        </Button>
        
        <Select
          value={filters.api_type as string}
          onChange={(e) => handleFilterChange('api_type', e.target.value)}
        >
          <option value="">All API Types</option>
          <option value="REST">REST</option>
          <option value="GraphQL">GraphQL</option>
          <option value="SOAP">SOAP</option>
        </Select>

        <Select
          value={filters.target_db_engine as string}
          onChange={(e) => handleFilterChange('target_db_engine', e.target.value)}
        >
          <option value="">All Target Engines</option>
          <option value="PostgreSQL">PostgreSQL</option>
          <option value="MariaDB">MariaDB</option>
          <option value="MSSQL">MSSQL</option>
          <option value="MongoDB">MongoDB</option>
          <option value="Oracle">Oracle</option>
        </Select>

        <Select
          value={filters.status as string}
          onChange={(e) => handleFilterChange('status', e.target.value)}
        >
          <option value="">All Statuses</option>
          <option value="SUCCESS">SUCCESS</option>
          <option value="ERROR">ERROR</option>
          <option value="IN_PROGRESS">IN_PROGRESS</option>
          <option value="PENDING">PENDING</option>
        </Select>

        <Select
          value={filters.active as string}
          onChange={(e) => handleFilterChange('active', e.target.value)}
        >
          <option value="">All</option>
          <option value="true">Active</option>
          <option value="false">Inactive</option>
        </Select>
      </FiltersContainer>

      <TableContainer>
        <Table $minWidth="1400px">
          <thead>
            <tr>
              <Th>API Name</Th>
              <Th>API Type</Th>
              <Th>Endpoint</Th>
              <Th>Method</Th>
              <Th>Auth Type</Th>
              <Th>Target Engine</Th>
              <Th>Target Schema</Th>
              <Th>Target Table</Th>
              <Th>Status</Th>
              <Th>Active</Th>
              <Th>Sync Interval</Th>
              <Th>Last Sync</Th>
              <Th>Last Status</Th>
              <Th>Actions</Th>
            </tr>
          </thead>
          <tbody>
            {data.map((entry) => (
              <TableRow key={entry.id}>
                <Td>{entry.api_name}</Td>
                <Td>{entry.api_type}</Td>
                <Td title={entry.base_url + entry.endpoint}>
                  {entry.endpoint}
                </Td>
                <Td>{entry.http_method}</Td>
                <Td>{entry.auth_type}</Td>
                <Td>{entry.target_db_engine}</Td>
                <Td>{entry.target_schema}</Td>
                <Td>{entry.target_table}</Td>
                <Td>
                  <StatusBadge $status={entry.status}>
                    {entry.status}
                  </StatusBadge>
                </Td>
                <Td>
                  <ActiveBadge $active={entry.active}>
                    {entry.active ? 'Yes' : 'No'}
                  </ActiveBadge>
                </Td>
                <Td>{entry.sync_interval}s</Td>
                <Td>
                  {entry.last_sync_time
                    ? format(new Date(entry.last_sync_time), 'yyyy-MM-dd HH:mm:ss')
                    : 'Never'}
                </Td>
                <Td>
                  {entry.last_sync_status ? (
                    <StatusBadge $status={entry.last_sync_status}>
                      {entry.last_sync_status}
                    </StatusBadge>
                  ) : (
                    '-'
                  )}
                </Td>
                <Td>
                  <ActionButton
                    onClick={() => handleToggleActive(entry.api_name, entry.active)}
                  >
                    {entry.active ? 'Deactivate' : 'Activate'}
                  </ActionButton>
                </Td>
              </TableRow>
            ))}
          </tbody>
        </Table>
      </TableContainer>

      {pagination.totalPages > 1 && (
        <>
          <PaginationInfo>
            Showing {((pagination.currentPage - 1) * pagination.limit) + 1} to{' '}
            {Math.min(pagination.currentPage * pagination.limit, pagination.total)} of{' '}
            {pagination.total} APIs
          </PaginationInfo>
          <Pagination>
            <PageButton
              onClick={() => setPage(Math.max(1, page - 1))}
              disabled={page === 1}
            >
              Previous
            </PageButton>
            <span>
              Page {pagination.currentPage} of {pagination.totalPages}
            </span>
            <PageButton
              onClick={() => setPage(Math.min(pagination.totalPages, page + 1))}
              disabled={page === pagination.totalPages}
            >
              Next
            </PageButton>
          </Pagination>
        </>
      )}

      {showAddModal && (
        <AddAPIModal
          onClose={() => setShowAddModal(false)}
          onSave={handleAdd}
        />
      )}
    </Container>
  );
};

export default APICatalog;
