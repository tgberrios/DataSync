import { useState, useEffect, useCallback, useRef } from 'react';
import { format } from 'date-fns';
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
  Pagination,
  PageButton,
  ErrorMessage,
  LoadingOverlay,
  SearchContainer,
  Input,
  Button,
  ActiveBadge,
  ActionButton,
  PlayButton,
} from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { customJobsApi } from '../services/api';
import type { CustomJobEntry } from '../services/api';
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

const QueryPreview = styled.div`
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 0.85em;
  color: ${theme.colors.text.secondary};
  cursor: help;
  
  &:hover {
    color: ${theme.colors.text.primary};
  }
`;

const CustomJobs = () => {
  const { page, limit, setPage } = usePagination(1, 20);
  const { filters, setFilter } = useTableFilters({
    source_db_engine: '',
    target_db_engine: '',
    active: '',
    enabled: ''
  });
  
  const [searchInput, setSearchInput] = useState('');
  const [search, setSearch] = useState('');
  const [data, setData] = useState<CustomJobEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 20
  });
  const isMountedRef = useRef(true);

  const fetchJobs = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(search, 100);
      const response = await customJobsApi.getJobs({
        page,
        limit,
        ...filters,
        search: sanitizedSearch
      });
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
    filters.source_db_engine, 
    filters.target_db_engine, 
    filters.active, 
    filters.enabled,
    search
  ]);

  useEffect(() => {
    isMountedRef.current = true;
    fetchJobs();
    return () => {
      isMountedRef.current = false;
    };
  }, [fetchJobs]);

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

  const handleToggleActive = useCallback(async (jobName: string, currentActive: boolean) => {
    try {
      await customJobsApi.updateActive(jobName, !currentActive);
      fetchJobs();
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [fetchJobs]);

  const handleExecute = useCallback(async (jobName: string) => {
    try {
      await customJobsApi.executeJob(jobName);
      alert(`Job "${jobName}" execution triggered. Check process_log for results.`);
      fetchJobs();
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [fetchJobs]);

  const handleDelete = useCallback(async (jobName: string) => {
    if (!confirm(`Are you sure you want to delete job "${jobName}"?`)) {
      return;
    }
    try {
      await customJobsApi.deleteJob(jobName);
      fetchJobs();
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [fetchJobs]);

  if (loading && data.length === 0) {
    return <LoadingOverlay>Loading Custom Jobs...</LoadingOverlay>;
  }

  return (
    <Container>
      <Header>■ Custom Jobs</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}

      <SearchContainer>
        <SearchInput
          type="text"
          placeholder="Search by job name, description, target schema/table..."
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
        <Select
          value={filters.source_db_engine as string}
          onChange={(e) => handleFilterChange('source_db_engine', e.target.value)}
        >
          <option value="">All Source Engines</option>
          <option value="PostgreSQL">PostgreSQL</option>
          <option value="MariaDB">MariaDB</option>
          <option value="MSSQL">MSSQL</option>
          <option value="MongoDB">MongoDB</option>
          <option value="Oracle">Oracle</option>
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
          value={filters.active as string}
          onChange={(e) => handleFilterChange('active', e.target.value)}
        >
          <option value="">All Active States</option>
          <option value="true">Active</option>
          <option value="false">Inactive</option>
        </Select>

        <Select
          value={filters.enabled as string}
          onChange={(e) => handleFilterChange('enabled', e.target.value)}
        >
          <option value="">All Enabled States</option>
          <option value="true">Enabled</option>
          <option value="false">Disabled</option>
        </Select>
      </FiltersContainer>

      <TableContainer>
        <Table $minWidth="1600px">
          <thead>
            <tr>
              <Th>Job Name</Th>
              <Th>Description</Th>
              <Th>Source Engine</Th>
              <Th>Target Engine</Th>
              <Th>Target Schema</Th>
              <Th>Target Table</Th>
              <Th>Schedule</Th>
              <Th>Active</Th>
              <Th>Enabled</Th>
              <Th>Query Preview</Th>
              <Th>Created</Th>
              <Th>Updated</Th>
              <Th>Actions</Th>
            </tr>
          </thead>
          <tbody>
            {data.map((job) => (
              <TableRow key={job.id}>
                <Td>{job.job_name}</Td>
                <Td title={job.description || ''}>
                  {job.description ? (job.description.length > 50 ? job.description.substring(0, 50) + '...' : job.description) : '-'}
                </Td>
                <Td>{job.source_db_engine}</Td>
                <Td>{job.target_db_engine}</Td>
                <Td>{job.target_schema}</Td>
                <Td>{job.target_table}</Td>
                <Td>{job.schedule_cron || 'Manual'}</Td>
                <Td>
                  <ActiveBadge $active={job.active}>
                    {job.active ? 'Yes' : 'No'}
                  </ActiveBadge>
                </Td>
                <Td>
                  <ActiveBadge $active={job.enabled}>
                    {job.enabled ? 'Yes' : 'No'}
                  </ActiveBadge>
                </Td>
                <Td>
                  <QueryPreview title={job.query_sql}>
                    {job.query_sql.length > 50 ? job.query_sql.substring(0, 50) + '...' : job.query_sql}
                  </QueryPreview>
                </Td>
                <Td>
                  {job.created_at
                    ? format(new Date(job.created_at), 'yyyy-MM-dd HH:mm')
                    : '-'}
                </Td>
                <Td>
                  {job.updated_at
                    ? format(new Date(job.updated_at), 'yyyy-MM-dd HH:mm')
                    : '-'}
                </Td>
                <Td>
                  <PlayButton
                    onClick={() => handleExecute(job.job_name)}
                    title={`Execute job: ${job.job_name}`}
                  >
                    ▶
                  </PlayButton>
                  <ActionButton
                    onClick={() => handleToggleActive(job.job_name, job.active)}
                  >
                    {job.active ? 'Deactivate' : 'Activate'}
                  </ActionButton>
                  <ActionButton
                    onClick={() => handleDelete(job.job_name)}
                    $variant="danger"
                  >
                    Delete
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
            {pagination.total} jobs
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
    </Container>
  );
};

export default CustomJobs;
