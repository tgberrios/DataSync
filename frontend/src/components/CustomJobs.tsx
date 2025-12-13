import { useState, useEffect } from 'react';
import styled from 'styled-components';
import { format } from 'date-fns';
import { customJobsApi } from '../services/api';
import type { CustomJobEntry } from '../services/api';

const CatalogContainer = styled.div`
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

const FiltersContainer = styled.div`
  display: flex;
  gap: 15px;
  margin-bottom: 20px;
  padding: 15px;
  background: #f5f5f5;
  border-radius: 6px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.03);
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.05s;
  animation-fill-mode: both;
`;

const Select = styled.select`
  padding: 8px 12px;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-family: monospace;
  background: white;
  transition: all 0.2s ease;
  cursor: pointer;
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.3);
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
  }
  
  &:focus {
    outline: none;
    border-color: #0d1b2a;
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
  }
`;

const TableContainer = styled.div`
  width: 100%;
  overflow-x: auto;
  margin-top: 20px;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

const Table = styled.table`
  width: 100%;
  border-collapse: collapse;
  background: white;
  min-width: 1600px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  border-radius: 6px;
  overflow: hidden;
`;

const Th = styled.th`
  padding: 12px;
  text-align: left;
  border-bottom: 2px solid #333;
  background: linear-gradient(180deg, #f5f5f5 0%, #fafafa 100%);
  font-weight: bold;
  position: sticky;
  top: 0;
  z-index: 10;
`;

const Td = styled.td`
  padding: 12px;
  border-bottom: 1px solid #eee;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 200px;
  transition: all 0.2s ease;
`;

const TableRow = styled.tr`
  transition: all 0.15s ease;
  
  &:hover {
    background: linear-gradient(90deg, #ffffff 0%, #f8f9fa 100%);
    transform: scale(1.001);
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
    
    ${Td} {
      border-bottom-color: rgba(10, 25, 41, 0.1);
    }
  }
`;

const ActiveBadge = styled.span<{ $active: boolean }>`
  padding: 4px 10px;
  border-radius: 6px;
  font-size: 0.9em;
  font-weight: bold;
  display: inline-block;
  transition: all 0.2s ease;
  background: ${props => props.$active ? '#e8f5e9' : '#ffebee'};
  color: ${props => props.$active ? '#2e7d32' : '#c62828'};
  
  &:hover {
    transform: scale(1.05);
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
  }
`;

const ActionButton = styled.button`
  padding: 6px 12px;
  border: 1px solid #ddd;
  border-radius: 6px;
  background: white;
  cursor: pointer;
  font-family: monospace;
  margin-right: 5px;
  transition: all 0.2s ease;
  font-weight: 500;
  
  &:hover {
    background: linear-gradient(135deg, #f5f5f5 0%, #ffffff 100%);
    border-color: rgba(10, 25, 41, 0.3);
    transform: translateY(-2px);
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
  }
  
  &:active {
    transform: translateY(0);
  }
`;

const PlayButton = styled.button`
  padding: 8px 14px;
  border: 1px solid #4caf50;
  border-radius: 6px;
  background: linear-gradient(135deg, #4caf50 0%, #45a049 100%);
  color: white;
  cursor: pointer;
  font-family: monospace;
  margin-right: 5px;
  transition: all 0.2s ease;
  font-weight: bold;
  font-size: 1.1em;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 40px;
  
  &:hover {
    background: linear-gradient(135deg, #45a049 0%, #3d8b40 100%);
    transform: translateY(-2px) scale(1.05);
    box-shadow: 0 4px 12px rgba(76, 175, 80, 0.4);
    border-color: #3d8b40;
  }
  
  &:active {
    transform: translateY(0) scale(1);
  }
`;

const LoadingOverlay = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(255, 255, 255, 0.9);
  display: flex;
  justify-content: center;
  align-items: center;
  font-size: 1.2em;
  z-index: 1000;
  animation: fadeIn 0.3s ease-in;
  backdrop-filter: blur(2px);
`;

const ErrorMessage = styled.div`
  padding: 15px;
  margin: 20px 0;
  background-color: #ffebee;
  color: #c62828;
  border-radius: 6px;
  border: 1px solid #ef9a9a;
  animation: slideUp 0.2s ease-out;
  box-shadow: 0 4px 12px rgba(220, 38, 38, 0.15);
`;

const Pagination = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 10px;
  margin-top: 20px;
  padding: 15px;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.15s;
  animation-fill-mode: both;
`;

const PageButton = styled.button<{ $active?: boolean }>`
  padding: 8px 14px;
  border: 1px solid #ddd;
  border-radius: 6px;
  background: ${props => props.$active ? '#0d1b2a' : 'white'};
  color: ${props => props.$active ? 'white' : '#333'};
  cursor: pointer;
  font-family: monospace;
  transition: all 0.2s ease;
  font-weight: ${props => props.$active ? 'bold' : 'normal'};
  
  &:hover:not(:disabled) {
    background: ${props => props.$active ? '#1e3a5f' : '#f5f5f5'};
    transform: translateY(-2px);
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
    border-color: ${props => props.$active ? '#0d1b2a' : 'rgba(10, 25, 41, 0.3)'};
  }
  
  &:disabled {
    cursor: not-allowed;
    opacity: 0.5;
  }
`;

const PaginationInfo = styled.div`
  text-align: center;
  margin-bottom: 10px;
  color: #666;
  font-size: 0.9em;
  animation: fadeIn 0.25s ease-in;
`;

const SearchContainer = styled.div`
  display: flex;
  gap: 10px;
  margin-bottom: 20px;
  padding: 15px;
  background: #f9f9f9;
  border-radius: 6px;
  border: 1px solid #e0e0e0;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.03);
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.08s;
  animation-fill-mode: both;
`;

const SearchInput = styled.input`
  flex: 1;
  padding: 10px 14px;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-family: monospace;
  font-size: 14px;
  transition: all 0.2s ease;
  
  &:focus {
    outline: none;
    border-color: #0d1b2a;
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
    transform: translateY(-1px);
  }
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.3);
  }
`;

const SearchButton = styled.button`
  padding: 10px 20px;
  border: 1px solid #0d1b2a;
  border-radius: 6px;
  background: #0d1b2a;
  color: white;
  cursor: pointer;
  font-family: monospace;
  transition: all 0.2s ease;
  font-weight: bold;
  
  &:hover {
    background: #1e3a5f;
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(13, 27, 42, 0.3);
  }
  
  &:active {
    transform: translateY(0);
  }
`;

const ClearSearchButton = styled.button`
  padding: 10px 15px;
  border: 1px solid #ddd;
  border-radius: 6px;
  background: white;
  cursor: pointer;
  font-family: monospace;
  transition: all 0.2s ease;
  
  &:hover {
    background: #f5f5f5;
    border-color: rgba(10, 25, 41, 0.3);
    transform: translateY(-2px);
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
  }
`;

const QueryPreview = styled.div`
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 0.85em;
  color: #666;
  cursor: help;
  
  &:hover {
    color: #333;
  }
`;

const CustomJobs = () => {
  const [filter, setFilter] = useState({
    source_db_engine: '',
    target_db_engine: '',
    active: '',
    enabled: ''
  });
  
  const [search, setSearch] = useState('');
  const [searchInput, setSearchInput] = useState('');

  const [data, setData] = useState<CustomJobEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 20
  });

  const fetchJobs = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await customJobsApi.getJobs({
        page,
        limit: 20,
        ...filter,
        search
      });
      setData(response.data);
      setPagination(response.pagination);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error loading custom jobs');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchJobs();
  }, [page, filter, search]);

  const handleSearch = () => {
    setSearch(searchInput);
    setPage(1);
  };

  const handleClearSearch = () => {
    setSearchInput('');
    setSearch('');
    setPage(1);
  };

  const handleFilterChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setFilter(prev => ({ ...prev, [e.target.name]: e.target.value }));
    setPage(1);
  };

  const handleToggleActive = async (jobName: string, currentActive: boolean) => {
    try {
      await customJobsApi.updateActive(jobName, !currentActive);
      fetchJobs();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error updating job status');
    }
  };

  const handleExecute = async (jobName: string) => {
    try {
      await customJobsApi.executeJob(jobName);
      alert(`Job "${jobName}" execution triggered. Check process_log for results.`);
      fetchJobs();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error executing job');
    }
  };

  const handleDelete = async (jobName: string) => {
    if (!confirm(`Are you sure you want to delete job "${jobName}"?`)) {
      return;
    }
    try {
      await customJobsApi.deleteJob(jobName);
      fetchJobs();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error deleting job');
    }
  };

  if (loading && data.length === 0) {
    return <LoadingOverlay>Loading Custom Jobs...</LoadingOverlay>;
  }

  return (
    <CatalogContainer>
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
          name="source_db_engine"
          value={filter.source_db_engine}
          onChange={handleFilterChange}
        >
          <option value="">All Source Engines</option>
          <option value="PostgreSQL">PostgreSQL</option>
          <option value="MariaDB">MariaDB</option>
          <option value="MSSQL">MSSQL</option>
          <option value="MongoDB">MongoDB</option>
          <option value="Oracle">Oracle</option>
        </Select>

        <Select
          name="target_db_engine"
          value={filter.target_db_engine}
          onChange={handleFilterChange}
        >
          <option value="">All Target Engines</option>
          <option value="PostgreSQL">PostgreSQL</option>
          <option value="MariaDB">MariaDB</option>
          <option value="MSSQL">MSSQL</option>
          <option value="MongoDB">MongoDB</option>
          <option value="Oracle">Oracle</option>
        </Select>

        <Select
          name="active"
          value={filter.active}
          onChange={handleFilterChange}
        >
          <option value="">All Active States</option>
          <option value="true">Active</option>
          <option value="false">Inactive</option>
        </Select>

        <Select
          name="enabled"
          value={filter.enabled}
          onChange={handleFilterChange}
        >
          <option value="">All Enabled States</option>
          <option value="true">Enabled</option>
          <option value="false">Disabled</option>
        </Select>
      </FiltersContainer>

      <TableContainer>
        <Table>
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
                    style={{ background: '#ffebee', borderColor: '#f44336' }}
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
              onClick={() => setPage(p => Math.max(1, p - 1))}
              disabled={page === 1}
            >
              Previous
            </PageButton>
            <span>
              Page {pagination.currentPage} of {pagination.totalPages}
            </span>
            <PageButton
              onClick={() => setPage(p => Math.min(pagination.totalPages, p + 1))}
              disabled={page === pagination.totalPages}
            >
              Next
            </PageButton>
          </Pagination>
        </>
      )}
    </CatalogContainer>
  );
};

export default CustomJobs;

