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
  FormGroup,
  Label,
  SearchInput,
  SearchButton,
  ClearSearchButton,
  PaginationInfo,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalTitle,
  TextArea,
} from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { customJobsApi } from '../services/api';
import type { CustomJobEntry } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { sanitizeSearch } from '../utils/validation';
import styled from 'styled-components';
import { theme } from '../theme/theme';
import CustomJobsTreeView from './CustomJobsTreeView';

const HeaderContent = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
`;

const CloseButton = styled.button`
  background: none;
  border: none;
  font-size: 1.5em;
  cursor: pointer;
  color: ${theme.colors.text.secondary};
  padding: 0;
  width: 30px;
  height: 30px;
  display: flex;
  align-items: center;
  justify-content: center;
  
  &:hover {
    color: ${theme.colors.text.primary};
  }
`;

const ScriptSelector = styled(Select)`
  margin-bottom: ${theme.spacing.md};
`;

const ButtonGroup = styled.div`
  display: flex;
  gap: ${theme.spacing.md};
  justify-content: flex-end;
  margin-top: ${theme.spacing.lg};
  padding-top: ${theme.spacing.md};
  border-top: 1px solid ${theme.colors.border.light};
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
  const [allJobs, setAllJobs] = useState<CustomJobEntry[]>([]);
  const [loadingTree, setLoadingTree] = useState(false);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 20
  });
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [editingJob, setEditingJob] = useState<CustomJobEntry | null>(null);
  const [duplicateData, setDuplicateData] = useState<CustomJobEntry | null>(null);
  const [availableScripts, setAvailableScripts] = useState<Array<{name: string, content: string}>>([]);
  const [selectedScript, setSelectedScript] = useState<string>('');
  const [jobForm, setJobForm] = useState({
    job_name: '',
    description: '',
    source_db_engine: 'PostgreSQL',
    source_connection_string: '',
    query_sql: '',
    target_db_engine: 'PostgreSQL',
    target_connection_string: '',
    target_schema: '',
    target_table: '',
    schedule_cron: '',
    active: true,
    enabled: true
  });
  const isMountedRef = useRef(true);

  const fetchAllJobs = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoadingTree(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(search, 100);
      const params: any = {
        page: 1,
        limit: 10000,
        search: sanitizedSearch
      };
      
      if (filters.source_db_engine) params.source_db_engine = filters.source_db_engine;
      if (filters.target_db_engine) params.target_db_engine = filters.target_db_engine;
      if (filters.active) params.active = filters.active;
      if (filters.enabled) params.enabled = filters.enabled;
      
      const response = await customJobsApi.getJobs(params);
      if (isMountedRef.current) {
        setAllJobs(response.data || []);
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    } finally {
      if (isMountedRef.current) {
        setLoadingTree(false);
      }
    }
  }, [
    filters.source_db_engine, 
    filters.target_db_engine, 
    filters.active, 
    filters.enabled,
    search
  ]);

  useEffect(() => {
    isMountedRef.current = true;
    fetchAllJobs();
    const interval = setInterval(() => {
      if (isMountedRef.current) {
        fetchAllJobs();
      }
    }, 30000);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchAllJobs]);

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
      fetchAllJobs();
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [fetchAllJobs]);

  const handleExecute = useCallback(async (jobName: string) => {
    try {
      await customJobsApi.executeJob(jobName);
      alert(`Job "${jobName}" execution triggered. Check process_log for results.`);
      fetchAllJobs();
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [fetchAllJobs]);

  const handleDelete = useCallback(async (jobName: string) => {
    if (!confirm(`Are you sure you want to delete job "${jobName}"?`)) {
      return;
    }
    try {
      await customJobsApi.deleteJob(jobName);
      fetchAllJobs();
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [fetchAllJobs]);

  const loadScripts = useCallback(async () => {
    try {
      const scripts = await customJobsApi.getScripts();
      setAvailableScripts(scripts);
    } catch (err) {
      console.error('Error loading scripts:', err);
    }
  }, []);

  const handleOpenModal = useCallback((job?: CustomJobEntry, isDuplicate?: boolean) => {
    if (job) {
      if (isDuplicate) {
        setEditingJob(null);
        setDuplicateData(job);
        setJobForm({
          job_name: `${job.job_name} (Copy)`,
          description: job.description || '',
          source_db_engine: job.source_db_engine,
          source_connection_string: job.source_connection_string || '',
          query_sql: job.query_sql || '',
          target_db_engine: job.target_db_engine,
          target_connection_string: job.target_connection_string || '',
          target_schema: job.target_schema || '',
          target_table: job.target_table || '',
          schedule_cron: job.schedule_cron || '',
          active: job.active,
          enabled: job.enabled
        });
      } else {
        setEditingJob(job);
        setDuplicateData(null);
        setJobForm({
          job_name: job.job_name,
          description: job.description || '',
          source_db_engine: job.source_db_engine,
          source_connection_string: job.source_connection_string || '',
          query_sql: job.query_sql || '',
          target_db_engine: job.target_db_engine,
          target_connection_string: job.target_connection_string || '',
          target_schema: job.target_schema || '',
          target_table: job.target_table || '',
          schedule_cron: job.schedule_cron || '',
          active: job.active,
          enabled: job.enabled
        });
      }
    } else {
      setEditingJob(null);
      setDuplicateData(null);
      setJobForm({
        job_name: '',
        description: '',
        source_db_engine: 'PostgreSQL',
        source_connection_string: '',
        query_sql: '',
        target_db_engine: 'PostgreSQL',
        target_connection_string: '',
        target_schema: '',
        target_table: '',
        schedule_cron: '',
        active: true,
        enabled: true
      });
    }
    setSelectedScript('');
    setIsModalOpen(true);
  }, []);

  const handleDuplicate = useCallback((job: CustomJobEntry) => {
    handleOpenModal(job, true);
  }, [handleOpenModal]);

  const handleCloseModal = useCallback(() => {
    setIsModalOpen(false);
    setEditingJob(null);
    setDuplicateData(null);
    setSelectedScript('');
  }, []);

  const handleScriptSelect = useCallback((scriptName: string) => {
    setSelectedScript(scriptName);
    const script = availableScripts.find(s => s.name === scriptName);
    if (script) {
      setJobForm(prev => ({ ...prev, query_sql: script.content }));
    }
  }, [availableScripts]);

  const handleSubmit = useCallback(async () => {
    try {
      if (!jobForm.job_name || !jobForm.target_schema || !jobForm.target_table) {
        alert('Please fill in all required fields');
        return;
      }
      
      if (jobForm.source_db_engine === 'Python' && !jobForm.query_sql.trim()) {
        alert('Please select a Python script or enter script content');
        return;
      }
      
      if (jobForm.source_db_engine !== 'Python' && !jobForm.query_sql.trim()) {
        alert('Please enter SQL query');
        return;
      }

      const response = await fetch('/api/custom-jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(jobForm)
      });
      
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.error || error.details || 'Error saving job');
      }
      
      await fetchAllJobs();
      handleCloseModal();
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [jobForm, fetchAllJobs, handleCloseModal]);

  useEffect(() => {
    if (isModalOpen && jobForm.source_db_engine === 'Python') {
      loadScripts();
    }
  }, [isModalOpen, jobForm.source_db_engine, loadScripts]);

  if (loadingTree && allJobs.length === 0) {
    return (
      <Container>
        <Header>Custom Jobs</Header>
        <LoadingOverlay>Loading Custom Jobs...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>
        <HeaderContent>
          <span>■ Custom Jobs</span>
          <Button onClick={() => handleOpenModal()}>
            + Add Job
          </Button>
        </HeaderContent>
      </Header>
      
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
          <option value="Python">Python</option>
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

      {loadingTree ? (
        <LoadingOverlay>Loading tree view...</LoadingOverlay>
      ) : (
        <CustomJobsTreeView 
          jobs={allJobs}
          onJobClick={(job) => handleOpenModal(job)}
          onJobEdit={(job) => handleOpenModal(job)}
          onJobExecute={handleExecute}
          onJobToggleActive={handleToggleActive}
          onJobDelete={handleDelete}
          onJobDuplicate={handleDuplicate}
        />
      )}

      <ModalOverlay $isOpen={isModalOpen} onClick={handleCloseModal}>
        <ModalContent onClick={(e) => e.stopPropagation()}>
          <ModalHeader>
            <ModalTitle>{editingJob ? 'Edit Job' : duplicateData ? 'Duplicate Job' : 'Create New Job'}</ModalTitle>
            <CloseButton onClick={handleCloseModal}>×</CloseButton>
          </ModalHeader>

          <FormGroup>
            <Label>Job Name *</Label>
            <Input
              value={jobForm.job_name}
              onChange={(e) => setJobForm(prev => ({ ...prev, job_name: e.target.value }))}
              placeholder="Enter unique job name"
              disabled={!!editingJob}
            />
          </FormGroup>

          <FormGroup>
            <Label>Description</Label>
            <Input
              value={jobForm.description}
              onChange={(e) => setJobForm(prev => ({ ...prev, description: e.target.value }))}
              placeholder="Job description"
            />
          </FormGroup>

          <FormGroup>
            <Label>Source DB Engine *</Label>
            <Select
              value={jobForm.source_db_engine}
              onChange={(e) => {
                setJobForm(prev => ({ 
                  ...prev, 
                  source_db_engine: e.target.value,
                  query_sql: e.target.value === 'Python' ? '' : prev.query_sql
                }));
                setSelectedScript('');
              }}
            >
              <option value="PostgreSQL">PostgreSQL</option>
              <option value="MariaDB">MariaDB</option>
              <option value="MSSQL">MSSQL</option>
              <option value="MongoDB">MongoDB</option>
              <option value="Oracle">Oracle</option>
              <option value="Python">Python</option>
            </Select>
          </FormGroup>

          {jobForm.source_db_engine !== 'Python' && (
            <FormGroup>
              <Label>Source Connection String</Label>
              <Input
                value={jobForm.source_connection_string}
                onChange={(e) => setJobForm(prev => ({ ...prev, source_connection_string: e.target.value }))}
                placeholder="host=localhost port=5432 dbname=..."
              />
            </FormGroup>
          )}

          {jobForm.source_db_engine === 'Python' && (
            <FormGroup>
              <Label>Select Python Script</Label>
              <ScriptSelector
                value={selectedScript}
                onChange={(e) => handleScriptSelect(e.target.value)}
              >
                <option value="">-- Select a script or write custom --</option>
                {availableScripts.map(script => (
                  <option key={script.name} value={script.name}>
                    {script.name}
                  </option>
                ))}
              </ScriptSelector>
            </FormGroup>
          )}

          <FormGroup>
            <Label>{jobForm.source_db_engine === 'Python' ? 'Python Script *' : 'SQL Query *'}</Label>
            <TextArea
              value={jobForm.query_sql}
              onChange={(e) => setJobForm(prev => ({ ...prev, query_sql: e.target.value }))}
              placeholder={jobForm.source_db_engine === 'Python' 
                ? 'import json\n\ndata = [...]\nprint(json.dumps(data))'
                : 'SELECT * FROM table...'}
            />
          </FormGroup>

          <FormGroup>
            <Label>Target DB Engine *</Label>
            <Select
              value={jobForm.target_db_engine}
              onChange={(e) => setJobForm(prev => ({ ...prev, target_db_engine: e.target.value }))}
            >
              <option value="PostgreSQL">PostgreSQL</option>
              <option value="MariaDB">MariaDB</option>
              <option value="MSSQL">MSSQL</option>
              <option value="MongoDB">MongoDB</option>
              <option value="Oracle">Oracle</option>
            </Select>
          </FormGroup>

          <FormGroup>
            <Label>Target Connection String *</Label>
            <Input
              value={jobForm.target_connection_string}
              onChange={(e) => setJobForm(prev => ({ ...prev, target_connection_string: e.target.value }))}
              placeholder="host=localhost port=5432 dbname=..."
            />
          </FormGroup>

          <FormGroup>
            <Label>Target Schema *</Label>
            <Input
              value={jobForm.target_schema}
              onChange={(e) => setJobForm(prev => ({ ...prev, target_schema: e.target.value }))}
              placeholder="public"
            />
          </FormGroup>

          <FormGroup>
            <Label>Target Table *</Label>
            <Input
              value={jobForm.target_table}
              onChange={(e) => setJobForm(prev => ({ ...prev, target_table: e.target.value }))}
              placeholder="table_name"
            />
          </FormGroup>

          <FormGroup>
            <Label>Schedule Cron (optional)</Label>
            <Input
              value={jobForm.schedule_cron}
              onChange={(e) => setJobForm(prev => ({ ...prev, schedule_cron: e.target.value }))}
              placeholder="* * * * * (minute hour day month day-of-week)"
            />
          </FormGroup>

          <FormGroup>
            <Label>
              <input
                type="checkbox"
                checked={jobForm.active}
                onChange={(e) => setJobForm(prev => ({ ...prev, active: e.target.checked }))}
              />
              {' '}Active
            </Label>
          </FormGroup>

          <FormGroup>
            <Label>
              <input
                type="checkbox"
                checked={jobForm.enabled}
                onChange={(e) => setJobForm(prev => ({ ...prev, enabled: e.target.checked }))}
              />
              {' '}Enabled
            </Label>
          </FormGroup>

          <ButtonGroup>
            <Button onClick={handleCloseModal} $variant="secondary">
              Cancel
            </Button>
            <Button onClick={handleSubmit}>
              {editingJob ? 'Update' : 'Create'}
            </Button>
          </ButtonGroup>
        </ModalContent>
      </ModalOverlay>
    </Container>
  );
};

export default CustomJobs;
