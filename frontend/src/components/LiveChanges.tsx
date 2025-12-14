import { useState, useEffect, useCallback, useRef } from 'react';
import styled from 'styled-components';
import {
  Container,
  Header,
  ErrorMessage,
  LoadingOverlay,
  Grid,
  Value,
  Button,
} from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { monitorApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { theme } from '../theme/theme';

const ControlsContainer = styled.div`
  display: flex;
  gap: ${theme.spacing.md};
  align-items: center;
`;

const RefreshToggle = styled(Button)`
  height: 36px;
  display: flex;
  align-items: center;
  justify-content: center;
`;

const SearchInput = styled.input`
  padding: 8px 12px;
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  font-size: 0.9em;
  font-family: ${theme.fonts.primary};
  width: 200px;
  height: 36px;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.3);
  }
  
  &:focus {
    outline: none;
    border-color: ${theme.colors.primary.main};
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
    transform: translateY(-1px);
  }
`;

const FilterSelect = styled.select`
  padding: 8px 12px;
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  font-size: 0.9em;
  font-family: ${theme.fonts.primary};
  background-color: ${theme.colors.background.main};
  height: 36px;
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.3);
  }
  
  &:focus {
    outline: none;
    border-color: ${theme.colors.primary.main};
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
  }
`;

const StatsContainer = styled(Grid)`
  margin-bottom: ${theme.spacing.xxl};
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

const StatCard = styled(Value)`
  padding: ${theme.spacing.lg};
  text-align: center;
  min-height: 100px;
`;

const StatValue = styled.div`
  font-size: 2em;
  font-weight: bold;
  color: ${theme.colors.text.primary};
  margin-bottom: 5px;
  transition: all ${theme.transitions.normal};
  
  ${StatCard}:hover & {
    transform: scale(1.1);
  }
`;

const StatLabel = styled.div`
  font-size: 0.9em;
  color: ${theme.colors.text.secondary};
  text-transform: uppercase;
  letter-spacing: 0.5px;
  font-weight: 500;
`;

const ProcessingList = styled.div`
  display: flex;
  flex-direction: column;
  gap: ${theme.spacing.sm};
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const ProcessingItem = styled.div`
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.md};
  background-color: ${theme.colors.background.secondary};
  overflow: hidden;
  transition: all ${theme.transitions.normal};
  box-shadow: ${theme.shadows.sm};
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.2);
    background-color: ${theme.colors.background.main};
    box-shadow: ${theme.shadows.md};
    transform: translateY(-2px);
  }
`;

const ProcessingSummary = styled.div`
  display: grid;
  grid-template-columns: max-content 120px 150px 1fr 140px 140px;
  align-items: center;
  padding: 12px 15px;
  cursor: pointer;
  gap: 10px;
  font-size: 0.9em;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    background: linear-gradient(90deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
  }
`;

const DataCell = styled.div`
  background-color: ${theme.colors.background.secondary};
  border-radius: ${theme.borderRadius.sm};
  padding: 8px 10px;
  text-align: center;
  font-size: 0.95em;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`;

const RegularCell = styled.div`
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`;

const ProcessingDetails = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '400px' : '0'};
  opacity: ${props => props.$isOpen ? '1' : '0'};
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  border-top: ${props => props.$isOpen ? `1px solid ${theme.colors.border.light}` : 'none'};
  background-color: ${theme.colors.background.main};
  overflow: hidden;
`;

const DetailGrid = styled.div`
  display: grid;
  grid-template-columns: 150px 1fr;
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

const PaginationContainer = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  gap: ${theme.spacing.sm};
  margin-top: ${theme.spacing.lg};
  padding: ${theme.spacing.md};
  background-color: ${theme.colors.background.secondary};
  border-radius: ${theme.borderRadius.sm};
`;

const PaginationButton = styled(Button)`
  padding: 8px 14px;
  font-size: 0.9em;
`;

const PaginationInfo = styled.div`
  font-size: 0.9em;
  color: ${theme.colors.text.secondary};
  margin: 0 ${theme.spacing.sm};
`;

/**
 * Live Changes component
 * Displays real-time processing events and changes from database synchronization
 */
const LiveChanges = () => {
  const { page, limit, setPage } = usePagination(1, 20);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [processings, setProcessings] = useState<any[]>([]);
  const [stats, setStats] = useState<any>({});
  const [openProcessingId, setOpenProcessingId] = useState<number | null>(null);
  const [isPaused, setIsPaused] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState('ALL');
  const [engineFilter, setEngineFilter] = useState('ALL');
  const [strategyFilter, setStrategyFilter] = useState('ALL');
  const [pagination, setPagination] = useState<any>({});
  const isMountedRef = useRef(true);

  const fetchData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setError(null);
      const [processingsResponse, statsData] = await Promise.all([
        monitorApi.getProcessingLogs(page, limit, strategyFilter !== 'ALL' ? strategyFilter : undefined),
        monitorApi.getProcessingStats()
      ]);
      if (isMountedRef.current) {
        setProcessings(processingsResponse.data || []);
        setPagination(processingsResponse.pagination || {});
        setStats(statsData || {});
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
  }, [page, limit, strategyFilter, searchTerm]);

  useEffect(() => {
    isMountedRef.current = true;
    fetchData();
    
    let interval: NodeJS.Timeout | null = null;
    if (!isPaused) {
      interval = setInterval(() => {
        if (isMountedRef.current) {
          fetchData();
        }
      }, 5000);
    }
    
    return () => {
      isMountedRef.current = false;
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [fetchData, isPaused]);

  const toggleProcessing = useCallback((id: number) => {
    setOpenProcessingId(prev => prev === id ? null : id);
  }, []);

  const toggleRefresh = useCallback(() => {
    setIsPaused(prev => !prev);
  }, []);

  const handlePageChange = useCallback((newPage: number) => {
    setPage(newPage);
    setOpenProcessingId(null);
  }, [setPage]);

  const formatTimestamp = useCallback((timestamp: string) => {
    return new Date(timestamp).toISOString();
  }, []);

  const filteredProcessings = processings.filter(processing => {
    const matchesSearch = searchTerm === '' || 
      (processing.schema_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      processing.table_name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
      processing.db_engine?.toLowerCase().includes(searchTerm.toLowerCase()));
    
    const matchesStatus = statusFilter === 'ALL' || processing.status === statusFilter;
    const matchesEngine = engineFilter === 'ALL' || processing.db_engine === engineFilter;
    const matchesStrategy = strategyFilter === 'ALL' || processing.pk_strategy === strategyFilter;
    
    return matchesSearch && matchesStatus && matchesEngine && matchesStrategy;
  });

  if (loading && processings.length === 0) {
    return (
      <Container>
        <Header>[*] Live Changes</Header>
        <LoadingOverlay>Loading live changes...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>
        <div>[*] Live Changes</div>
        <ControlsContainer>
          <SearchInput
            type="text"
            placeholder="Search tables..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
          <FilterSelect
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
          >
            <option value="ALL">All Status</option>
            <option value="FULL_LOAD">Full Load</option>
            <option value="LISTENING_CHANGES">Listening</option>
            <option value="ERROR">Error</option>
            <option value="NO_DATA">No Data</option>
          </FilterSelect>
          <FilterSelect
            value={engineFilter}
            onChange={(e) => setEngineFilter(e.target.value)}
          >
            <option value="ALL">All Engines</option>
            <option value="MSSQL">MSSQL</option>
            <option value="MariaDB">MariaDB</option>
            <option value="PostgreSQL">PostgreSQL</option>
          </FilterSelect>
          <FilterSelect
            value={strategyFilter}
            onChange={(e) => setStrategyFilter(e.target.value)}
          >
            <option value="ALL">All Strategies</option>
            <option value="PK">PK</option>
            <option value="OFFSET">OFFSET</option>
          </FilterSelect>
          <RefreshToggle onClick={toggleRefresh}>
            {isPaused ? '[>] Resume' : '[||] Pause'}
          </RefreshToggle>
        </ControlsContainer>
      </Header>

      {error && <ErrorMessage>{error}</ErrorMessage>}

      {!loading && !error && (
        <>
          <StatsContainer $columns="repeat(auto-fit, minmax(200px, 1fr))">
            <StatCard>
              <StatValue>{stats.total || 0}</StatValue>
              <StatLabel>Total Events</StatLabel>
            </StatCard>
            <StatCard>
              <StatValue>{stats.last24h || 0}</StatValue>
              <StatLabel>Last 24h</StatLabel>
            </StatCard>
            <StatCard>
              <StatValue>{stats.listeningChanges || 0}</StatValue>
              <StatLabel>Listening</StatLabel>
            </StatCard>
            <StatCard>
              <StatValue>{stats.fullLoad || 0}</StatValue>
              <StatLabel>Full Load</StatLabel>
            </StatCard>
            <StatCard>
              <StatValue>{stats.errors || 0}</StatValue>
              <StatLabel>Errors</StatLabel>
            </StatCard>
          </StatsContainer>

          <ProcessingList>
            {filteredProcessings.length === 0 ? (
              <div style={{ padding: '40px', textAlign: 'center', color: theme.colors.text.secondary }}>
                No processing events found
              </div>
            ) : (
              filteredProcessings.map((processing) => (
                <ProcessingItem key={processing.id}>
                  <ProcessingSummary onClick={() => toggleProcessing(processing.id)}>
                    <RegularCell style={{ overflow: 'visible', textOverflow: 'unset', whiteSpace: 'nowrap' }}>
                      {formatTimestamp(processing.processed_at)}
                    </RegularCell>
                    <RegularCell>{processing.schema_name}</RegularCell>
                    <RegularCell>{processing.table_name}</RegularCell>
                    <RegularCell>
                      {processing.db_engine} - {processing.status}
                    </RegularCell>
                    {processing.pk_strategy === 'PK' && processing.new_pk ? (
                      <>
                        <DataCell>
                          <div style={{ fontSize: '0.8em', color: theme.colors.text.secondary, marginBottom: '2px' }}>Last PK</div>
                          {processing.new_pk || 'N/A'}
                        </DataCell>
                        <DataCell style={{ visibility: 'hidden' }} />
                        <DataCell style={{ visibility: 'hidden' }} />
                        <DataCell style={{ visibility: 'hidden' }} />
                      </>
                    ) : (
                      <>
                        <DataCell>
                          <div style={{ fontSize: '0.8em', color: theme.colors.text.secondary, marginBottom: '2px' }}>Record Count</div>
                          {processing.record_count !== null && processing.record_count !== undefined ? processing.record_count.toLocaleString() : 'N/A'}
                        </DataCell>
                        <DataCell style={{ visibility: 'hidden' }} />
                        <DataCell style={{ visibility: 'hidden' }} />
                        <DataCell style={{ visibility: 'hidden' }} />
                      </>
                    )}
                  </ProcessingSummary>
                  
                  <ProcessingDetails $isOpen={openProcessingId === processing.id}>
                    <DetailGrid>
                      <DetailLabel>Schema:</DetailLabel>
                      <DetailValue>{processing.schema_name}</DetailValue>
                      
                      <DetailLabel>Table:</DetailLabel>
                      <DetailValue>{processing.table_name}</DetailValue>
                      
                      <DetailLabel>Engine:</DetailLabel>
                      <DetailValue>{processing.db_engine}</DetailValue>
                      
                      <DetailLabel>Status:</DetailLabel>
                      <DetailValue>{processing.status}</DetailValue>
                      
                      <DetailLabel>PK Strategy:</DetailLabel>
                      <DetailValue>{processing.pk_strategy || 'N/A'}</DetailValue>
                      
                      {processing.new_pk && (
                        <>
                          <DetailLabel>Last PK:</DetailLabel>
                          <DetailValue>{processing.new_pk}</DetailValue>
                        </>
                      )}
                      
                      <DetailLabel>Record Count:</DetailLabel>
                      <DetailValue>
                        {processing.record_count !== null && processing.record_count !== undefined 
                          ? processing.record_count.toLocaleString() 
                          : 'N/A'}
                      </DetailValue>
                      
                      <DetailLabel>Processed At:</DetailLabel>
                      <DetailValue>{processing.processed_at ? new Date(processing.processed_at).toLocaleString() : 'N/A'}</DetailValue>
                    </DetailGrid>
                  </ProcessingDetails>
                </ProcessingItem>
              ))
            )}
          </ProcessingList>

          {pagination.totalPages > 1 && (
            <PaginationContainer>
              <PaginationButton 
                onClick={() => handlePageChange(Math.max(1, page - 1))} 
                disabled={!pagination.hasPrev}
              >
                {'[<]'} Prev
              </PaginationButton>
              
              {Array.from({ length: Math.min(5, pagination.totalPages) }, (_, i) => {
                const pageNum = i + 1;
                return (
                  <PaginationButton
                    key={pageNum}
                    onClick={() => handlePageChange(pageNum)}
                    $variant={page === pageNum ? 'primary' : 'secondary'}
                  >
                    {pageNum}
                  </PaginationButton>
                );
              })}
              
              <PaginationButton 
                onClick={() => handlePageChange(Math.min(pagination.totalPages, page + 1))} 
                disabled={!pagination.hasNext}
              >
                Next {'[>]'}
              </PaginationButton>
              
              <PaginationInfo>
                Page {pagination.page || page} of {pagination.totalPages} 
                ({pagination.total} total events)
              </PaginationInfo>
            </PaginationContainer>
          )}
        </>
      )}
    </Container>
  );
};

export default LiveChanges;
