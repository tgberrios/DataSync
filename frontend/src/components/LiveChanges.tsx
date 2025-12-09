import { useState, useEffect } from 'react';
import styled from 'styled-components';
import { monitorApi } from '../services/api';

const LiveChangesContainer = styled.div`
  background-color: white;
  color: #333;
  padding: 20px;
  font-family: monospace;
  animation: fadeIn 0.25s ease-in;
`;

const Header = styled.div`
  border: 2px solid #333;
  padding: 15px 20px;
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
  display: flex;
  justify-content: space-between;
  align-items: center;
  
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

const ControlsContainer = styled.div`
  display: flex;
  gap: 15px;
  align-items: center;
`;

const RefreshToggle = styled.button`
  background-color: #0d1b2a;
  color: white;
  border: none;
  padding: 8px 16px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.9em;
  font-weight: 500;
  transition: all 0.2s ease;
  height: 36px;
  display: flex;
  align-items: center;
  justify-content: center;
  
  &:hover {
    background-color: #1e3a5f;
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(13, 27, 42, 0.3);
  }
  
  &:active {
    transform: translateY(0);
  }
`;

const SearchInput = styled.input`
  padding: 8px 12px;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-size: 0.9em;
  font-family: monospace;
  width: 200px;
  height: 36px;
  transition: all 0.2s ease;
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.3);
  }
  
  &:focus {
    outline: none;
    border-color: #0d1b2a;
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
    transform: translateY(-1px);
  }
`;

const FilterSelect = styled.select`
  padding: 8px 12px;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-size: 0.9em;
  font-family: monospace;
  background-color: white;
  height: 36px;
  transition: all 0.2s ease;
  cursor: pointer;
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.3);
  }
  
  &:focus {
    outline: none;
    border-color: #0d1b2a;
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
  }
`;

const StatsContainer = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 20px;
  margin-bottom: 30px;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

const StatCard = styled.div`
  border: 1px solid #ddd;
  border-radius: 6px;
  padding: 20px;
  background: linear-gradient(135deg, #fafafa 0%, #ffffff 100%);
  text-align: center;
  transition: all 0.2s ease;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.03);
  
  &:hover {
    transform: translateY(-5px);
    box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
    border-color: rgba(10, 25, 41, 0.2);
    background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
  }
`;

const StatValue = styled.div`
  font-size: 2em;
  font-weight: bold;
  color: #0d1b2a;
  margin-bottom: 5px;
  transition: all 0.2s ease;
  
  ${StatCard}:hover & {
    transform: scale(1.1);
  }
`;

const StatLabel = styled.div`
  font-size: 0.9em;
  color: #666;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  font-weight: 500;
`;

const ProcessingList = styled.div`
  display: flex;
  flex-direction: column;
  gap: 12px;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const ProcessingItem = styled.div`
  border: 1px solid #eee;
  border-radius: 6px;
  background-color: #fafafa;
  overflow: hidden;
  transition: all 0.2s ease;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.03);
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.2);
    background-color: #ffffff;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
    transform: translateY(-2px);
  }
`;

const ProcessingSummary = styled.div`
  display: grid;
  grid-template-columns: max-content 120px 150px 1fr 140px 140px 140px 140px;
  align-items: center;
  padding: 12px 15px;
  cursor: pointer;
  gap: 10px;
  font-size: 0.9em;
  transition: all 0.2s ease;
  
  &:hover {
    background: linear-gradient(90deg, #f0f0f0 0%, #f8f9fa 100%);
  }
`;

const DataCell = styled.div`
  background-color: #f0f0f0;
  border-radius: 4px;
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
  border-top: ${props => props.$isOpen ? '1px solid #eee' : 'none'};
  background-color: white;
  overflow: hidden;
  animation: ${props => props.$isOpen ? 'fadeIn 0.2s ease-in' : 'none'};
`;

const DetailGrid = styled.div`
  display: grid;
  grid-template-columns: 150px 1fr;
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


const LoadingSpinner = styled.div`
  text-align: center;
  padding: 40px;
  color: #666;
  font-size: 1.1em;
`;

const ErrorMessage = styled.div`
  color: #f44336;
  padding: 20px;
  text-align: center;
  background-color: #ffebee;
  border-radius: 4px;
  margin: 20px 0;
`;

const EmptyState = styled.div`
  text-align: center;
  padding: 40px;
  color: #666;
  font-size: 1.1em;
`;

const PaginationContainer = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 10px;
  margin-top: 20px;
  padding: 15px;
  background-color: #f8f8f8;
  border-radius: 4px;
`;

const PaginationButton = styled.button`
  background-color: ${props => props.$active ? '#0d1b2a' : 'white'};
  color: ${props => props.$active ? 'white' : '#333'};
  border: 1px solid #ddd;
  padding: 8px 14px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.9em;
  font-weight: ${props => props.$active ? 'bold' : '500'};
  transition: all 0.2s ease;
  
  &:hover:not(:disabled) {
    background-color: ${props => props.$active ? '#1e3a5f' : '#f5f5f5'};
    transform: translateY(-2px);
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
    border-color: ${props => props.$active ? '#0d1b2a' : 'rgba(10, 25, 41, 0.3)'};
  }
  
  &:disabled {
    background-color: #f0f0f0;
    color: #999;
    cursor: not-allowed;
    opacity: 0.5;
  }
`;

const PaginationInfo = styled.div`
  font-size: 0.9em;
  color: #666;
  margin: 0 10px;
`;

const LiveChanges = () => {
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
  const [currentPage, setCurrentPage] = useState(1);
  const [pagination, setPagination] = useState<any>({});

  useEffect(() => {
    const fetchData = async () => {
      try {
        setError(null);
        const [processingsResponse, statsData] = await Promise.all([
          monitorApi.getProcessingLogs(currentPage, 20, strategyFilter !== 'ALL' ? strategyFilter : undefined),
          monitorApi.getProcessingStats()
        ]);
        setProcessings(processingsResponse.data);
        setPagination(processingsResponse.pagination);
        setStats(statsData);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading data');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    
    if (!isPaused) {
      const interval = setInterval(fetchData, 5000); // Refresh every 5 seconds
      return () => clearInterval(interval);
    }
  }, [isPaused, currentPage, strategyFilter]);

  const toggleProcessing = (id: number) => {
    setOpenProcessingId(openProcessingId === id ? null : id);
  };

  const toggleRefresh = () => {
    setIsPaused(!isPaused);
  };

  const handlePageChange = (page: number) => {
    setCurrentPage(page);
    setOpenProcessingId(null); // Close any open details
  };

  const handlePrevPage = () => {
    if (pagination.hasPrev) {
      setCurrentPage(currentPage - 1);
      setOpenProcessingId(null);
    }
  };

  const handleNextPage = () => {
    if (pagination.hasNext) {
      setCurrentPage(currentPage + 1);
      setOpenProcessingId(null);
    }
  };

  const formatTimestamp = (timestamp: string) => {
    return new Date(timestamp).toISOString();
  };

  // Filter and search logic
  const filteredProcessings = processings.filter(processing => {
    const matchesSearch = searchTerm === '' || 
      processing.schema_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      processing.table_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      processing.db_engine.toLowerCase().includes(searchTerm.toLowerCase());
    
    const matchesStatus = statusFilter === 'ALL' || processing.status === statusFilter;
    const matchesEngine = engineFilter === 'ALL' || processing.db_engine === engineFilter;
    const matchesStrategy = strategyFilter === 'ALL' || processing.pk_strategy === strategyFilter;
    
    return matchesSearch && matchesStatus && matchesEngine && matchesStrategy;
  });

  return (
    <LiveChangesContainer>
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
          <RefreshToggle 
            onClick={toggleRefresh}
            $isPaused={isPaused}
          >
            {isPaused ? '[>] Resume' : '[||] Pause'}
          </RefreshToggle>
        </ControlsContainer>
      </Header>

      {loading && (
        <LoadingSpinner>
          Loading live changes...
        </LoadingSpinner>
      )}

      {error && (
        <ErrorMessage>
          {error}
        </ErrorMessage>
      )}

      {!loading && !error && (
        <>
          <StatsContainer>
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
              <EmptyState>
                No processing events found
              </EmptyState>
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
                    {processing.pk_strategy === 'PK' ? (
                      <>
                        <DataCell>
                          <div style={{ fontSize: '0.8em', color: '#666', marginBottom: '2px' }}>Old PK</div>
                          {processing.old_pk || '0'}
                        </DataCell>
                        <DataCell>
                          <div style={{ fontSize: '0.8em', color: '#666', marginBottom: '2px' }}>New PK</div>
                          {processing.new_pk || '0'}
                        </DataCell>
                        <DataCell style={{ visibility: 'hidden' }} />
                        <DataCell style={{ visibility: 'hidden' }} />
                      </>
                    ) : (
                      <>
                        <DataCell>
                          <div style={{ fontSize: '0.8em', color: '#666', marginBottom: '2px' }}>Old Offset</div>
                          {processing.old_offset || '0'}
                        </DataCell>
                        <DataCell>
                          <div style={{ fontSize: '0.8em', color: '#666', marginBottom: '2px' }}>New Offset</div>
                          {processing.new_offset || '1000'}
                        </DataCell>
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
                      
                      {processing.pk_strategy === 'PK' ? (
                        <>
                          <DetailLabel>Old PK:</DetailLabel>
                          <DetailValue>{processing.old_pk || '0'}</DetailValue>
                          
                          <DetailLabel>New PK:</DetailLabel>
                          <DetailValue>{processing.new_pk || '0'}</DetailValue>
                        </>
                      ) : (
                        <>
                          <DetailLabel>Old Offset:</DetailLabel>
                          <DetailValue>{processing.old_offset || '0'}</DetailValue>
                          
                          <DetailLabel>New Offset:</DetailLabel>
                          <DetailValue>{processing.new_offset || '1000'}</DetailValue>
                        </>
                      )}
                      
                      <DetailLabel>Processed At:</DetailLabel>
                      <DetailValue>{new Date(processing.processed_at).toLocaleString()}</DetailValue>
                    </DetailGrid>
                  </ProcessingDetails>
                </ProcessingItem>
              ))
            )}
          </ProcessingList>

          {pagination.totalPages > 1 && (
            <PaginationContainer>
              <PaginationButton 
                onClick={handlePrevPage} 
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
                    $active={currentPage === pageNum}
                  >
                    {pageNum}
                  </PaginationButton>
                );
              })}
              
              <PaginationButton 
                onClick={handleNextPage} 
                disabled={!pagination.hasNext}
              >
                Next {'[>]'}
              </PaginationButton>
              
              <PaginationInfo>
                Page {pagination.page} of {pagination.totalPages} 
                ({pagination.total} total events)
              </PaginationInfo>
            </PaginationContainer>
          )}
        </>
      )}
    </LiveChangesContainer>
  );
};

export default LiveChanges;
