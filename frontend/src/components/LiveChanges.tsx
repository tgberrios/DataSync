import { useState, useEffect } from 'react';
import styled from 'styled-components';
import { monitorApi } from '../services/api';

const LiveChangesContainer = styled.div`
  background-color: white;
  color: #333;
  padding: 20px;
  font-family: monospace;
`;

const Header = styled.div`
  border: 2px solid #333;
  padding: 15px;
  text-align: center;
  margin-bottom: 30px;
  font-size: 1.5em;
  font-weight: bold;
  background-color: #f5f5f5;
  border-radius: 4px;
  display: flex;
  justify-content: space-between;
  align-items: center;
`;

const ControlsContainer = styled.div`
  display: flex;
  gap: 15px;
  align-items: center;
`;

const RefreshToggle = styled.button`
  background-color: #f5f5f5;
  color: #333;
  border: 1px solid #ddd;
  padding: 8px 12px;
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.9em;
  font-weight: 500;
  transition: background-color 0.2s ease;
  height: 36px;
  display: flex;
  align-items: center;
  justify-content: center;
  
  &:hover {
    background-color: #e0e0e0;
  }
`;

const SearchInput = styled.input`
  padding: 8px 12px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-size: 0.9em;
  font-family: monospace;
  width: 200px;
  height: 36px;
  
  &:focus {
    outline: none;
    border-color: #4a9eff;
  }
`;

const FilterSelect = styled.select`
  padding: 8px 12px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-size: 0.9em;
  font-family: monospace;
  background-color: white;
  height: 36px;
  
  &:focus {
    outline: none;
    border-color: #4a9eff;
  }
`;

const StatsContainer = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 20px;
  margin-bottom: 30px;
`;

const StatCard = styled.div`
  border: 1px solid #ddd;
  border-radius: 4px;
  padding: 15px;
  background-color: #fafafa;
  text-align: center;
`;

const StatValue = styled.div`
  font-size: 2em;
  font-weight: bold;
  color: #333;
  margin-bottom: 5px;
`;

const StatLabel = styled.div`
  font-size: 0.9em;
  color: #666;
  text-transform: uppercase;
  letter-spacing: 0.5px;
`;

const ProcessingList = styled.div`
  display: flex;
  flex-direction: column;
  gap: 8px;
`;

const ProcessingItem = styled.div`
  border: 1px solid #eee;
  border-radius: 4px;
  background-color: #fafafa;
  overflow: hidden;
  transition: all 0.2s ease;
  
  &:hover {
    border-color: #ddd;
    background-color: #f5f5f5;
  }
`;

const ProcessingSummary = styled.div<{ $showOffset: boolean; $showPK: boolean }>`
  display: grid;
  grid-template-columns: 120px 120px 150px 1fr ${props => 
    (props.$showOffset ? '140px 140px ' : '') + 
    (props.$showPK ? '140px 140px' : '')
  };
  align-items: center;
  padding: 12px 15px;
  cursor: pointer;
  gap: 10px;
  font-size: 0.9em;
  
  &:hover {
    background-color: #f0f0f0;
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
  transition: all 0.3s ease;
  border-top: ${props => props.$isOpen ? '1px solid #eee' : 'none'};
  background-color: white;
  overflow: hidden;
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
  background-color: ${props => props.$active ? '#333' : '#f5f5f5'};
  color: ${props => props.$active ? 'white' : '#333'};
  border: 1px solid #ddd;
  padding: 8px 12px;
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.9em;
  font-weight: 500;
  transition: all 0.2s ease;
  
  &:hover {
    background-color: ${props => props.$active ? '#333' : '#e0e0e0'};
  }
  
  &:disabled {
    background-color: #f0f0f0;
    color: #999;
    cursor: not-allowed;
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
  const [allProcessings, setAllProcessings] = useState<any[]>([]);
  const [stats, setStats] = useState<any>({});
  const [openProcessingId, setOpenProcessingId] = useState<number | null>(null);
  const [isPaused, setIsPaused] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState('ALL');
  const [engineFilter, setEngineFilter] = useState('ALL');
  const [strategyFilter, setStrategyFilter] = useState('ALL');
  const [currentPage, setCurrentPage] = useState(1);
  const [pagination, setPagination] = useState<any>({});
  const itemsPerPage = 20;

  useEffect(() => {
    const fetchData = async () => {
      try {
        setError(null);
        const [processingsResponse, statsData] = await Promise.all([
          monitorApi.getProcessingLogs(1, 1000), // Get all data
          monitorApi.getProcessingStats()
        ]);
        setAllProcessings(processingsResponse.data);
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
  }, [isPaused]);

  // Reset to page 1 when filters change
  useEffect(() => {
    setCurrentPage(1);
  }, [searchTerm, statusFilter, engineFilter, strategyFilter]);

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
    if (currentPage > 1) {
      setCurrentPage(currentPage - 1);
      setOpenProcessingId(null);
    }
  };

  const handleNextPage = () => {
    const totalPages = Math.ceil(filteredProcessings.length / itemsPerPage);
    if (currentPage < totalPages) {
      setCurrentPage(currentPage + 1);
      setOpenProcessingId(null);
    }
  };

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    
    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffMins < 1440) return `${Math.floor(diffMins / 60)}h ago`;
    return date.toLocaleString();
  };

  // Helper function to determine which fields to show based on pk_strategy
  const getFieldVisibility = (pkStrategy: string) => {
    const strategy = pkStrategy || 'PK'; // Default to PK if null/undefined
    const showOffset = strategy === 'OFFSET';
    const showPK = strategy === 'PK' || strategy === 'TEMPORAL_PK';
    return { showOffset, showPK };
  };

  // Filter and search logic
  const filteredProcessings = allProcessings.filter(processing => {
    const matchesSearch = searchTerm === '' || 
      processing.schema_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      processing.table_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      processing.db_engine.toLowerCase().includes(searchTerm.toLowerCase());
    
    const matchesStatus = statusFilter === 'ALL' || processing.status === statusFilter;
    const matchesEngine = engineFilter === 'ALL' || processing.db_engine === engineFilter;
    const matchesStrategy = strategyFilter === 'ALL' || (processing.pk_strategy || 'PK') === strategyFilter;
    
    return matchesSearch && matchesStatus && matchesEngine && matchesStrategy;
  });

  // Pagination logic
  const totalPages = Math.ceil(filteredProcessings.length / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const paginatedProcessings = filteredProcessings.slice(startIndex, endIndex);

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
            <option value="TEMPORAL_PK">TEMPORAL_PK</option>
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
            {paginatedProcessings.length === 0 ? (
              <EmptyState>
                No processing events found
              </EmptyState>
            ) : (
              paginatedProcessings.map((processing) => {
                const { showOffset, showPK } = getFieldVisibility(processing.pk_strategy);
                
                return (
                  <ProcessingItem key={processing.id}>
                    <ProcessingSummary 
                      onClick={() => toggleProcessing(processing.id)}
                      $showOffset={showOffset}
                      $showPK={showPK}
                    >
                      <RegularCell>
                        {formatTimestamp(processing.processed_at)}
                      </RegularCell>
                      <RegularCell>{processing.schema_name}</RegularCell>
                      <RegularCell>{processing.table_name}</RegularCell>
                      <RegularCell>
                        {processing.db_engine} - {processing.status}
                      </RegularCell>
                      {showOffset && (
                        <>
                          <DataCell>
                            <div style={{ fontSize: '0.8em', color: '#666', marginBottom: '2px' }}>Old Offset</div>
                            {processing.old_offset || 0}
                          </DataCell>
                          <DataCell>
                            <div style={{ fontSize: '0.8em', color: '#666', marginBottom: '2px' }}>New Offset</div>
                            {processing.new_offset || 0}
                          </DataCell>
                        </>
                      )}
                      {showPK && (
                        <>
                          <DataCell>
                            <div style={{ fontSize: '0.8em', color: '#666', marginBottom: '2px' }}>
                              {processing.pk_strategy === 'TEMPORAL_PK' ? 'Old Offset' : 'Old PK'}
                            </div>
                            {processing.old_pk || 'N/A'}
                          </DataCell>
                          <DataCell>
                            <div style={{ fontSize: '0.8em', color: '#666', marginBottom: '2px' }}>
                              {processing.pk_strategy === 'TEMPORAL_PK' ? 'New Offset' : 'New PK'}
                            </div>
                            {processing.new_pk || 'N/A'}
                          </DataCell>
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
                      
                      <DetailLabel>Strategy:</DetailLabel>
                      <DetailValue>{processing.pk_strategy || 'PK'}</DetailValue>
                      
                      {showOffset && (
                        <>
                          <DetailLabel>Old Offset:</DetailLabel>
                          <DetailValue>{processing.old_offset || 0}</DetailValue>
                          
                          <DetailLabel>New Offset:</DetailLabel>
                          <DetailValue>{processing.new_offset || 0}</DetailValue>
                        </>
                      )}
                      
                      {showPK && (
                        <>
                          <DetailLabel>Old PK:</DetailLabel>
                          <DetailValue>{processing.old_pk || 'N/A'}</DetailValue>
                          
                          <DetailLabel>New PK:</DetailLabel>
                          <DetailValue>{processing.new_pk || 'N/A'}</DetailValue>
                        </>
                      )}
                      
                      <DetailLabel>Processed At:</DetailLabel>
                      <DetailValue>{new Date(processing.processed_at).toLocaleString()}</DetailValue>
                    </DetailGrid>
                  </ProcessingDetails>
                </ProcessingItem>
                );
              })
            )}
          </ProcessingList>

          {totalPages > 1 && (
            <PaginationContainer>
              <PaginationButton 
                onClick={handlePrevPage} 
                disabled={currentPage <= 1}
              >
                {'[<]'} Prev
              </PaginationButton>
              
              {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
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
                disabled={currentPage >= totalPages}
              >
                Next {'[>]'}
              </PaginationButton>
              
              <PaginationInfo>
                Page {currentPage} of {totalPages} 
                ({filteredProcessings.length} total events)
              </PaginationInfo>
            </PaginationContainer>
          )}
        </>
      )}
    </LiveChangesContainer>
  );
};

export default LiveChanges;
