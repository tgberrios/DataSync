import { useState, useEffect } from 'react';
import styled from 'styled-components';
import { monitorApi } from '../services/api';

const MonitorContainer = styled.div`
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
`;

const FilterContainer = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 15px;
  margin-bottom: 20px;
  padding: 15px;
  background-color: #f8f8f8;
  border: 1px solid #ddd;
  border-radius: 4px;
`;

const FilterLabel = styled.label`
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.9em;
  color: #333;
  cursor: pointer;
`;

const FilterCheckbox = styled.input`
  width: 16px;
  height: 16px;
  cursor: pointer;
`;

const QueryCount = styled.div`
  font-size: 0.9em;
  color: #666;
  background-color: #e8f5e9;
  padding: 5px 10px;
  border-radius: 3px;
`;

const CopyButton = styled.button`
  background-color: #2196f3;
  color: white;
  border: none;
  padding: 6px 12px;
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.8em;
  font-weight: 500;
  transition: background-color 0.2s ease;
  margin-top: 10px;
  
  &:hover {
    background-color: #1976d2;
  }
  
  &:active {
    background-color: #1565c0;
  }
`;

const CopySuccess = styled.div`
  color: #4caf50;
  font-size: 0.8em;
  margin-top: 5px;
  font-weight: 500;
`;

const QueryList = styled.div`
  display: flex;
  flex-direction: column;
  gap: 8px;
`;

const QueryItem = styled.div`
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

const QuerySummary = styled.div`
  display: grid;
  grid-template-columns: 80px 120px 120px 1fr 100px 100px;
  align-items: center;
  padding: 10px 15px;
  cursor: pointer;
  gap: 10px;
  font-size: 0.9em;
  
  &:hover {
    background-color: #f0f0f0;
  }
`;

const QueryDetails = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '500px' : '0'};
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

const QueryText = styled.pre`
  margin: 0;
  padding: 15px;
  background-color: #f8f8f8;
  border-radius: 4px;
  overflow-x: auto;
  font-size: 0.9em;
  border: 1px solid #eee;
`;

const QueryState = styled.span<{ $state: string }>`
  padding: 3px 8px;
  border-radius: 3px;
  font-size: 0.85em;
  font-weight: 500;
  background-color: ${props => {
    switch (props.$state) {
      case 'active': return '#e8f5e9';
      case 'idle in transaction': return '#fff3e0';
      case 'idle in transaction (aborted)': return '#ffebee';
      default: return '#f5f5f5';
    }
  }};
  color: ${props => {
    switch (props.state) {
      case 'active': return '#2e7d32';
      case 'idle in transaction': return '#ef6c00';
      case 'idle in transaction (aborted)': return '#c62828';
      default: return '#757575';
    }
  }};
`;

const Monitor = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [queries, setQueries] = useState<any[]>([]);
  const [openQueryId, setOpenQueryId] = useState<number | null>(null);
  const [showDataLakeOnly, setShowDataLakeOnly] = useState(true);
  const [copiedQueryId, setCopiedQueryId] = useState<number | null>(null);

  useEffect(() => {
    const fetchQueries = async () => {
      try {
        setError(null);
        const data = await monitorApi.getActiveQueries();
        setQueries(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading queries');
      } finally {
        setLoading(false);
      }
    };

    fetchQueries();
    const interval = setInterval(fetchQueries, 5000);
    return () => clearInterval(interval);
  }, []);

  const toggleQuery = (pid: number) => {
    setOpenQueryId(openQueryId === pid ? null : pid);
  };

  const copyQuery = async (query: string, pid: number) => {
    try {
      await navigator.clipboard.writeText(query);
      setCopiedQueryId(pid);
      // Reset the copied state after 2 seconds
      setTimeout(() => setCopiedQueryId(null), 2000);
    } catch (err) {
      console.error('Failed to copy query:', err);
      // Fallback for older browsers
      const textArea = document.createElement('textarea');
      textArea.value = query;
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand('copy');
      document.body.removeChild(textArea);
      setCopiedQueryId(pid);
      setTimeout(() => setCopiedQueryId(null), 2000);
    }
  };

  // Filtrar queries para mostrar solo DataLake
  const filteredQueries = queries.filter(query => {
    if (!showDataLakeOnly) return true;
    
    // Filtrar solo por nombre de base de datos que sea exactamente "DataLake" o contenga "datalake"
    const isDataLakeDB = query.datname && 
      (query.datname === 'DataLake' || 
       query.datname.toLowerCase().includes('datalake'));
    
    return isDataLakeDB;
  });

  return (
    <MonitorContainer>
      <Header>
        Query Monitor
      </Header>

      {!loading && !error && (
        <FilterContainer>
          <FilterLabel>
            <FilterCheckbox
              type="checkbox"
              checked={showDataLakeOnly}
              onChange={(e) => setShowDataLakeOnly(e.target.checked)}
            />
            Show only DataLake queries
          </FilterLabel>
          <QueryCount>
            {filteredQueries.length} of {queries.length} queries
          </QueryCount>
        </FilterContainer>
      )}

      {loading && (
        <div style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
          Loading queries...
        </div>
      )}

      {error && (
        <div style={{ color: 'red', padding: '20px', textAlign: 'center' }}>
          {error}
        </div>
      )}

      {!loading && !error && (
        <QueryList>
          {filteredQueries.length === 0 ? (
            <div style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
              {showDataLakeOnly 
                ? 'No DataLake queries found' 
                : 'No active queries found'
              }
            </div>
          ) : (
            filteredQueries.map((query) => (
              <QueryItem key={query.pid}>
                <QuerySummary onClick={() => toggleQuery(query.pid)}>
                  <div>PID: {query.pid}</div>
                  <div>{query.usename}</div>
                  <div>{query.datname}</div>
                  <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {query.query?.substring(0, 50)}...
                  </div>
                  <div>{query.duration}</div>
                  <QueryState $state={query.state}>
                    {query.state === 'idle in transaction (aborted)' ? 'aborted' :
                     query.state === 'idle in transaction' ? 'in trans' : 
                     query.state}
                  </QueryState>
                </QuerySummary>
                
                <QueryDetails $isOpen={openQueryId === query.pid}>
                  <DetailGrid>
                    <DetailLabel>Application:</DetailLabel>
                    <DetailValue>{query.application_name || '-'}</DetailValue>
                    
                    <DetailLabel>Client Address:</DetailLabel>
                    <DetailValue>{query.client_addr || '-'}</DetailValue>
                    
                    <DetailLabel>Started At:</DetailLabel>
                    <DetailValue>{new Date(query.query_start).toLocaleString()}</DetailValue>
                    
                    <DetailLabel>Wait Event:</DetailLabel>
                    <DetailValue>
                      {query.wait_event_type ? `${query.wait_event_type} (${query.wait_event})` : 'None'}
                    </DetailValue>
                    
                    <DetailLabel>Full Query:</DetailLabel>
                    <div>
                      <QueryText>{query.query}</QueryText>
                      <CopyButton onClick={() => copyQuery(query.query, query.pid)}>
                        {copiedQueryId === query.pid ? '✓ Copied!' : 'Copy Query'}
                      </CopyButton>
                      {copiedQueryId === query.pid && (
                        <CopySuccess>Query copied to clipboard!</CopySuccess>
                      )}
                    </div>
                  </DetailGrid>
                </QueryDetails>
              </QueryItem>
            ))
          )}
        </QueryList>
      )}
    </MonitorContainer>
  );
};

export default Monitor;