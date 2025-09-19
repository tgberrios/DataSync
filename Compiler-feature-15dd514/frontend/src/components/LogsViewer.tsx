import { useState, useEffect, useRef } from 'react';
import styled from 'styled-components';
import { logsApi, type LogEntry, type LogInfo } from '../services/api';

const LogsContainer = styled.div`
  background-color: white;
  color: #333;
  padding: 20px;
  font-family: monospace;
  box-sizing: border-box;
  height: 100vh;
  display: flex;
  flex-direction: column;
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

const Section = styled.div`
  margin-bottom: 30px;
  padding: 20px;
  border: 1px solid #eee;
  border-radius: 4px;
  background-color: #fafafa;
`;

const SectionTitle = styled.h3`
  margin-bottom: 15px;
  font-size: 1.2em;
  color: #222;
  border-bottom: 2px solid #333;
  padding-bottom: 8px;
`;

const Controls = styled.div`
  display: flex;
  gap: 20px;
  margin-bottom: 20px;
  padding: 20px;
  background-color: #fafafa;
  border: 1px solid #eee;
  border-radius: 4px;
  flex-wrap: wrap;
  align-items: end;
`;

const ControlGroup = styled.div`
  display: flex;
  flex-direction: column;
  gap: 8px;
  min-width: 120px;
`;

const Label = styled.label`
  font-size: 1em;
  font-weight: bold;
  color: #333;
  font-family: monospace;
`;

const Select = styled.select`
  padding: 8px 12px;
  border: 1px solid #ddd;
  border-radius: 4px;
  background-color: white;
  font-family: monospace;
  font-size: 1em;

  &:focus {
    outline: none;
    border-color: #666;
  }
`;

const Input = styled.input`
  padding: 8px 12px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-family: monospace;
  font-size: 1em;
  width: 100px;

  &:focus {
    outline: none;
    border-color: #666;
  }
`;

const Button = styled.button<{ $variant?: 'primary' | 'secondary' }>`
  padding: 8px 16px;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-family: monospace;
  font-size: 1em;
  background-color: ${props => props.$variant === 'secondary' ? '#e8f5e9' : '#333'};
  color: ${props => props.$variant === 'secondary' ? '#2e7d32' : 'white'};
  height: fit-content;

  &:hover {
    background-color: ${props => props.$variant === 'secondary' ? '#c8e6c9' : '#555'};
  }

  &:disabled {
    background-color: #ccc;
    cursor: not-allowed;
  }
`;

const LogsArea = styled.div`
  flex: 1;
  border: 1px solid #ddd;
  border-radius: 4px;
  background-color: white;
  color: #333;
  overflow-y: auto;
  padding: 15px;
  font-size: 0.9em;
  line-height: 1.4;
  max-height: 500px;
`;

const LogLine = styled.div<{ level: string }>`
  margin-bottom: 2px;
  padding: 2px 0;
  border-left: 3px solid ${props => {
    switch (props.level) {
      case 'ERROR':
      case 'CRITICAL':
        return '#ff6b6b';
      case 'WARNING':
        return '#ffa726';
      case 'INFO':
        return '#42a5f5';
      case 'DEBUG':
        return '#66bb6a';
      default:
        return '#666';
    }
  }};
  padding-left: 8px;

  &:hover {
    background-color: #f5f5f5;
  }
`;

const LogTimestamp = styled.span`
  color: #666;
  margin-right: 10px;
  font-size: 0.9em;
`;

const LogLevel = styled.span<{ level: string }>`
  font-weight: bold;
  margin-right: 10px;
  color: ${props => {
    switch (props.level) {
      case 'ERROR':
      case 'CRITICAL':
        return '#d32f2f';
      case 'WARNING':
        return '#f57c00';
      case 'INFO':
        return '#1976d2';
      case 'DEBUG':
        return '#388e3c';
      default:
        return '#333';
    }
  }};
`;

const LogFunction = styled.span`
  color: #7b1fa2;
  margin-right: 10px;
  font-size: 0.9em;
`;

const LogMessage = styled.span`
  color: #333;
`;

const StatusBar = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 15px;
  padding: 15px;
  background-color: #f5f5f5;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-size: 1em;
`;

const StatusItem = styled.div`
  color: #333;
  font-family: monospace;
  padding: 8px;
  background-color: white;
  border: 1px solid #eee;
  border-radius: 4px;
  text-align: center;
`;

const ErrorMessage = styled.div`
  color: #ff6b6b;
  padding: 20px;
  text-align: center;
  border: 1px solid #ff6b6b;
  border-radius: 4px;
  margin: 20px;
  background-color: #fff5f5;
`;

const LoadingMessage = styled.div`
  color: #666;
  padding: 20px;
  text-align: center;
`;

const Pagination = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 10px;
  margin-top: 15px;
  padding: 15px;
  background-color: #fafafa;
  border: 1px solid #eee;
  border-radius: 4px;
`;

const PageButton = styled.button<{ $active?: boolean }>`
  padding: 8px 12px;
  border: 1px solid #ddd;
  border-radius: 4px;
  background-color: ${props => props.$active ? '#333' : 'white'};
  color: ${props => props.$active ? 'white' : '#333'};
  cursor: pointer;
  font-family: monospace;
  font-size: 0.9em;

  &:hover {
    background-color: ${props => props.$active ? '#555' : '#f5f5f5'};
  }

  &:disabled {
    background-color: #f5f5f5;
    color: #ccc;
    cursor: not-allowed;
  }
`;

const PageInfo = styled.span`
  color: #666;
  font-family: monospace;
  font-size: 0.9em;
`;

const LogsViewer = () => {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [logInfo, setLogInfo] = useState<LogInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [lines, setLines] = useState(100);
  const [level, setLevel] = useState('ALL');
  const [isRefreshing, setIsRefreshing] = useState(false);
  
  // Pagination state
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [allLogs, setAllLogs] = useState<LogEntry[]>([]);
  
  const logsEndRef = useRef<HTMLDivElement>(null);

  const fetchLogs = async () => {
    try {
      setIsRefreshing(true);
      setError(null);
      
      // Fetch more logs for pagination (get 1000 lines)
      const [logsData, infoData] = await Promise.all([
        logsApi.getLogs({ lines: 1000, level }),
        logsApi.getLogInfo()
      ]);
      
      setAllLogs(logsData.logs);
      setLogInfo(infoData);
      
      // Calculate pagination
      const logsPerPage = 50;
      const totalPages = Math.ceil(logsData.logs.length / logsPerPage);
      setTotalPages(totalPages);
      
      // Set current page logs
      const startIndex = (currentPage - 1) * logsPerPage;
      const endIndex = startIndex + logsPerPage;
      setLogs(logsData.logs.slice(startIndex, endIndex));
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error loading logs');
    } finally {
      setLoading(false);
      setIsRefreshing(false);
    }
  };

  const scrollToBottom = () => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  const goToPage = (page: number) => {
    if (page >= 1 && page <= totalPages) {
      setCurrentPage(page);
      const logsPerPage = 50;
      const startIndex = (page - 1) * logsPerPage;
      const endIndex = startIndex + logsPerPage;
      setLogs(allLogs.slice(startIndex, endIndex));
    }
  };

  const goToFirstPage = () => goToPage(1);
  const goToLastPage = () => goToPage(totalPages);
  const goToPreviousPage = () => goToPage(currentPage - 1);
  const goToNextPage = () => goToPage(currentPage + 1);

  useEffect(() => {
    fetchLogs();
  }, [lines, level]);

  useEffect(() => {
    if (autoRefresh) {
      const interval = setInterval(fetchLogs, 5000); // Refresh every 5 seconds
      return () => clearInterval(interval);
    }
  }, [autoRefresh, lines, level]);

  // Update logs when allLogs or currentPage changes
  useEffect(() => {
    if (allLogs.length > 0) {
      const logsPerPage = 50;
      const startIndex = (currentPage - 1) * logsPerPage;
      const endIndex = startIndex + logsPerPage;
      setLogs(allLogs.slice(startIndex, endIndex));
    }
  }, [allLogs, currentPage]);

  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString();
  };

  if (loading) {
    return (
      <LogsContainer>
        <Header>DataSync Logs Viewer</Header>
        <LoadingMessage>Loading logs...</LoadingMessage>
      </LogsContainer>
    );
  }

  if (error) {
    return (
      <LogsContainer>
        <Header>DataSync Logs Viewer</Header>
        <ErrorMessage>
          <div style={{ fontWeight: 'bold', marginBottom: '10px' }}>Error loading logs:</div>
          <div>{error}</div>
          <Button 
            onClick={fetchLogs}
            style={{ marginTop: '10px' }}
          >
            Retry
          </Button>
        </ErrorMessage>
      </LogsContainer>
    );
  }

  if (!logInfo?.exists) {
    return (
      <LogsContainer>
        <Header>DataSync Logs Viewer</Header>
        <ErrorMessage>
          <div style={{ fontWeight: 'bold', marginBottom: '10px' }}>No log file found</div>
          <div>Make sure the DataSync application is running and generating logs.</div>
        </ErrorMessage>
      </LogsContainer>
    );
  }

  return (
    <LogsContainer>
      <Header>DataSync Logs Viewer</Header>
      
      <Section>
        <SectionTitle>⚙ LOG CONTROLS</SectionTitle>
        <Controls>
          <ControlGroup>
            <Label>Lines to show:</Label>
            <Input
              type="number"
              value={lines}
              onChange={(e) => setLines(Math.max(10, parseInt(e.target.value) || 100))}
              min="10"
              max="10000"
            />
          </ControlGroup>
          
          <ControlGroup>
            <Label>Log Level:</Label>
            <Select value={level} onChange={(e) => setLevel(e.target.value)}>
              <option value="ALL">All Levels</option>
              <option value="DEBUG">DEBUG</option>
              <option value="INFO">INFO</option>
              <option value="WARNING">WARNING</option>
              <option value="ERROR">ERROR</option>
              <option value="CRITICAL">CRITICAL</option>
            </Select>
          </ControlGroup>
          
          <ControlGroup>
            <Label>Auto Refresh:</Label>
            <Button
              $variant={autoRefresh ? 'secondary' : 'primary'}
              onClick={() => setAutoRefresh(!autoRefresh)}
              style={{ width: '120px' }}
            >
              {autoRefresh ? 'ON' : 'OFF'}
            </Button>
          </ControlGroup>
          
          <Button onClick={fetchLogs} disabled={isRefreshing}>
            {isRefreshing ? 'Refreshing...' : 'Refresh Now'}
          </Button>
          
          <Button $variant="secondary" onClick={scrollToBottom}>
            Scroll to Bottom
          </Button>
        </Controls>
      </Section>

      <Section>
        <SectionTitle>■ LOG ENTRIES</SectionTitle>
        <LogsArea>
          {logs.map((log) => (
            <LogLine key={log.id} level={log.level}>
              <LogTimestamp>{log.timestamp}</LogTimestamp>
              <LogLevel level={log.level}>[{log.level}]</LogLevel>
              {log.function && <LogFunction>[{log.function}]</LogFunction>}
              <LogMessage>{log.message}</LogMessage>
            </LogLine>
          ))}
          <div ref={logsEndRef} />
        </LogsArea>
        
        {totalPages > 1 && (
          <Pagination>
            <PageButton onClick={goToFirstPage} disabled={currentPage === 1}>
              ««
            </PageButton>
            <PageButton onClick={goToPreviousPage} disabled={currentPage === 1}>
              «
            </PageButton>
            
            {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
              const startPage = Math.max(1, currentPage - 2);
              const page = startPage + i;
              if (page > totalPages) return null;
              
              return (
                <PageButton
                  key={page}
                  $active={currentPage === page}
                  onClick={() => goToPage(page)}
                >
                  {page}
                </PageButton>
              );
            })}
            
            <PageButton onClick={goToNextPage} disabled={currentPage === totalPages}>
              »
            </PageButton>
            <PageButton onClick={goToLastPage} disabled={currentPage === totalPages}>
              »»
            </PageButton>
            
            <PageInfo>
              Page {currentPage} of {totalPages}
            </PageInfo>
          </Pagination>
        )}
      </Section>

      <Section>
        <SectionTitle>■ LOG FILE STATUS</SectionTitle>
        <StatusBar>
          <StatusItem>
            Showing {logs.length} of {logInfo.totalLines} log entries
          </StatusItem>
          <StatusItem>
            File: {logInfo.filePath}
          </StatusItem>
          <StatusItem>
            Size: {formatFileSize(logInfo.size || 0)}
          </StatusItem>
          <StatusItem>
            Last modified: {logInfo.lastModified ? formatDate(logInfo.lastModified) : 'Unknown'}
          </StatusItem>
          <StatusItem>
            Auto refresh: {autoRefresh ? 'ON' : 'OFF'}
          </StatusItem>
        </StatusBar>
      </Section>
    </LogsContainer>
  );
};

export default LogsViewer;
