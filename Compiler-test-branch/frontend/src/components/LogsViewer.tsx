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

const LogLine = styled.div<{ level: string; category: string }>`
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
  position: relative;

  &:hover {
    background-color: #f5f5f5;
  }

  &::before {
    content: '';
    position: absolute;
    left: 0;
    top: 0;
    bottom: 0;
    width: 1px;
    background-color: ${props => {
      switch (props.category) {
        case 'DATABASE':
          return '#9c27b0';
        case 'TRANSFER':
          return '#ff9800';
        case 'CONFIG':
          return '#2196f3';
        case 'VALIDATION':
          return '#4caf50';
        case 'MAINTENANCE':
          return '#607d8b';
        case 'MONITORING':
          return '#00bcd4';
        case 'DDL_EXPORT':
          return '#795548';
        case 'METRICS':
          return '#e91e63';
        case 'GOVERNANCE':
          return '#3f51b5';
        case 'QUALITY':
          return '#009688';
        default:
          return 'transparent';
      }
    }};
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

const LogCategory = styled.span<{ category: string }>`
  color: ${props => {
    switch (props.category) {
      case 'DATABASE':
        return '#9c27b0';
      case 'TRANSFER':
        return '#ff9800';
      case 'CONFIG':
        return '#2196f3';
      case 'VALIDATION':
        return '#4caf50';
      case 'MAINTENANCE':
        return '#607d8b';
      case 'MONITORING':
        return '#00bcd4';
      case 'DDL_EXPORT':
        return '#795548';
      case 'METRICS':
        return '#e91e63';
      case 'GOVERNANCE':
        return '#3f51b5';
      case 'QUALITY':
        return '#009688';
      default:
        return '#666';
    }
  }};
  margin-right: 10px;
  font-size: 0.8em;
  font-weight: bold;
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
  const [lines, setLines] = useState(10000);
  const [level, setLevel] = useState('ALL');
  const [category, setCategory] = useState('ALL');
  const [search, setSearch] = useState('');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isClearing, setIsClearing] = useState(false);
  const [showClearDialog, setShowClearDialog] = useState(false);
  const [refreshCountdown, setRefreshCountdown] = useState(5);
  const [isCopying, setIsCopying] = useState(false);
  const [copySuccess, setCopySuccess] = useState(false);
  
  // Pagination state
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [allLogs, setAllLogs] = useState<LogEntry[]>([]);
  
  const logsEndRef = useRef<HTMLDivElement>(null);
  const countdownIntervalRef = useRef<NodeJS.Timeout | null>(null);

  const fetchLogs = async () => {
    try {
      setIsRefreshing(true);
      setError(null);
      
      // Fetch logs with specified number of lines
      const [logsData, infoData] = await Promise.all([
        logsApi.getLogs({ 
          lines, 
          level, 
          category,
          search,
          startDate,
          endDate
        }),
        logsApi.getLogInfo()
      ]);
      
      setAllLogs(logsData.logs);
      setLogInfo(infoData);
      
      // Calculate pagination - Page 1 shows most recent logs
      const logsPerPage = 50;
      const totalPages = Math.ceil(logsData.logs.length / logsPerPage);
      setTotalPages(totalPages);
      
      // Reverse logs so most recent appear first, then paginate
      const reversedLogs = [...logsData.logs].reverse();
      const startIndex = (currentPage - 1) * logsPerPage;
      const endIndex = startIndex + logsPerPage;
      setLogs(reversedLogs.slice(startIndex, endIndex));
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
      // Reverse logs so most recent appear first, then paginate
      const reversedLogs = [...allLogs].reverse();
      const startIndex = (page - 1) * logsPerPage;
      const endIndex = startIndex + logsPerPage;
      setLogs(reversedLogs.slice(startIndex, endIndex));
    }
  };

  const goToFirstPage = () => goToPage(1);
  const goToLastPage = () => goToPage(totalPages);
  const goToPreviousPage = () => goToPage(currentPage - 1);
  const goToNextPage = () => goToPage(currentPage + 1);

  const handleClearLogs = async () => {
    try {
      setIsClearing(true);
      setError(null);
      
      await logsApi.clearLogs();
      
      setShowClearDialog(false);
      setCurrentPage(1);
      await fetchLogs();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error clearing logs');
    } finally {
      setIsClearing(false);
    }
  };

  const clearFilters = () => {
    setLines(10000);
    setLevel('ALL');
    setCategory('ALL');
    setSearch('');
    setStartDate('');
    setEndDate('');
    setCurrentPage(1);
  };

  const handleCopyAllLogs = async () => {
    try {
      setIsCopying(true);
      setCopySuccess(false);
      
      // Get all logs (not just current page)
      const allLogsData = await logsApi.getLogs({ 
        lines: 10000, 
        level, 
        category,
        search,
        startDate,
        endDate
      });
      
      // Format logs for copying
      const logsText = allLogsData.logs.map(log => {
        const timestamp = log.timestamp;
        const level = `[${log.level}]`;
        const func = log.function ? `[${log.function}]` : '';
        const message = log.message;
        return `${timestamp} ${level} ${func} ${message}`.trim();
      }).join('\n');
      
      // Add header information
      const header = `DataSync Logs - ${new Date().toLocaleString()}\n` +
                    `Total Entries: ${allLogsData.logs.length}\n` +
                    `Level Filter: ${level}\n` +
                    `Category Filter: ${category}\n` +
                    `File: ${logInfo?.filePath || 'Unknown'}\n` +
                    `Size: ${logInfo ? formatFileSize(logInfo.size || 0) : 'Unknown'}\n` +
                    `Last Modified: ${logInfo?.lastModified ? formatDate(logInfo.lastModified) : 'Unknown'}\n` +
                    `${'='.repeat(80)}\n\n`;
      
      const fullText = header + logsText;
      
      // Copy to clipboard
      await navigator.clipboard.writeText(fullText);
      
      setCopySuccess(true);
      setTimeout(() => setCopySuccess(false), 3000); // Hide success message after 3 seconds
      
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error copying logs');
    } finally {
      setIsCopying(false);
    }
  };

  useEffect(() => {
    fetchLogs();
  }, [lines, level, category, search, startDate, endDate]);

  useEffect(() => {
    // Clear any existing countdown interval
    if (countdownIntervalRef.current) {
      clearInterval(countdownIntervalRef.current);
      countdownIntervalRef.current = null;
    }

    if (autoRefresh) {
      setRefreshCountdown(5);
      
      // Countdown timer
      countdownIntervalRef.current = setInterval(() => {
        setRefreshCountdown(prev => {
          if (prev <= 1) {
            return 5; // Reset to 5 when it reaches 0
          }
          return prev - 1;
        });
      }, 1000);

      // Auto refresh timer
      const refreshInterval = setInterval(() => {
        fetchLogs();
        // Auto-follow: always go to page 1 (most recent logs) when auto-refreshing
        setCurrentPage(1);
        setRefreshCountdown(5); // Reset countdown after refresh
      }, 5000); // Refresh every 5 seconds

      return () => {
        clearInterval(refreshInterval);
        if (countdownIntervalRef.current) {
          clearInterval(countdownIntervalRef.current);
          countdownIntervalRef.current = null;
        }
      };
    } else {
      setRefreshCountdown(5);
    }
  }, [autoRefresh, lines, level, category, search, startDate, endDate]);

  // Update logs when allLogs or currentPage changes
  useEffect(() => {
    if (allLogs.length > 0) {
      const logsPerPage = 50;
      // Reverse logs so most recent appear first, then paginate
      const reversedLogs = [...allLogs].reverse();
      const startIndex = (currentPage - 1) * logsPerPage;
      const endIndex = startIndex + logsPerPage;
      setLogs(reversedLogs.slice(startIndex, endIndex));
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

  // Cleanup countdown on unmount
  useEffect(() => {
    return () => {
      if (countdownIntervalRef.current) {
        clearInterval(countdownIntervalRef.current);
      }
    };
  }, []);

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
              onChange={(e) => setLines(Math.max(10, parseInt(e.target.value) || 10000))}
              min="10"
              max="100000"
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
            <Label>Category:</Label>
            <Select value={category} onChange={(e) => setCategory(e.target.value)}>
              <option value="ALL">All Categories</option>
              <option value="SYSTEM">SYSTEM</option>
              <option value="DATABASE">DATABASE</option>
              <option value="TRANSFER">TRANSFER</option>
              <option value="CONFIG">CONFIG</option>
              <option value="VALIDATION">VALIDATION</option>
              <option value="MAINTENANCE">MAINTENANCE</option>
              <option value="MONITORING">MONITORING</option>
              <option value="DDL_EXPORT">DDL_EXPORT</option>
              <option value="METRICS">METRICS</option>
              <option value="GOVERNANCE">GOVERNANCE</option>
              <option value="QUALITY">QUALITY</option>
            </Select>
          </ControlGroup>
          
          
          <ControlGroup>
            <Label>Search:</Label>
            <Input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search in logs..."
              style={{ width: '150px' }}
            />
          </ControlGroup>
          
          <ControlGroup>
            <Label>Start Date:</Label>
            <Input
              type="datetime-local"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              style={{ width: '180px' }}
            />
          </ControlGroup>
          
          <ControlGroup>
            <Label>End Date:</Label>
            <Input
              type="datetime-local"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              style={{ width: '180px' }}
            />
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
          
          {autoRefresh && (
            <ControlGroup>
              <Label>Next Refresh:</Label>
              <div style={{
                padding: '8px 12px',
                border: '1px solid #ddd',
                borderRadius: '4px',
                backgroundColor: '#f8f9fa',
                textAlign: 'center',
                fontFamily: 'monospace',
                fontSize: '1em',
                color: '#333',
                minWidth: '60px'
              }}>
                {refreshCountdown}s
              </div>
            </ControlGroup>
          )}
          
          <Button onClick={fetchLogs} disabled={isRefreshing}>
            {isRefreshing ? 'Refreshing...' : 'Refresh Now'}
          </Button>
          
          <Button $variant="secondary" onClick={clearFilters}>
            Clear Filters
          </Button>
          
          <Button $variant="secondary" onClick={scrollToBottom}>
            Scroll to Bottom
          </Button>
          
          <Button $variant="secondary" onClick={() => goToPage(1)}>
            Go to Latest
          </Button>
          
          <Button 
            $variant="secondary" 
            onClick={handleCopyAllLogs}
            disabled={isCopying || isRefreshing}
          >
            {isCopying ? 'Copying...' : 'Copy Logs'}
          </Button>
          
          <Button 
            $variant="secondary" 
            onClick={() => setShowClearDialog(true)}
            disabled={isClearing || isRefreshing}
            style={{ backgroundColor: '#ff6b6b', color: 'white' }}
          >
            {isClearing ? 'Clearing...' : 'Clear Logs'}
          </Button>
        </Controls>
        
        {copySuccess && (
          <div style={{
            marginTop: '15px',
            padding: '10px 15px',
            backgroundColor: '#e8f5e9',
            border: '1px solid #4caf50',
            borderRadius: '4px',
            color: '#2e7d32',
            textAlign: 'center',
            fontFamily: 'monospace',
            fontSize: '0.9em'
          }}>
            ✅ Logs copied to clipboard successfully!
          </div>
        )}
      </Section>

      <Section>
        <SectionTitle>■ LOG ENTRIES</SectionTitle>
        <LogsArea>
          {logs.map((log, index) => (
            <LogLine key={log.id || index} level={log.level} category={log.category || 'SYSTEM'}>
              <LogTimestamp>{log.timestamp}</LogTimestamp>
              <LogLevel level={log.level}>[{log.level}]</LogLevel>
              {log.category && <LogCategory category={log.category}>[{log.category}]</LogCategory>}
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
            
            {Array.from({ length: Math.min(20, totalPages) }, (_, i) => {
              const startPage = Math.max(1, currentPage - 9);
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
            
            {totalPages > 20 && currentPage < totalPages - 9 && (
              <PageInfo style={{ color: '#999', fontSize: '0.8em' }}>
                ...
              </PageInfo>
            )}
            
            <PageButton onClick={goToNextPage} disabled={currentPage === totalPages}>
              »
            </PageButton>
            <PageButton onClick={goToLastPage} disabled={currentPage === totalPages}>
              »»
            </PageButton>
            
            <PageInfo>
              Page {currentPage} of {totalPages}
            </PageInfo>
            
            <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
              <span style={{ fontSize: '0.9em', color: '#666' }}>Go to:</span>
              <Input
                type="number"
                min="1"
                max={totalPages}
                style={{ width: '60px', padding: '4px 8px', fontSize: '0.9em' }}
                onKeyPress={(e) => {
                  if (e.key === 'Enter') {
                    const targetPage = parseInt((e.target as HTMLInputElement).value);
                    if (targetPage >= 1 && targetPage <= totalPages) {
                      goToPage(targetPage);
                      (e.target as HTMLInputElement).value = '';
                    }
                  }
                }}
                placeholder={currentPage.toString()}
              />
            </div>
          </Pagination>
        )}
      </Section>

      <Section>
        <SectionTitle>■ LOG FILE STATUS</SectionTitle>
        <StatusBar>
          <StatusItem>
            Showing {logs.length} of {logInfo.totalLines} log entries (Page {currentPage} - Most Recent First)
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
            Auto refresh: {autoRefresh ? `ON (Next: ${refreshCountdown}s)` : 'OFF'}
          </StatusItem>
        </StatusBar>
      </Section>

      {showClearDialog && (
        <div style={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          backgroundColor: 'rgba(0, 0, 0, 0.5)',
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          zIndex: 1000
        }}>
          <div style={{
            backgroundColor: 'white',
            padding: '30px',
            borderRadius: '8px',
            border: '2px solid #333',
            maxWidth: '500px',
            textAlign: 'center',
            fontFamily: 'monospace'
          }}>
            <h3 style={{ marginBottom: '20px', color: '#ff6b6b' }}>
              ⚠️ CLEAR LOGS CONFIRMATION
            </h3>
            <p style={{ marginBottom: '25px', lineHeight: '1.5' }}>
              This action will permanently delete:
              <br />
              • All entries from DataSync.log file
              <br />
              • All rotated log files (DataSync.log.1, .2, .3, etc.)
              <br />
              <strong>This operation cannot be undone!</strong>
            </p>
            <div style={{ display: 'flex', gap: '15px', justifyContent: 'center' }}>
              <Button
                onClick={() => setShowClearDialog(false)}
                disabled={isClearing}
                style={{ backgroundColor: '#666', color: 'white' }}
              >
                Cancel
              </Button>
              <Button
                onClick={handleClearLogs}
                disabled={isClearing}
                style={{ backgroundColor: '#ff6b6b', color: 'white' }}
              >
                {isClearing ? 'Clearing...' : 'Clear Logs'}
              </Button>
            </div>
          </div>
        </div>
      )}
    </LogsContainer>
  );
};

export default LogsViewer;
