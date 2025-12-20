import { useState, useEffect, useCallback, useRef } from 'react';
import styled from 'styled-components';
import {
  Container,
  Header,
  ErrorMessage,
  LoadingOverlay,
  Button,
} from './shared/BaseComponents';
import { logsApi, type LogEntry, type LogInfo } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { sanitizeSearch } from '../utils/validation';
import { theme } from '../theme/theme';

const Section = styled.div`
  margin-bottom: ${theme.spacing.xxl};
  padding: ${theme.spacing.lg};
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.md};
  background-color: ${theme.colors.background.secondary};
  box-shadow: ${theme.shadows.sm};
  transition: all ${theme.transitions.normal};
  animation: slideUp 0.25s ease-out;
  animation-fill-mode: both;
  
  &:hover {
    box-shadow: ${theme.shadows.md};
    transform: translateY(-2px);
    border-color: rgba(10, 25, 41, 0.2);
  }
  
  &:nth-child(1) { animation-delay: 0.05s; }
  &:nth-child(2) { animation-delay: 0.1s; }
`;

const SectionTitle = styled.h3`
  margin-bottom: ${theme.spacing.md};
  font-size: 1.2em;
  color: ${theme.colors.text.primary};
  border-bottom: 2px solid ${theme.colors.border.dark};
  padding-bottom: 8px;
  position: relative;
  
  &::after {
    content: '';
    position: absolute;
    bottom: -2px;
    left: 0;
    width: 60px;
    height: 2px;
    background: linear-gradient(90deg, ${theme.colors.primary.main}, ${theme.colors.primary.dark});
    transition: width 0.3s ease;
  }
  
  &:hover::after {
    width: 100%;
  }
`;

const Controls = styled.div`
  display: flex;
  gap: ${theme.spacing.lg};
  margin-bottom: ${theme.spacing.lg};
  padding: ${theme.spacing.lg};
  background-color: ${theme.colors.background.secondary};
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.md};
  flex-wrap: wrap;
  align-items: end;
  box-shadow: ${theme.shadows.sm};
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.08s;
  animation-fill-mode: both;
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
  color: ${theme.colors.text.primary};
  font-family: ${theme.fonts.primary};
`;

const Select = styled.select`
  padding: 8px 12px;
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  background-color: ${theme.colors.background.main};
  font-family: ${theme.fonts.primary};
  font-size: 1em;
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

const Input = styled.input`
  padding: 8px 12px;
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  font-family: ${theme.fonts.primary};
  font-size: 1em;
  width: 100px;
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

const LogsArea = styled.div<{ $isTransitioning?: boolean }>`
  flex: 1;
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  background-color: ${theme.colors.background.main};
  color: ${theme.colors.text.primary};
  overflow-y: auto;
  padding: ${theme.spacing.md};
  font-size: 0.9em;
  line-height: 1.6;
  max-height: 500px;
  box-shadow: ${theme.shadows.sm};
  transition: all ${theme.transitions.normal};
  animation: ${props => props.$isTransitioning ? 'pageTransition 0.2s ease-out' : 'none'};
  
  &:hover {
    box-shadow: ${theme.shadows.md};
  }
`;

const LogLine = styled.div<{ $level: string; $category: string }>`
  margin-bottom: 2px;
  padding: 4px 0;
  border-left: 3px solid ${props => {
    switch (props.$level) {
      case 'ERROR':
      case 'CRITICAL':
        return theme.colors.status.error.text;
      case 'WARNING':
        return theme.colors.status.warning.text;
      case 'INFO':
        return theme.colors.primary.main;
      case 'DEBUG':
        return theme.colors.text.secondary;
      default:
        return theme.colors.text.light;
    }
  }};
  padding-left: 8px;
  position: relative;
  transition: all 0.15s ease;

  &:hover {
    background-color: ${theme.colors.background.secondary};
    transform: translateX(2px);
  }
`;

const LogTimestamp = styled.span`
  color: ${theme.colors.text.secondary};
  margin-right: 10px;
  font-size: 0.9em;
`;

const LogLevel = styled.span<{ $level: string }>`
  font-weight: bold;
  margin-right: 10px;
  color: ${props => {
    switch (props.$level) {
      case 'ERROR':
      case 'CRITICAL':
        return theme.colors.status.error.text;
      case 'WARNING':
        return theme.colors.status.warning.text;
      case 'INFO':
        return theme.colors.primary.main;
      case 'DEBUG':
        return theme.colors.text.secondary;
      default:
        return theme.colors.text.primary;
    }
  }};
`;

const LogFunction = styled.span`
  color: ${theme.colors.text.secondary};
  margin-right: 10px;
  font-size: 0.9em;
`;

const LogCategory = styled.span<{ $category: string }>`
  color: ${theme.colors.text.secondary};
  margin-right: 10px;
  font-size: 0.8em;
  font-weight: 500;
  background-color: ${theme.colors.background.secondary};
  padding: 2px 6px;
  border-radius: ${theme.borderRadius.sm};
`;

const LogMessage = styled.span`
  color: ${theme.colors.text.primary};
`;

const Pagination = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  gap: ${theme.spacing.sm};
  margin-top: ${theme.spacing.md};
  padding: ${theme.spacing.md};
  background-color: ${theme.colors.background.secondary};
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.sm};
`;

const PageButton = styled(Button)`
  padding: 8px 14px;
  font-size: 0.9em;
`;

const PageInfo = styled.span`
  color: ${theme.colors.text.secondary};
  font-family: ${theme.fonts.primary};
  font-size: 0.9em;
`;

const SuccessMessage = styled.div`
  margin-top: ${theme.spacing.md};
  padding: ${theme.spacing.sm} ${theme.spacing.md};
  backgroundColor: ${theme.colors.status.success.bg};
  border: 1px solid ${theme.colors.status.success.text};
  borderRadius: ${theme.borderRadius.sm};
  color: ${theme.colors.status.success.text};
  textAlign: center;
  fontFamily: ${theme.fonts.primary};
  fontSize: 0.9em;
`;

const ModalOverlay = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
  background-color: rgba(0, 0, 0, 0.5);
`;

const ModalContent = styled.div`
  background: ${theme.colors.background.main};
  padding: ${theme.spacing.xxl};
  borderRadius: ${theme.borderRadius.lg};
  border: 2px solid ${theme.colors.border.dark};
  maxWidth: 500px;
  textAlign: center;
  fontFamily: ${theme.fonts.primary};
`;

/**
 * Logs Viewer component
 * Displays and manages application logs with filtering, pagination, and export capabilities
 */
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
  const [func, setFunc] = useState('ALL');
  const [categoriesList, setCategoriesList] = useState<string[]>(['ALL']);
  const [functionsList, setFunctionsList] = useState<string[]>(['ALL']);
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [isClearing, setIsClearing] = useState(false);
  const [showClearDialog, setShowClearDialog] = useState(false);
  const [refreshCountdown, setRefreshCountdown] = useState(5);
  const [isCopying, setIsCopying] = useState(false);
  const [copySuccess, setCopySuccess] = useState(false);
  const [isPageTransitioning, setIsPageTransitioning] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [allLogs, setAllLogs] = useState<LogEntry[]>([]);
  
  const logsEndRef = useRef<HTMLDivElement>(null);
  const countdownIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const isMountedRef = useRef(true);

  const isInitialLoadRef = useRef(true);
  
  const fetchLogs = useCallback(async () => {
    if (!isMountedRef.current) return;
    const isInitialLoad = isInitialLoadRef.current;
    try {
      setIsRefreshing(true);
      setError(null);
      if (isInitialLoad) {
        setLoading(true);
      }
      const sanitizedSearch = sanitizeSearch(search, 200);
      
      const [logsData, infoData] = await Promise.all([
        logsApi.getLogs({ 
          lines, 
          level, 
          category,
          function: func,
          search: sanitizedSearch,
          startDate,
          endDate
        }),
        logsApi.getLogInfo()
      ]);
      
      if (isMountedRef.current) {
        setAllLogs(logsData.logs || []);
        setLogInfo(infoData);
        
        const logsPerPage = 50;
        const totalPages = Math.ceil((logsData.logs || []).length / logsPerPage);
        setTotalPages(totalPages);
        
        const startIndex = (currentPage - 1) * logsPerPage;
        const endIndex = startIndex + logsPerPage;
        setLogs((logsData.logs || []).slice(startIndex, endIndex));
        isInitialLoadRef.current = false;
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    } finally {
      if (isMountedRef.current) {
        setLoading(false);
        setIsRefreshing(false);
      }
    }
  }, [lines, level, category, func, search, startDate, endDate, currentPage]);

  useEffect(() => {
    const loadFilters = async () => {
      try {
        if (!isMountedRef.current) return;
        const [cats, funcs] = await Promise.all([
          logsApi.getCategories(),
          logsApi.getFunctions(),
        ]);
        if (isMountedRef.current) {
          setCategoriesList(['ALL', ...(cats || []).filter(Boolean)]);
          setFunctionsList(['ALL', ...(funcs || []).filter(Boolean)]);
        }
      } catch (err) {
        if (isMountedRef.current) {
          console.error('Error loading filters:', err);
        }
      }
    };
    loadFilters();
  }, []);

  useEffect(() => {
    isMountedRef.current = true;
    fetchLogs();
    return () => {
      isMountedRef.current = false;
    };
  }, [fetchLogs]);

  useEffect(() => {
    if (countdownIntervalRef.current) {
      clearInterval(countdownIntervalRef.current);
      countdownIntervalRef.current = null;
    }

    if (autoRefresh && isMountedRef.current) {
      setRefreshCountdown(5);
      
      countdownIntervalRef.current = setInterval(() => {
        if (isMountedRef.current) {
          setRefreshCountdown(prev => {
            if (prev <= 1) {
              return 5;
            }
            return prev - 1;
          });
        }
      }, 1000);

      const refreshInterval = setInterval(() => {
        if (isMountedRef.current) {
          fetchLogs();
          setCurrentPage(1);
          setRefreshCountdown(5);
        }
      }, 5000);

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
  }, [autoRefresh, fetchLogs]);

  useEffect(() => {
    if (allLogs.length > 0 && isMountedRef.current) {
      const logsPerPage = 50;
      const totalPages = Math.ceil(allLogs.length / logsPerPage);
      setTotalPages(totalPages);
      const startIndex = (currentPage - 1) * logsPerPage;
      const endIndex = startIndex + logsPerPage;
      setLogs(allLogs.slice(startIndex, endIndex));
    }
  }, [allLogs, currentPage]);

  useEffect(() => {
    return () => {
      if (countdownIntervalRef.current) {
        clearInterval(countdownIntervalRef.current);
      }
    };
  }, []);

  const scrollToBottom = useCallback(() => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, []);

  const goToPage = useCallback((pageNum: number) => {
    if (pageNum >= 1 && pageNum <= totalPages && pageNum !== currentPage) {
      setIsPageTransitioning(true);
      setTimeout(() => {
        setCurrentPage(pageNum);
        const logsPerPage = 50;
        const startIndex = (pageNum - 1) * logsPerPage;
        const endIndex = startIndex + logsPerPage;
        setLogs(allLogs.slice(startIndex, endIndex));
        setTimeout(() => setIsPageTransitioning(false), 200);
      }, 50);
    }
  }, [totalPages, currentPage, allLogs]);

  const goToFirstPage = useCallback(() => goToPage(1), [goToPage]);
  const goToLastPage = useCallback(() => goToPage(totalPages), [goToPage, totalPages]);
  const goToPreviousPage = useCallback(() => goToPage(currentPage - 1), [goToPage, currentPage]);
  const goToNextPage = useCallback(() => goToPage(currentPage + 1), [goToPage, currentPage]);

  const handleClearLogs = useCallback(async () => {
    try {
      if (!isMountedRef.current) return;
      setIsClearing(true);
      setError(null);
      
      await logsApi.clearLogs();
      
      if (isMountedRef.current) {
        setShowClearDialog(false);
        setCurrentPage(1);
        await fetchLogs();
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    } finally {
      if (isMountedRef.current) {
        setIsClearing(false);
      }
    }
  }, [fetchLogs]);

  const clearFilters = useCallback(() => {
    setLines(10000);
    setLevel('ALL');
    setCategory('ALL');
    setSearch('');
    setStartDate('');
    setEndDate('');
    setCurrentPage(1);
  }, []);

  const handleCopyAllLogs = useCallback(async () => {
    try {
      if (!isMountedRef.current) return;
      setIsCopying(true);
      setCopySuccess(false);
      
      const allLogsData = await logsApi.getLogs({ 
        lines: 10000, 
        level, 
        category,
        search: sanitizeSearch(search, 200),
        startDate,
        endDate
      });
      
      const logsText = (allLogsData.logs || []).map(log => {
        const timestamp = log.timestamp;
        const levelStr = `[${log.level}]`;
        const funcStr = log.function ? `[${log.function}]` : '';
        const message = log.message;
        return `${timestamp} ${levelStr} ${funcStr} ${message}`.trim();
      }).join('\n');
      
      const header = `DataSync Logs - ${new Date().toLocaleString()}\n` +
                    `Total Entries: ${(allLogsData.logs || []).length}\n` +
                    `Level Filter: ${level}\n` +
                    `Category Filter: ${category}\n` +
                    `File: ${logInfo?.filePath || 'Unknown'}\n` +
                    `Size: ${logInfo ? formatFileSize(logInfo.size || 0) : 'Unknown'}\n` +
                    `Last Modified: ${logInfo?.lastModified ? formatDate(logInfo.lastModified) : 'Unknown'}\n` +
                    `${'='.repeat(80)}\n\n`;
      
      const fullText = header + logsText;
      
      await navigator.clipboard.writeText(fullText);
      
      if (isMountedRef.current) {
        setCopySuccess(true);
        setTimeout(() => {
          if (isMountedRef.current) {
            setCopySuccess(false);
          }
        }, 3000);
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    } finally {
      if (isMountedRef.current) {
        setIsCopying(false);
      }
    }
  }, [level, category, search, startDate, endDate, logInfo]);

  const formatFileSize = useCallback((bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }, []);

  const formatDate = useCallback((dateString: string) => {
    return new Date(dateString).toLocaleString();
  }, []);

  if (loading) {
    return (
      <Container>
        <Header>DataSync Logs Viewer</Header>
        <LoadingOverlay>Loading logs...</LoadingOverlay>
      </Container>
    );
  }

  if (error && logs.length === 0) {
    return (
      <Container>
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
      </Container>
    );
  }

  return (
    <Container>
      <Header>DataSync Logs Viewer</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}
      
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
              {categoriesList.map((c) => (
                <option key={c} value={c}>{c}</option>
              ))}
            </Select>
          </ControlGroup>

          <ControlGroup>
            <Label>Function:</Label>
            <Select value={func} onChange={(e) => setFunc(e.target.value)}>
              {functionsList.map((f) => (
                <option key={f} value={f}>{f}</option>
              ))}
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
                border: `1px solid ${theme.colors.border.medium}`,
                borderRadius: theme.borderRadius.sm,
                backgroundColor: theme.colors.background.secondary,
                textAlign: 'center',
                fontFamily: theme.fonts.primary,
                fontSize: '1em',
                color: theme.colors.text.primary,
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
            style={{ backgroundColor: theme.colors.status.error.bg, color: theme.colors.status.error.text }}
          >
            {isClearing ? 'Clearing...' : 'Clear Logs'}
          </Button>
        </Controls>
        
        {copySuccess && (
          <SuccessMessage>
            ✅ Logs copied to clipboard successfully!
          </SuccessMessage>
        )}
      </Section>

      <Section>
        <SectionTitle>■ LOG ENTRIES (DB)</SectionTitle>
        <LogsArea $isTransitioning={isPageTransitioning}>
          {logs.map((log, index) => (
            <LogLine key={log.id || index} $level={log.level} $category={log.category || 'SYSTEM'}>
              <LogTimestamp>{log.timestamp ? formatDate(log.timestamp) : ''}</LogTimestamp>
              <LogLevel $level={log.level}>[{(log.level || '').toUpperCase()}]</LogLevel>
              {log.category && <LogCategory $category={log.category}>[{(log.category || '').toUpperCase()}]</LogCategory>}
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
                  $variant={currentPage === page ? 'primary' : 'secondary'}
                  onClick={() => goToPage(page)}
                >
                  {page}
                </PageButton>
              );
            })}
            
            {totalPages > 20 && currentPage < totalPages - 9 && (
              <PageInfo style={{ color: theme.colors.text.light, fontSize: '0.8em' }}>
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
              <span style={{ fontSize: '0.9em', color: theme.colors.text.secondary }}>Go to:</span>
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

      {showClearDialog && (
        <ModalOverlay>
          <ModalContent>
            <h3 style={{ marginBottom: '20px', color: theme.colors.status.error.text }}>
              ⚠️ CLEAR LOGS CONFIRMATION
            </h3>
            <p style={{ marginBottom: '25px', lineHeight: '1.5' }}>
              This action will TRUNCATE the database table:
              <br />
              • metadata.logs (all log entries will be removed)
              <br />
              <strong>This operation cannot be undone!</strong>
            </p>
            <div style={{ display: 'flex', gap: '15px', justifyContent: 'center' }}>
              <Button
                onClick={() => setShowClearDialog(false)}
                disabled={isClearing}
                $variant="secondary"
                style={{ backgroundColor: theme.colors.text.secondary, color: theme.colors.text.white }}
              >
                Cancel
              </Button>
              <Button
                onClick={handleClearLogs}
                disabled={isClearing}
                style={{ backgroundColor: theme.colors.status.error.bg, color: theme.colors.status.error.text }}
              >
                {isClearing ? 'Clearing...' : 'Clear Logs'}
              </Button>
            </div>
          </ModalContent>
        </ModalOverlay>
      )}
    </Container>
  );
};

export default LogsViewer;
