import { useState, useEffect, useCallback, useRef } from 'react';
import styled from 'styled-components';
import {
  Container,
  Header,
  ErrorMessage,
  LoadingOverlay,
  Button,
} from './shared/BaseComponents';
import { monitorApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { theme } from '../theme/theme';

const CopyButton = styled(Button)`
  margin-top: ${theme.spacing.sm};
`;

const CopySuccess = styled.div`
  color: ${theme.colors.status.success.text};
  font-size: 0.8em;
  margin-top: 5px;
  font-weight: 500;
  animation: fadeIn 0.3s ease-in;
`;

const QueryList = styled.div`
  display: flex;
  flex-direction: column;
  gap: ${theme.spacing.sm};
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

const QueryItem = styled.div`
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

const QuerySummary = styled.div`
  display: grid;
  grid-template-columns: 80px 120px 120px 1fr 100px 100px;
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

const QueryDetails = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '500px' : '0'};
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

const QueryText = styled.pre`
  margin: 0;
  padding: ${theme.spacing.md};
  background-color: ${theme.colors.background.secondary};
  border-radius: ${theme.borderRadius.md};
  overflow-x: auto;
  font-size: 0.9em;
  border: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.2);
    box-shadow: ${theme.shadows.sm};
  }
`;

const QueryState = styled.span<{ $state: string }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.85em;
  font-weight: 500;
  display: inline-block;
  transition: all ${theme.transitions.normal};
  background-color: ${props => {
    switch (props.$state) {
      case 'active': return theme.colors.status.success.bg;
      case 'idle in transaction': return theme.colors.status.warning.bg;
      case 'idle in transaction (aborted)': return theme.colors.status.error.bg;
      default: return theme.colors.background.secondary;
    }
  }};
  color: ${props => {
    switch (props.$state) {
      case 'active': return theme.colors.status.success.text;
      case 'idle in transaction': return theme.colors.status.warning.text;
      case 'idle in transaction (aborted)': return theme.colors.status.error.text;
      default: return theme.colors.text.secondary;
    }
  }};
  
  &:hover {
    transform: scale(1.05);
    box-shadow: ${theme.shadows.sm};
  }
`;

/**
 * Monitor component
 * Displays active database queries with real-time updates and query details
 */
const Monitor = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [queries, setQueries] = useState<any[]>([]);
  const [openQueryId, setOpenQueryId] = useState<number | null>(null);
  const [copiedQueryId, setCopiedQueryId] = useState<number | null>(null);
  const isMountedRef = useRef(true);

  const fetchQueries = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setError(null);
      const data = await monitorApi.getActiveQueries();
      if (isMountedRef.current) {
        setQueries(data || []);
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
  }, []);

  useEffect(() => {
    isMountedRef.current = true;
    fetchQueries();
    const interval = setInterval(() => {
      if (isMountedRef.current) {
        fetchQueries();
      }
    }, 5000);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchQueries]);

  const toggleQuery = useCallback((pid: number) => {
    setOpenQueryId(prev => prev === pid ? null : pid);
  }, []);

  const copyQuery = useCallback(async (query: string, pid: number) => {
    try {
      await navigator.clipboard.writeText(query);
      if (isMountedRef.current) {
        setCopiedQueryId(pid);
        setTimeout(() => {
          if (isMountedRef.current) {
            setCopiedQueryId(null);
          }
        }, 2000);
      }
    } catch (err) {
      console.error('Failed to copy query:', err);
      const textArea = document.createElement('textarea');
      textArea.value = query;
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand('copy');
      document.body.removeChild(textArea);
      if (isMountedRef.current) {
        setCopiedQueryId(pid);
        setTimeout(() => {
          if (isMountedRef.current) {
            setCopiedQueryId(null);
          }
        }, 2000);
      }
    }
  }, []);

  const formatDate = useCallback((dateString: string) => {
    return new Date(dateString).toLocaleString();
  }, []);

  if (loading && queries.length === 0) {
    return (
      <Container>
        <Header>Query Monitor</Header>
        <LoadingOverlay>Loading queries...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>Query Monitor</Header>

      {error && <ErrorMessage>{error}</ErrorMessage>}

      {!loading && !error && (
        <QueryList>
          {queries.length === 0 ? (
            <div style={{ textAlign: 'center', padding: '20px', color: theme.colors.text.secondary }}>
              No active queries found
            </div>
          ) : (
            queries.map((query) => (
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
                    <DetailValue>{formatDate(query.query_start)}</DetailValue>
                    
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
    </Container>
  );
};

export default Monitor;
