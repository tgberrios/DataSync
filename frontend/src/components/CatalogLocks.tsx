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
import { catalogLocksApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { theme } from '../theme/theme';

const MetricsGrid = styled(Grid)`
  margin-bottom: ${theme.spacing.xxl};
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

const MetricCard = styled(Value)`
  padding: ${theme.spacing.md};
  min-height: 80px;
`;

const MetricLabel = styled.div`
  font-size: 0.85em;
  color: ${theme.colors.text.secondary};
  margin-bottom: ${theme.spacing.xs};
  font-weight: 500;
`;

const MetricValue = styled.div`
  font-size: 1.5em;
  font-weight: bold;
  color: ${theme.colors.text.primary};
`;

const ActionBar = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: ${theme.spacing.lg};
  gap: ${theme.spacing.sm};
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.15s;
  animation-fill-mode: both;
`;

const DangerButton = styled(Button)`
  background-color: ${theme.colors.status.error.bg};
  color: ${theme.colors.status.error.text};
  border-color: ${theme.colors.status.error.text};
  
  &:hover:not(:disabled) {
    background-color: #ffcdd2;
    border-color: ${theme.colors.status.error.text};
  }
`;

const LocksTable = styled.div`
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  overflow: hidden;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const TableHeader = styled.div`
  display: grid;
  grid-template-columns: 200px 150px 150px 150px 100px 1fr 120px;
  background: ${theme.colors.gradient.primary};
  padding: 12px 15px;
  font-weight: bold;
  font-size: 0.85em;
  border-bottom: 2px solid ${theme.colors.border.dark};
  gap: 10px;
`;

const TableRow = styled.div<{ $expired?: boolean; $warning?: boolean }>`
  display: grid;
  grid-template-columns: 200px 150px 150px 150px 100px 1fr 120px;
  padding: 12px 15px;
  border-bottom: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  gap: 10px;
  align-items: center;
  font-size: 0.85em;
  background-color: ${props => {
    if (props.$expired) return theme.colors.status.error.bg;
    if (props.$warning) return theme.colors.status.warning.bg;
    return theme.colors.background.main;
  }};
  
  &:hover {
    background: ${props => {
      if (props.$expired) return '#ffcdd2';
      if (props.$warning) return '#ffe0b2';
      return theme.colors.background.secondary;
    }};
    border-left: 3px solid ${theme.colors.primary.main};
  }
  
  &:last-child {
    border-bottom: none;
  }
`;

const TableCell = styled.div`
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`;

const Badge = styled.span<{ $status?: string }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.75em;
  font-weight: 500;
  display: inline-block;
  transition: all ${theme.transitions.normal};
  
  ${props => {
    switch (props.$status) {
      case 'active': return `background-color: ${theme.colors.status.success.bg}; color: ${theme.colors.status.success.text};`;
      case 'expired': return `background-color: ${theme.colors.status.error.bg}; color: ${theme.colors.status.error.text};`;
      case 'warning': return `background-color: ${theme.colors.status.warning.bg}; color: ${theme.colors.status.warning.text};`;
      default: return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.secondary};`;
    }
  }}
  
  &:hover {
    transform: scale(1.05);
    box-shadow: ${theme.shadows.sm};
  }
`;

const UnlockButton = styled(Button)`
  padding: 6px 12px;
  font-size: 0.8em;
  background-color: ${theme.colors.status.error.bg};
  color: ${theme.colors.status.error.text};
  border-color: ${theme.colors.status.error.text};
  
  &:hover:not(:disabled) {
    background-color: #ffcdd2;
    border-color: ${theme.colors.status.error.text};
  }
`;

const SuccessMessage = styled.div`
  background-color: ${theme.colors.status.success.bg};
  color: ${theme.colors.status.success.text};
  padding: ${theme.spacing.md};
  border-radius: ${theme.borderRadius.md};
  margin-bottom: ${theme.spacing.lg};
  border: 1px solid ${theme.colors.status.success.text};
`;

/**
 * Catalog Locks Monitor component
 * Displays and manages database locks used to prevent race conditions during catalog operations
 */
const CatalogLocks = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [locks, setLocks] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const isMountedRef = useRef(true);

  const fetchData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      setSuccess(null);
      const [locksData, metricsData] = await Promise.all([
        catalogLocksApi.getLocks(),
        catalogLocksApi.getMetrics()
      ]);
      if (isMountedRef.current) {
        setLocks(locksData || []);
        setMetrics(metricsData || {});
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
    fetchData();
    const interval = setInterval(() => {
      if (isMountedRef.current) {
        fetchData();
      }
    }, 5000);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchData]);

  const handleUnlock = useCallback(async (lockName: string) => {
    if (!confirm(`Are you sure you want to force unlock "${lockName}"? This may interrupt operations.`)) {
      return;
    }

    try {
      if (!isMountedRef.current) return;
      setError(null);
      setSuccess(null);
      await catalogLocksApi.unlock(lockName);
      if (isMountedRef.current) {
        setSuccess(`Lock "${lockName}" has been released successfully`);
        await fetchData();
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [fetchData]);

  const handleCleanExpired = useCallback(async () => {
    try {
      if (!isMountedRef.current) return;
      setError(null);
      setSuccess(null);
      const result = await catalogLocksApi.cleanExpired();
      if (isMountedRef.current) {
        setSuccess(result.message || 'Expired locks cleaned successfully');
        await fetchData();
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [fetchData]);

  const getLockStatus = useCallback((expiresAt: string) => {
    if (!expiresAt) return { status: 'unknown', label: 'Unknown' };
    const expires = new Date(expiresAt);
    const now = new Date();
    const diffMs = expires.getTime() - now.getTime();
    const diffMins = diffMs / (1000 * 60);

    if (diffMs < 0) {
      return { status: 'expired', label: 'Expired' };
    } else if (diffMins < 5) {
      return { status: 'warning', label: 'Expiring Soon' };
    } else {
      return { status: 'active', label: 'Active' };
    }
  }, []);

  const formatTimeRemaining = useCallback((expiresAt: string) => {
    if (!expiresAt) return 'N/A';
    const expires = new Date(expiresAt);
    const now = new Date();
    const diffMs = expires.getTime() - now.getTime();

    if (diffMs < 0) {
      const expiredMs = Math.abs(diffMs);
      const expiredMins = Math.floor(expiredMs / (1000 * 60));
      return `Expired ${expiredMins}m ago`;
    }

    const mins = Math.floor(diffMs / (1000 * 60));
    const hours = Math.floor(mins / 60);
    const days = Math.floor(hours / 24);

    if (days > 0) return `${days}d ${hours % 24}h`;
    if (hours > 0) return `${hours}h ${mins % 60}m`;
    return `${mins}m`;
  }, []);

  const formatDate = useCallback((date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  }, []);

  if (loading && locks.length === 0) {
    return (
      <Container>
        <Header>Catalog Locks Monitor</Header>
        <LoadingOverlay>Loading catalog locks...</LoadingOverlay>
      </Container>
    );
  }

  const expiredLocks = locks.filter(lock => {
    if (!lock.expires_at) return false;
    return new Date(lock.expires_at) < new Date();
  });

  return (
    <Container>
      <Header>Catalog Locks Monitor</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}
      {success && <SuccessMessage>{success}</SuccessMessage>}
      
      <MetricsGrid $columns="repeat(auto-fit, minmax(200px, 1fr))">
        <MetricCard>
          <MetricLabel>Total Locks</MetricLabel>
          <MetricValue>{metrics.total_locks || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Active Locks</MetricLabel>
          <MetricValue>{metrics.active_locks || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Expired Locks</MetricLabel>
          <MetricValue>{metrics.expired_locks || 0}</MetricValue>
        </MetricCard>
        <MetricCard>
          <MetricLabel>Unique Hosts</MetricLabel>
          <MetricValue>{metrics.unique_hosts || 0}</MetricValue>
        </MetricCard>
      </MetricsGrid>

      <ActionBar>
        <div style={{ fontSize: '0.9em', color: theme.colors.text.secondary }}>
          Locks are used to prevent race conditions during catalog operations. Expired locks are automatically cleaned.
        </div>
        <DangerButton onClick={handleCleanExpired} disabled={expiredLocks.length === 0}>
          Clean Expired ({expiredLocks.length})
        </DangerButton>
      </ActionBar>

      <LocksTable>
        <TableHeader>
          <TableCell>Lock Name</TableCell>
          <TableCell>Acquired By</TableCell>
          <TableCell>Acquired At</TableCell>
          <TableCell>Expires At</TableCell>
          <TableCell>Status</TableCell>
          <TableCell>Time Remaining</TableCell>
          <TableCell>Actions</TableCell>
        </TableHeader>
        {locks.length === 0 ? (
          <div style={{ padding: '40px', textAlign: 'center', color: theme.colors.text.secondary }}>
            No active locks. Locks will appear here when catalog operations are running.
          </div>
        ) : (
          locks.map((lock) => {
            const status = getLockStatus(lock.expires_at);
            const isExpired = status.status === 'expired';
            const isWarning = status.status === 'warning';
            
            return (
              <TableRow key={lock.lock_name} $expired={isExpired} $warning={isWarning}>
                <TableCell>
                  <strong>{lock.lock_name}</strong>
                </TableCell>
                <TableCell>{lock.acquired_by || 'N/A'}</TableCell>
                <TableCell>{formatDate(lock.acquired_at)}</TableCell>
                <TableCell>{formatDate(lock.expires_at)}</TableCell>
                <TableCell>
                  <Badge $status={status.status}>{status.label}</Badge>
                </TableCell>
                <TableCell>{formatTimeRemaining(lock.expires_at)}</TableCell>
                <TableCell>
                  <UnlockButton onClick={() => handleUnlock(lock.lock_name)}>
                    Force Unlock
                  </UnlockButton>
                </TableCell>
              </TableRow>
            );
          })
        )}
      </LocksTable>
    </Container>
  );
};

export default CatalogLocks;
