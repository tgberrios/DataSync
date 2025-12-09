import { useState, useEffect } from 'react';
import styled from 'styled-components';
import { catalogLocksApi } from '../services/api';

const CatalogLocksContainer = styled.div`
  background-color: white;
  color: #333;
  padding: 20px;
  font-family: monospace;
  animation: fadeIn 0.25s ease-in;
`;

const Header = styled.div`
  border: 2px solid #333;
  padding: 15px;
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

const MetricsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 15px;
  margin-bottom: 30px;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

const MetricCard = styled.div`
  border: 1px solid #ddd;
  border-radius: 6px;
  padding: 15px;
  background: linear-gradient(135deg, #fafafa 0%, #ffffff 100%);
  transition: all 0.2s ease;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.03);
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.3);
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
  }
`;

const MetricLabel = styled.div`
  font-size: 0.85em;
  color: #666;
  margin-bottom: 8px;
  font-weight: 500;
`;

const MetricValue = styled.div`
  font-size: 1.5em;
  font-weight: bold;
  color: #0d1b2a;
`;

const ActionBar = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  gap: 10px;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.15s;
  animation-fill-mode: both;
`;

const ActionButton = styled.button<{ $danger?: boolean }>`
  padding: 10px 20px;
  border: 1px solid ${props => props.$danger ? '#c62828' : '#0d1b2a'};
  border-radius: 6px;
  background: ${props => props.$danger ? '#ffebee' : '#0d1b2a'};
  color: ${props => props.$danger ? '#c62828' : 'white'};
  cursor: pointer;
  font-family: monospace;
  font-size: 0.9em;
  font-weight: 500;
  transition: all 0.2s ease;
  
  &:hover {
    background: ${props => props.$danger ? '#ffcdd2' : '#1e3a5f'};
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  }
  
  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
    transform: none;
  }
`;

const LocksTable = styled.div`
  border: 1px solid #ddd;
  border-radius: 6px;
  overflow: hidden;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const TableHeader = styled.div`
  display: grid;
  grid-template-columns: 200px 150px 150px 150px 100px 1fr 120px;
  background: linear-gradient(135deg, #f5f5f5 0%, #ffffff 100%);
  padding: 12px 15px;
  font-weight: bold;
  font-size: 0.85em;
  border-bottom: 2px solid #ddd;
  gap: 10px;
`;

const TableRow = styled.div<{ $expired?: boolean; $warning?: boolean }>`
  display: grid;
  grid-template-columns: 200px 150px 150px 150px 100px 1fr 120px;
  padding: 12px 15px;
  border-bottom: 1px solid #eee;
  transition: all 0.2s ease;
  gap: 10px;
  align-items: center;
  font-size: 0.85em;
  background-color: ${props => {
    if (props.$expired) return '#ffebee';
    if (props.$warning) return '#fff3e0';
    return 'white';
  }};
  
  &:hover {
    background: ${props => {
      if (props.$expired) return '#ffcdd2';
      if (props.$warning) return '#ffe0b2';
      return '#f0f0f0';
    }};
    border-left: 3px solid #0d1b2a;
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
  border-radius: 6px;
  font-size: 0.75em;
  font-weight: 500;
  display: inline-block;
  transition: all 0.2s ease;
  
  ${props => {
    switch (props.$status) {
      case 'active': return 'background-color: #e8f5e9; color: #2e7d32;';
      case 'expired': return 'background-color: #ffebee; color: #c62828;';
      case 'warning': return 'background-color: #fff3e0; color: #ef6c00;';
      default: return 'background-color: #f5f5f5; color: #757575;';
    }
  }}
  
  &:hover {
    transform: scale(1.05);
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
  }
`;

const UnlockButton = styled.button`
  padding: 6px 12px;
  border: 1px solid #c62828;
  border-radius: 6px;
  background: #ffebee;
  color: #c62828;
  cursor: pointer;
  font-family: monospace;
  font-size: 0.8em;
  font-weight: 500;
  transition: all 0.2s ease;
  
  &:hover {
    background: #ffcdd2;
    transform: translateY(-2px);
    box-shadow: 0 2px 8px rgba(198, 40, 40, 0.3);
  }
  
  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
    transform: none;
  }
`;

const Loading = styled.div`
  text-align: center;
  padding: 40px;
  color: #666;
  font-size: 1.1em;
`;

const Error = styled.div`
  background-color: #ffebee;
  color: #c62828;
  padding: 15px;
  border-radius: 6px;
  margin-bottom: 20px;
  border: 1px solid #ef9a9a;
`;

const Success = styled.div`
  background-color: #e8f5e9;
  color: #2e7d32;
  padding: 15px;
  border-radius: 6px;
  margin-bottom: 20px;
  border: 1px solid #a5d6a7;
`;

const CatalogLocks = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [locks, setLocks] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        setSuccess(null);
        const [locksData, metricsData] = await Promise.all([
          catalogLocksApi.getLocks(),
          catalogLocksApi.getMetrics()
        ]);
        setLocks(locksData || []);
        setMetrics(metricsData || {});
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading catalog locks');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleUnlock = async (lockName: string) => {
    if (!confirm(`Are you sure you want to force unlock "${lockName}"? This may interrupt operations.`)) {
      return;
    }

    try {
      setError(null);
      setSuccess(null);
      await catalogLocksApi.unlock(lockName);
      setSuccess(`Lock "${lockName}" has been released successfully`);
      const [locksData, metricsData] = await Promise.all([
        catalogLocksApi.getLocks(),
        catalogLocksApi.getMetrics()
      ]);
      setLocks(locksData || []);
      setMetrics(metricsData || {});
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error unlocking lock');
    }
  };

  const handleCleanExpired = async () => {
    try {
      setError(null);
      setSuccess(null);
      const result = await catalogLocksApi.cleanExpired();
      setSuccess(result.message || 'Expired locks cleaned successfully');
      const [locksData, metricsData] = await Promise.all([
        catalogLocksApi.getLocks(),
        catalogLocksApi.getMetrics()
      ]);
      setLocks(locksData || []);
      setMetrics(metricsData || {});
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error cleaning expired locks');
    }
  };

  const getLockStatus = (expiresAt: string) => {
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
  };

  const formatTimeRemaining = (expiresAt: string) => {
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
  };

  const formatDate = (date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  };

  if (loading && locks.length === 0) {
    return (
      <CatalogLocksContainer>
        <Header>Catalog Locks Monitor</Header>
        <Loading>Loading catalog locks...</Loading>
      </CatalogLocksContainer>
    );
  }

  const expiredLocks = locks.filter(lock => {
    if (!lock.expires_at) return false;
    return new Date(lock.expires_at) < new Date();
  });

  const activeLocks = locks.filter(lock => {
    if (!lock.expires_at) return true;
    return new Date(lock.expires_at) >= new Date();
  });

  return (
    <CatalogLocksContainer>
      <Header>Catalog Locks Monitor</Header>
      
      {error && <Error>{error}</Error>}
      {success && <Success>{success}</Success>}
      
      <MetricsGrid>
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
        <div style={{ fontSize: '0.9em', color: '#666' }}>
          Locks are used to prevent race conditions during catalog operations. Expired locks are automatically cleaned.
        </div>
        <ActionButton $danger onClick={handleCleanExpired} disabled={expiredLocks.length === 0}>
          Clean Expired ({expiredLocks.length})
        </ActionButton>
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
          <div style={{ padding: '40px', textAlign: 'center', color: '#666' }}>
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
    </CatalogLocksContainer>
  );
};

export default CatalogLocks;

