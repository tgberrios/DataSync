import { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import styled, { keyframes } from 'styled-components';
import CatalogLocksTreeView from './CatalogLocksTreeView';
import {
  Container,
  Header,
  ErrorMessage,
  LoadingOverlay,
  Grid,
  Value,
  Button,
  TableContainer,
  Table,
  Th,
  Td,
  TableRow,
} from './shared/BaseComponents';
import { catalogLocksApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { theme } from '../theme/theme';

const fadeIn = keyframes`
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
`;

const slideUp = keyframes`
  from {
    opacity: 0;
    transform: translateY(20px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
`;

const pulse = keyframes`
  0%, 100% {
    opacity: 1;
  }
  50% {
    opacity: 0.7;
  }
`;

const MetricsGrid = styled(Grid)`
  margin-bottom: ${theme.spacing.xxl};
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  animation: ${slideUp} 0.4s cubic-bezier(0.4, 0, 0.2, 1);
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

const MetricCard = styled(Value)<{ $index?: number }>`
  padding: ${theme.spacing.lg};
  min-height: 100px;
  background: linear-gradient(135deg, ${theme.colors.background.main} 0%, ${theme.colors.background.secondary} 100%);
  border: 2px solid ${theme.colors.border.light};
  border-left: 4px solid ${theme.colors.primary.main};
  border-radius: ${theme.borderRadius.lg};
  box-shadow: ${theme.shadows.md};
  transition: all ${theme.transitions.normal};
  animation: ${fadeIn} 0.4s cubic-bezier(0.4, 0, 0.2, 1);
  animation-delay: ${props => (props.$index || 0) * 0.1}s;
  animation-fill-mode: both;
  position: relative;
  overflow: hidden;
  
  &::before {
    content: '';
    position: absolute;
    top: 0;
    left: -100%;
    width: 100%;
    height: 100%;
    background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.2), transparent);
    transition: left 0.5s;
  }
  
  &:hover {
    transform: translateY(-4px) scale(1.02);
    box-shadow: ${theme.shadows.xl};
    border-color: ${theme.colors.primary.main};
    border-left-color: ${theme.colors.primary.dark};
    
    &::before {
      left: 100%;
    }
  }
`;

const MetricLabel = styled.div`
  font-size: 0.9em;
  color: ${theme.colors.text.secondary};
  margin-bottom: ${theme.spacing.sm};
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  display: flex;
  align-items: center;
  gap: 6px;
`;

const MetricValue = styled.div`
  font-size: 2.2em;
  font-weight: 700;
  color: ${theme.colors.primary.main};
  text-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  background: linear-gradient(135deg, ${theme.colors.primary.main} 0%, ${theme.colors.primary.light} 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
`;

const ActionBar = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: ${theme.spacing.lg};
  gap: ${theme.spacing.sm};
  padding: ${theme.spacing.md};
  background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
  border-radius: ${theme.borderRadius.md};
  border: 1px solid ${theme.colors.border.light};
  border-left: 4px solid ${theme.colors.primary.main};
  box-shadow: ${theme.shadows.sm};
  animation: ${slideUp} 0.4s cubic-bezier(0.4, 0, 0.2, 1);
  animation-delay: 0.15s;
  animation-fill-mode: both;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    box-shadow: ${theme.shadows.md};
    transform: translateY(-1px);
  }
`;

const DangerButton = styled(Button)`
  background: linear-gradient(135deg, ${theme.colors.status.error.bg} 0%, ${theme.colors.status.error.text}15 100%);
  color: ${theme.colors.status.error.text};
  border-color: ${theme.colors.status.error.text};
  font-weight: 600;
  padding: 10px 20px;
  border-radius: ${theme.borderRadius.md};
  transition: all ${theme.transitions.normal};
  box-shadow: ${theme.shadows.sm};
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  position: relative;
  overflow: hidden;
  
  &::before {
    content: '';
    position: absolute;
    top: 0;
    left: -100%;
    width: 100%;
    height: 100%;
    background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.2), transparent);
    transition: left 0.5s;
  }
  
  &:hover:not(:disabled) {
    background: linear-gradient(135deg, #ffcdd2 0%, ${theme.colors.status.error.bg} 100%);
    border-color: ${theme.colors.status.error.text};
    transform: translateY(-2px) scale(1.02);
    box-shadow: ${theme.shadows.lg};
    
    &::before {
      left: 100%;
    }
  }
  
  &:active:not(:disabled) {
    transform: translateY(0) scale(0.98);
  }
  
  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
`;

const StyledTableRow = styled(TableRow)<{ $expired?: boolean; $warning?: boolean; $delay?: number }>`
  background: ${props => {
    if (props.$expired) return `linear-gradient(135deg, ${theme.colors.status.error.bg} 0%, ${theme.colors.status.error.text}08 100%)`;
    if (props.$warning) return `linear-gradient(135deg, ${theme.colors.status.warning.bg} 0%, ${theme.colors.status.warning.text}08 100%)`;
    return theme.colors.background.main;
  }};
  border-left: 4px solid ${props => {
    if (props.$expired) return theme.colors.status.error.text;
    if (props.$warning) return theme.colors.status.warning.text;
    return theme.colors.primary.main;
  }};
  transition: all ${theme.transitions.normal};
  animation: ${fadeIn} 0.4s cubic-bezier(0.4, 0, 0.2, 1);
  animation-delay: ${props => (props.$delay || 0) * 0.05}s;
  animation-fill-mode: both;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  position: relative;
  
  &::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 0;
    height: 100%;
    background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.1), transparent);
    transition: width 0.3s;
  }
  
  &:hover {
    background: ${props => {
      if (props.$expired) return `linear-gradient(135deg, #ffcdd2 0%, ${theme.colors.status.error.bg} 100%)`;
      if (props.$warning) return `linear-gradient(135deg, #ffe0b2 0%, ${theme.colors.status.warning.bg} 100%)`;
      return `linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%)`;
    }} !important;
    transform: translateX(4px) scale(1.01);
    box-shadow: ${theme.shadows.md};
    border-left-width: 6px;
    
    &::before {
      width: 100%;
    }
  }
  
  &:active {
    transform: translateX(2px) scale(0.99);
  }
`;

const SortableTh = styled(Th)<{ $sortable?: boolean; $active?: boolean; $direction?: "asc" | "desc" }>`
  cursor: ${props => props.$sortable ? "pointer" : "default"};
  user-select: none;
  position: relative;
  transition: all ${theme.transitions.normal};
  
  ${props => props.$sortable && `
    &:hover {
      background: linear-gradient(180deg, ${theme.colors.primary.light} 0%, ${theme.colors.primary.main} 100%);
      color: ${theme.colors.text.white};
    }
  `}
  
  ${props => props.$active && `
    background: linear-gradient(180deg, ${theme.colors.primary.main} 0%, ${theme.colors.primary.dark} 100%);
    color: ${theme.colors.text.white};
    
    &::after {
      content: "${props.$direction === "asc" ? "▲" : "▼"}";
      position: absolute;
      right: 8px;
      font-size: 0.8em;
    }
  `}
`;

const Badge = styled.span<{ $status?: string }>`
  padding: 6px 12px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.8em;
  font-weight: 600;
  display: inline-flex;
  align-items: center;
  gap: 6px;
  transition: all ${theme.transitions.normal};
  border: 2px solid transparent;
  box-shadow: ${theme.shadows.sm};
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  position: relative;
  overflow: hidden;
  
  ${props => {
    switch (props.$status) {
      case 'active': return `
        background: linear-gradient(135deg, ${theme.colors.status.success.bg} 0%, ${theme.colors.status.success.text}15 100%);
        color: ${theme.colors.status.success.text};
        border-color: ${theme.colors.status.success.text}40;
      `;
      case 'expired': return `
        background: linear-gradient(135deg, ${theme.colors.status.error.bg} 0%, ${theme.colors.status.error.text}15 100%);
        color: ${theme.colors.status.error.text};
        border-color: ${theme.colors.status.error.text}40;
        animation: ${pulse} 2s ease-in-out infinite;
      `;
      case 'warning': return `
        background: linear-gradient(135deg, ${theme.colors.status.warning.bg} 0%, ${theme.colors.status.warning.text}15 100%);
        color: ${theme.colors.status.warning.text};
        border-color: ${theme.colors.status.warning.text}40;
      `;
      default: return `
        background: ${theme.colors.background.secondary};
        color: ${theme.colors.text.secondary};
        border-color: ${theme.colors.border.medium};
      `;
    }
  }}
  
  &:hover {
    transform: translateY(-2px) scale(1.08);
    box-shadow: ${theme.shadows.lg};
    border-width: 2px;
  }
  
  &::before {
    content: "${props => {
      switch (props.$status) {
        case 'active': return '✓';
        case 'expired': return '✗';
        case 'warning': return '⚠';
        default: return '';
      }
    }}";
    font-weight: bold;
    font-size: 1.1em;
  }
`;

const UnlockButton = styled(Button)`
  padding: 8px 16px;
  font-size: 0.85em;
  font-weight: 600;
  background: linear-gradient(135deg, ${theme.colors.status.error.bg} 0%, ${theme.colors.status.error.text}15 100%);
  color: ${theme.colors.status.error.text};
  border-color: ${theme.colors.status.error.text};
  border-radius: ${theme.borderRadius.md};
  transition: all ${theme.transitions.normal};
  box-shadow: ${theme.shadows.sm};
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  position: relative;
  overflow: hidden;
  
  &::before {
    content: '';
    position: absolute;
    top: 0;
    left: -100%;
    width: 100%;
    height: 100%;
    background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.2), transparent);
    transition: left 0.5s;
  }
  
  &:hover:not(:disabled) {
    background: linear-gradient(135deg, #ffcdd2 0%, ${theme.colors.status.error.bg} 100%);
    border-color: ${theme.colors.status.error.text};
    transform: translateY(-2px) scale(1.02);
    box-shadow: ${theme.shadows.lg};
    
    &::before {
      left: 100%;
    }
  }
  
  &:active:not(:disabled) {
    transform: translateY(0) scale(0.98);
  }
`;

const StyledTableContainer = styled(TableContainer)`
  border-radius: ${theme.borderRadius.lg};
  border: 1px solid ${theme.colors.border.light};
  box-shadow: ${theme.shadows.md};
  background: ${theme.colors.background.main};
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  
  &::-webkit-scrollbar {
    width: 10px;
    height: 10px;
  }
  
  &::-webkit-scrollbar-track {
    background: ${theme.colors.background.secondary};
    border-radius: ${theme.borderRadius.sm};
  }
  
  &::-webkit-scrollbar-thumb {
    background: ${theme.colors.border.medium};
    border-radius: ${theme.borderRadius.sm};
    transition: background ${theme.transitions.normal};
    
    &:hover {
      background: ${theme.colors.primary.main};
    }
  }
`;

const TableActions = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: ${theme.spacing.md};
  gap: ${theme.spacing.sm};
  padding: ${theme.spacing.md};
  background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
  border-radius: ${theme.borderRadius.md};
  border: 1px solid ${theme.colors.border.light};
  box-shadow: ${theme.shadows.sm};
  animation: ${fadeIn} 0.3s ease-out;
`;

const ExportButton = styled(Button)`
  padding: 8px 16px;
  font-size: 0.9em;
  display: flex;
  align-items: center;
  gap: 6px;
`;


const SuccessMessage = styled.div`
  background: linear-gradient(135deg, ${theme.colors.status.success.bg} 0%, ${theme.colors.status.success.text}15 100%);
  color: ${theme.colors.status.success.text};
  padding: ${theme.spacing.md};
  border-radius: ${theme.borderRadius.md};
  margin-bottom: ${theme.spacing.lg};
  border: 2px solid ${theme.colors.status.success.text}30;
  box-shadow: ${theme.shadows.md};
  animation: ${slideUp} 0.3s ease-out;
  font-weight: 500;
  display: flex;
  align-items: center;
  gap: 8px;
  
  &::before {
    content: '✓';
    font-size: 1.2em;
    font-weight: bold;
  }
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
  const [sortField, setSortField] = useState<string>("");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  const [viewMode, setViewMode] = useState<"table" | "tree">("tree");
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

  const sortedLocks = useMemo(() => {
    if (!sortField) return locks;
    return [...locks].sort((a, b) => {
      let aVal: any = a[sortField as keyof typeof a];
      let bVal: any = b[sortField as keyof typeof b];
      
      if (sortField === "expires_at" || sortField === "acquired_at") {
        aVal = aVal ? new Date(aVal).getTime() : 0;
        bVal = bVal ? new Date(bVal).getTime() : 0;
        return sortDirection === "asc" ? aVal - bVal : bVal - aVal;
      }
      
      if (aVal === null || aVal === undefined) aVal = "";
      if (bVal === null || bVal === undefined) bVal = "";
      
      if (typeof aVal === "string" && typeof bVal === "string") {
        return sortDirection === "asc" 
          ? aVal.localeCompare(bVal)
          : bVal.localeCompare(aVal);
      }
      
      return sortDirection === "asc"
        ? String(aVal).localeCompare(String(bVal))
        : String(bVal).localeCompare(String(aVal));
    });
  }, [locks, sortField, sortDirection]);

  const handleSort = useCallback((field: string) => {
    if (sortField === field) {
      setSortDirection(prev => prev === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDirection("desc");
    }
  }, [sortField]);

  const handleExportCSV = useCallback(() => {
    const headers = ["Lock Name", "Acquired By", "Acquired At", "Expires At", "Status", "Time Remaining"];
    const rows = sortedLocks.map(lock => {
      const status = getLockStatus(lock.expires_at);
      return [
        lock.lock_name,
        lock.acquired_by || "",
        formatDate(lock.acquired_at),
        formatDate(lock.expires_at),
        status.label,
        formatTimeRemaining(lock.expires_at)
      ];
    });
    
    const csvContent = [
      headers.join(","),
      ...rows.map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(","))
    ].join("\n");
    
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    link.setAttribute("href", url);
    link.setAttribute("download", `catalog_locks_export_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }, [sortedLocks, getLockStatus, formatDate, formatTimeRemaining]);

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
        <MetricCard $index={0}>
          <MetricLabel>
            <span>🔒</span>
            Total Locks
          </MetricLabel>
          <MetricValue>{metrics.total_locks || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={1}>
          <MetricLabel>
            <span>✓</span>
            Active Locks
          </MetricLabel>
          <MetricValue>{metrics.active_locks || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={2}>
          <MetricLabel>
            <span>⚠</span>
            Expired Locks
          </MetricLabel>
          <MetricValue>{metrics.expired_locks || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={3}>
          <MetricLabel>
            <span>🖥️</span>
            Unique Hosts
          </MetricLabel>
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

      <TableActions>
        <div style={{ fontSize: "0.9em", color: theme.colors.text.secondary }}>
          {viewMode === "table" 
            ? `Showing ${sortedLocks.length} lock${sortedLocks.length !== 1 ? 's' : ''}`
            : `Total: ${locks.length} lock${locks.length !== 1 ? 's' : ''}`
          }
        </div>
        <div style={{ display: 'flex', gap: theme.spacing.sm }}>
          <Button
            $variant={viewMode === "table" ? "primary" : "secondary"}
            onClick={() => setViewMode("table")}
            style={{ padding: "6px 12px", fontSize: "0.85em" }}
          >
            Table View
          </Button>
          <Button
            $variant={viewMode === "tree" ? "primary" : "secondary"}
            onClick={() => setViewMode("tree")}
            style={{ padding: "6px 12px", fontSize: "0.85em" }}
          >
            Tree View
          </Button>
          <ExportButton $variant="secondary" onClick={handleExportCSV}>
            Export CSV
          </ExportButton>
        </div>
      </TableActions>

      {viewMode === "tree" ? (
        <CatalogLocksTreeView 
          locks={locks}
          onLockClick={(lock) => handleUnlock(lock.lock_name)}
        />
      ) : (
      <StyledTableContainer>
        <Table $minWidth="1200px">
          <thead>
            <tr>
              <SortableTh 
                $sortable 
                $active={sortField === "lock_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("lock_name")}
              >
                Lock Name
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "acquired_by"} 
                $direction={sortDirection}
                onClick={() => handleSort("acquired_by")}
              >
                Acquired By
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "acquired_at"} 
                $direction={sortDirection}
                onClick={() => handleSort("acquired_at")}
              >
                Acquired At
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "expires_at"} 
                $direction={sortDirection}
                onClick={() => handleSort("expires_at")}
              >
                Expires At
              </SortableTh>
              <Th>Status</Th>
              <Th>Time Remaining</Th>
              <Th>Actions</Th>
            </tr>
          </thead>
          <tbody>
            {sortedLocks.length === 0 ? (
              <TableRow>
                <Td colSpan={7} style={{ padding: '60px 40px', textAlign: 'center', color: theme.colors.text.secondary }}>
                  <div style={{ 
                    fontSize: '3em', 
                    marginBottom: theme.spacing.md,
                    animation: `${fadeIn} 0.5s ease-out`,
                    fontFamily: "'Courier New', monospace",
                    opacity: 0.5
                  }}>
                    █
                  </div>
                  <div style={{ 
                    fontSize: '1.1em',
                    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", "Roboto", "Oxygen", "Ubuntu", "Cantarell", sans-serif',
                    fontWeight: 500,
                    marginBottom: theme.spacing.sm
                  }}>
                    No active locks
                  </div>
                  <div style={{ fontSize: '0.9em', opacity: 0.7 }}>
                    Locks will appear here when catalog operations are running.
                  </div>
                </Td>
              </TableRow>
            ) : (
              sortedLocks.map((lock, index) => {
                const status = getLockStatus(lock.expires_at);
                const isExpired = status.status === 'expired';
                const isWarning = status.status === 'warning';
                
                return (
                  <StyledTableRow 
                    key={lock.lock_name} 
                    $expired={isExpired} 
                    $warning={isWarning}
                    $delay={index}
                  >
                    <Td>
                      <strong style={{ 
                        color: isExpired ? theme.colors.status.error.text : theme.colors.text.primary 
                      }}>
                        {lock.lock_name}
                      </strong>
                    </Td>
                    <Td>
                      <span style={{ 
                        padding: "2px 8px", 
                        borderRadius: theme.borderRadius.sm,
                        backgroundColor: theme.colors.background.secondary,
                        fontSize: "0.85em"
                      }}>
                        {lock.acquired_by || 'N/A'}
                      </span>
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {formatDate(lock.acquired_at)}
                    </Td>
                    <Td style={{ 
                      color: isExpired ? theme.colors.status.error.text : theme.colors.text.secondary 
                    }}>
                      {formatDate(lock.expires_at)}
                    </Td>
                    <Td>
                      <Badge $status={status.status}>{status.label}</Badge>
                    </Td>
                    <Td style={{ 
                      color: isWarning ? theme.colors.status.warning.text : theme.colors.text.secondary,
                      fontWeight: isWarning ? "bold" : "normal"
                    }}>
                      {formatTimeRemaining(lock.expires_at)}
                    </Td>
                    <Td>
                      <UnlockButton onClick={() => handleUnlock(lock.lock_name)}>
                        Force Unlock
                      </UnlockButton>
                    </Td>
                  </StyledTableRow>
                );
              })
            )}
          </tbody>
        </Table>
      </StyledTableContainer>
      )}
    </Container>
  );
};

export default CatalogLocks;
