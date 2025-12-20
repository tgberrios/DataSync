import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import styled, { keyframes } from 'styled-components';
import {
  Container,
  Header,
  ErrorMessage,
  LoadingOverlay,
  Grid,
  Value,
  Pagination,
  PageButton,
  FiltersContainer,
  Select,
  Input,
  TableContainer,
  Table,
  Th,
  Td,
  TableRow,
  Button,
} from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { governanceCatalogApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { sanitizeSearch } from '../utils/validation';
import { theme } from '../theme/theme';
import GovernanceCatalogMSSQLTreeView from './GovernanceCatalogMSSQLTreeView';

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
  border-left: 4px solid ${theme.colors.primary.main};
  box-shadow: ${theme.shadows.sm};
  animation: ${slideUp} 0.4s cubic-bezier(0.4, 0, 0.2, 1);
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    box-shadow: ${theme.shadows.md};
    transform: translateY(-1px);
  }
`;

const ExportButton = styled(Button)`
  padding: 8px 16px;
  font-size: 0.9em;
  display: flex;
  align-items: center;
  gap: 6px;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    transform: translateY(-2px);
    box-shadow: ${theme.shadows.md};
  }
`;

const PaginationInfo = styled.div`
  text-align: center;
  margin-bottom: ${theme.spacing.sm};
  color: ${theme.colors.text.secondary};
  font-size: 0.9em;
  animation: fadeIn 0.25s ease-in;
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

const Badge = styled.span<{ $status?: string; $type?: string; $level?: number }>`
  padding: 6px 12px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.8em;
  font-weight: 600;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  transition: all ${theme.transitions.normal};
  border: 2px solid transparent;
  box-shadow: ${theme.shadows.sm};
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  position: relative;
  overflow: hidden;
  
  ${props => {
    if (props.$status) {
      const statusColors: Record<string, { bg: string; text: string; border: string }> = {
        'HEALTHY': { bg: theme.colors.status.success.bg, text: theme.colors.status.success.text, border: `${theme.colors.status.success.text}40` },
        'WARNING': { bg: theme.colors.status.warning.bg, text: theme.colors.status.warning.text, border: `${theme.colors.status.warning.text}40` },
        'CRITICAL': { bg: theme.colors.status.error.bg, text: theme.colors.status.error.text, border: `${theme.colors.status.error.text}40` },
      };
      const colors = statusColors[props.$status] || { bg: theme.colors.background.secondary, text: theme.colors.text.secondary, border: theme.colors.border.medium };
      return `
        background: linear-gradient(135deg, ${colors.bg} 0%, ${colors.text}15 100%);
        color: ${colors.text};
        border-color: ${colors.border};
      `;
    }
    if (props.$type) {
      return `
        background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
        color: ${theme.colors.text.primary};
        border-color: ${theme.colors.border.medium};
      `;
    }
    if (props.$level !== undefined) {
      if (props.$level === 0) return `
        background: linear-gradient(135deg, ${theme.colors.status.success.bg} 0%, ${theme.colors.status.success.text}15 100%);
        color: ${theme.colors.status.success.text};
        border-color: ${theme.colors.status.success.text}40;
      `;
      if (props.$level === 1) return `
        background: linear-gradient(135deg, ${theme.colors.status.warning.bg} 0%, ${theme.colors.status.warning.text}15 100%);
        color: ${theme.colors.status.warning.text};
        border-color: ${theme.colors.status.warning.text}40;
      `;
      if (props.$level === 2) return `
        background: linear-gradient(135deg, ${theme.colors.status.error.bg} 0%, ${theme.colors.status.error.text}15 100%);
        color: ${theme.colors.status.error.text};
        border-color: ${theme.colors.status.error.text}40;
      `;
      return `
        background: ${theme.colors.background.secondary};
        color: ${theme.colors.text.secondary};
        border-color: ${theme.colors.border.medium};
      `;
    }
    return `
      background: ${theme.colors.background.secondary};
      color: ${theme.colors.text.secondary};
      border-color: ${theme.colors.border.medium};
    `;
  }}
  
  &:hover {
    transform: translateY(-2px) scale(1.08);
    box-shadow: ${theme.shadows.lg};
    border-width: 2px;
  }
`;

const StyledTableRow = styled(TableRow)<{ $delay?: number }>`
  transition: all ${theme.transitions.normal};
  animation: ${fadeIn} 0.4s cubic-bezier(0.4, 0, 0.2, 1);
  animation-delay: ${props => (props.$delay || 0) * 0.05}s;
  animation-fill-mode: both;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  border-left: 3px solid transparent;
  position: relative;
  
  &::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 0;
    height: 100%;
    background: linear-gradient(90deg, transparent, rgba(13, 27, 42, 0.05), transparent);
    transition: width 0.3s;
  }
  
  &:hover {
    background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%) !important;
    transform: translateX(4px) scale(1.01);
    box-shadow: ${theme.shadows.md};
    border-left-color: ${theme.colors.primary.main};
    
    &::before {
      width: 100%;
    }
  }
  
  &:active {
    transform: translateX(2px) scale(0.99);
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

const DetailsPanel = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '700px' : '0'};
  opacity: ${props => props.$isOpen ? '1' : '0'};
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  border-top: ${props => props.$isOpen ? `1px solid ${theme.colors.border.light}` : 'none'};
  background-color: ${theme.colors.background.main};
  overflow: hidden;
`;

const DetailGrid = styled.div`
  display: grid;
  grid-template-columns: 200px 1fr;
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

const PerformanceGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: ${theme.spacing.sm};
  padding: ${theme.spacing.md};
  background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.main} 100%);
  border-radius: ${theme.borderRadius.md};
  border: 1px solid ${theme.colors.border.light};
  margin: ${theme.spacing.md};
`;

const PerformanceMetric = styled.div`
  padding: ${theme.spacing.sm};
  background: ${theme.colors.background.main};
  border-radius: ${theme.borderRadius.md};
  border: 1px solid ${theme.colors.border.medium};
`;

const PerformanceLabel = styled.div`
  font-size: 0.8em;
  color: ${theme.colors.text.secondary};
  margin-bottom: 5px;
`;

const PerformanceValue = styled.div`
  font-size: 1.1em;
  font-weight: bold;
  color: ${theme.colors.text.primary};
`;

/**
 * Governance Catalog component for MSSQL
 * Displays governance metadata for MSSQL objects including tables, views, and stored procedures
 */
const GovernanceCatalogMSSQL = () => {
  const { page, limit, setPage } = usePagination(1, 20);
  const { filters, setFilter, clearFilters } = useTableFilters({
    server_name: '',
    database_name: '',
    object_type: '',
    health_status: '',
    access_frequency: '',
    search: ''
  });
  
  const [sortField, setSortField] = useState<string>("");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [items, setItems] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [openItemId, setOpenItemId] = useState<number | null>(null);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 20
  });
  const [servers, setServers] = useState<string[]>([]);
  const [databases, setDatabases] = useState<string[]>([]);
  const [viewMode, setViewMode] = useState<"table" | "tree">("tree");
  const [allItems, setAllItems] = useState<any[]>([]);
  const [loadingTree, setLoadingTree] = useState(false);
  const isMountedRef = useRef(true);

  const fetchData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(filters.search as string, 100);
      const [itemsData, metricsData, serversData] = await Promise.all([
        governanceCatalogApi.getMSSQLItems({
          page,
          limit,
          server_name: filters.server_name as string,
          database_name: filters.database_name as string,
          object_type: filters.object_type as string,
          health_status: filters.health_status as string,
          access_frequency: filters.access_frequency as string,
          search: sanitizedSearch
        }),
        governanceCatalogApi.getMSSQLMetrics(),
        governanceCatalogApi.getMSSQLServers()
      ]);
      if (isMountedRef.current) {
        setItems(itemsData.data || []);
        setPagination(itemsData.pagination || {
          total: 0,
          totalPages: 0,
          currentPage: 1,
          limit: 20
        });
        setMetrics(metricsData || {});
        setServers(serversData || []);
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
  }, [
    page, 
    limit, 
    filters.server_name, 
    filters.database_name, 
    filters.object_type, 
    filters.health_status, 
    filters.access_frequency, 
    filters.search
  ]);

  const fetchAllItems = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoadingTree(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(filters.search as string, 100);
      const itemsData = await governanceCatalogApi.getMSSQLItems({
        page: 1,
        limit: 10000,
        server_name: filters.server_name as string,
        database_name: filters.database_name as string,
        object_type: filters.object_type as string,
        health_status: filters.health_status as string,
        access_frequency: filters.access_frequency as string,
        search: sanitizedSearch
      });
      if (isMountedRef.current) {
        setAllItems(itemsData.data || []);
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    } finally {
      if (isMountedRef.current) {
        setLoadingTree(false);
      }
    }
  }, [
    filters.server_name,
    filters.database_name,
    filters.object_type,
    filters.health_status,
    filters.access_frequency,
    filters.search
  ]);

  useEffect(() => {
    isMountedRef.current = true;
    if (viewMode === "table") {
      fetchData();
    } else {
      fetchAllItems();
    }
    const interval = setInterval(() => {
      if (isMountedRef.current) {
        if (viewMode === "table") {
          fetchData();
        } else {
          fetchAllItems();
        }
      }
    }, 30000);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchData, fetchAllItems, viewMode]);

  useEffect(() => {
    if (viewMode === "tree") {
      fetchAllItems();
    }
  }, [viewMode, fetchAllItems]);

  useEffect(() => {
    const fetchDatabases = async () => {
      if (filters.server_name && isMountedRef.current) {
        try {
          const databasesData = await governanceCatalogApi.getMSSQLDatabases(filters.server_name as string);
          if (isMountedRef.current) {
            setDatabases(databasesData || []);
          }
        } catch (err) {
          if (isMountedRef.current) {
            console.error('Error loading databases:', err);
          }
        }
      } else {
        setDatabases([]);
      }
    };
    fetchDatabases();
  }, [filters.server_name]);

  const toggleItem = useCallback((id: number) => {
    setOpenItemId(prev => prev === id ? null : id);
  }, []);

  const formatBytes = useCallback((mb: number | string | null | undefined) => {
    if (mb === null || mb === undefined) return 'N/A';
    const num = Number(mb);
    if (isNaN(num)) return 'N/A';
    if (num < 1) return `${(num * 1024).toFixed(2)} KB`;
    return `${num.toFixed(2)} MB`;
  }, []);

  const formatNumber = useCallback((value: number | string | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    const num = Number(value);
    if (isNaN(num)) return 'N/A';
    return num.toLocaleString();
  }, []);

  const formatPercentage = useCallback((value: number | string | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    const num = Number(value);
    if (isNaN(num)) return 'N/A';
    return `${num.toFixed(2)}%`;
  }, []);

  const formatDate = useCallback((date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  }, []);

  const formatTime = useCallback((seconds: number | string | null | undefined) => {
    if (seconds === null || seconds === undefined) return 'N/A';
    const num = Number(seconds);
    if (isNaN(num)) return 'N/A';
    if (num < 1) return `${(num * 1000).toFixed(2)} ms`;
    return `${num.toFixed(2)} s`;
  }, []);

  const handleFilterChange = useCallback((key: string, value: string) => {
    setFilter(key as any, value);
    if (key === 'server_name') {
      setFilter('database_name' as any, '');
    }
    setPage(1);
  }, [setFilter, setPage]);

  const handleSort = useCallback((field: string) => {
    if (sortField === field) {
      setSortDirection(prev => prev === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDirection("desc");
    }
    setPage(1);
  }, [sortField, setPage]);

  const sortedItems = useMemo(() => {
    if (!sortField) return items;
    return [...items].sort((a, b) => {
      let aVal: any = a[sortField as keyof typeof a];
      let bVal: any = b[sortField as keyof typeof b];
      
      if (aVal === null || aVal === undefined) aVal = "";
      if (bVal === null || bVal === undefined) bVal = "";
      
      if (typeof aVal === "string" && typeof bVal === "string") {
        return sortDirection === "asc" 
          ? aVal.localeCompare(bVal)
          : bVal.localeCompare(aVal);
      }
      
      const aNum = Number(aVal);
      const bNum = Number(bVal);
      if (!isNaN(aNum) && !isNaN(bNum)) {
        return sortDirection === "asc" ? aNum - bNum : bNum - aNum;
      }
      
      return sortDirection === "asc"
        ? String(aVal).localeCompare(String(bVal))
        : String(bVal).localeCompare(String(aVal));
    });
  }, [items, sortField, sortDirection]);

  const handleExportCSV = useCallback(() => {
    const headers = ["Server", "Database", "Schema", "Object", "Type", "Rows", "Size (MB)", "Health", "Access"];
    const rows = sortedItems.map(item => [
      item.server_name || "",
      item.database_name || "",
      item.schema_name || "",
      item.object_name || "",
      item.object_type || "",
      formatNumber(item.row_count),
      item.total_size_mb || 0,
      item.health_status || "",
      item.access_frequency || ""
    ]);
    
    const csvContent = [
      headers.join(","),
      ...rows.map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(","))
    ].join("\n");
    
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    link.setAttribute("href", url);
    link.setAttribute("download", `governance_catalog_mssql_export_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }, [sortedItems, formatNumber]);

  if ((loading && items.length === 0 && viewMode === "table") || (loadingTree && allItems.length === 0 && viewMode === "tree")) {
    return (
      <Container>
        <Header>Governance Catalog - MSSQL</Header>
        <LoadingOverlay>Loading governance catalog...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>Governance Catalog - MSSQL</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}
      
      <MetricsGrid $columns="repeat(auto-fit, minmax(180px, 1fr))">
        <MetricCard $index={0}>
          <MetricLabel>
            <span>■</span>
            Total Objects
          </MetricLabel>
          <MetricValue>{metrics.total_objects || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={1}>
          <MetricLabel>
            <span>■</span>
            Total Size
          </MetricLabel>
          <MetricValue>{formatBytes(metrics.total_size_mb)}</MetricValue>
        </MetricCard>
        <MetricCard $index={2}>
          <MetricLabel>
            <span>✓</span>
            Healthy
          </MetricLabel>
          <MetricValue>{metrics.healthy_count || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={3}>
          <MetricLabel>
            <span>!</span>
            Warning
          </MetricLabel>
          <MetricValue>{metrics.warning_count || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={4}>
          <MetricLabel>
            <span>×</span>
            Critical
          </MetricLabel>
          <MetricValue>{metrics.critical_count || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={5}>
          <MetricLabel>
            <span>■</span>
            Unique Servers
          </MetricLabel>
          <MetricValue>{metrics.unique_servers || 0}</MetricValue>
        </MetricCard>
      </MetricsGrid>

      <FiltersContainer>
        <Select
          value={filters.server_name as string}
          onChange={(e) => handleFilterChange('server_name', e.target.value)}
        >
          <option value="">All Servers</option>
          {servers.map(server => (
            <option key={server} value={server}>{server}</option>
          ))}
        </Select>
        
        <Select
          value={filters.database_name as string}
          onChange={(e) => handleFilterChange('database_name', e.target.value)}
          disabled={!filters.server_name}
        >
          <option value="">All Databases</option>
          {databases.map(db => (
            <option key={db} value={db}>{db}</option>
          ))}
        </Select>
        
        <Select
          value={filters.object_type as string}
          onChange={(e) => handleFilterChange('object_type', e.target.value)}
        >
          <option value="">All Types</option>
          <option value="TABLE">TABLE</option>
          <option value="VIEW">VIEW</option>
          <option value="STORED_PROCEDURE">STORED_PROCEDURE</option>
        </Select>
        
        <Select
          value={filters.health_status as string}
          onChange={(e) => handleFilterChange('health_status', e.target.value)}
        >
          <option value="">All Health Status</option>
          <option value="EXCELLENT">Excellent</option>
          <option value="HEALTHY">Healthy</option>
          <option value="WARNING">Warning</option>
          <option value="CRITICAL">Critical</option>
        </Select>
        
        <Select
          value={filters.access_frequency as string}
          onChange={(e) => handleFilterChange('access_frequency', e.target.value)}
        >
          <option value="">All Access Frequency</option>
          <option value="REAL_TIME">Real Time</option>
          <option value="HIGH">High</option>
          <option value="MEDIUM">Medium</option>
          <option value="LOW">Low</option>
          <option value="RARE">Rare</option>
        </Select>
        
        <Input
          type="text"
          placeholder="Search object name..."
          value={filters.search as string}
          onChange={(e) => handleFilterChange('search', e.target.value)}
          style={{ flex: 1, minWidth: "200px" }}
        />
        
        <Button
          $variant="secondary"
          onClick={() => {
            clearFilters();
            setPage(1);
          }}
          style={{ padding: "8px 16px", fontSize: "0.9em" }}
        >
          Reset All
        </Button>
      </FiltersContainer>

      <TableActions>
        <PaginationInfo>
          {viewMode === "table" 
            ? `Showing ${sortedItems.length} of ${pagination.total} entries (Page ${pagination.currentPage} of ${pagination.totalPages})`
            : `Total: ${allItems.length} entries`
          }
        </PaginationInfo>
        <div style={{ display: 'flex', gap: theme.spacing.sm, alignItems: 'center' }}>
          <Button
            $variant={viewMode === "table" ? "primary" : "secondary"}
            onClick={() => setViewMode("table")}
            style={{ padding: "6px 12px", fontSize: "0.85em" }}
          >
            Table View
          </Button>
          <Button
            $variant={viewMode === "tree" ? "primary" : "secondary"}
            onClick={() => {
              setViewMode("tree");
              fetchAllItems();
            }}
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
        loadingTree ? (
          <LoadingOverlay>Loading tree view...</LoadingOverlay>
        ) : (
          <GovernanceCatalogMSSQLTreeView items={allItems} onItemClick={(item: any) => toggleItem(item.id)} />
        )
      ) : (
        <StyledTableContainer>
        <Table $minWidth="1400px">
          <thead>
            <tr>
              <SortableTh 
                $sortable 
                $active={sortField === "server_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("server_name")}
              >
                Server
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "database_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("database_name")}
              >
                Database
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "schema_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("schema_name")}
              >
                Schema
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "object_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("object_name")}
              >
                Object
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "object_type"} 
                $direction={sortDirection}
                onClick={() => handleSort("object_type")}
              >
                Type
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "row_count"} 
                $direction={sortDirection}
                onClick={() => handleSort("row_count")}
              >
                Rows
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "table_size_mb"} 
                $direction={sortDirection}
                onClick={() => handleSort("table_size_mb")}
              >
                Size
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "health_status"} 
                $direction={sortDirection}
                onClick={() => handleSort("health_status")}
              >
                Health
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "access_frequency"} 
                $direction={sortDirection}
                onClick={() => handleSort("access_frequency")}
              >
                Access
              </SortableTh>
            </tr>
          </thead>
          <tbody>
            {sortedItems.length === 0 ? (
              <TableRow>
                <Td colSpan={9} style={{ padding: '60px 40px', textAlign: 'center', color: theme.colors.text.secondary }}>
                  <div style={{ 
                    fontSize: '3em', 
                    marginBottom: theme.spacing.md,
                    animation: `${fadeIn} 0.5s ease-out`,
                    fontFamily: "'Courier New', monospace",
                    opacity: 0.5
                  }}>
                    ■
                  </div>
                  <div style={{ 
                    fontSize: '1.1em',
                    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", "Roboto", "Oxygen", "Ubuntu", "Cantarell", sans-serif',
                    fontWeight: 500,
                    marginBottom: theme.spacing.sm
                  }}>
                    No governance data available
                  </div>
                  <div style={{ fontSize: '0.9em', opacity: 0.7 }}>
                    Data will appear here once collected.
                  </div>
                </Td>
              </TableRow>
            ) : (
              sortedItems.map((item, index) => (
                <React.Fragment key={item.id}>
                  <StyledTableRow onClick={() => toggleItem(item.id)} style={{ cursor: 'pointer' }} $delay={index}>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {item.server_name || 'N/A'}
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {item.database_name || 'N/A'}
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {item.schema_name || 'N/A'}
                    </Td>
                    <Td>
                      <strong style={{ color: theme.colors.primary.main }}>
                        {item.object_name || item.table_name || 'N/A'}
                      </strong>
                    </Td>
                    <Td>
                      <Badge $type={item.object_type}>
                        {item.object_type || 'N/A'}
                      </Badge>
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {formatNumber(item.row_count)}
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {formatBytes(item.table_size_mb)}
                    </Td>
                    <Td>
                      <Badge $status={item.health_status}>
                        {item.health_status || 'N/A'}
                      </Badge>
                    </Td>
                    <Td>
                      <Badge $status={item.access_frequency}>
                        {item.access_frequency || 'N/A'}
                      </Badge>
                    </Td>
                  </StyledTableRow>
                  {openItemId === item.id && (
                    <TableRow>
                      <Td colSpan={9} style={{ padding: 0, border: 'none' }}>
                        <DetailsPanel $isOpen={openItemId === item.id}>
                <DetailGrid>
                  <DetailLabel>Object ID:</DetailLabel>
                  <DetailValue>{formatNumber(item.object_id)}</DetailValue>
                  
                  <DetailLabel>Index Name:</DetailLabel>
                  <DetailValue>{item.index_name || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Index ID:</DetailLabel>
                  <DetailValue>{formatNumber(item.index_id)}</DetailValue>
                  
                  <DetailLabel>Fragmentation:</DetailLabel>
                  <DetailValue>{formatPercentage(item.fragmentation_pct)}</DetailValue>
                  
                  <DetailLabel>Page Count:</DetailLabel>
                  <DetailValue>{formatNumber(item.page_count)}</DetailValue>
                  
                  <DetailLabel>Fill Factor:</DetailLabel>
                  <DetailValue>{item.fill_factor ? `${item.fill_factor}%` : 'N/A'}</DetailValue>
                  
                  <DetailLabel>Index Key Columns:</DetailLabel>
                  <DetailValue>{item.index_key_columns || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Index Include Columns:</DetailLabel>
                  <DetailValue>{item.index_include_columns || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Has Missing Index:</DetailLabel>
                  <DetailValue>{item.has_missing_index ? 'Yes' : 'No'}</DetailValue>
                  
                  <DetailLabel>Is Unused:</DetailLabel>
                  <DetailValue>{item.is_unused ? 'Yes' : 'No'}</DetailValue>
                  
                  <DetailLabel>Compatibility Level:</DetailLabel>
                  <DetailValue>{item.compatibility_level || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Recovery Model:</DetailLabel>
                  <DetailValue>{item.recovery_model || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Health Score:</DetailLabel>
                  <DetailValue>{item.health_score ? `${Number(item.health_score).toFixed(2)}` : 'N/A'}</DetailValue>
                  
                  <DetailLabel>Recommendation:</DetailLabel>
                  <DetailValue>{item.recommendation_summary || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Last Full Backup:</DetailLabel>
                  <DetailValue>{formatDate(item.last_full_backup)}</DetailValue>
                  
                  <DetailLabel>Last Diff Backup:</DetailLabel>
                  <DetailValue>{formatDate(item.last_diff_backup)}</DetailValue>
                  
                  <DetailLabel>Last Log Backup:</DetailLabel>
                  <DetailValue>{formatDate(item.last_log_backup)}</DetailValue>
                  
                  <DetailLabel>Snapshot Date:</DetailLabel>
                  <DetailValue>{formatDate(item.snapshot_date)}</DetailValue>
                </DetailGrid>
                
                {(item.user_seeks || item.user_scans || item.user_lookups || item.user_updates || item.execution_count) && (
                  <PerformanceGrid>
                    <PerformanceMetric>
                      <PerformanceLabel>User Seeks</PerformanceLabel>
                      <PerformanceValue>{formatNumber(item.user_seeks)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>User Scans</PerformanceLabel>
                      <PerformanceValue>{formatNumber(item.user_scans)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>User Lookups</PerformanceLabel>
                      <PerformanceValue>{formatNumber(item.user_lookups)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>User Updates</PerformanceLabel>
                      <PerformanceValue>{formatNumber(item.user_updates)}</PerformanceValue>
                    </PerformanceMetric>
                    {item.execution_count && (
                      <PerformanceMetric>
                        <PerformanceLabel>Execution Count</PerformanceLabel>
                        <PerformanceValue>{formatNumber(item.execution_count)}</PerformanceValue>
                      </PerformanceMetric>
                    )}
                    {item.avg_execution_time_seconds && (
                      <PerformanceMetric>
                        <PerformanceLabel>Avg Execution Time</PerformanceLabel>
                        <PerformanceValue>{formatTime(item.avg_execution_time_seconds)}</PerformanceValue>
                      </PerformanceMetric>
                    )}
                    {item.avg_logical_reads && (
                      <PerformanceMetric>
                        <PerformanceLabel>Avg Logical Reads</PerformanceLabel>
                        <PerformanceValue>{formatNumber(item.avg_logical_reads)}</PerformanceValue>
                      </PerformanceMetric>
                    )}
                    {item.avg_physical_reads && (
                      <PerformanceMetric>
                        <PerformanceLabel>Avg Physical Reads</PerformanceLabel>
                        <PerformanceValue>{formatNumber(item.avg_physical_reads)}</PerformanceValue>
                      </PerformanceMetric>
                    )}
                  </PerformanceGrid>
                )}
                        </DetailsPanel>
                      </Td>
                    </TableRow>
                  )}
                </React.Fragment>
              ))
            )}
          </tbody>
        </Table>
      </StyledTableContainer>
      )}

      {viewMode === "table" && pagination.totalPages > 1 && (
        <Pagination>
          <PageButton
            onClick={() => setPage(Math.max(1, page - 1))}
            disabled={page === 1}
          >
            Previous
          </PageButton>
          <span>
            Page {pagination.currentPage} of {pagination.totalPages} ({pagination.total} total)
          </span>
          <PageButton
            onClick={() => setPage(Math.min(pagination.totalPages, page + 1))}
            disabled={page === pagination.totalPages}
          >
            Next
          </PageButton>
        </Pagination>
      )}
    </Container>
  );
};

export default GovernanceCatalogMSSQL;
