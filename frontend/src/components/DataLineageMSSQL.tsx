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
  PaginationInfo,
} from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { dataLineageMSSQLApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { sanitizeSearch } from '../utils/validation';
import { theme } from '../theme/theme';
import DataLineageMSSQLTreeView from './DataLineageMSSQLTreeView';

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

const Badge = styled.span<{ $type?: string; $level?: number }>`
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

const RelationshipArrow = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  color: ${theme.colors.primary.main};
  font-weight: bold;
  font-size: 1.2em;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    transform: scale(1.2);
    color: ${theme.colors.primary.dark};
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

const LineageDetails = styled.div<{ $isOpen: boolean }>`
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

const PerformanceGrid = styled(Grid)`
  padding: ${theme.spacing.md};
  background: ${theme.colors.gradient.primary};
  border-radius: ${theme.borderRadius.md};
  border: 1px solid ${theme.colors.border.light};
  margin: ${theme.spacing.md};
`;

const PerformanceMetric = styled(Value)`
  padding: ${theme.spacing.sm};
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

const DefinitionText = styled.pre`
  margin: 0;
  padding: ${theme.spacing.md};
  background-color: ${theme.colors.background.secondary};
  border-radius: ${theme.borderRadius.md};
  overflow-x: auto;
  font-size: 0.85em;
  border: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.2);
    box-shadow: ${theme.shadows.sm};
  }
`;

const DataLineageMSSQL = () => {
  const { page, limit, setPage } = usePagination(1, 20);
  const { filters, setFilter, clearFilters } = useTableFilters({
    server_name: '',
    instance_name: '',
    database_name: '',
    object_type: '',
    relationship_type: '',
    search: ''
  });
  
  const [sortField, setSortField] = useState<string>("");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lineage, setLineage] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [openEdgeId, setOpenEdgeId] = useState<number | null>(null);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 20
  });
  const [servers, setServers] = useState<string[]>([]);
  const [instances, setInstances] = useState<string[]>([]);
  const [databases, setDatabases] = useState<string[]>([]);
  const [viewMode, setViewMode] = useState<"table" | "tree">("tree");
  const [allEdges, setAllEdges] = useState<any[]>([]);
  const [loadingTree, setLoadingTree] = useState(false);
  const isMountedRef = useRef(true);

  const fetchData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(filters.search as string, 100);
      const [lineageData, metricsData, serversData] = await Promise.all([
        dataLineageMSSQLApi.getMSSQLLineage({
          page,
          limit,
          server_name: filters.server_name as string,
          instance_name: filters.instance_name as string,
          database_name: filters.database_name as string,
          object_type: filters.object_type as string,
          relationship_type: filters.relationship_type as string,
          search: sanitizedSearch
        }),
        dataLineageMSSQLApi.getMSSQLMetrics(),
        dataLineageMSSQLApi.getMSSQLServers()
      ]);
      if (isMountedRef.current) {
        console.log("MSSQL Lineage Data:", lineageData);
        console.log("MSSQL Metrics Data:", metricsData);
        console.log("MSSQL Metrics Data Type:", typeof metricsData);
        console.log("MSSQL Metrics Data Keys:", metricsData ? Object.keys(metricsData) : 'null');
        setLineage(lineageData.data || []);
        setPagination(lineageData.pagination || {
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
    filters.instance_name, 
    filters.database_name, 
    filters.object_type, 
    filters.relationship_type, 
    filters.search
  ]);

  const fetchMetrics = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      console.log("MSSQL fetchMetrics: Starting...");
      const metricsData = await dataLineageMSSQLApi.getMSSQLMetrics().catch(err => {
        console.error("MSSQL getMSSQLMetrics error:", err);
        throw err;
      });
      console.log("MSSQL fetchMetrics: Received data:", metricsData);
      if (isMountedRef.current) {
        setMetrics(metricsData || {});
      }
    } catch (err) {
      console.error("MSSQL fetchMetrics error:", err);
    }
  }, []);

  const fetchAllEdges = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoadingTree(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(filters.search as string, 100);
      const lineageData = await dataLineageMSSQLApi.getMSSQLLineage({
        page: 1,
        limit: 10000,
        server_name: filters.server_name as string,
        instance_name: filters.instance_name as string,
        database_name: filters.database_name as string,
        object_type: filters.object_type as string,
        relationship_type: filters.relationship_type as string,
        search: sanitizedSearch
      });
      if (isMountedRef.current) {
        setAllEdges(lineageData.data || []);
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
    filters.instance_name,
    filters.database_name,
    filters.object_type,
    filters.relationship_type,
    filters.search
  ]);

  useEffect(() => {
    isMountedRef.current = true;
    fetchMetrics();
    if (viewMode === "table") {
      fetchData();
    } else {
      fetchAllEdges();
    }
    const interval = setInterval(() => {
      if (isMountedRef.current) {
        fetchMetrics();
        if (viewMode === "table") {
          fetchData();
        } else {
          fetchAllEdges();
        }
      }
    }, 30000);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchData, fetchAllEdges, fetchMetrics, viewMode]);

  useEffect(() => {
    if (viewMode === "tree") {
      fetchAllEdges();
    }
  }, [viewMode, fetchAllEdges]);

  useEffect(() => {
    const fetchInstances = async () => {
      if (filters.server_name && isMountedRef.current) {
        try {
          const instancesData = await dataLineageMSSQLApi.getMSSQLInstances(filters.server_name as string);
          if (isMountedRef.current) {
            setInstances(instancesData || []);
          }
        } catch (err) {
          if (isMountedRef.current) {
            console.error('Error loading instances:', err);
          }
        }
      } else {
        setInstances([]);
      }
    };
    fetchInstances();
  }, [filters.server_name]);

  useEffect(() => {
    const fetchDatabases = async () => {
      if (filters.server_name && filters.instance_name && isMountedRef.current) {
        try {
          const databasesData = await dataLineageMSSQLApi.getMSSQLDatabases(
            filters.server_name as string,
            filters.instance_name as string
          );
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
  }, [filters.server_name, filters.instance_name]);

  const toggleEdge = useCallback((id: number) => {
    setOpenEdgeId(prev => prev === id ? null : id);
  }, []);

  const formatDate = useCallback((date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  }, []);

  const formatConfidence = useCallback((score: number | string | null | undefined) => {
    if (score === null || score === undefined) return 'N/A';
    const numScore = Number(score);
    if (isNaN(numScore)) return 'N/A';
    return `${(numScore * 100).toFixed(1)}%`;
  }, []);

  const formatNumber = useCallback((value: number | string | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    const num = Number(value);
    if (isNaN(num)) return 'N/A';
    return num.toLocaleString();
  }, []);

  const formatTime = useCallback((ms: number | string | null | undefined) => {
    if (ms === null || ms === undefined) return 'N/A';
    const num = Number(ms);
    if (isNaN(num)) return 'N/A';
    if (num < 1000) return `${num.toFixed(2)} ms`;
    return `${(num / 1000).toFixed(2)} s`;
  }, []);

  const handleFilterChange = useCallback((key: string, value: string) => {
    setFilter(key as any, value);
    if (key === 'server_name') {
      setFilter('instance_name' as any, '');
      setFilter('database_name' as any, '');
    }
    if (key === 'instance_name') {
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

  const sortedLineage = useMemo(() => {
    if (!sortField) return lineage;
    return [...lineage].sort((a, b) => {
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
  }, [lineage, sortField, sortDirection]);

  const handleExportCSV = useCallback(() => {
    const headers = ["Source Object", "Source Type", "Relationship", "Target Object", "Target Type", "Server", "Instance", "Confidence", "Method"];
    const rows = sortedLineage.map(edge => [
      edge.object_name || "",
      edge.object_type || "",
      edge.relationship_type || "",
      edge.target_object_name || "",
      edge.target_object_type || "",
      edge.server_name || "",
      edge.instance_name || "",
      formatConfidence(edge.confidence_score),
      edge.discovery_method || ""
    ]);
    
    const csvContent = [
      headers.join(","),
      ...rows.map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(","))
    ].join("\n");
    
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    link.setAttribute("href", url);
    link.setAttribute("download", `lineage_mssql_export_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }, [sortedLineage, formatConfidence]);

  if ((loading && lineage.length === 0 && viewMode === "table") || (loadingTree && allEdges.length === 0 && viewMode === "tree")) {
    return (
      <Container>
        <Header>Data Lineage - MSSQL</Header>
        <LoadingOverlay>Loading data lineage...</LoadingOverlay>
      </Container>
    );
  }

  console.log("MSSQL Component Render - Metrics State:", metrics);
  console.log("MSSQL Component Render - Metrics total_relationships:", metrics.total_relationships);
  console.log("MSSQL Component Render - Metrics type:", typeof metrics.total_relationships);

  return (
    <Container>
      <Header>Data Lineage - MSSQL</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}
      
      <MetricsGrid $columns="repeat(auto-fit, minmax(180px, 1fr))">
        <MetricCard $index={0}>
          <MetricLabel>
            <span>→</span>
            Total Relationships
          </MetricLabel>
          <MetricValue>{metrics.total_relationships || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={1}>
          <MetricLabel>
            <span>■</span>
            Unique Objects
          </MetricLabel>
          <MetricValue>{metrics.unique_objects || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={2}>
          <MetricLabel>
            <span>[S]</span>
            Unique Servers
          </MetricLabel>
          <MetricValue>{metrics.unique_servers || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={3}>
          <MetricLabel>
            <span>[I]</span>
            Unique Instances
          </MetricLabel>
          <MetricValue>{metrics.unique_instances || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={4}>
          <MetricLabel>
            <span>[DB]</span>
            Unique Databases
          </MetricLabel>
          <MetricValue>{metrics.unique_databases || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={5}>
          <MetricLabel>
            <span>[SC]</span>
            Unique Schemas
          </MetricLabel>
          <MetricValue>{metrics.unique_schemas || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={6}>
          <MetricLabel>
            <span>&lt;-&gt;</span>
            Relationship Types
          </MetricLabel>
          <MetricValue>{metrics.unique_relationship_types || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={7}>
          <MetricLabel>
            <span>✓</span>
            High Confidence
          </MetricLabel>
          <MetricValue>{metrics.high_confidence || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={8}>
          <MetricLabel>
            <span>[!]</span>
            Low Confidence
          </MetricLabel>
          <MetricValue>{metrics.low_confidence || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={9}>
          <MetricLabel>
            <span>%</span>
            Avg Confidence
          </MetricLabel>
          <MetricValue>{metrics.avg_confidence ? `${(Number(metrics.avg_confidence) * 100).toFixed(1)}%` : 'N/A'}</MetricValue>
        </MetricCard>
        <MetricCard $index={10}>
          <MetricLabel>
            <span>[!]</span>
            Total Executions
          </MetricLabel>
          <MetricValue>{formatNumber(metrics.total_executions)}</MetricValue>
        </MetricCard>
        <MetricCard $index={11}>
          <MetricLabel>
            <span>[T]</span>
            Avg Duration
          </MetricLabel>
          <MetricValue>{formatTime(metrics.avg_duration_ms)}</MetricValue>
        </MetricCard>
        <MetricCard $index={12}>
          <MetricLabel>
            <span>[+]</span>
            Discovered (24h)
          </MetricLabel>
          <MetricValue>{metrics.discovered_last_24h || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={13}>
          <MetricLabel>
            <span>[*]</span>
            Discovery Methods
          </MetricLabel>
          <MetricValue>{metrics.unique_discovery_methods || 0}</MetricValue>
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
          value={filters.instance_name as string}
          onChange={(e) => handleFilterChange('instance_name', e.target.value)}
          disabled={!filters.server_name}
        >
          <option value="">All Instances</option>
          {instances.map(instance => (
            <option key={instance} value={instance}>{instance}</option>
          ))}
        </Select>
        
        <Select
          value={filters.database_name as string}
          onChange={(e) => handleFilterChange('database_name', e.target.value)}
          disabled={!filters.server_name || !filters.instance_name}
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
          <option value="FOREIGN_KEY">FOREIGN_KEY</option>
        </Select>
        
        <Select
          value={filters.relationship_type as string}
          onChange={(e) => handleFilterChange('relationship_type', e.target.value)}
        >
          <option value="">All Relationships</option>
          <option value="DEPENDS_ON">DEPENDS_ON</option>
          <option value="REFERENCES">REFERENCES</option>
          <option value="CONTAINS">CONTAINS</option>
          <option value="EXECUTES">EXECUTES</option>
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
            ? `Showing ${sortedLineage.length} of ${pagination.total} relationships (Page ${pagination.currentPage} of ${pagination.totalPages})`
            : `Total: ${allEdges.length} relationships`
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
              fetchAllEdges();
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
          <DataLineageMSSQLTreeView edges={allEdges} onEdgeClick={(edge) => toggleEdge(edge.id)} />
        )
      ) : (
        <StyledTableContainer>
        <Table $minWidth="1400px">
          <thead>
            <tr>
              <SortableTh 
                $sortable 
                $active={sortField === "object_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("object_name")}
              >
                Source Object
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "object_type"} 
                $direction={sortDirection}
                onClick={() => handleSort("object_type")}
              >
                Source Type
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "relationship_type"} 
                $direction={sortDirection}
                onClick={() => handleSort("relationship_type")}
              >
                Relationship
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "target_object_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("target_object_name")}
              >
                Target Object
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "target_object_type"} 
                $direction={sortDirection}
                onClick={() => handleSort("target_object_type")}
              >
                Target Type
              </SortableTh>
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
                $active={sortField === "instance_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("instance_name")}
              >
                Instance
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "confidence_score"} 
                $direction={sortDirection}
                onClick={() => handleSort("confidence_score")}
              >
                Confidence
              </SortableTh>
              <Th>Method</Th>
            </tr>
          </thead>
          <tbody>
            {sortedLineage.length === 0 ? (
              <TableRow>
                <Td colSpan={9} style={{ padding: '60px 40px', textAlign: 'center', color: theme.colors.text.secondary }}>
                  <div style={{ 
                    fontSize: '3em', 
                    marginBottom: theme.spacing.md,
                    animation: `${fadeIn} 0.5s ease-out`,
                    fontFamily: "'Courier New', monospace",
                    opacity: 0.5
                  }}>
                    →
                  </div>
                  <div style={{ 
                    fontSize: '1.1em',
                    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", "Roboto", "Oxygen", "Ubuntu", "Cantarell", sans-serif',
                    fontWeight: 500,
                    marginBottom: theme.spacing.sm
                  }}>
                    No lineage data available
                  </div>
                  <div style={{ fontSize: '0.9em', opacity: 0.7 }}>
                    Lineage relationships will appear here once extracted.
                  </div>
                </Td>
              </TableRow>
            ) : (
              sortedLineage.map((edge, index) => (
                <React.Fragment key={edge.id}>
                  <StyledTableRow onClick={() => toggleEdge(edge.id)} style={{ cursor: 'pointer' }} $delay={index}>
                    <Td>
                      <strong style={{ color: theme.colors.primary.main }}>
                        {edge.object_name || 'N/A'}
                      </strong>
                      {edge.column_name && (
                        <div style={{ fontSize: '0.8em', color: theme.colors.text.secondary }}>.{edge.column_name}</div>
                      )}
                    </Td>
                    <Td>
                      <Badge $type={edge.object_type}>{edge.object_type || 'N/A'}</Badge>
                    </Td>
                    <Td>
                      <RelationshipArrow>→</RelationshipArrow>
                      <div style={{ fontSize: '0.75em', color: theme.colors.text.secondary, marginTop: '4px' }}>
                        {edge.relationship_type || 'N/A'}
                      </div>
                    </Td>
                    <Td>
                      <strong>{edge.target_object_name || 'N/A'}</strong>
                      {edge.target_column_name && (
                        <div style={{ fontSize: '0.8em', color: theme.colors.text.secondary }}>.{edge.target_column_name}</div>
                      )}
                    </Td>
                    <Td>
                      <Badge $type={edge.target_object_type}>{edge.target_object_type || 'N/A'}</Badge>
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {edge.server_name || 'N/A'}
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {edge.instance_name || 'N/A'}
                    </Td>
                    <Td>
                      <Badge $level={edge.confidence_score ? (edge.confidence_score >= 0.8 ? 0 : edge.confidence_score >= 0.5 ? 1 : 2) : 2}>
                        {formatConfidence(edge.confidence_score)}
                      </Badge>
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {edge.discovery_method || 'N/A'}
                    </Td>
                  </StyledTableRow>
                  {openEdgeId === edge.id && (
                    <TableRow>
                      <Td colSpan={9} style={{ padding: 0, border: 'none' }}>
                        <LineageDetails $isOpen={openEdgeId === edge.id}>
                <DetailGrid>
                  <DetailLabel>Edge Key:</DetailLabel>
                  <DetailValue>{edge.edge_key || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Schema:</DetailLabel>
                  <DetailValue>{edge.schema_name || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Source Column:</DetailLabel>
                  <DetailValue>{edge.column_name || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Target Column:</DetailLabel>
                  <DetailValue>{edge.target_column_name || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Dependency Level:</DetailLabel>
                  <DetailValue>{edge.dependency_level !== null && edge.dependency_level !== undefined ? edge.dependency_level : 'N/A'}</DetailValue>
                  
                  <DetailLabel>Discovered By:</DetailLabel>
                  <DetailValue>{edge.discovered_by || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Consumer Type:</DetailLabel>
                  <DetailValue>{edge.consumer_type || 'N/A'}</DetailValue>
                  
                  <DetailLabel>Consumer Name:</DetailLabel>
                  <DetailValue>{edge.consumer_name || 'N/A'}</DetailValue>
                  
                  <DetailLabel>First Seen:</DetailLabel>
                  <DetailValue>{formatDate(edge.first_seen_at)}</DetailValue>
                  
                  <DetailLabel>Last Seen:</DetailLabel>
                  <DetailValue>{formatDate(edge.last_seen_at)}</DetailValue>
                  
                  <DetailLabel>Created At:</DetailLabel>
                  <DetailValue>{formatDate(edge.created_at)}</DetailValue>
                  
                  <DetailLabel>Updated At:</DetailLabel>
                  <DetailValue>{formatDate(edge.updated_at)}</DetailValue>
                </DetailGrid>
                
                {(edge.execution_count || edge.avg_duration_ms || edge.avg_cpu_ms || edge.avg_logical_reads || edge.avg_physical_reads) && (
                  <PerformanceGrid $columns="repeat(auto-fit, minmax(200px, 1fr))">
                    <PerformanceMetric>
                      <PerformanceLabel>Execution Count</PerformanceLabel>
                      <PerformanceValue>{formatNumber(edge.execution_count)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>Avg Duration</PerformanceLabel>
                      <PerformanceValue>{formatTime(edge.avg_duration_ms)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>Avg CPU Time</PerformanceLabel>
                      <PerformanceValue>{formatTime(edge.avg_cpu_ms)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>Avg Logical Reads</PerformanceLabel>
                      <PerformanceValue>{formatNumber(edge.avg_logical_reads)}</PerformanceValue>
                    </PerformanceMetric>
                    <PerformanceMetric>
                      <PerformanceLabel>Avg Physical Reads</PerformanceLabel>
                      <PerformanceValue>{formatNumber(edge.avg_physical_reads)}</PerformanceValue>
                    </PerformanceMetric>
                  </PerformanceGrid>
                )}
                
                {edge.definition_text && (
                  <>
                    <div style={{ padding: '15px 15px 5px 15px', fontWeight: 'bold', color: theme.colors.text.secondary }}>
                      Definition:
                    </div>
                    <DefinitionText>{edge.definition_text}</DefinitionText>
                  </>
                )}
                        </LineageDetails>
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

export default DataLineageMSSQL;
