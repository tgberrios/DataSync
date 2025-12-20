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
import { dataLineageMongoDBApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { sanitizeSearch } from '../utils/validation';
import { theme } from '../theme/theme';
import DataLineageMongoDBTreeView from './DataLineageMongoDBTreeView';

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

const DataLineageMongoDB = () => {
  const { page, limit, setPage } = usePagination(1, 20);
  const { filters, setFilter, clearFilters } = useTableFilters({
    server_name: '',
    database_name: '',
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
        dataLineageMongoDBApi.getMongoDBLineage({
          page,
          limit,
          server_name: filters.server_name as string,
          database_name: filters.database_name as string,
          relationship_type: filters.relationship_type as string,
          search: sanitizedSearch
        }),
        dataLineageMongoDBApi.getMongoDBMetrics(),
        dataLineageMongoDBApi.getMongoDBServers()
      ]);
      if (isMountedRef.current) {
        console.log("MongoDB Lineage Data:", lineageData);
        console.log("MongoDB Metrics Data:", metricsData);
        console.log("MongoDB Metrics Data Type:", typeof metricsData);
        console.log("MongoDB Metrics Data Keys:", metricsData ? Object.keys(metricsData) : 'null');
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
    filters.database_name, 
    filters.relationship_type, 
    filters.search
  ]);

  const fetchMetrics = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      console.log("MongoDB fetchMetrics: Starting...");
      const metricsData = await dataLineageMongoDBApi.getMongoDBMetrics().catch(err => {
        console.error("MongoDB getMongoDBMetrics error:", err);
        throw err;
      });
      console.log("MongoDB fetchMetrics: Received data:", metricsData);
      if (isMountedRef.current) {
        setMetrics(metricsData || {});
      }
    } catch (err) {
      console.error("MongoDB fetchMetrics error:", err);
    }
  }, []);

  const fetchAllEdges = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoadingTree(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(filters.search as string, 100);
      const lineageData = await dataLineageMongoDBApi.getMongoDBLineage({
        page: 1,
        limit: 10000,
        server_name: filters.server_name as string,
        database_name: filters.database_name as string,
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
    filters.database_name,
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
    const fetchDatabases = async () => {
      if (filters.server_name && isMountedRef.current) {
        try {
          const databasesData = await dataLineageMongoDBApi.getMongoDBDatabases(filters.server_name as string);
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
    const headers = ["Source Collection", "Source Field", "Relationship", "Target Collection", "Target Field", "Server", "Database", "Confidence", "Method"];
    const rows = sortedLineage.map(edge => [
      edge.source_collection || "",
      edge.source_field || "",
      edge.relationship_type || "",
      edge.target_collection || "",
      edge.target_field || "",
      edge.server_name || "",
      edge.database_name || "",
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
    link.setAttribute("download", `lineage_mongodb_export_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }, [sortedLineage, formatConfidence]);

  if ((loading && lineage.length === 0 && viewMode === "table") || (loadingTree && allEdges.length === 0 && viewMode === "tree")) {
    return (
      <Container>
        <Header>Data Lineage - MongoDB</Header>
        <LoadingOverlay>Loading data lineage...</LoadingOverlay>
      </Container>
    );
  }

  console.log("MongoDB Component Render - Metrics State:", metrics);
  console.log("MongoDB Component Render - Metrics total_relationships:", metrics.total_relationships);
  console.log("MongoDB Component Render - Metrics type:", typeof metrics.total_relationships);

  return (
    <Container>
      <Header>Data Lineage - MongoDB</Header>
      
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
            <span>[C]</span>
            Unique Collections
          </MetricLabel>
          <MetricValue>{metrics.unique_collections || 0}</MetricValue>
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
            <span>[DB]</span>
            Unique Databases
          </MetricLabel>
          <MetricValue>{metrics.unique_databases || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={4}>
          <MetricLabel>
            <span>[SC]</span>
            Unique Schemas
          </MetricLabel>
          <MetricValue>{metrics.unique_schemas || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={5}>
          <MetricLabel>
            <span>&lt;-&gt;</span>
            Relationship Types
          </MetricLabel>
          <MetricValue>{metrics.unique_relationship_types || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={6}>
          <MetricLabel>
            <span>✓</span>
            High Confidence
          </MetricLabel>
          <MetricValue>{metrics.high_confidence || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={7}>
          <MetricLabel>
            <span>[!]</span>
            Low Confidence
          </MetricLabel>
          <MetricValue>{metrics.low_confidence || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={8}>
          <MetricLabel>
            <span>%</span>
            Avg Confidence
          </MetricLabel>
          <MetricValue>{metrics.avg_confidence ? `${(Number(metrics.avg_confidence) * 100).toFixed(1)}%` : 'N/A'}</MetricValue>
        </MetricCard>
        <MetricCard $index={9}>
          <MetricLabel>
            <span>[#]</span>
            Avg Dependency Level
          </MetricLabel>
          <MetricValue>{metrics.avg_dependency_level ? Number(metrics.avg_dependency_level).toFixed(1) : 'N/A'}</MetricValue>
        </MetricCard>
        <MetricCard $index={10}>
          <MetricLabel>
            <span>[+]</span>
            Discovered (24h)
          </MetricLabel>
          <MetricValue>{metrics.discovered_last_24h || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={11}>
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
          value={filters.relationship_type as string}
          onChange={(e) => handleFilterChange('relationship_type', e.target.value)}
        >
          <option value="">All Relationships</option>
          <option value="VIEW_DEPENDENCY">VIEW_DEPENDENCY</option>
          <option value="AGGREGATION_PIPELINE">AGGREGATION_PIPELINE</option>
          <option value="REFERENCE">REFERENCE</option>
          <option value="EMBEDDED">EMBEDDED</option>
        </Select>
        
        <Input
          type="text"
          placeholder="Search collection name..."
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
          <DataLineageMongoDBTreeView edges={allEdges} onEdgeClick={(edge) => toggleEdge(edge.id)} />
        )
      ) : (
        <StyledTableContainer>
        <Table $minWidth="1400px">
          <thead>
            <tr>
              <SortableTh 
                $sortable 
                $active={sortField === "source_collection"} 
                $direction={sortDirection}
                onClick={() => handleSort("source_collection")}
              >
                Source Collection
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "source_field"} 
                $direction={sortDirection}
                onClick={() => handleSort("source_field")}
              >
                Source Field
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
                $active={sortField === "target_collection"} 
                $direction={sortDirection}
                onClick={() => handleSort("target_collection")}
              >
                Target Collection
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "target_field"} 
                $direction={sortDirection}
                onClick={() => handleSort("target_field")}
              >
                Target Field
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
                $active={sortField === "database_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("database_name")}
              >
                Database
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
                        {edge.source_collection || 'N/A'}
                      </strong>
                      {edge.source_field && (
                        <div style={{ fontSize: '0.8em', color: theme.colors.text.secondary }}>.{edge.source_field}</div>
                      )}
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {edge.source_field || 'N/A'}
                    </Td>
                    <Td>
                      <RelationshipArrow>→</RelationshipArrow>
                      <div style={{ fontSize: '0.75em', color: theme.colors.text.secondary, marginTop: '4px' }}>
                        {edge.relationship_type || 'N/A'}
                      </div>
                    </Td>
                    <Td>
                      <strong>{edge.target_collection || 'N/A'}</strong>
                      {edge.target_field && (
                        <div style={{ fontSize: '0.8em', color: theme.colors.text.secondary }}>.{edge.target_field}</div>
                      )}
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {edge.target_field || 'N/A'}
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {edge.server_name || 'N/A'}
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {edge.database_name || 'N/A'}
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
                            
                            <DetailLabel>Source Collection:</DetailLabel>
                            <DetailValue>{edge.source_collection || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Source Field:</DetailLabel>
                            <DetailValue>{edge.source_field || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Target Collection:</DetailLabel>
                            <DetailValue>{edge.target_collection || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Target Field:</DetailLabel>
                            <DetailValue>{edge.target_field || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Dependency Level:</DetailLabel>
                            <DetailValue>{edge.dependency_level !== null && edge.dependency_level !== undefined ? edge.dependency_level : 'N/A'}</DetailValue>
                            
                            <DetailLabel>Discovery Method:</DetailLabel>
                            <DetailValue>{edge.discovery_method || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Snapshot Date:</DetailLabel>
                            <DetailValue>{formatDate(edge.snapshot_date)}</DetailValue>
                          </DetailGrid>
                          
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

export default DataLineageMongoDB;
