import { useState, useEffect, useCallback, useRef } from 'react';
import styled, { keyframes } from 'styled-components';
import {
  Container,
  Header,
  ErrorMessage,
  LoadingOverlay,
  Grid,
  Value,
  FiltersContainer,
  Select,
  Input,
  Button,
} from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { governanceCatalogMongoDBApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { sanitizeSearch } from '../utils/validation';
import { theme } from '../theme/theme';
import GovernanceCatalogMongoDBTreeView from './GovernanceCatalogMongoDBTreeView';

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
  animation: ${fadeIn} 0.25s ease-in;
`;

const DetailLabel = styled.div`
  color: ${theme.colors.text.secondary};
  font-weight: 500;
  font-size: 0.85em;
  margin-bottom: 5px;
`;

const DetailValue = styled.div`
  color: ${theme.colors.text.primary};
  font-size: 1.1em;
  font-weight: 500;
`;

const MainLayout = styled.div<{ $hasDetails?: boolean }>`
  display: grid;
  grid-template-columns: ${props => props.$hasDetails ? '1fr 500px' : '1fr'};
  gap: ${theme.spacing.lg};
  margin-top: ${theme.spacing.lg};
`;

const DetailsPanel = styled.div`
  background: ${theme.colors.background.main};
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.lg};
  padding: ${theme.spacing.lg};
  overflow-y: auto;
  box-shadow: ${theme.shadows.md};
  position: sticky;
  top: ${theme.spacing.md};
  max-height: calc(100vh - 200px);
  
  &::-webkit-scrollbar {
    width: 8px;
  }
  
  &::-webkit-scrollbar-track {
    background: ${theme.colors.background.secondary};
    border-radius: ${theme.borderRadius.sm};
  }
  
  &::-webkit-scrollbar-thumb {
    background: ${theme.colors.border.medium};
    border-radius: ${theme.borderRadius.sm};
    
    &:hover {
      background: ${theme.colors.primary.main};
    }
  }
`;

const TabContainer = styled.div`
  display: flex;
  gap: ${theme.spacing.xs};
  margin-bottom: ${theme.spacing.md};
  border-bottom: 2px solid ${theme.colors.border.light};
  padding-bottom: ${theme.spacing.sm};
  flex-wrap: wrap;
`;

const Tab = styled.button<{ $active: boolean }>`
  padding: ${theme.spacing.sm} ${theme.spacing.md};
  border: none;
  background: ${props => props.$active ? theme.colors.primary.main : 'transparent'};
  color: ${props => props.$active ? theme.colors.text.white : theme.colors.text.secondary};
  border-radius: ${theme.borderRadius.md} ${theme.borderRadius.md} 0 0;
  cursor: pointer;
  font-weight: ${props => props.$active ? '600' : '500'};
  font-size: 0.85em;
  transition: all ${theme.transitions.normal};
  white-space: nowrap;
  
  &:hover {
    background: ${props => props.$active ? theme.colors.primary.dark : theme.colors.background.secondary};
    color: ${props => props.$active ? theme.colors.text.white : theme.colors.text.primary};
  }
`;

const TabContent = styled.div`
  animation: ${fadeIn} 0.3s ease-out;
`;

const DetailsSection = styled.div`
  margin-bottom: ${theme.spacing.lg};
  padding-bottom: ${theme.spacing.lg};
  border-bottom: 1px solid ${theme.colors.border.light};
  
  &:last-child {
    border-bottom: none;
    margin-bottom: 0;
    padding-bottom: 0;
  }
`;

const SectionTitle = styled.h3`
  font-size: 1em;
  font-weight: 600;
  color: ${theme.colors.text.primary};
  margin: 0 0 ${theme.spacing.md} 0;
  padding-bottom: ${theme.spacing.xs};
  border-bottom: 2px solid ${theme.colors.primary.main};
`;

const DetailsGrid = styled.div`
  display: grid;
  grid-template-columns: 1fr;
  gap: ${theme.spacing.md};
`;

const Badge = styled.span<{ $status?: string }>`
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
  
  ${props => {
    if (props.$status === 'HEALTHY' || props.$status === 'EXCELLENT') {
      return `
        background: linear-gradient(135deg, ${theme.colors.status.success.bg} 0%, ${theme.colors.status.success.text}15 100%);
        color: ${theme.colors.status.success.text};
        border-color: ${theme.colors.status.success.text}40;
      `;
    }
    if (props.$status === 'WARNING') {
      return `
        background: linear-gradient(135deg, ${theme.colors.status.warning.bg} 0%, ${theme.colors.status.warning.text}15 100%);
        color: ${theme.colors.status.warning.text};
        border-color: ${theme.colors.status.warning.text}40;
      `;
    }
    if (props.$status === 'CRITICAL') {
      return `
        background: linear-gradient(135deg, ${theme.colors.status.error.bg} 0%, ${theme.colors.status.error.text}15 100%);
        color: ${theme.colors.status.error.text};
        border-color: ${theme.colors.status.error.text}40;
      `;
    }
    return `
      background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
      color: ${theme.colors.text.primary};
      border-color: ${theme.colors.border.medium};
    `;
  }}
`;

/**
 * Governance Catalog component for MongoDB
 * Displays governance metadata for MongoDB collections including health status, access frequency, and recommendations
 */
const GovernanceCatalogMongoDB = () => {
  const { setPage } = usePagination(1, 20);
  const { filters, setFilter, clearFilters } = useTableFilters({
    server_name: '',
    database_name: '',
    health_status: '',
    access_frequency: '',
    search: ''
  });
  
  const [error, setError] = useState<string | null>(null);
  const [selectedItem, setSelectedItem] = useState<any | null>(null);
  const [activeTab, setActiveTab] = useState<'overview' | 'indexes' | 'performance' | 'recommendations'>('overview');
  const [metrics, setMetrics] = useState<any>({});
  const [servers, setServers] = useState<string[]>([]);
  const [databases, setDatabases] = useState<string[]>([]);
  const [allItems, setAllItems] = useState<any[]>([]);
  const [loadingTree, setLoadingTree] = useState(false);
  const isMountedRef = useRef(true);

  const fetchAllItems = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoadingTree(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(filters.search as string, 100);
      const itemsData = await governanceCatalogMongoDBApi.getMongoDBItems({
        page: 1,
        limit: 10000,
        server_name: filters.server_name as string,
        database_name: filters.database_name as string,
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
    filters.health_status,
    filters.access_frequency,
    filters.search
  ]);

  useEffect(() => {
    isMountedRef.current = true;
    const loadData = async () => {
      console.log("MongoDB: Starting to load data...");
      await fetchAllItems();
      try {
        console.log("MongoDB: Fetching metrics...");
        const metricsData = await governanceCatalogMongoDBApi.getMongoDBMetrics();
        console.log("MongoDB: Metrics received:", metricsData);
        if (isMountedRef.current) {
          setMetrics(metricsData || {});
          console.log("MongoDB: Metrics set in state:", metricsData);
        }
      } catch (err) {
        console.error("MongoDB: Error loading metrics:", err);
        if (isMountedRef.current) {
          console.error('MongoDB: Error loading metrics:', err);
        }
      }
    };
    loadData();
    const interval = setInterval(() => {
      if (isMountedRef.current) {
        fetchAllItems();
        governanceCatalogMongoDBApi.getMongoDBMetrics().then(metricsData => {
          console.log("MongoDB: Interval metrics received:", metricsData);
          if (isMountedRef.current) {
            setMetrics(metricsData || {});
          }
        }).catch(err => {
          console.error('MongoDB: Error loading metrics in interval:', err);
        });
      }
    }, 30000);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchAllItems]);

  useEffect(() => {
    const fetchDatabases = async () => {
      if (filters.server_name && isMountedRef.current) {
        try {
          const databasesData = await governanceCatalogMongoDBApi.getMongoDBDatabases(filters.server_name as string);
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

  const handleItemClick = useCallback((item: any) => {
    setSelectedItem((prev: any) => {
      if (prev?.id === item.id) {
        return null;
      }
      return item;
    });
    setActiveTab('overview');
  }, []);

  const formatBoolean = useCallback((value: boolean | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    return value ? 'Yes' : 'No';
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

  const formatDate = useCallback((date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  }, []);

  const handleFilterChange = useCallback((key: string, value: string) => {
    setFilter(key as any, value);
    if (key === 'server_name') {
      setFilter('database_name' as any, '');
    }
    setPage(1);
  }, [setFilter, setPage]);

  const renderOverviewTab = useCallback((item: any) => {
    return (
      <TabContent>
        <DetailsSection>
          <SectionTitle>Basic Information</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Server:</DetailLabel>
            <DetailValue>{item.server_name || 'N/A'}</DetailValue>
            
            <DetailLabel>Database:</DetailLabel>
            <DetailValue>{item.database_name || 'N/A'}</DetailValue>
            
            <DetailLabel>Collection:</DetailLabel>
            <DetailValue>{item.collection_name || 'N/A'}</DetailValue>
            
            <DetailLabel>Health Status:</DetailLabel>
            <DetailValue>
              {item.health_status ? (
                <Badge $status={item.health_status}>{item.health_status}</Badge>
              ) : 'N/A'}
            </DetailValue>
            
            <DetailLabel>Access Frequency:</DetailLabel>
            <DetailValue>{item.access_frequency || 'N/A'}</DetailValue>
            
            <DetailLabel>Health Score:</DetailLabel>
            <DetailValue>
              {item.health_score !== null && item.health_score !== undefined
                ? `${Number(item.health_score).toFixed(2)}`
                : 'N/A'}
            </DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>Storage</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Document Count:</DetailLabel>
            <DetailValue>{formatNumber(item.document_count)}</DetailValue>
            
            <DetailLabel>Collection Size:</DetailLabel>
            <DetailValue>{formatBytes(item.collection_size_mb)}</DetailValue>
            
            <DetailLabel>Index Size:</DetailLabel>
            <DetailValue>{formatBytes(item.index_size_mb)}</DetailValue>
            
            <DetailLabel>Total Size:</DetailLabel>
            <DetailValue>{formatBytes(item.total_size_mb)}</DetailValue>
            
            <DetailLabel>Storage Size:</DetailLabel>
            <DetailValue>{formatBytes(item.storage_size_mb)}</DetailValue>
            
            <DetailLabel>Avg Object Size:</DetailLabel>
            <DetailValue>
              {item.avg_object_size_bytes
                ? `${(Number(item.avg_object_size_bytes) / 1024).toFixed(2)} KB`
                : 'N/A'}
            </DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>MongoDB Configuration</SectionTitle>
          <DetailsGrid>
            <DetailLabel>MongoDB Version:</DetailLabel>
            <DetailValue>{item.mongodb_version || 'N/A'}</DetailValue>
            
            <DetailLabel>Storage Engine:</DetailLabel>
            <DetailValue>{item.storage_engine || 'N/A'}</DetailValue>
            
            <DetailLabel>Replica Set:</DetailLabel>
            <DetailValue>{item.replica_set_name || 'N/A'}</DetailValue>
            
            <DetailLabel>Is Sharded:</DetailLabel>
            <DetailValue>
              <Badge $status={item.is_sharded ? 'WARNING' : 'HEALTHY'}>
                {formatBoolean(item.is_sharded)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Shard Key:</DetailLabel>
            <DetailValue>{item.shard_key || 'N/A'}</DetailValue>
            
            <DetailLabel>Index Count:</DetailLabel>
            <DetailValue>{formatNumber(item.index_count)}</DetailValue>
            
            <DetailLabel>Snapshot Date:</DetailLabel>
            <DetailValue>{formatDate(item.snapshot_date)}</DetailValue>
          </DetailsGrid>
        </DetailsSection>
      </TabContent>
    );
  }, [formatBytes, formatNumber, formatBoolean, formatDate]);

  const renderIndexesTab = useCallback((item: any) => {
    return (
      <TabContent>
        <DetailsSection>
          <SectionTitle>Index Information</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Index Name:</DetailLabel>
            <DetailValue>{item.index_name || 'N/A'}</DetailValue>
            
            <DetailLabel>Index Keys:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace' }}>
              {item.index_keys || 'N/A'}
            </DetailValue>
            
            <DetailLabel>Index Type:</DetailLabel>
            <DetailValue>{item.index_type || 'N/A'}</DetailValue>
            
            <DetailLabel>Unique Index:</DetailLabel>
            <DetailValue>
              <Badge $status={item.index_unique ? 'HEALTHY' : 'WARNING'}>
                {formatBoolean(item.index_unique)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Sparse Index:</DetailLabel>
            <DetailValue>
              <Badge $status={item.index_sparse ? 'WARNING' : 'HEALTHY'}>
                {formatBoolean(item.index_sparse)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Index Size:</DetailLabel>
            <DetailValue>{formatBytes(item.index_size_mb)}</DetailValue>
          </DetailsGrid>
        </DetailsSection>
      </TabContent>
    );
  }, [formatBoolean, formatBytes]);

  const renderPerformanceTab = useCallback((item: any) => {
    return (
      <TabContent>
        <DetailsSection>
          <SectionTitle>Performance Metrics</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Health Score:</DetailLabel>
            <DetailValue>
              {item.health_score !== null && item.health_score !== undefined
                ? `${Number(item.health_score).toFixed(2)}`
                : 'N/A'}
            </DetailValue>
            
            <DetailLabel>Document Count:</DetailLabel>
            <DetailValue>{formatNumber(item.document_count)}</DetailValue>
            
            <DetailLabel>Total Size:</DetailLabel>
            <DetailValue>{formatBytes(item.total_size_mb)}</DetailValue>
            
            <DetailLabel>Index Count:</DetailLabel>
            <DetailValue>{formatNumber(item.index_count)}</DetailValue>
            
            <DetailLabel>Avg Object Size:</DetailLabel>
            <DetailValue>
              {item.avg_object_size_bytes
                ? `${(Number(item.avg_object_size_bytes) / 1024).toFixed(2)} KB`
                : 'N/A'}
            </DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>Sharding</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Is Sharded:</DetailLabel>
            <DetailValue>
              <Badge $status={item.is_sharded ? 'WARNING' : 'HEALTHY'}>
                {formatBoolean(item.is_sharded)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Shard Key:</DetailLabel>
            <DetailValue style={{ fontFamily: 'monospace' }}>
              {item.shard_key || 'N/A'}
            </DetailValue>
          </DetailsGrid>
        </DetailsSection>
      </TabContent>
    );
  }, [formatBytes, formatNumber, formatBoolean]);

  const renderRecommendationsTab = useCallback((item: any) => {
    return (
      <TabContent>
        <DetailsSection>
          <SectionTitle>Recommendations</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Recommendation Summary:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.recommendation_summary || 'No recommendations available'}
            </DetailValue>
          </DetailsGrid>
        </DetailsSection>
      </TabContent>
    );
  }, []);

  const handleExportCSV = useCallback(() => {
    const headers = ["Server", "Database", "Collection", "Document Count", "Size (MB)", "Health", "Access", "Storage Engine"];
    const rows = allItems.map(item => [
      item.server_name || "",
      item.database_name || "",
      item.collection_name || "",
      formatNumber(item.document_count),
      item.total_size_mb || 0,
      item.health_status || "",
      item.access_frequency || "",
      item.storage_engine || ""
    ]);
    
    const csvContent = [
      headers.join(","),
      ...rows.map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(","))
    ].join("\n");
    
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    link.setAttribute("href", url);
    link.setAttribute("download", `governance_catalog_mongodb_export_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }, [allItems, formatNumber]);

  if (loadingTree && allItems.length === 0) {
    return (
      <Container>
        <Header>Governance Catalog - MongoDB</Header>
        <LoadingOverlay>Loading governance catalog...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>Governance Catalog - MongoDB</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}
      
      <MetricsGrid $columns="repeat(auto-fit, minmax(180px, 1fr))">
        {(() => {
          console.log("MongoDB: Rendering metrics cards, metrics object:", metrics);
          console.log("MongoDB: total_collections:", metrics.total_collections);
          console.log("MongoDB: total_size_mb:", metrics.total_size_mb);
          console.log("MongoDB: total_documents:", metrics.total_documents);
          console.log("MongoDB: healthy_count:", metrics.healthy_count);
          console.log("MongoDB: warning_count:", metrics.warning_count);
          console.log("MongoDB: critical_count:", metrics.critical_count);
          console.log("MongoDB: unique_servers:", metrics.unique_servers);
          return null;
        })()}
        <MetricCard $index={0}>
          <MetricLabel>
            <span>■</span>
            Total Collections
          </MetricLabel>
          <MetricValue>{metrics.total_collections || 0}</MetricValue>
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
            <span>■</span>
            Total Documents
          </MetricLabel>
          <MetricValue>{formatNumber(metrics.total_documents)}</MetricValue>
        </MetricCard>
        <MetricCard $index={3}>
          <MetricLabel>
            <span>✓</span>
            Healthy
          </MetricLabel>
          <MetricValue>{metrics.healthy_count || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={4}>
          <MetricLabel>
            <span>!</span>
            Warning
          </MetricLabel>
          <MetricValue>{metrics.warning_count || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={5}>
          <MetricLabel>
            <span>×</span>
            Critical
          </MetricLabel>
          <MetricValue>{metrics.critical_count || 0}</MetricValue>
        </MetricCard>
        <MetricCard $index={6}>
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
          Total: {allItems.length} entries
        </PaginationInfo>
        <ExportButton $variant="secondary" onClick={handleExportCSV}>
          Export CSV
        </ExportButton>
      </TableActions>

      {loadingTree ? (
        <LoadingOverlay>Loading tree view...</LoadingOverlay>
      ) : (
        <MainLayout $hasDetails={!!selectedItem}>
          <GovernanceCatalogMongoDBTreeView 
            items={allItems} 
            onItemClick={handleItemClick} 
          />
          
          {selectedItem && (
            <DetailsPanel>
              <TabContainer>
                <Tab $active={activeTab === 'overview'} onClick={() => setActiveTab('overview')}>
                  Overview
                </Tab>
                <Tab $active={activeTab === 'indexes'} onClick={() => setActiveTab('indexes')}>
                  Indexes
                </Tab>
                <Tab $active={activeTab === 'performance'} onClick={() => setActiveTab('performance')}>
                  Performance
                </Tab>
                <Tab $active={activeTab === 'recommendations'} onClick={() => setActiveTab('recommendations')}>
                  Recommendations
                </Tab>
              </TabContainer>
              
              {activeTab === 'overview' && renderOverviewTab(selectedItem)}
              {activeTab === 'indexes' && renderIndexesTab(selectedItem)}
              {activeTab === 'performance' && renderPerformanceTab(selectedItem)}
              {activeTab === 'recommendations' && renderRecommendationsTab(selectedItem)}
            </DetailsPanel>
          )}
        </MainLayout>
      )}
    </Container>
  );
};

export default GovernanceCatalogMongoDB;
