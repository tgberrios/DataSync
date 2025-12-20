import { useState, useEffect, useCallback, useRef } from 'react';
import styled, { keyframes } from 'styled-components';
import {
  Container,
  Header,
  ErrorMessage,
  LoadingOverlay,
  Select,
  FiltersContainer,
  Button,
  Grid,
  Value,
} from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { governanceApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { theme } from '../theme/theme';
import GovernanceTreeView from './GovernanceTreeView';

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
  font-size: 0.85em;
  margin-bottom: 5px;
  font-weight: 500;
`;

const DetailValue = styled.div`
  font-size: 1.1em;
  font-weight: 500;
  color: ${theme.colors.text.primary};
`;

const Badge = styled.span<{ $status?: string; $type?: string; $level?: number; type?: string }>`
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
    const badgeType = props.$status || props.$type || props.type || '';
    if (badgeType === 'EXCELLENT' || badgeType === 'HEALTHY') {
      return `
        background: linear-gradient(135deg, ${theme.colors.status.success.bg} 0%, ${theme.colors.status.success.text}15 100%);
        color: ${theme.colors.status.success.text};
        border-color: ${theme.colors.status.success.text}40;
      `;
    }
    if (badgeType === 'WARNING') {
      return `
        background: linear-gradient(135deg, ${theme.colors.status.warning.bg} 0%, ${theme.colors.status.warning.text}15 100%);
        color: ${theme.colors.status.warning.text};
        border-color: ${theme.colors.status.warning.text}40;
      `;
    }
    if (badgeType === 'CRITICAL' || badgeType === 'EMERGENCY') {
      return `
        background: linear-gradient(135deg, ${theme.colors.status.error.bg} 0%, ${theme.colors.status.error.text}15 100%);
        color: ${theme.colors.status.error.text};
        border-color: ${theme.colors.status.error.text}40;
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
    }
    return `
      background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
      color: ${theme.colors.text.primary};
      border-color: ${theme.colors.border.medium};
    `;
  }}
  
  &:hover {
    transform: translateY(-2px) scale(1.08);
    box-shadow: ${theme.shadows.lg};
    border-width: 2px;
  }
`;

const QualityScore = styled.span<{ $score: number }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.9em;
  font-weight: 500;
  display: inline-block;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    transform: scale(1.05);
    box-shadow: ${theme.shadows.sm};
  }
  background-color: ${props => {
    if (props.$score >= 90) return theme.colors.status.success.bg;
    if (props.$score >= 70) return '#f1f8e9';
    if (props.$score >= 50) return theme.colors.status.warning.bg;
    return theme.colors.status.error.bg;
  }};
  color: ${props => {
    if (props.$score >= 90) return theme.colors.status.success.text;
    if (props.$score >= 70) return '#558b2f';
    if (props.$score >= 50) return theme.colors.status.warning.text;
    return theme.colors.status.error.text;
  }};
`;

const Tooltip = styled.div`
  position: relative;
  display: inline-block;
  
  &:hover .tooltip-content {
    visibility: visible;
    opacity: 1;
  }
`;

const TooltipContent = styled.div`
  visibility: hidden;
  opacity: 0;
  position: absolute;
  z-index: 1000;
  bottom: 125%;
  left: 50%;
  transform: translateX(-50%);
  background-color: ${theme.colors.text.primary};
  color: ${theme.colors.text.white};
  text-align: center;
  border-radius: ${theme.borderRadius.sm};
  padding: 8px 12px;
  font-size: 0.85em;
  white-space: nowrap;
  min-width: 200px;
  max-width: 300px;
  white-space: normal;
  box-shadow: ${theme.shadows.lg};
  transition: opacity ${theme.transitions.normal};
  
  &:after {
    content: "";
    position: absolute;
    top: 100%;
    left: 50%;
    margin-left: -5px;
    border-width: 5px;
    border-style: solid;
    border-color: ${theme.colors.text.primary} transparent transparent transparent;
  }
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

/**
 * Governance component
 * Displays data governance catalog with filtering, sorting, and detailed information
 */
const Governance = () => {
  const { setPage } = usePagination(1, 10);
  const { filters, setFilter, clearFilters } = useTableFilters({
    engine: '',
    category: '',
    health: '',
    domain: '',
    sensitivity: ''
  });
  
  const [error, setError] = useState<string | null>(null);
  const [selectedItem, setSelectedItem] = useState<any | null>(null);
  const [activeTab, setActiveTab] = useState<'overview' | 'ownership' | 'security' | 'privacy' | 'retention' | 'legal' | 'quality' | 'integration' | 'documentation'>('overview');
  const [allItems, setAllItems] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [loadingTree, setLoadingTree] = useState(false);
  const isMountedRef = useRef(true);

  const fetchAllItems = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoadingTree(true);
      setError(null);
      const response = await governanceApi.getGovernanceData({
        page: 1,
        limit: 10000,
        engine: filters.engine as string,
        category: filters.category as string,
        health: filters.health as string,
        domain: filters.domain as string,
        sensitivity: filters.sensitivity as string
      });
      if (isMountedRef.current) {
        setAllItems(response.data || []);
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
    filters.engine,
    filters.category,
    filters.health,
    filters.domain,
    filters.sensitivity
  ]);

  useEffect(() => {
    isMountedRef.current = true;
    const loadData = async () => {
      await fetchAllItems();
      try {
        const metricsData = await governanceApi.getGovernanceMetrics();
        if (isMountedRef.current) {
          setMetrics(metricsData || {});
        }
      } catch (err) {
        if (isMountedRef.current) {
          console.error('Error loading metrics:', err);
        }
      }
    };
    loadData();
    const interval = setInterval(() => {
      if (isMountedRef.current) {
        fetchAllItems();
        governanceApi.getGovernanceMetrics().then(metricsData => {
          if (isMountedRef.current) {
            setMetrics(metricsData || {});
          }
        }).catch(err => {
          console.error('Error loading metrics:', err);
        });
      }
    }, 30000);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchAllItems]);

  const handleItemClick = useCallback((item: any) => {
    setSelectedItem((prev: any) => {
      if (prev?.id === item.id) {
        return null;
      }
      return item;
    });
    setActiveTab('overview');
  }, []);

  const formatDate = useCallback((date: string) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  }, []);

  const formatBoolean = useCallback((value: boolean | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    return value ? 'Yes' : 'No';
  }, []);

  const formatPercentage = useCallback((value: number | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    return `${Number(value).toFixed(2)}%`;
  }, []);

  const formatBytes = useCallback((mb: number | null | undefined) => {
    if (mb === null || mb === undefined) return 'N/A';
    const size = Number(mb);
    if (isNaN(size)) return 'N/A';
    if (size >= 1024) {
      return `${(size / 1024).toFixed(2)} GB`;
    }
    return `${size.toFixed(2)} MB`;
  }, []);

  const formatNumber = useCallback((value: number | string | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    const num = Number(value);
    if (isNaN(num)) return 'N/A';
    return num.toLocaleString();
  }, []);


  const handleExportCSV = useCallback(() => {
    const headers = ["Schema", "Table", "Engine", "Category", "Domain", "Health", "Sensitivity", "Quality Score", "Size (MB)", "Total Rows", "Access Frequency", "Last Analyzed"];
    const rows = allItems.map(item => [
      item.schema_name,
      item.table_name,
      item.inferred_source_engine || "",
      item.data_category || "",
      item.business_domain || "",
      item.health_status || "",
      item.sensitivity_level || "",
      item.data_quality_score || 0,
      item.table_size_mb || 0,
      item.total_rows || 0,
      item.access_frequency || "",
      formatDate(item.last_analyzed)
    ]);
    
    const csvContent = [
      headers.join(","),
      ...rows.map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(","))
    ].join("\n");
    
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    link.setAttribute("href", url);
    link.setAttribute("download", `governance_export_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }, [allItems, formatDate]);


  const getCategoryDescription = useCallback((category: string) => {
    const descriptions: { [key: string]: string } = {
      'TRANSACTIONAL': 'Data that represents business transactions and events that occur in real-time',
      'ANALYTICAL': 'Data used for analysis, reporting, and business intelligence purposes',
      'REFERENCE': 'Lookup tables and master data used across multiple systems',
      'MASTER_DATA': 'Core business entities like customers, products, and suppliers',
      'OPERATIONAL': 'Data used for day-to-day operational processes and workflows',
      'TEMPORAL': 'Time-series data that tracks changes over time',
      'GEOSPATIAL': 'Location-based data including coordinates and geographic information',
      'FINANCIAL': 'Financial transactions, accounts, and monetary data',
      'COMPLIANCE': 'Data required for regulatory compliance and auditing',
      'TECHNICAL': 'System-generated data for technical monitoring and maintenance',
      'SPORTS': 'Sports-related data including scores, statistics, and player information'
    };
    return descriptions[category] || 'Data category classification';
  }, []);

  const getHealthDescription = useCallback((health: string) => {
    const descriptions: { [key: string]: string } = {
      'EXCELLENT': 'Optimal data quality with no issues detected',
      'HEALTHY': 'Good data quality with minor or no issues',
      'WARNING': 'Some data quality issues detected that need attention',
      'CRITICAL': 'Significant data quality issues requiring immediate attention',
      'EMERGENCY': 'Severe data quality issues that may impact system functionality'
    };
    return descriptions[health] || 'Health status of the data';
  }, []);

  const getSensitivityDescription = useCallback((sensitivity: string) => {
    const descriptions: { [key: string]: string } = {
      'PUBLIC': 'Data that can be freely shared and accessed by anyone',
      'LOW': 'Data with minimal sensitivity requirements',
      'MEDIUM': 'Data with moderate sensitivity requiring controlled access',
      'HIGH': 'Data with high sensitivity requiring strict access controls',
      'CRITICAL': 'Data with critical sensitivity requiring maximum security measures'
    };
    return descriptions[sensitivity] || 'Data sensitivity level';
  }, []);

  const handleFilterChange = useCallback((key: string, value: string) => {
    setFilter(key as any, value);
    setPage(1);
  }, [setFilter, setPage]);

  const renderOverviewTab = useCallback((item: any) => {
    return (
      <TabContent>
        <DetailsSection>
          <SectionTitle>Basic Information</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Schema:</DetailLabel>
            <DetailValue>{item.schema_name || 'N/A'}</DetailValue>
            
            <DetailLabel>Table:</DetailLabel>
            <DetailValue>{item.table_name || 'N/A'}</DetailValue>
            
            <DetailLabel>Source Engine:</DetailLabel>
            <DetailValue>{item.inferred_source_engine || 'N/A'}</DetailValue>
            
            <DetailLabel>Data Category:</DetailLabel>
            <DetailValue>
              {item.data_category ? (
                <Tooltip>
                  <Badge $type={item.data_category}>{item.data_category}</Badge>
                  <TooltipContent className="tooltip-content">
                    {getCategoryDescription(item.data_category)}
                  </TooltipContent>
                </Tooltip>
              ) : 'N/A'}
            </DetailValue>
            
            <DetailLabel>Business Domain:</DetailLabel>
            <DetailValue>{item.business_domain || 'N/A'}</DetailValue>
            
            <DetailLabel>Sensitivity Level:</DetailLabel>
            <DetailValue>
              {item.sensitivity_level ? (
                <Tooltip>
                  <Badge $status={item.sensitivity_level}>{item.sensitivity_level}</Badge>
                  <TooltipContent className="tooltip-content">
                    {getSensitivityDescription(item.sensitivity_level)}
                  </TooltipContent>
                </Tooltip>
              ) : 'N/A'}
            </DetailValue>
            
            <DetailLabel>Health Status:</DetailLabel>
            <DetailValue>
              {item.health_status ? (
                <Tooltip>
                  <Badge $status={item.health_status}>{item.health_status}</Badge>
                  <TooltipContent className="tooltip-content">
                    {getHealthDescription(item.health_status)}
                  </TooltipContent>
                </Tooltip>
              ) : 'N/A'}
            </DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>Metrics</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Total Columns:</DetailLabel>
            <DetailValue>{item.total_columns?.toLocaleString() || 'N/A'}</DetailValue>
            
            <DetailLabel>Total Rows:</DetailLabel>
            <DetailValue>{item.total_rows?.toLocaleString() || 'N/A'}</DetailValue>
            
            <DetailLabel>Table Size:</DetailLabel>
            <DetailValue>{formatBytes(item.table_size_mb)}</DetailValue>
            
            <DetailLabel>Primary Key Columns:</DetailLabel>
            <DetailValue>{item.primary_key_columns || 'N/A'}</DetailValue>
            
            <DetailLabel>Index Count:</DetailLabel>
            <DetailValue>{item.index_count?.toLocaleString() || 'N/A'}</DetailValue>
            
            <DetailLabel>Constraint Count:</DetailLabel>
            <DetailValue>{item.constraint_count?.toLocaleString() || 'N/A'}</DetailValue>
            
            <DetailLabel>Data Quality Score:</DetailLabel>
            <DetailValue>
              {item.data_quality_score !== null && item.data_quality_score !== undefined ? (
                <QualityScore $score={Number(item.data_quality_score)}>
                  {Number(item.data_quality_score).toFixed(2)}%
                </QualityScore>
              ) : 'N/A'}
            </DetailValue>
            
            <DetailLabel>Null Percentage:</DetailLabel>
            <DetailValue>{formatPercentage(item.null_percentage)}</DetailValue>
            
            <DetailLabel>Duplicate Percentage:</DetailLabel>
            <DetailValue>{formatPercentage(item.duplicate_percentage)}</DetailValue>
            
            <DetailLabel>Fragmentation:</DetailLabel>
            <DetailValue>{formatPercentage(item.fragmentation_percentage)}</DetailValue>
            
            <DetailLabel>Access Frequency:</DetailLabel>
            <DetailValue>{item.access_frequency || 'N/A'}</DetailValue>
            
            <DetailLabel>Query Count (Daily):</DetailLabel>
            <DetailValue>{item.query_count_daily?.toLocaleString() || 'N/A'}</DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>Timestamps</SectionTitle>
          <DetailsGrid>
            <DetailLabel>First Discovered:</DetailLabel>
            <DetailValue>{formatDate(item.first_discovered)}</DetailValue>
            
            <DetailLabel>Last Analyzed:</DetailLabel>
            <DetailValue>{formatDate(item.last_analyzed)}</DetailValue>
            
            <DetailLabel>Last Accessed:</DetailLabel>
            <DetailValue>{formatDate(item.last_accessed)}</DetailValue>
            
            <DetailLabel>Last Vacuum:</DetailLabel>
            <DetailValue>{formatDate(item.last_vacuum)}</DetailValue>
          </DetailsGrid>
        </DetailsSection>
      </TabContent>
    );
  }, [formatDate, formatBytes, formatPercentage, getCategoryDescription, getSensitivityDescription, getHealthDescription]);

  const renderOwnershipTab = useCallback((item: any) => {
    return (
      <TabContent>
        <DetailsSection>
          <SectionTitle>Ownership</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Data Owner:</DetailLabel>
            <DetailValue>{item.data_owner || 'N/A'}</DetailValue>
            
            <DetailLabel>Owner Email:</DetailLabel>
            <DetailValue>
              {item.owner_email ? (
                <a href={`mailto:${item.owner_email}`} style={{ color: theme.colors.primary.main }}>
                  {item.owner_email}
                </a>
              ) : 'N/A'}
            </DetailValue>
            
            <DetailLabel>Data Steward:</DetailLabel>
            <DetailValue>{item.data_steward || 'N/A'}</DetailValue>
            
            <DetailLabel>Steward Email:</DetailLabel>
            <DetailValue>
              {item.steward_email ? (
                <a href={`mailto:${item.steward_email}`} style={{ color: theme.colors.primary.main }}>
                  {item.steward_email}
                </a>
              ) : 'N/A'}
            </DetailValue>
            
            <DetailLabel>Data Custodian:</DetailLabel>
            <DetailValue>{item.data_custodian || 'N/A'}</DetailValue>
          </DetailsGrid>
        </DetailsSection>
      </TabContent>
    );
  }, []);

  const renderSecurityTab = useCallback((item: any) => {
    return (
      <TabContent>
        <DetailsSection>
          <SectionTitle>Encryption</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Encryption at Rest:</DetailLabel>
            <DetailValue>
              <Badge $status={item.encryption_at_rest ? 'HEALTHY' : 'WARNING'}>
                {formatBoolean(item.encryption_at_rest)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Encryption in Transit:</DetailLabel>
            <DetailValue>
              <Badge $status={item.encryption_in_transit ? 'HEALTHY' : 'WARNING'}>
                {formatBoolean(item.encryption_in_transit)}
              </Badge>
            </DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>Data Masking</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Masking Policy Applied:</DetailLabel>
            <DetailValue>
              <Badge $status={item.masking_policy_applied ? 'HEALTHY' : 'WARNING'}>
                {formatBoolean(item.masking_policy_applied)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Masking Policy Name:</DetailLabel>
            <DetailValue>{item.masking_policy_name || 'N/A'}</DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>Access Control</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Row Level Security:</DetailLabel>
            <DetailValue>
              <Badge $status={item.row_level_security_enabled ? 'HEALTHY' : 'WARNING'}>
                {formatBoolean(item.row_level_security_enabled)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Column Level Security:</DetailLabel>
            <DetailValue>
              <Badge $status={item.column_level_security_enabled ? 'HEALTHY' : 'WARNING'}>
                {formatBoolean(item.column_level_security_enabled)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Access Control Policy:</DetailLabel>
            <DetailValue>{item.access_control_policy || 'N/A'}</DetailValue>
          </DetailsGrid>
        </DetailsSection>
      </TabContent>
    );
  }, [formatBoolean]);

  const renderPrivacyTab = useCallback((item: any) => {
    return (
      <TabContent>
        <DetailsSection>
          <SectionTitle>Consent & Legal Basis</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Consent Required:</DetailLabel>
            <DetailValue>
              <Badge $status={item.consent_required ? 'WARNING' : 'HEALTHY'}>
                {formatBoolean(item.consent_required)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Consent Type:</DetailLabel>
            <DetailValue>{item.consent_type || 'N/A'}</DetailValue>
            
            <DetailLabel>Legal Basis:</DetailLabel>
            <DetailValue>{item.legal_basis || 'N/A'}</DetailValue>
            
            <DetailLabel>Data Subject Rights:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.data_subject_rights || 'N/A'}
            </DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>PII/PHI Detection</SectionTitle>
          <DetailsGrid>
            <DetailLabel>PII Detection Method:</DetailLabel>
            <DetailValue>{item.pii_detection_method || 'N/A'}</DetailValue>
            
            <DetailLabel>PII Confidence Score:</DetailLabel>
            <DetailValue>
              {item.pii_confidence_score !== null && item.pii_confidence_score !== undefined
                ? `${Number(item.pii_confidence_score).toFixed(2)}%`
                : 'N/A'}
            </DetailValue>
            
            <DetailLabel>PII Categories:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.pii_categories || 'N/A'}
            </DetailValue>
            
            <DetailLabel>PHI Detection Method:</DetailLabel>
            <DetailValue>{item.phi_detection_method || 'N/A'}</DetailValue>
            
            <DetailLabel>PHI Confidence Score:</DetailLabel>
            <DetailValue>
              {item.phi_confidence_score !== null && item.phi_confidence_score !== undefined
                ? `${Number(item.phi_confidence_score).toFixed(2)}%`
                : 'N/A'}
            </DetailValue>
            
            <DetailLabel>Sensitive Data Count:</DetailLabel>
            <DetailValue>{item.sensitive_data_count?.toLocaleString() || '0'}</DetailValue>
            
            <DetailLabel>Last PII Scan:</DetailLabel>
            <DetailValue>{formatDate(item.last_pii_scan)}</DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>Cross-Border Transfer</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Cross-Border Transfer:</DetailLabel>
            <DetailValue>
              <Badge $status={item.cross_border_transfer ? 'WARNING' : 'HEALTHY'}>
                {formatBoolean(item.cross_border_transfer)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Cross-Border Countries:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.cross_border_countries || 'N/A'}
            </DetailValue>
            
            <DetailLabel>Data Processing Agreement:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.data_processing_agreement || 'N/A'}
            </DetailValue>
            
            <DetailLabel>Privacy Impact Assessment:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.privacy_impact_assessment || 'N/A'}
            </DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>Breach Management</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Breach Notification Required:</DetailLabel>
            <DetailValue>
              <Badge $status={item.breach_notification_required ? 'WARNING' : 'HEALTHY'}>
                {formatBoolean(item.breach_notification_required)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Last Breach Check:</DetailLabel>
            <DetailValue>{formatDate(item.last_breach_check)}</DetailValue>
          </DetailsGrid>
        </DetailsSection>
      </TabContent>
    );
  }, [formatBoolean, formatDate]);

  const renderRetentionTab = useCallback((item: any) => {
    return (
      <TabContent>
        <DetailsSection>
          <SectionTitle>Retention Policy</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Retention Enforced:</DetailLabel>
            <DetailValue>
              <Badge $status={item.retention_enforced ? 'HEALTHY' : 'WARNING'}>
                {formatBoolean(item.retention_enforced)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Data Expiration Date:</DetailLabel>
            <DetailValue>{formatDate(item.data_expiration_date)}</DetailValue>
            
            <DetailLabel>Auto Delete Enabled:</DetailLabel>
            <DetailValue>
              <Badge $status={item.auto_delete_enabled ? 'WARNING' : 'HEALTHY'}>
                {formatBoolean(item.auto_delete_enabled)}
              </Badge>
            </DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>Archival</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Archival Policy:</DetailLabel>
            <DetailValue>{item.archival_policy || 'N/A'}</DetailValue>
            
            <DetailLabel>Archival Location:</DetailLabel>
            <DetailValue>{item.archival_location || 'N/A'}</DetailValue>
            
            <DetailLabel>Last Archived At:</DetailLabel>
            <DetailValue>{formatDate(item.last_archived_at)}</DetailValue>
          </DetailsGrid>
        </DetailsSection>
      </TabContent>
    );
  }, [formatBoolean, formatDate]);

  const renderLegalTab = useCallback((item: any) => {
    return (
      <TabContent>
        <DetailsSection>
          <SectionTitle>Legal Hold</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Legal Hold:</DetailLabel>
            <DetailValue>
              <Badge $status={item.legal_hold ? 'CRITICAL' : 'HEALTHY'}>
                {formatBoolean(item.legal_hold)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Legal Hold Reason:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.legal_hold_reason || 'N/A'}
            </DetailValue>
            
            <DetailLabel>Legal Hold Until:</DetailLabel>
            <DetailValue>{formatDate(item.legal_hold_until)}</DetailValue>
          </DetailsGrid>
        </DetailsSection>
      </TabContent>
    );
  }, [formatBoolean, formatDate]);

  const renderQualityTab = useCallback((item: any) => {
    return (
      <TabContent>
        <DetailsSection>
          <SectionTitle>Quality Metrics</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Quality SLA Score:</DetailLabel>
            <DetailValue>
              {item.quality_sla_score !== null && item.quality_sla_score !== undefined ? (
                <QualityScore $score={Number(item.quality_sla_score)}>
                  {Number(item.quality_sla_score).toFixed(2)}%
                </QualityScore>
              ) : 'N/A'}
            </DetailValue>
            
            <DetailLabel>Quality Checks Automated:</DetailLabel>
            <DetailValue>
              <Badge $status={item.quality_checks_automated ? 'HEALTHY' : 'WARNING'}>
                {formatBoolean(item.quality_checks_automated)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Anomaly Detection Enabled:</DetailLabel>
            <DetailValue>
              <Badge $status={item.anomaly_detection_enabled ? 'HEALTHY' : 'WARNING'}>
                {formatBoolean(item.anomaly_detection_enabled)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Last Anomaly Detected:</DetailLabel>
            <DetailValue>{formatDate(item.last_anomaly_detected)}</DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>Data Freshness</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Freshness Threshold (Hours):</DetailLabel>
            <DetailValue>{item.data_freshness_threshold_hours?.toLocaleString() || 'N/A'}</DetailValue>
            
            <DetailLabel>Last Freshness Check:</DetailLabel>
            <DetailValue>{formatDate(item.last_freshness_check)}</DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>Schema Evolution</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Schema Evolution Tracking:</DetailLabel>
            <DetailValue>
              <Badge $status={item.schema_evolution_tracking ? 'HEALTHY' : 'WARNING'}>
                {formatBoolean(item.schema_evolution_tracking)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Last Schema Change:</DetailLabel>
            <DetailValue>{formatDate(item.last_schema_change)}</DetailValue>
          </DetailsGrid>
        </DetailsSection>
      </TabContent>
    );
  }, [formatBoolean, formatDate]);

  const renderIntegrationTab = useCallback((item: any) => {
    return (
      <TabContent>
        <DetailsSection>
          <SectionTitle>ETL Pipeline</SectionTitle>
          <DetailsGrid>
            <DetailLabel>ETL Pipeline Name:</DetailLabel>
            <DetailValue>{item.etl_pipeline_name || 'N/A'}</DetailValue>
            
            <DetailLabel>ETL Pipeline ID:</DetailLabel>
            <DetailValue>{item.etl_pipeline_id || 'N/A'}</DetailValue>
            
            <DetailLabel>Transformation Rules:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.transformation_rules || 'N/A'}
            </DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>Systems</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Source Systems:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.source_systems || 'N/A'}
            </DetailValue>
            
            <DetailLabel>Downstream Systems:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.downstream_systems || 'N/A'}
            </DetailValue>
            
            <DetailLabel>BI Tools Used:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.bi_tools_used || 'N/A'}
            </DetailValue>
            
            <DetailLabel>API Endpoints:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.api_endpoints || 'N/A'}
            </DetailValue>
          </DetailsGrid>
        </DetailsSection>
      </TabContent>
    );
  }, []);

  const renderDocumentationTab = useCallback((item: any) => {
    return (
      <TabContent>
        <DetailsSection>
          <SectionTitle>Documentation</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Business Glossary Term:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.business_glossary_term || 'N/A'}
            </DetailValue>
            
            <DetailLabel>Data Dictionary Description:</DetailLabel>
            <DetailValue style={{ whiteSpace: 'pre-wrap' }}>
              {item.data_dictionary_description || 'N/A'}
            </DetailValue>
          </DetailsGrid>
        </DetailsSection>

        <DetailsSection>
          <SectionTitle>Approval</SectionTitle>
          <DetailsGrid>
            <DetailLabel>Approval Required:</DetailLabel>
            <DetailValue>
              <Badge $status={item.approval_required ? 'WARNING' : 'HEALTHY'}>
                {formatBoolean(item.approval_required)}
              </Badge>
            </DetailValue>
            
            <DetailLabel>Last Approved By:</DetailLabel>
            <DetailValue>{item.last_approved_by || 'N/A'}</DetailValue>
            
            <DetailLabel>Last Approved At:</DetailLabel>
            <DetailValue>{formatDate(item.last_approved_at)}</DetailValue>
          </DetailsGrid>
        </DetailsSection>
      </TabContent>
    );
  }, [formatBoolean, formatDate]);

  if (loadingTree && allItems.length === 0) {
    return (
      <Container>
        <Header>Data Governance Catalog</Header>
        <LoadingOverlay>Loading governance data...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>Data Governance Catalog</Header>

      {error && <ErrorMessage>{error}</ErrorMessage>}

      <MetricsGrid $columns="repeat(auto-fit, minmax(180px, 1fr))">
        <MetricCard $index={0}>
          <MetricLabel>
            <span>■</span>
            Total Tables
          </MetricLabel>
          <MetricValue>{formatNumber(metrics.total_tables)}</MetricValue>
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
            Total Rows
          </MetricLabel>
          <MetricValue>{formatNumber(metrics.total_rows)}</MetricValue>
        </MetricCard>
        <MetricCard $index={3}>
          <MetricLabel>
            <span>✓</span>
            Healthy
          </MetricLabel>
          <MetricValue>{formatNumber(metrics.healthy_count)}</MetricValue>
        </MetricCard>
        <MetricCard $index={4}>
          <MetricLabel>
            <span>!</span>
            Warning
          </MetricLabel>
          <MetricValue>{formatNumber(metrics.warning_count)}</MetricValue>
        </MetricCard>
        <MetricCard $index={5}>
          <MetricLabel>
            <span>×</span>
            Critical
          </MetricLabel>
          <MetricValue>{formatNumber(metrics.critical_count)}</MetricValue>
        </MetricCard>
        <MetricCard $index={6}>
          <MetricLabel>
            <span>■</span>
            Unique Engines
          </MetricLabel>
          <MetricValue>{formatNumber(metrics.unique_engines)}</MetricValue>
        </MetricCard>
      </MetricsGrid>

      <FiltersContainer>
        <Select 
          value={filters.engine as string}
          onChange={(e) => handleFilterChange('engine', e.target.value)}
        >
          <option value="">All Engines</option>
          <option value="PostgreSQL">PostgreSQL</option>
          <option value="MongoDB">MongoDB</option>
          <option value="MSSQL">MSSQL</option>
          <option value="MariaDB">MariaDB</option>
        </Select>

        <Select
          value={filters.category as string}
          onChange={(e) => handleFilterChange('category', e.target.value)}
        >
          <option value="">All Categories</option>
          <option value="TRANSACTIONAL">Transactional</option>
          <option value="ANALYTICAL">Analytical</option>
          <option value="REFERENCE">Reference</option>
          <option value="MASTER_DATA">Master Data</option>
          <option value="OPERATIONAL">Operational</option>
          <option value="TEMPORAL">Temporal</option>
          <option value="GEOSPATIAL">Geospatial</option>
          <option value="FINANCIAL">Financial</option>
          <option value="COMPLIANCE">Compliance</option>
          <option value="TECHNICAL">Technical</option>
          <option value="SPORTS">Sports</option>
        </Select>

        <Select
          value={filters.health as string}
          onChange={(e) => handleFilterChange('health', e.target.value)}
        >
          <option value="">All Health Status</option>
          <option value="EXCELLENT">Excellent</option>
          <option value="HEALTHY">Healthy</option>
          <option value="WARNING">Warning</option>
          <option value="CRITICAL">Critical</option>
          <option value="EMERGENCY">Emergency</option>
        </Select>

        <Select
          value={filters.sensitivity as string}
          onChange={(e) => handleFilterChange('sensitivity', e.target.value)}
        >
          <option value="">All Sensitivity</option>
          <option value="PUBLIC">Public</option>
          <option value="LOW">Low</option>
          <option value="MEDIUM">Medium</option>
          <option value="HIGH">High</option>
          <option value="CRITICAL">Critical</option>
        </Select>

        <Select
          value={filters.domain as string}
          onChange={(e) => handleFilterChange('domain', e.target.value)}
        >
          <option value="">All Domains</option>
          <option value="CUSTOMER">Customer</option>
          <option value="SALES">Sales</option>
          <option value="MARKETING">Marketing</option>
          <option value="HR">HR</option>
          <option value="FINANCE">Finance</option>
          <option value="INVENTORY">Inventory</option>
          <option value="OPERATIONS">Operations</option>
          <option value="SUPPORT">Support</option>
          <option value="SECURITY">Security</option>
          <option value="ANALYTICS">Analytics</option>
          <option value="COMMUNICATION">Communication</option>
          <option value="LEGAL">Legal</option>
          <option value="RESEARCH">Research</option>
          <option value="MANUFACTURING">Manufacturing</option>
          <option value="LOGISTICS">Logistics</option>
          <option value="HEALTHCARE">Healthcare</option>
          <option value="EDUCATION">Education</option>
          <option value="REAL_ESTATE">Real Estate</option>
          <option value="INSURANCE">Insurance</option>
          <option value="SPORTS">Sports</option>
          <option value="GENERAL">General</option>
        </Select>

        <Button
          $variant="secondary"
          onClick={() => {
            clearFilters();
            setPage(1);
          }}
        >
          Reset All
        </Button>
      </FiltersContainer>

      {!loadingTree && !error && (
        <>
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
              <GovernanceTreeView 
                items={allItems} 
                onItemClick={handleItemClick} 
              />
              
              {selectedItem && (
                <DetailsPanel>
                  <TabContainer>
                    <Tab $active={activeTab === 'overview'} onClick={() => setActiveTab('overview')}>
                      Overview
                    </Tab>
                    <Tab $active={activeTab === 'ownership'} onClick={() => setActiveTab('ownership')}>
                      Ownership
                    </Tab>
                    <Tab $active={activeTab === 'security'} onClick={() => setActiveTab('security')}>
                      Security
                    </Tab>
                    <Tab $active={activeTab === 'privacy'} onClick={() => setActiveTab('privacy')}>
                      Privacy/GDPR
                    </Tab>
                    <Tab $active={activeTab === 'retention'} onClick={() => setActiveTab('retention')}>
                      Retention
                    </Tab>
                    <Tab $active={activeTab === 'legal'} onClick={() => setActiveTab('legal')}>
                      Legal Hold
                    </Tab>
                    <Tab $active={activeTab === 'quality'} onClick={() => setActiveTab('quality')}>
                      Data Quality
                    </Tab>
                    <Tab $active={activeTab === 'integration'} onClick={() => setActiveTab('integration')}>
                      Integration
                    </Tab>
                    <Tab $active={activeTab === 'documentation'} onClick={() => setActiveTab('documentation')}>
                      Documentation
                    </Tab>
                  </TabContainer>
                  
                  {activeTab === 'overview' && renderOverviewTab(selectedItem)}
                  {activeTab === 'ownership' && renderOwnershipTab(selectedItem)}
                  {activeTab === 'security' && renderSecurityTab(selectedItem)}
                  {activeTab === 'privacy' && renderPrivacyTab(selectedItem)}
                  {activeTab === 'retention' && renderRetentionTab(selectedItem)}
                  {activeTab === 'legal' && renderLegalTab(selectedItem)}
                  {activeTab === 'quality' && renderQualityTab(selectedItem)}
                  {activeTab === 'integration' && renderIntegrationTab(selectedItem)}
                  {activeTab === 'documentation' && renderDocumentationTab(selectedItem)}
                </DetailsPanel>
              )}
            </MainLayout>
          )}
        </>
      )}

    </Container>
  );
};

export default Governance;
