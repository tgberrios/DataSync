import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import styled, { keyframes } from 'styled-components';
import {
  Container,
  Header,
  ErrorMessage,
  LoadingOverlay,
  Select,
  Pagination,
  PageButton,
  FiltersContainer,
  TableContainer,
  Table,
  Th,
  Td,
  TableRow,
  Button,
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

const GovernanceDetails = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '800px' : '0'};
  opacity: ${props => props.$isOpen ? '1' : '0'};
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  border-top: ${props => props.$isOpen ? `1px solid ${theme.colors.border.light}` : 'none'};
  background-color: ${theme.colors.background.main};
  overflow: hidden;
`;

const DetailsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  padding: ${theme.spacing.md};
  gap: ${theme.spacing.md};
`;

const DetailCard = styled.div`
  background: ${theme.colors.background.main};
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.md};
  padding: ${theme.spacing.sm};
  transition: all ${theme.transitions.normal};
  animation: fadeIn 0.2s ease-in;
  animation-fill-mode: both;
  
  &:hover {
    transform: translateY(-3px);
    box-shadow: ${theme.shadows.md};
    border-color: rgba(10, 25, 41, 0.2);
    background: linear-gradient(135deg, ${theme.colors.background.main} 0%, ${theme.colors.background.tertiary} 100%);
  }
  
  &:nth-child(1) { animation-delay: 0.1s; }
  &:nth-child(2) { animation-delay: 0.15s; }
  &:nth-child(3) { animation-delay: 0.2s; }
  &:nth-child(4) { animation-delay: 0.25s; }
  &:nth-child(5) { animation-delay: 0.15s; }
  &:nth-child(6) { animation-delay: 0.35s; }
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

const CriticalStatusBox = styled.div`
  margin: ${theme.spacing.md};
  padding: ${theme.spacing.md};
  backgroundColor: ${theme.colors.status.error.bg};
  border: 1px solid ${theme.colors.status.error.text};
  borderRadius: ${theme.borderRadius.sm};
`;

/**
 * Governance component
 * Displays data governance catalog with filtering, sorting, and detailed information
 */
const Governance = () => {
  const { page, limit, setPage } = usePagination(1, 10);
  const { filters, setFilter, clearFilters } = useTableFilters({
    engine: '',
    category: '',
    health: '',
    domain: '',
    sensitivity: ''
  });
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<any[]>([]);
  const [openItemId, setOpenItemId] = useState<number | null>(null);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 10
  });
  const [sortField, setSortField] = useState('health_status');
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">('desc');
  const [allItems, setAllItems] = useState<any[]>([]);
  const [loadingTree, setLoadingTree] = useState(false);
  const isMountedRef = useRef(true);

  const fetchData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      const response = await governanceApi.getGovernanceData({
        page,
        limit,
        engine: filters.engine as string,
        category: filters.category as string,
        health: filters.health as string,
        domain: filters.domain as string,
        sensitivity: filters.sensitivity as string
      });
      if (isMountedRef.current) {
        setData(response.data || []);
        setPagination(response.pagination || {
          total: 0,
          totalPages: 0,
          currentPage: 1,
          limit: 20
        });
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
    filters.engine, 
    filters.category, 
    filters.health, 
      filters.domain, 
      filters.sensitivity, 
      sortField, 
      sortDirection
  ]);

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
    fetchAllItems();
    const interval = setInterval(() => {
      if (isMountedRef.current) {
        fetchAllItems();
      }
    }, 30000);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchAllItems]);

  const toggleItem = useCallback((id: number) => {
    setOpenItemId(prev => prev === id ? null : id);
  }, []);

  const formatDate = useCallback((date: string) => {
    if (!date) return '-';
    return new Date(date).toLocaleString();
  }, []);

  const handleSort = useCallback((field: string) => {
    if (sortField === field) {
      setSortDirection(prev => prev === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDirection("asc");
    }
    setPage(1);
  }, [sortField, setPage]);

  const sortedData = useMemo(() => {
    if (!sortField) return data;
    return [...data].sort((a, b) => {
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
  }, [data, sortField, sortDirection]);

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

  const formatSize = useCallback((mb: number | null | undefined) => {
    if (mb == null) return '-';
    const size = Number(mb);
    if (isNaN(size)) return '-';
    if (size >= 1024) {
      return `${(size / 1024).toFixed(2)} GB`;
    }
    return `${size.toFixed(2)} MB`;
  }, []);

  const formatNumber = useCallback((num: number) => {
    return num?.toLocaleString() || '0';
  }, []);

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
            <GovernanceTreeView items={allItems} onItemClick={(item: any) => toggleItem(item.id)} />
          )}
        </>
      )}

    </Container>
  );
};

export default Governance;
