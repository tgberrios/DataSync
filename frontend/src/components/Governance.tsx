import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import styled from 'styled-components';
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
  StatusBadge,
} from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { governanceApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { theme } from '../theme/theme';

const TableActions = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: ${theme.spacing.md};
  gap: ${theme.spacing.sm};
`;

const ExportButton = styled(Button)`
  padding: 8px 16px;
  font-size: 0.9em;
  display: flex;
  align-items: center;
  gap: 6px;
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

const Badge = styled.span<{ type: string }>`
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
    switch (props.type) {
      case 'EXCELLENT': return theme.colors.status.success.bg;
      case 'HEALTHY': return theme.colors.status.success.bg;
      case 'WARNING': return theme.colors.status.warning.bg;
      case 'CRITICAL': return theme.colors.status.error.bg;
      case 'EMERGENCY': return '#fce4ec';
      case 'REAL_TIME': return '#e1f5fe';
      case 'HIGH': return '#e3f2fd';
      case 'MEDIUM': return '#f3e5f5';
      case 'LOW': return theme.colors.background.secondary;
      case 'RARE': return theme.colors.background.secondary;
      case 'ARCHIVED': return '#eeeeee';
      case 'PUBLIC_SENSITIVITY': return '#f1f8e9';
      case 'LOW_SENSITIVITY': return '#f1f8e9';
      case 'MEDIUM_SENSITIVITY': return theme.colors.status.warning.bg;
      case 'HIGH_SENSITIVITY': return theme.colors.status.error.bg;
      case 'CRITICAL_SENSITIVITY': return '#fce4ec';
      case 'TRANSACTIONAL': return '#e8eaf6';
      case 'ANALYTICAL': return '#f3e5f5';
      case 'REFERENCE': return '#e0f2f1';
      case 'MASTER_DATA': return theme.colors.status.success.bg;
      case 'OPERATIONAL': return theme.colors.status.warning.bg;
      case 'TEMPORAL': return '#e1f5fe';
      case 'GEOSPATIAL': return '#f1f8e9';
      case 'FINANCIAL': return theme.colors.status.error.bg;
      case 'COMPLIANCE': return '#fce4ec';
      case 'TECHNICAL': return theme.colors.background.secondary;
      case 'SPORTS': return '#e3f2fd';
      default: return theme.colors.background.secondary;
    }
  }};
  color: ${props => {
    switch (props.type) {
      case 'EXCELLENT': return '#1b5e20';
      case 'HEALTHY': return '#2e7d32';
      case 'WARNING': return theme.colors.status.warning.text;
      case 'CRITICAL': return theme.colors.status.error.text;
      case 'EMERGENCY': return '#ad1457';
      case 'REAL_TIME': return '#0277bd';
      case 'HIGH': return '#1565c0';
      case 'MEDIUM': return '#6a1b9a';
      case 'LOW': return '#616161';
      case 'RARE': return '#757575';
      case 'ARCHIVED': return '#9e9e9e';
      case 'PUBLIC_SENSITIVITY': return '#388e3c';
      case 'LOW_SENSITIVITY': return '#558b2f';
      case 'MEDIUM_SENSITIVITY': return theme.colors.status.warning.text;
      case 'HIGH_SENSITIVITY': return theme.colors.status.error.text;
      case 'CRITICAL_SENSITIVITY': return '#ad1457';
      case 'TRANSACTIONAL': return '#3949ab';
      case 'ANALYTICAL': return '#7b1fa2';
      case 'REFERENCE': return '#00796b';
      case 'MASTER_DATA': return '#2e7d32';
      case 'OPERATIONAL': return '#f57c00';
      case 'TEMPORAL': return '#0277bd';
      case 'GEOSPATIAL': return '#388e3c';
      case 'FINANCIAL': return '#d32f2f';
      case 'COMPLIANCE': return '#c2185b';
      case 'TECHNICAL': return '#616161';
      case 'SPORTS': return '#1976d2';
      default: return theme.colors.text.secondary;
    }
  }};
`;

const QualityScore = styled.span<{ score: number }>`
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
    if (props.score >= 90) return theme.colors.status.success.bg;
    if (props.score >= 70) return '#f1f8e9';
    if (props.score >= 50) return theme.colors.status.warning.bg;
    return theme.colors.status.error.bg;
  }};
  color: ${props => {
    if (props.score >= 90) return theme.colors.status.success.text;
    if (props.score >= 70) return '#558b2f';
    if (props.score >= 50) return theme.colors.status.warning.text;
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

  useEffect(() => {
    isMountedRef.current = true;
    fetchData();
    const interval = setInterval(() => {
      if (isMountedRef.current) {
        fetchData();
      }
    }, 30000);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchData]);

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
    const rows = sortedData.map(item => [
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
  }, [sortedData, formatDate]);

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

  if (loading && data.length === 0) {
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

      {!loading && !error && (
        <>
          <TableActions>
            <PaginationInfo>
              Showing {sortedData.length} of {pagination.total} entries (Page{" "}
              {pagination.currentPage} of {pagination.totalPages})
            </PaginationInfo>
            <ExportButton $variant="secondary" onClick={handleExportCSV}>
              Export CSV
            </ExportButton>
          </TableActions>

          <TableContainer>
            <Table $minWidth="1600px">
              <thead>
                <tr>
                  <SortableTh 
                    $sortable 
                    $active={sortField === "schema_name"} 
                    $direction={sortDirection}
                    onClick={() => handleSort("schema_name")}
                  >
                    Schema.Table
                  </SortableTh>
                  <SortableTh 
                    $sortable 
                    $active={sortField === "inferred_source_engine"} 
                    $direction={sortDirection}
                    onClick={() => handleSort("inferred_source_engine")}
                  >
                    Engine
                  </SortableTh>
                  <SortableTh 
                    $sortable 
                    $active={sortField === "data_category"} 
                    $direction={sortDirection}
                    onClick={() => handleSort("data_category")}
                  >
                    Category
                  </SortableTh>
                  <SortableTh 
                    $sortable 
                    $active={sortField === "business_domain"} 
                    $direction={sortDirection}
                    onClick={() => handleSort("business_domain")}
                  >
                    Domain
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
                    $active={sortField === "sensitivity_level"} 
                    $direction={sortDirection}
                    onClick={() => handleSort("sensitivity_level")}
                  >
                    Sensitivity
                  </SortableTh>
                  <SortableTh 
                    $sortable 
                    $active={sortField === "data_quality_score"} 
                    $direction={sortDirection}
                    onClick={() => handleSort("data_quality_score")}
                  >
                    Quality
                  </SortableTh>
                  <SortableTh 
                    $sortable 
                    $active={sortField === "table_size_mb"} 
                    $direction={sortDirection}
                    onClick={() => handleSort("table_size_mb")}
                  >
                    Size (MB)
                  </SortableTh>
                  <SortableTh 
                    $sortable 
                    $active={sortField === "total_rows"} 
                    $direction={sortDirection}
                    onClick={() => handleSort("total_rows")}
                  >
                    Rows
                  </SortableTh>
                  <SortableTh 
                    $sortable 
                    $active={sortField === "access_frequency"} 
                    $direction={sortDirection}
                    onClick={() => handleSort("access_frequency")}
                  >
                    Access
                  </SortableTh>
                  <SortableTh 
                    $sortable 
                    $active={sortField === "last_analyzed"} 
                    $direction={sortDirection}
                    onClick={() => handleSort("last_analyzed")}
                  >
                    Last Analyzed
                  </SortableTh>
                </tr>
              </thead>
              <tbody>
                {sortedData.length === 0 ? (
                  <TableRow>
                    <Td colSpan={11} style={{ padding: '40px', textAlign: 'center', color: theme.colors.text.secondary }}>
                      No governance data found
                    </Td>
                  </TableRow>
                ) : (
                  sortedData.map((item) => (
                    <React.Fragment key={item.id}>
                      <TableRow onClick={() => toggleItem(item.id)} style={{ cursor: 'pointer' }}>
                        <Td>
                          <strong style={{ color: theme.colors.primary.main }}>
                            {item.schema_name}
                          </strong>
                          <span style={{ color: theme.colors.text.secondary }}>.
                            {item.table_name}
                          </span>
                        </Td>
                        <Td>
                          <span style={{ 
                            padding: "2px 8px", 
                            borderRadius: theme.borderRadius.sm,
                            backgroundColor: theme.colors.background.secondary,
                            fontSize: "0.85em"
                          }}>
                            {item.inferred_source_engine}
                          </span>
                        </Td>
                        <Td>
                          <Badge type={item.data_category}>
                            {item.data_category}
                          </Badge>
                        </Td>
                        <Td style={{ color: theme.colors.text.secondary }}>
                          {item.business_domain || "-"}
                        </Td>
                        <Td>
                          <StatusBadge $status={item.health_status}>
                            {item.health_status}
                          </StatusBadge>
                        </Td>
                        <Td>
                          <Badge type={`${item.sensitivity_level}_SENSITIVITY`}>
                            {item.sensitivity_level}
                          </Badge>
                        </Td>
                        <Td>
                          <QualityScore score={item.data_quality_score}>
                            {item.data_quality_score}%
                          </QualityScore>
                        </Td>
                        <Td style={{ color: theme.colors.text.secondary }}>
                          {formatSize(item.table_size_mb)}
                        </Td>
                        <Td style={{ color: theme.colors.text.secondary }}>
                          {formatNumber(item.total_rows)}
                        </Td>
                        <Td>
                          <Badge type={item.access_frequency}>
                            {item.access_frequency}
                          </Badge>
                        </Td>
                        <Td style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>
                          {formatDate(item.last_analyzed)}
                        </Td>
                      </TableRow>
                      {openItemId === item.id && (
                        <TableRow>
                          <Td colSpan={11} style={{ padding: 0, border: 'none' }}>
                            <GovernanceDetails $isOpen={openItemId === item.id}>
                              <DetailsGrid>
                                <DetailCard>
                                  <DetailLabel>Schema Name</DetailLabel>
                                  <DetailValue>{item.schema_name}</DetailValue>
                                </DetailCard>
                                <DetailCard>
                                  <DetailLabel>Table Name</DetailLabel>
                                  <DetailValue>{item.table_name}</DetailValue>
                                </DetailCard>
                                <DetailCard>
                                  <DetailLabel>Database Engine</DetailLabel>
                                  <DetailValue>{item.inferred_source_engine}</DetailValue>
                                </DetailCard>
                                <DetailCard>
                                  <DetailLabel>Data Category</DetailLabel>
                                  <DetailValue>
                                    <Tooltip>
                                      <Badge type={item.data_category}>
                                        {item.data_category}
                                      </Badge>
                                      <TooltipContent className="tooltip-content">
                                        {getCategoryDescription(item.data_category)}
                                      </TooltipContent>
                                    </Tooltip>
                                  </DetailValue>
                                </DetailCard>
                                <DetailCard>
                                  <DetailLabel>Business Domain</DetailLabel>
                                  <DetailValue>{item.business_domain || 'N/A'}</DetailValue>
                                </DetailCard>
                                <DetailCard>
                                  <DetailLabel>Health Status</DetailLabel>
                                  <DetailValue>
                                    <Tooltip>
                                      <Badge type={item.health_status}>
                                        {item.health_status}
                                      </Badge>
                                      <TooltipContent className="tooltip-content">
                                        {getHealthDescription(item.health_status)}
                                      </TooltipContent>
                                    </Tooltip>
                                  </DetailValue>
                                </DetailCard>
                                <DetailCard>
                                  <DetailLabel>Sensitivity Level</DetailLabel>
                                  <DetailValue>
                                    <Tooltip>
                                      <Badge type={`${item.sensitivity_level}_SENSITIVITY`}>
                                        {item.sensitivity_level}
                                      </Badge>
                                      <TooltipContent className="tooltip-content">
                                        {getSensitivityDescription(item.sensitivity_level)}
                                      </TooltipContent>
                                    </Tooltip>
                                  </DetailValue>
                                </DetailCard>
                                <DetailCard>
                                  <DetailLabel>Data Quality Score</DetailLabel>
                                  <DetailValue>
                                    <QualityScore score={item.data_quality_score}>
                                      {item.data_quality_score}%
                                    </QualityScore>
                                  </DetailValue>
                                </DetailCard>
                                <DetailCard>
                                  <DetailLabel>Table Size</DetailLabel>
                                  <DetailValue>{formatSize(item.table_size_mb)}</DetailValue>
                                </DetailCard>
                                <DetailCard>
                                  <DetailLabel>Total Rows</DetailLabel>
                                  <DetailValue>{formatNumber(item.total_rows)}</DetailValue>
                                </DetailCard>
                                <DetailCard>
                                  <DetailLabel>Access Frequency</DetailLabel>
                                  <DetailValue>
                                    <Badge type={item.access_frequency}>
                                      {item.access_frequency}
                                    </Badge>
                                  </DetailValue>
                                </DetailCard>
                                <DetailCard>
                                  <DetailLabel>Last Analyzed</DetailLabel>
                                  <DetailValue>{formatDate(item.last_analyzed)}</DetailValue>
                                </DetailCard>
                                {item.data_classification && (
                                  <DetailCard>
                                    <DetailLabel>Data Classification</DetailLabel>
                                    <DetailValue>{item.data_classification}</DetailValue>
                                  </DetailCard>
                                )}
                                {item.retention_policy && (
                                  <DetailCard>
                                    <DetailLabel>Retention Policy</DetailLabel>
                                    <DetailValue>{item.retention_policy}</DetailValue>
                                  </DetailCard>
                                )}
                                {item.owner && (
                                  <DetailCard>
                                    <DetailLabel>Data Owner</DetailLabel>
                                    <DetailValue>{item.owner}</DetailValue>
                                  </DetailCard>
                                )}
                                {item.compliance_requirements && (
                                  <DetailCard>
                                    <DetailLabel>Compliance Requirements</DetailLabel>
                                    <DetailValue>{item.compliance_requirements}</DetailValue>
                                  </DetailCard>
                                )}
                                {item.null_percentage != null && (
                                  <DetailCard>
                                    <DetailLabel>Null Percentage</DetailLabel>
                                    <DetailValue>{item.null_percentage}%</DetailValue>
                                  </DetailCard>
                                )}
                                {item.duplicate_percentage != null && (
                                  <DetailCard>
                                    <DetailLabel>Duplicate Percentage</DetailLabel>
                                    <DetailValue>{item.duplicate_percentage}%</DetailValue>
                                  </DetailCard>
                                )}
                                {item.fragmentation_percentage != null && (
                                  <DetailCard>
                                    <DetailLabel>Fragmentation Percentage</DetailLabel>
                                    <DetailValue>{item.fragmentation_percentage}%</DetailValue>
                                  </DetailCard>
                                )}
                                {item.query_count_daily != null && (
                                  <DetailCard>
                                    <DetailLabel>Daily Query Count</DetailLabel>
                                    <DetailValue>{formatNumber(item.query_count_daily)}</DetailValue>
                                  </DetailCard>
                                )}
                                {item.last_vacuum && (
                                  <DetailCard>
                                    <DetailLabel>Last Vacuum</DetailLabel>
                                    <DetailValue>{formatDate(item.last_vacuum)}</DetailValue>
                                  </DetailCard>
                                )}
                                {item.total_columns != null && (
                                  <DetailCard>
                                    <DetailLabel>Total Columns</DetailLabel>
                                    <DetailValue>{formatNumber(item.total_columns)}</DetailValue>
                                  </DetailCard>
                                )}
                                {item.index_count != null && (
                                  <DetailCard>
                                    <DetailLabel>Index Count</DetailLabel>
                                    <DetailValue>{formatNumber(item.index_count)}</DetailValue>
                                  </DetailCard>
                                )}
                                {item.constraint_count != null && (
                                  <DetailCard>
                                    <DetailLabel>Constraint Count</DetailLabel>
                                    <DetailValue>{formatNumber(item.constraint_count)}</DetailValue>
                                  </DetailCard>
                                )}
                                {item.primary_key_columns && (
                                  <DetailCard>
                                    <DetailLabel>Primary Key Columns</DetailLabel>
                                    <DetailValue>{item.primary_key_columns}</DetailValue>
                                  </DetailCard>
                                )}
                                {item.first_discovered && (
                                  <DetailCard>
                                    <DetailLabel>First Discovered</DetailLabel>
                                    <DetailValue>{formatDate(item.first_discovered)}</DetailValue>
                                  </DetailCard>
                                )}
                                {item.last_accessed && (
                                  <DetailCard>
                                    <DetailLabel>Last Accessed</DetailLabel>
                                    <DetailValue>{formatDate(item.last_accessed)}</DetailValue>
                                  </DetailCard>
                                )}
                              </DetailsGrid>
                              {item.health_status === 'CRITICAL' && (
                                <CriticalStatusBox>
                                  <div style={{ 
                                    fontWeight: 'bold', 
                                    color: theme.colors.status.error.text, 
                                    marginBottom: '10px',
                                    fontSize: '1.1em'
                                  }}>
                                    CRITICAL STATUS REASONS:
                                  </div>
                                  <div style={{ color: theme.colors.text.primary, lineHeight: '1.5' }}>
                                    {item.data_quality_score < 50 && (
                                      <div>• Low data quality score ({item.data_quality_score}%)</div>
                                    )}
                                    {item.null_percentage > 20 && (
                                      <div>• High null percentage ({item.null_percentage}%)</div>
                                    )}
                                    {item.duplicate_percentage > 10 && (
                                      <div>• High duplicate percentage ({item.duplicate_percentage}%)</div>
                                    )}
                                    {item.fragmentation_percentage > 30 && (
                                      <div>• High fragmentation ({item.fragmentation_percentage}%)</div>
                                    )}
                                    {item.query_count_daily === 0 && (
                                      <div>• No recent queries detected</div>
                                    )}
                                    {item.last_vacuum && (() => {
                                      const lastVacuum = new Date(item.last_vacuum);
                                      const daysSinceVacuum = Math.floor((Date.now() - lastVacuum.getTime()) / (1000 * 60 * 60 * 24));
                                      return daysSinceVacuum > 7 && <div>• No vacuum in {daysSinceVacuum} days</div>;
                                    })()}
                                    {(!item.null_percentage || item.null_percentage <= 20) && 
                                     (!item.duplicate_percentage || item.duplicate_percentage <= 10) && 
                                     (!item.fragmentation_percentage || item.fragmentation_percentage <= 30) && 
                                     item.data_quality_score >= 50 && (
                                      <div>• Manual classification or business rule violation</div>
                                    )}
                                  </div>
                                </CriticalStatusBox>
                              )}
                            </GovernanceDetails>
                          </Td>
                        </TableRow>
                      )}
                    </React.Fragment>
                  ))
                )}
              </tbody>
            </Table>
          </TableContainer>

          {pagination.totalPages > 1 && (
            <Pagination>
              <PageButton
                disabled={page === 1}
                onClick={() => setPage(Math.max(1, page - 1))}
              >
                Previous
              </PageButton>
              
              {Array.from({ length: pagination.totalPages }, (_, i) => i + 1)
                .filter(p => Math.abs(p - page) <= 2 || p === 1 || p === pagination.totalPages)
                .map((p, i, arr) => (
                  <React.Fragment key={p}>
                    {i > 0 && arr[i - 1] !== p - 1 && <span>...</span>}
                    <PageButton
                      $active={p === page}
                      onClick={() => setPage(p)}
                    >
                      {p}
                    </PageButton>
                  </React.Fragment>
                ))
              }
              
              <PageButton
                disabled={page === pagination.totalPages}
                onClick={() => setPage(Math.min(pagination.totalPages, page + 1))}
              >
                Next
              </PageButton>
            </Pagination>
          )}
        </>
      )}
    </Container>
  );
};

export default Governance;
