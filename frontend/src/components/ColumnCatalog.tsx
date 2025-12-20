import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import styled from 'styled-components';
import {
  Container,
  Header,
  ErrorMessage,
  LoadingOverlay,
  Grid,
  Value,
  Pagination,
  PageButton,
  Button,
  FiltersContainer,
  Select,
  TableContainer,
  Table,
  Th,
  Td,
  TableRow,
  Input,
} from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { columnCatalogApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { sanitizeSearch } from '../utils/validation';
import { theme } from '../theme/theme';
import ColumnCatalogTreeView from './ColumnCatalogTreeView';

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
  text-align: left;
  border-radius: ${theme.borderRadius.sm};
  padding: 10px 14px;
  font-size: 0.85em;
  white-space: normal;
  min-width: 200px;
  max-width: 350px;
  box-shadow: ${theme.shadows.lg};
  transition: opacity ${theme.transitions.normal};
  line-height: 1.4;
  
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

const ThWithTooltip = styled(Th)<{ $sortable?: boolean; $active?: boolean; $direction?: "asc" | "desc" }>`
  position: relative;
  cursor: ${props => props.$sortable ? "pointer" : "default"};
  user-select: none;
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
  
  &:hover .tooltip-content {
    visibility: visible;
    opacity: 1;
  }
`;

const FilterWithTooltip = styled.div`
  position: relative;
  display: inline-block;
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

const Badge = styled.span<{ $type?: string; $level?: string; $flag?: boolean }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.75em;
  font-weight: 500;
  display: inline-block;
  transition: all ${theme.transitions.normal};
  
  ${props => {
    if (props.$type) {
      return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.primary};`;
    }
    if (props.$level) {
      switch (props.$level) {
        case 'HIGH': return `background-color: ${theme.colors.status.error.bg}; color: ${theme.colors.status.error.text};`;
        case 'MEDIUM': return `background-color: ${theme.colors.status.warning.bg}; color: ${theme.colors.status.warning.text};`;
        case 'LOW': return `background-color: ${theme.colors.status.success.bg}; color: ${theme.colors.status.success.text};`;
        default: return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.secondary};`;
      }
    }
    if (props.$flag !== undefined) {
      return props.$flag 
        ? `background-color: ${theme.colors.status.error.bg}; color: ${theme.colors.status.error.text};`
        : `background-color: ${theme.colors.status.success.bg}; color: ${theme.colors.status.success.text};`;
    }
    return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.secondary};`;
  }}
  
  &:hover {
    transform: scale(1.05);
    box-shadow: ${theme.shadows.sm};
  }
`;

const ColumnDetails = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '800px' : '0'};
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

const FlagsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: ${theme.spacing.sm};
  padding: ${theme.spacing.md};
  background: ${theme.colors.background.secondary};
  border-radius: ${theme.borderRadius.md};
  margin: ${theme.spacing.md};
`;

const FlagItem = styled.div`
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.9em;
`;

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


/**
 * Column Catalog component
 * Displays detailed metadata about database columns including data types, sensitivity levels, and PII/PHI flags
 */
const ColumnCatalog = () => {
  const { page, limit, setPage } = usePagination(1, 20);
  const { filters, setFilter, clearFilters } = useTableFilters({
    schema_name: '',
    table_name: '',
    db_engine: '',
    data_type: '',
    sensitivity_level: '',
    contains_pii: '',
    contains_phi: '',
    search: ''
  });
  
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [columns, setColumns] = useState<any[]>([]);
  const [metrics, setMetrics] = useState<any>({});
  const [openColumnId, setOpenColumnId] = useState<number | null>(null);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 20
  });
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  const [sortField, setSortField] = useState<string>("");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");
  const [viewMode, setViewMode] = useState<"table" | "tree">("tree");
  const [allColumns, setAllColumns] = useState<any[]>([]);
  const [loadingTree, setLoadingTree] = useState(false);
  const isMountedRef = useRef(true);

  const fetchData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(filters.search as string, 100);
      const [columnsData, metricsData, schemasData] = await Promise.all([
        columnCatalogApi.getColumns({
          page,
          limit,
          schema_name: filters.schema_name as string,
          table_name: filters.table_name as string,
          db_engine: filters.db_engine as string,
          data_type: filters.data_type as string,
          sensitivity_level: filters.sensitivity_level as string,
          contains_pii: filters.contains_pii as string,
          contains_phi: filters.contains_phi as string,
          search: sanitizedSearch
        }),
        columnCatalogApi.getMetrics(),
        columnCatalogApi.getSchemas()
      ]);
      if (isMountedRef.current) {
        setColumns(columnsData.data || []);
        setPagination(columnsData.pagination || {
          total: 0,
          totalPages: 0,
          currentPage: 1,
          limit: 20
        });
        setMetrics(metricsData || {});
        setSchemas(schemasData || []);
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
    filters.schema_name, 
    filters.table_name, 
    filters.db_engine, 
    filters.data_type, 
    filters.sensitivity_level, 
    filters.contains_pii, 
    filters.contains_phi, 
    filters.search
  ]);

  const fetchAllColumns = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoadingTree(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(filters.search as string, 100);
      const columnsData = await columnCatalogApi.getColumns({
        page: 1,
        limit: 10000,
        schema_name: filters.schema_name as string,
        table_name: filters.table_name as string,
        db_engine: filters.db_engine as string,
        data_type: filters.data_type as string,
        sensitivity_level: filters.sensitivity_level as string,
        contains_pii: filters.contains_pii as string,
        contains_phi: filters.contains_phi as string,
        search: sanitizedSearch
      });
      if (isMountedRef.current) {
        setAllColumns(columnsData.data || []);
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
    filters.schema_name,
    filters.table_name,
    filters.db_engine,
    filters.data_type,
    filters.sensitivity_level,
    filters.contains_pii,
    filters.contains_phi,
    filters.search
  ]);

  useEffect(() => {
    isMountedRef.current = true;
    fetchData();
    if (viewMode === "tree") {
      fetchAllColumns();
    }
    const interval = setInterval(() => {
      if (isMountedRef.current) {
        fetchData();
        if (viewMode === "tree") {
          fetchAllColumns();
        }
      }
    }, 30000);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchData, fetchAllColumns, viewMode]);

  useEffect(() => {
    const fetchTables = async () => {
      if (filters.schema_name && isMountedRef.current) {
        try {
          const tablesData = await columnCatalogApi.getTables(filters.schema_name as string);
          if (isMountedRef.current) {
            setTables(tablesData || []);
          }
        } catch (err) {
          if (isMountedRef.current) {
            console.error('Error loading tables:', err);
          }
        }
      } else {
        setTables([]);
      }
    };
    fetchTables();
  }, [filters.schema_name]);

  const toggleColumn = useCallback((id: number) => {
    setOpenColumnId(prev => prev === id ? null : id);
  }, []);

  const formatNumber = useCallback((num: number | string | null | undefined) => {
    if (num === null || num === undefined) return 'N/A';
    const numVal = Number(num);
    if (isNaN(numVal)) return 'N/A';
    if (numVal >= 1000000) return `${(numVal / 1000000).toFixed(2)}M`;
    if (numVal >= 1000) return `${(numVal / 1000).toFixed(2)}K`;
    return numVal.toString();
  }, []);

  const formatPercentage = useCallback((val: number | string | null | undefined) => {
    if (val === null || val === undefined) return 'N/A';
    const numVal = Number(val);
    if (isNaN(numVal)) return 'N/A';
    return `${numVal.toFixed(2)}%`;
  }, []);

  const formatDate = useCallback((date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  }, []);

  const handleFilterChange = useCallback((key: string, value: string) => {
    setFilter(key as any, value);
    if (key === 'schema_name') {
      setFilter('table_name' as any, '');
    }
    setPage(1);
  }, [setFilter, setPage]);

  const handleResetFilters = useCallback(() => {
    clearFilters();
    setPage(1);
  }, [clearFilters, setPage]);

  const handleSort = useCallback((field: string) => {
    if (sortField === field) {
      setSortDirection(prev => prev === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDirection("asc");
    }
    setPage(1);
  }, [sortField, setPage]);

  const handleExportCSV = useCallback(() => {
    const headers = ["Schema", "Table", "Column", "Engine", "Data Type", "Position", "Nullable", "Sensitivity", "PII", "PHI", "Primary Key", "Foreign Key", "Unique", "Indexed"];
    const rows = columns.map(col => [
      col.schema_name,
      col.table_name,
      col.column_name,
      col.db_engine || "",
      col.data_type || "",
      col.ordinal_position || "",
      col.is_nullable ? "Yes" : "No",
      col.sensitivity_level || "",
      col.contains_pii ? "Yes" : "No",
      col.contains_phi ? "Yes" : "No",
      col.is_primary_key ? "Yes" : "No",
      col.is_foreign_key ? "Yes" : "No",
      col.is_unique ? "Yes" : "No",
      col.is_indexed ? "Yes" : "No"
    ]);
    
    const csvContent = [
      headers.join(","),
      ...rows.map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(","))
    ].join("\n");
    
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    link.setAttribute("href", url);
    link.setAttribute("download", `column_catalog_export_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }, [columns]);

  const sortedColumns = useMemo(() => {
    if (!sortField) return columns;
    return [...columns].sort((a, b) => {
      let aVal = a[sortField as keyof typeof a];
      let bVal = b[sortField as keyof typeof b];
      
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
  }, [columns, sortField, sortDirection]);

  if (loading && columns.length === 0) {
    return (
      <Container>
        <Header>Column Catalog</Header>
        <LoadingOverlay>Loading column catalog data...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>Column Catalog</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}
      
      <MetricsGrid $columns="repeat(auto-fit, minmax(180px, 1fr))">
        <Tooltip>
          <MetricCard>
            <MetricLabel>Total Columns</MetricLabel>
            <MetricValue>{formatNumber(metrics.total_columns)}</MetricValue>
          </MetricCard>
          <TooltipContent className="tooltip-content">
            Total number of columns cataloged across all schemas and tables in the system.
          </TooltipContent>
        </Tooltip>
        <Tooltip>
          <MetricCard>
            <MetricLabel>PII Columns</MetricLabel>
            <MetricValue>{formatNumber(metrics.pii_columns)}</MetricValue>
          </MetricCard>
          <TooltipContent className="tooltip-content">
            Columns containing Personally Identifiable Information (PII) such as names, emails, phone numbers, or social security numbers.
          </TooltipContent>
        </Tooltip>
        <Tooltip>
          <MetricCard>
            <MetricLabel>PHI Columns</MetricLabel>
            <MetricValue>{formatNumber(metrics.phi_columns)}</MetricValue>
          </MetricCard>
          <TooltipContent className="tooltip-content">
            Columns containing Protected Health Information (PHI) such as medical records, health conditions, or patient identifiers.
          </TooltipContent>
        </Tooltip>
        <Tooltip>
          <MetricCard>
            <MetricLabel>High Sensitivity</MetricLabel>
            <MetricValue>{formatNumber(metrics.high_sensitivity)}</MetricValue>
          </MetricCard>
          <TooltipContent className="tooltip-content">
            Columns classified with HIGH sensitivity level, indicating they contain sensitive data requiring special protection.
          </TooltipContent>
        </Tooltip>
        <Tooltip>
          <MetricCard>
            <MetricLabel>Primary Keys</MetricLabel>
            <MetricValue>{formatNumber(metrics.primary_keys)}</MetricValue>
          </MetricCard>
          <TooltipContent className="tooltip-content">
            Columns that serve as primary keys, uniquely identifying each row in their respective tables.
          </TooltipContent>
        </Tooltip>
        <Tooltip>
          <MetricCard>
            <MetricLabel>Indexed Columns</MetricLabel>
            <MetricValue>{formatNumber(metrics.indexed_columns)}</MetricValue>
          </MetricCard>
          <TooltipContent className="tooltip-content">
            Columns that have database indexes created on them, improving query performance for searches and joins.
          </TooltipContent>
        </Tooltip>
      </MetricsGrid>

      <FiltersContainer>
        <FilterWithTooltip>
          <Tooltip>
            <Select
              value={filters.schema_name as string}
              onChange={(e) => handleFilterChange('schema_name', e.target.value)}
            >
              <option value="">All Schemas</option>
              {schemas.map(schema => (
                <option key={schema} value={schema}>{schema}</option>
              ))}
            </Select>
            <TooltipContent className="tooltip-content">
              Filter columns by database schema. A schema is a logical container that groups related database objects.
            </TooltipContent>
          </Tooltip>
        </FilterWithTooltip>
        
        <FilterWithTooltip>
          <Tooltip>
            <Select
              value={filters.table_name as string}
              onChange={(e) => handleFilterChange('table_name', e.target.value)}
              disabled={!filters.schema_name}
            >
              <option value="">All Tables</option>
              {tables.map(table => (
                <option key={table} value={table}>{table}</option>
              ))}
            </Select>
            <TooltipContent className="tooltip-content">
              Filter columns by table name. Select a schema first to see available tables. A table is a collection of related data organized in rows and columns.
            </TooltipContent>
          </Tooltip>
        </FilterWithTooltip>
        
        <FilterWithTooltip>
          <Tooltip>
            <Select
              value={filters.db_engine as string}
              onChange={(e) => handleFilterChange('db_engine', e.target.value)}
            >
              <option value="">All Engines</option>
              <option value="PostgreSQL">PostgreSQL</option>
              <option value="MariaDB">MariaDB</option>
              <option value="MSSQL">MSSQL</option>
            </Select>
            <TooltipContent className="tooltip-content">
              Filter columns by database engine type. Different engines may have different data type representations and capabilities.
            </TooltipContent>
          </Tooltip>
        </FilterWithTooltip>
        
        <FilterWithTooltip>
          <Tooltip>
            <Select
              value={filters.data_type as string}
              onChange={(e) => handleFilterChange('data_type', e.target.value)}
            >
              <option value="">All Types</option>
              <option value="varchar">VARCHAR</option>
              <option value="integer">INTEGER</option>
              <option value="bigint">BIGINT</option>
              <option value="numeric">NUMERIC</option>
              <option value="timestamp">TIMESTAMP</option>
              <option value="boolean">BOOLEAN</option>
            </Select>
            <TooltipContent className="tooltip-content">
              Filter columns by their data type. Data types define what kind of data can be stored in a column (text, numbers, dates, etc.).
            </TooltipContent>
          </Tooltip>
        </FilterWithTooltip>
        
        <FilterWithTooltip>
          <Tooltip>
            <Select
              value={filters.sensitivity_level as string}
              onChange={(e) => handleFilterChange('sensitivity_level', e.target.value)}
            >
              <option value="">All Sensitivity</option>
              <option value="HIGH">HIGH</option>
              <option value="MEDIUM">MEDIUM</option>
              <option value="LOW">LOW</option>
            </Select>
            <TooltipContent className="tooltip-content">
              Filter columns by sensitivity classification. HIGH = very sensitive (PII/PHI), MEDIUM = moderately sensitive, LOW = public or non-sensitive data.
            </TooltipContent>
          </Tooltip>
        </FilterWithTooltip>
        
        <FilterWithTooltip>
          <Tooltip>
            <Select
              value={filters.contains_pii as string}
              onChange={(e) => handleFilterChange('contains_pii', e.target.value)}
            >
              <option value="">All PII</option>
              <option value="true">Has PII</option>
              <option value="false">No PII</option>
            </Select>
            <TooltipContent className="tooltip-content">
              Filter columns that contain Personally Identifiable Information (PII). PII includes data that can identify an individual like names, emails, or IDs.
            </TooltipContent>
          </Tooltip>
        </FilterWithTooltip>
        
        <Tooltip>
          <Input
            type="text"
            placeholder="Search column name..."
            value={filters.search as string}
            onChange={(e) => handleFilterChange('search', e.target.value)}
            style={{ flex: 1, minWidth: "200px" }}
          />
          <TooltipContent className="tooltip-content">
            Search for columns by name. This will search across all column names in the catalog.
          </TooltipContent>
        </Tooltip>
        
        <Button
          $variant="secondary"
          onClick={handleResetFilters}
          style={{ padding: "8px 16px", fontSize: "0.9em" }}
        >
          Reset All
        </Button>
      </FiltersContainer>

      <TableActions>
        <div style={{ fontSize: "0.9em", color: theme.colors.text.secondary }}>
          {viewMode === "table" 
            ? `Showing ${sortedColumns.length} of ${pagination.total} columns`
            : `Total: ${allColumns.length} columns`
          }
        </div>
        <div style={{ display: "flex", gap: theme.spacing.sm, alignItems: "center" }}>
          <div style={{ display: "flex", gap: "4px", background: theme.colors.background.secondary, padding: "4px", borderRadius: theme.borderRadius.sm }}>
            <Button
              $variant={viewMode === "table" ? "primary" : "secondary"}
              onClick={() => setViewMode("table")}
              style={{ padding: "6px 12px", fontSize: "0.85em" }}
            >
              Schema View
            </Button>
            <Button
              $variant={viewMode === "tree" ? "primary" : "secondary"}
              onClick={() => {
                setViewMode("tree");
                fetchAllColumns();
              }}
              style={{ padding: "6px 12px", fontSize: "0.85em" }}
            >
              Tree View
            </Button>
          </div>
          <ExportButton $variant="secondary" onClick={handleExportCSV}>
            Export CSV
          </ExportButton>
        </div>
      </TableActions>

      {viewMode === "tree" ? (
        loadingTree ? (
          <LoadingOverlay>Loading tree view...</LoadingOverlay>
        ) : (
          <ColumnCatalogTreeView 
            columns={allColumns}
            onColumnClick={(column) => toggleColumn(column.id)}
          />
        )
      ) : (
        <TableContainer>
        <Table $minWidth="1400px">
          <thead>
            <tr>
              <ThWithTooltip 
                $sortable 
                $active={sortField === "schema_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("schema_name")}
              >
                Schema
                <TooltipContent className="tooltip-content">
                  Database schema name. A schema is a logical container that groups related database objects like tables and views.
                </TooltipContent>
              </ThWithTooltip>
              <ThWithTooltip 
                $sortable 
                $active={sortField === "table_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("table_name")}
              >
                Table
                <TooltipContent className="tooltip-content">
                  Table name containing this column. Tables store data in rows and columns.
                </TooltipContent>
              </ThWithTooltip>
              <ThWithTooltip 
                $sortable 
                $active={sortField === "column_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("column_name")}
              >
                Column
                <TooltipContent className="tooltip-content">
                  Column name. Each column represents a specific attribute or field in the table.
                </TooltipContent>
              </ThWithTooltip>
              <ThWithTooltip 
                $sortable 
                $active={sortField === "db_engine"} 
                $direction={sortDirection}
                onClick={() => handleSort("db_engine")}
              >
                Engine
                <TooltipContent className="tooltip-content">
                  Database engine type (PostgreSQL, MariaDB, MSSQL, etc.). Different engines have different features and data type support.
                </TooltipContent>
              </ThWithTooltip>
              <ThWithTooltip 
                $sortable 
                $active={sortField === "data_type"} 
                $direction={sortDirection}
                onClick={() => handleSort("data_type")}
              >
                Data Type
                <TooltipContent className="tooltip-content">
                  The data type of the column (VARCHAR, INTEGER, TIMESTAMP, etc.). Defines what kind of data can be stored.
                </TooltipContent>
              </ThWithTooltip>
              <ThWithTooltip 
                $sortable 
                $active={sortField === "ordinal_position"} 
                $direction={sortDirection}
                onClick={() => handleSort("ordinal_position")}
              >
                Position
                <TooltipContent className="tooltip-content">
                  The ordinal position of the column in the table (1-based). Indicates the order of columns as defined in the table schema.
                </TooltipContent>
              </ThWithTooltip>
              <ThWithTooltip>
                Nullable
                <TooltipContent className="tooltip-content">
                  Whether the column allows NULL values. NULL means the value is unknown or not applicable.
                </TooltipContent>
              </ThWithTooltip>
              <ThWithTooltip 
                $sortable 
                $active={sortField === "sensitivity_level"} 
                $direction={sortDirection}
                onClick={() => handleSort("sensitivity_level")}
              >
                Sensitivity
                <TooltipContent className="tooltip-content">
                  Data sensitivity classification: HIGH (PII/PHI), MEDIUM (moderately sensitive), LOW (public data). Used for data governance and compliance.
                </TooltipContent>
              </ThWithTooltip>
              <ThWithTooltip>
                PII
                <TooltipContent className="tooltip-content">
                  Personally Identifiable Information flag. Indicates if the column contains data that can identify an individual (names, emails, IDs, etc.).
                </TooltipContent>
              </ThWithTooltip>
              <ThWithTooltip>
                PHI
                <TooltipContent className="tooltip-content">
                  Protected Health Information flag. Indicates if the column contains health-related data that requires special protection under HIPAA.
                </TooltipContent>
              </ThWithTooltip>
              <ThWithTooltip>
                Flags
                <TooltipContent className="tooltip-content">
                  Column flags: PK (Primary Key), FK (Foreign Key), UQ (Unique), IDX (Indexed). These indicate special properties and constraints.
                </TooltipContent>
              </ThWithTooltip>
            </tr>
          </thead>
          <tbody>
            {sortedColumns.length === 0 ? (
              <TableRow>
                <Td colSpan={11} style={{ padding: '40px', textAlign: 'center', color: theme.colors.text.secondary }}>
                  No column data available. Columns will appear here once cataloged.
                </Td>
              </TableRow>
            ) : (
              sortedColumns.map((column) => (
                <React.Fragment key={column.id}>
                  <TableRow onClick={() => toggleColumn(column.id)} style={{ cursor: 'pointer' }}>
                    <Td>
                      <strong style={{ color: theme.colors.primary.main }}>
                        {column.schema_name}
                      </strong>
                    </Td>
                    <Td>
                      <span style={{ color: theme.colors.text.secondary }}>
                        {column.table_name}
                      </span>
                    </Td>
                    <Td>
                      <strong>{column.column_name}</strong>
                    </Td>
                    <Td>
                      <span style={{ 
                        padding: "2px 8px", 
                        borderRadius: theme.borderRadius.sm,
                        backgroundColor: theme.colors.background.secondary,
                        fontSize: "0.85em"
                      }}>
                        {column.db_engine || 'N/A'}
                      </span>
                    </Td>
                    <Td>
                      <Badge $type={column.data_type}>{column.data_type}</Badge>
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary }}>
                      {column.ordinal_position || 'N/A'}
                    </Td>
                    <Td>
                      {column.is_nullable ? (
                        <span style={{ color: theme.colors.status.warning.text }}>Yes</span>
                      ) : (
                        <span style={{ color: theme.colors.status.success.text }}>No</span>
                      )}
                    </Td>
                    <Td>
                      {column.sensitivity_level && (
                        <Badge $level={column.sensitivity_level}>{column.sensitivity_level}</Badge>
                      )}
                    </Td>
                    <Td>
                      {column.contains_pii && <Badge $flag={true}>PII</Badge>}
                    </Td>
                    <Td>
                      {column.contains_phi && <Badge $flag={true}>PHI</Badge>}
                    </Td>
                    <Td>
                      <div style={{ display: "flex", gap: "4px", flexWrap: "wrap" }}>
                        {column.is_primary_key && <Badge $type="PK">PK</Badge>}
                        {column.is_foreign_key && <Badge $type="FK">FK</Badge>}
                        {column.is_unique && <Badge $type="UQ">UQ</Badge>}
                        {column.is_indexed && <Badge $type="IDX">IDX</Badge>}
                      </div>
                    </Td>
                  </TableRow>
                  {openColumnId === column.id && (
                    <TableRow>
                      <Td colSpan={11} style={{ padding: 0, border: 'none' }}>
                        <ColumnDetails $isOpen={openColumnId === column.id}>
                          <DetailGrid>
                            <DetailLabel>Ordinal Position:</DetailLabel>
                            <DetailValue>{column.ordinal_position || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Data Type:</DetailLabel>
                            <DetailValue>{column.data_type || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Character Max Length:</DetailLabel>
                            <DetailValue>{column.character_maximum_length || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Numeric Precision:</DetailLabel>
                            <DetailValue>{column.numeric_precision || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Numeric Scale:</DetailLabel>
                            <DetailValue>{column.numeric_scale || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Column Default:</DetailLabel>
                            <DetailValue>{column.column_default || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Data Category:</DetailLabel>
                            <DetailValue>{column.data_category || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Null Count:</DetailLabel>
                            <DetailValue>{formatNumber(column.null_count)}</DetailValue>
                            
                            <DetailLabel>Null Percentage:</DetailLabel>
                            <DetailValue>{formatPercentage(column.null_percentage)}</DetailValue>
                            
                            <DetailLabel>Distinct Count:</DetailLabel>
                            <DetailValue>{formatNumber(column.distinct_count)}</DetailValue>
                            
                            <DetailLabel>Distinct Percentage:</DetailLabel>
                            <DetailValue>{formatPercentage(column.distinct_percentage)}</DetailValue>
                            
                            <DetailLabel>Min Value:</DetailLabel>
                            <DetailValue>{column.min_value || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Max Value:</DetailLabel>
                            <DetailValue>{column.max_value || 'N/A'}</DetailValue>
                            
                            <DetailLabel>Avg Value:</DetailLabel>
                            <DetailValue>{column.avg_value || 'N/A'}</DetailValue>
                            
                            <DetailLabel>First Seen:</DetailLabel>
                            <DetailValue>{formatDate(column.first_seen_at)}</DetailValue>
                            
                            <DetailLabel>Last Seen:</DetailLabel>
                            <DetailValue>{formatDate(column.last_seen_at)}</DetailValue>
                            
                            <DetailLabel>Last Analyzed:</DetailLabel>
                            <DetailValue>{formatDate(column.last_analyzed_at)}</DetailValue>
                          </DetailGrid>
                          
                          <FlagsGrid>
                            <FlagItem>
                              <Badge $flag={column.is_primary_key}>Primary Key</Badge>
                            </FlagItem>
                            <FlagItem>
                              <Badge $flag={column.is_foreign_key}>Foreign Key</Badge>
                            </FlagItem>
                            <FlagItem>
                              <Badge $flag={column.is_unique}>Unique</Badge>
                            </FlagItem>
                            <FlagItem>
                              <Badge $flag={column.is_indexed}>Indexed</Badge>
                            </FlagItem>
                            <FlagItem>
                              <Badge $flag={column.is_auto_increment}>Auto Increment</Badge>
                            </FlagItem>
                            <FlagItem>
                              <Badge $flag={column.is_generated}>Generated</Badge>
                            </FlagItem>
                            <FlagItem>
                              <Badge $flag={column.is_nullable}>Nullable</Badge>
                            </FlagItem>
                            <FlagItem>
                              <Badge $flag={column.contains_pii}>Contains PII</Badge>
                            </FlagItem>
                            <FlagItem>
                              <Badge $flag={column.contains_phi}>Contains PHI</Badge>
                            </FlagItem>
                          </FlagsGrid>
                        </ColumnDetails>
                      </Td>
                    </TableRow>
                  )}
                </React.Fragment>
              ))
            )}
          </tbody>
        </Table>
      </TableContainer>
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

export default ColumnCatalog;
