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
import { qualityApi } from '../services/api';
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

const QualityDetails = styled.div<{ $isOpen: boolean }>`
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
    border-color: ${theme.colors.border.dark};
  }
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

const ValidationStatus = styled.span<{ $status: string }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.85em;
  font-weight: bold;
  display: inline-block;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    transform: scale(1.05);
    box-shadow: ${theme.shadows.sm};
  }
  background-color: ${props => {
    switch (props.$status) {
      case 'PASSED': return theme.colors.status.success.bg;
      case 'WARNING': return theme.colors.status.warning.bg;
      case 'FAILED': return theme.colors.status.error.bg;
      default: return theme.colors.background.secondary;
    }
  }};
  color: ${props => {
    switch (props.$status) {
      case 'PASSED': return theme.colors.status.success.text;
      case 'WARNING': return theme.colors.status.warning.text;
      case 'FAILED': return theme.colors.status.error.text;
      default: return theme.colors.text.secondary;
    }
  }};
`;

const QualityScore = styled.span<{ $score: number }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.9em;
  font-weight: bold;
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
    if (props.$score >= 70) return '#33691e';
    if (props.$score >= 50) return theme.colors.status.warning.text;
    return theme.colors.status.error.text;
  }};
`;

const ErrorDetails = styled.pre`
  margin: ${theme.spacing.md};
  padding: ${theme.spacing.sm};
  background-color: ${theme.colors.background.secondary};
  border-radius: ${theme.borderRadius.sm};
  font-size: 0.9em;
  overflow-x: auto;
  border: 1px solid ${theme.colors.border.light};
  white-space: pre-wrap;
  color: ${theme.colors.text.primary};
`;

const ActionButton = styled(Button)`
  padding: 6px 12px;
  margin-right: 5px;
  font-size: 0.9em;
`;

const Quality = () => {
  const isMountedRef = useRef(true);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [qualityData, setQualityData] = useState<any[]>([]);
  const [openItemId, setOpenItemId] = useState<number | null>(null);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 10
  });

  const { page, setPage } = usePagination(1, 10);
  const { filters, setFilter, clearFilters } = useTableFilters({
    engine: '',
    status: ''
  });

  const [sortField, setSortField] = useState("check_timestamp");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");

  const formatNumber = useCallback((num: number) => num.toLocaleString(), []);

  const formatDate = useCallback((date: string) => {
    if (!date) return '-';
    return new Date(date).toLocaleString();
  }, []);

  const sortedData = useMemo(() => {
    if (!sortField) return qualityData;
    return [...qualityData].sort((a, b) => {
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
  }, [qualityData, sortField, sortDirection]);

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
    const headers = ["Schema", "Table", "Total Rows", "Status", "Quality Score", "Missing Values", "Duplicates", "Type Mismatches", "Range Violations", "Referential Issues", "Constraint Violations", "Last Check"];
    const rows = sortedData.map(item => [
      item.schema_name,
      item.table_name,
      item.total_rows || 0,
      item.validation_status || "",
      item.quality_score || 0,
      item.null_count || 0,
      item.duplicate_count || 0,
      item.invalid_type_count || 0,
      item.out_of_range_count || 0,
      item.referential_integrity_errors || 0,
      item.constraint_violation_count || 0,
      formatDate(item.check_timestamp)
    ]);
    
    const csvContent = [
      headers.join(","),
      ...rows.map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(","))
    ].join("\n");
    
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    link.setAttribute("href", url);
    link.setAttribute("download", `quality_export_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }, [sortedData, formatDate]);

  const isInitialLoadRef = useRef(true);
  
  const fetchQualityData = useCallback(async () => {
    if (!isMountedRef.current) return;
    
    const isInitialLoad = isInitialLoadRef.current;
    
    try {
      setError(null);
      if (isInitialLoad) {
        setLoading(true);
      }
      const response = await qualityApi.getQualityMetrics({
        page,
        limit: 10,
        search: filters.engine as string ? `engine:${filters.engine}` : undefined,
        status: filters.status as string || undefined
      });
      
      if (isMountedRef.current) {
        setQualityData(response.data);
        setPagination(response.pagination);
        isInitialLoadRef.current = false;
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
  }, [page, filters.engine, filters.status]);

  useEffect(() => {
    isMountedRef.current = true;
    fetchQualityData();
    
    const interval = setInterval(() => {
      if (isMountedRef.current) {
        fetchQualityData();
      }
    }, 30000);
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
    };
  }, [fetchQualityData]);

  const toggleItem = useCallback((id: number) => {
    setOpenItemId(prev => prev === id ? null : id);
  }, []);

  return (
    <Container>
      {loading && <LoadingOverlay>Loading quality metrics...</LoadingOverlay>}

      <Header>Data Quality Monitor</Header>

      {error && <ErrorMessage>{error}</ErrorMessage>}

      <FiltersContainer>
        <Select 
          value={filters.engine as string}
          onChange={(e) => {
            setFilter('engine', e.target.value);
            setPage(1);
          }}
        >
          <option value="">All Engines</option>
          <option value="PostgreSQL">PostgreSQL</option>
          <option value="MongoDB">MongoDB</option>
          <option value="MSSQL">MSSQL</option>
          <option value="MariaDB">MariaDB</option>
          <option value="Oracle">Oracle</option>
        </Select>

        <Select
          value={filters.status as string}
          onChange={(e) => {
            setFilter('status', e.target.value);
            setPage(1);
          }}
        >
          <option value="">All Status</option>
          <option value="PASSED">Passed</option>
          <option value="WARNING">Warning</option>
          <option value="FAILED">Failed</option>
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
        <Table $minWidth="1400px">
          <thead>
            <tr>
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
                $active={sortField === "table_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("table_name")}
              >
                Table
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "total_rows"} 
                $direction={sortDirection}
                onClick={() => handleSort("total_rows")}
              >
                Total Rows
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "validation_status"} 
                $direction={sortDirection}
                onClick={() => handleSort("validation_status")}
              >
                Status
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "quality_score"} 
                $direction={sortDirection}
                onClick={() => handleSort("quality_score")}
              >
                Quality Score
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "check_timestamp"} 
                $direction={sortDirection}
                onClick={() => handleSort("check_timestamp")}
              >
                Last Check
              </SortableTh>
              <Th>Actions</Th>
            </tr>
          </thead>
          <tbody>
            {sortedData.length === 0 ? (
              <TableRow>
                <Td colSpan={7} style={{ textAlign: 'center', padding: '20px', color: theme.colors.text.secondary }}>
                  No quality metrics found
                </Td>
              </TableRow>
            ) : (
              sortedData.map((item) => (
                <React.Fragment key={item.id}>
                  <TableRow>
                    <Td>
                      <strong style={{ color: theme.colors.primary.main }}>
                        {item.schema_name}
                      </strong>
                    </Td>
                    <Td>{item.table_name}</Td>
                    <Td>{formatNumber(item.total_rows || 0)}</Td>
                    <Td>
                      <ValidationStatus $status={item.validation_status}>
                        {item.validation_status}
                      </ValidationStatus>
                    </Td>
                    <Td>
                      <QualityScore $score={item.quality_score || 0}>
                        {item.quality_score || 0}%
                      </QualityScore>
                    </Td>
                    <Td style={{ color: theme.colors.text.secondary, fontSize: '0.9em' }}>
                      {formatDate(item.check_timestamp)}
                    </Td>
                    <Td>
                      <ActionButton
                        $variant="secondary"
                        onClick={() => toggleItem(item.id)}
                      >
                        {openItemId === item.id ? 'Hide Details' : 'Show Details'}
                      </ActionButton>
                    </Td>
                  </TableRow>
                  {openItemId === item.id && (
                    <TableRow>
                      <Td colSpan={7} style={{ padding: 0 }}>
                        <QualityDetails $isOpen={true}>
                          <DetailsGrid>
                            <DetailCard>
                              <DetailValue>{formatNumber(item.null_count || 0)} rows</DetailValue>
                              <DetailLabel>Missing Values</DetailLabel>
                            </DetailCard>
                            <DetailCard>
                              <DetailValue>{formatNumber(item.duplicate_count || 0)} rows</DetailValue>
                              <DetailLabel>Duplicate Records</DetailLabel>
                            </DetailCard>
                            <DetailCard>
                              <DetailValue>{formatNumber(item.invalid_type_count || 0)} fields</DetailValue>
                              <DetailLabel>Type Mismatches</DetailLabel>
                            </DetailCard>
                            <DetailCard>
                              <DetailValue>{formatNumber(item.out_of_range_count || 0)} values</DetailValue>
                              <DetailLabel>Range Violations</DetailLabel>
                            </DetailCard>
                            <DetailCard>
                              <DetailValue>{formatNumber(item.referential_integrity_errors || 0)} errors</DetailValue>
                              <DetailLabel>Referential Issues</DetailLabel>
                            </DetailCard>
                            <DetailCard>
                              <DetailValue>{formatNumber(item.constraint_violation_count || 0)} violations</DetailValue>
                              <DetailLabel>Constraint Issues</DetailLabel>
                            </DetailCard>
                            <DetailCard>
                              <DetailValue>{(item.check_duration_ms / 1000 || 0).toFixed(2)}s</DetailValue>
                              <DetailLabel>Analysis Time</DetailLabel>
                            </DetailCard>
                          </DetailsGrid>

                          {item.type_mismatch_details && (
                            <>
                              <DetailLabel style={{ margin: `0 ${theme.spacing.md}` }}>Type Mismatch Details:</DetailLabel>
                              <ErrorDetails>
                                {Object.entries(item.type_mismatch_details).map(([column, details]) => (
                                  `Column: ${column}\n${JSON.stringify(details, null, 2)}\n\n`
                                ))}
                              </ErrorDetails>
                            </>
                          )}

                          {item.integrity_check_details && (
                            <>
                              <DetailLabel style={{ margin: `0 ${theme.spacing.md}` }}>Integrity Check Details:</DetailLabel>
                              <ErrorDetails>
                                {Object.entries(item.integrity_check_details).map(([column, details]) => (
                                  `Column: ${column}\n${JSON.stringify(details, null, 2)}\n\n`
                                ))}
                              </ErrorDetails>
                            </>
                          )}

                          {item.error_details && (
                            <>
                              <DetailLabel style={{ margin: `0 ${theme.spacing.md}` }}>Error Details:</DetailLabel>
                              <ErrorDetails>{item.error_details}</ErrorDetails>
                            </>
                          )}
                        </QualityDetails>
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
            onClick={() => setPage(page - 1)}
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
            onClick={() => setPage(page + 1)}
          >
            Next
          </PageButton>
        </Pagination>
      )}
    </Container>
  );
};

export default Quality;
