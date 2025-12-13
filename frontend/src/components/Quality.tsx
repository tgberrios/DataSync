import React, { useState, useEffect, useCallback, useRef } from 'react';
import styled from 'styled-components';
import { qualityApi } from '../services/api';
import { Container, Header, Select, FiltersContainer, Pagination, PageButton, LoadingOverlay, ErrorMessage } from './shared/BaseComponents';
import { usePagination } from '../hooks/usePagination';
import { useTableFilters } from '../hooks/useTableFilters';
import { extractApiError } from '../utils/errorHandler';

const QualityList = styled.div`
  display: flex;
  flex-direction: column;
  gap: 12px;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

const QualityItem = styled.div`
  border: 1px solid #eee;
  border-radius: 6px;
  background-color: #fafafa;
  overflow: hidden;
  transition: all 0.2s ease;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.03);
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.2);
    background-color: #ffffff;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
    transform: translateY(-2px);
  }
`;

  const QualityHeader = styled.div`
  display: grid;
  grid-template-columns: 150px 150px 100px 100px 100px 1fr;
  align-items: center;
  padding: 10px 15px;
  background: #f8f9fa;
  border-bottom: 2px solid #dee2e6;
  font-weight: bold;
  font-size: 0.9em;
  
  & > div {
    text-align: left;
    padding: 0 8px;
  }
  
  & > div:nth-child(3) {
    text-align: right;
  }
  
  & > div:nth-child(4) {
    text-align: center;
  }
  
  & > div:nth-child(5) {
    text-align: center;
  }
  
  & > div:last-child {
    text-align: right;
  }
`;

  const QualitySummary = styled.div`
  display: grid;
  grid-template-columns: 150px 150px 100px 100px 100px 1fr;
  align-items: center;
  padding: 12px 15px;
  cursor: pointer;
  gap: 10px;
  font-size: 0.9em;
  transition: all 0.2s ease;
  
  &:hover {
    background: linear-gradient(90deg, #f0f0f0 0%, #f8f9fa 100%);
  }
  
  & > div {
    text-align: left;
    padding: 0 8px;
  }
  
  & > div:nth-child(3) {
    text-align: right;
  }
  
  & > div:nth-child(4) {
    text-align: center;
  }
  
  & > div:nth-child(5) {
    text-align: center;
  }
  
  & > div:last-child {
    text-align: right;
  }
`;

const QualityDetails = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '800px' : '0'};
  opacity: ${props => props.$isOpen ? '1' : '0'};
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  border-top: ${props => props.$isOpen ? '1px solid #eee' : 'none'};
  background-color: white;
  overflow: hidden;
`;

const MetricsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  padding: 15px;
  gap: 15px;
  
  & > div {
    position: relative;
  }
  
  & > div::before {
    content: attr(data-label);
    position: absolute;
    top: -8px;
    left: 12px;
    background: white;
    padding: 0 4px;
    font-size: 0.75em;
    color: #666;
  }
`;

const MetricCard = styled.div`
  background: white;
  border: 1px solid #eee;
  border-radius: 6px;
  padding: 12px;
  transition: all 0.2s ease;
  animation: fadeIn 0.2s ease-in;
  animation-fill-mode: both;
  
  &:hover {
    transform: translateY(-3px);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
    border-color: rgba(10, 25, 41, 0.2);
    background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
  }
  
  &:nth-child(1) { animation-delay: 0.1s; }
  &:nth-child(2) { animation-delay: 0.15s; }
  &:nth-child(3) { animation-delay: 0.2s; }
  &:nth-child(4) { animation-delay: 0.25s; }
  &:nth-child(5) { animation-delay: 0.15s; }
  &:nth-child(6) { animation-delay: 0.35s; }
  &:nth-child(7) { animation-delay: 0.2s; }
`;

const MetricLabel = styled.div`
  color: #666;
  font-size: 0.85em;
  margin-bottom: 5px;
  font-weight: 500;
`;

const MetricValue = styled.div`
  font-size: 1.1em;
  font-weight: 500;
`;

const ValidationStatus = styled.span<{ $status: string }>`
  padding: 4px 10px;
  border-radius: 6px;
  font-size: 0.85em;
  font-weight: 500;
  display: inline-block;
  transition: all 0.2s ease;
  
  &:hover {
    transform: scale(1.05);
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
  }
  background-color: ${props => {
    switch (props.$status) {
      case 'PASSED': return '#e8f5e9';
      case 'WARNING': return '#fff3e0';
      case 'FAILED': return '#ffebee';
      default: return '#f5f5f5';
    }
  }};
  color: ${props => {
    switch (props.$status) {
      case 'PASSED': return '#2e7d32';
      case 'WARNING': return '#ef6c00';
      case 'FAILED': return '#c62828';
      default: return '#757575';
    }
  }};
`;

const QualityScore = styled.span<{ $score: number }>`
  padding: 4px 10px;
  border-radius: 6px;
  font-size: 0.9em;
  font-weight: 500;
  display: inline-block;
  transition: all 0.2s ease;
  
  &:hover {
    transform: scale(1.05);
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
  }
  background-color: ${props => {
    if (props.$score >= 90) return '#e8f5e9';
    if (props.$score >= 70) return '#f1f8e9';
    if (props.$score >= 50) return '#fff3e0';
    return '#ffebee';
  }};
  color: ${props => {
    if (props.$score >= 90) return '#1b5e20';
    if (props.$score >= 70) return '#33691e';
    if (props.$score >= 50) return '#e65100';
    return '#333333';
  }};
`;

const ErrorDetails = styled.pre`
  margin: 15px;
  padding: 12px;
  background-color: #f8f8f8;
  border-radius: 4px;
  font-size: 0.9em;
  overflow-x: auto;
  border: 1px solid #eee;
  white-space: pre-wrap;
`;


/**
 * Componente para monitorear la calidad de los datos
 * Muestra métricas de calidad, validaciones y detalles de problemas encontrados
 */
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
  const { filters, setFilter } = useTableFilters({
    engine: '',
    status: ''
  });

  /**
   * Obtiene los datos de calidad desde la API
   */
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

  /**
   * Alterna la expansión de un item de calidad
   */
  const toggleItem = useCallback((id: number) => {
    setOpenItemId(prev => prev === id ? null : id);
  }, []);

  /**
   * Formatea un número con separadores de miles
   */
  const formatNumber = useCallback((num: number) => num.toLocaleString(), []);


  return (
    <Container>
      <Header>
        Data Quality Monitor
      </Header>

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
      </FiltersContainer>

      {loading && <LoadingOverlay>Loading quality metrics...</LoadingOverlay>}
      {error && <ErrorMessage>{error}</ErrorMessage>}

      {!loading && !error && (
        <>
          <QualityList>
            {qualityData.length === 0 ? (
              <div style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
                No quality metrics found
              </div>
            ) : (
              <>
                <QualityHeader>
                  <div>Schema</div>
                  <div>Table</div>
                  <div>Total Rows</div>
                  <div>Status</div>
                  <div>Quality</div>
                  <div>Last Check</div>
                </QualityHeader>
                {qualityData.map((item) => (
                <QualityItem key={item.id}>
                  <QualitySummary onClick={() => toggleItem(item.id)}>
                    <div>{item.schema_name}</div>
                    <div>{item.table_name}</div>
                    <div>{formatNumber(item.total_rows)}</div>
                    <ValidationStatus $status={item.validation_status}>
                      {item.validation_status}
                    </ValidationStatus>
                    <QualityScore $score={item.quality_score}>
                      {item.quality_score}%
                    </QualityScore>
                    <div style={{ textAlign: 'right', color: '#666', fontSize: '0.85em' }}>
                      {new Date(item.check_timestamp).toLocaleString()}
                    </div>
                  </QualitySummary>

                  <QualityDetails $isOpen={openItemId === item.id}>
                    <MetricsGrid>
                      <MetricCard data-label="Missing Values">
                        <MetricValue>{formatNumber(item.null_count)} rows</MetricValue>
                        <MetricLabel>Rows with NULL values</MetricLabel>
                      </MetricCard>
                      <MetricCard data-label="Duplicate Records">
                        <MetricValue>{formatNumber(item.duplicate_count)} rows</MetricValue>
                        <MetricLabel>Duplicate entries found</MetricLabel>
                      </MetricCard>
                      <MetricCard data-label="Type Mismatches">
                        <MetricValue>{formatNumber(item.invalid_type_count)} fields</MetricValue>
                        <MetricLabel>Fields with incorrect data types</MetricLabel>
                      </MetricCard>
                      <MetricCard data-label="Range Violations">
                        <MetricValue>{formatNumber(item.out_of_range_count)} values</MetricValue>
                        <MetricLabel>Values outside valid range</MetricLabel>
                      </MetricCard>
                      <MetricCard data-label="Referential Issues">
                        <MetricValue>{formatNumber(item.referential_integrity_errors)} errors</MetricValue>
                        <MetricLabel>Foreign key violations</MetricLabel>
                      </MetricCard>
                      <MetricCard data-label="Constraint Issues">
                        <MetricValue>{formatNumber(item.constraint_violation_count)} violations</MetricValue>
                        <MetricLabel>Failed constraint checks</MetricLabel>
                      </MetricCard>
                      <MetricCard data-label="Analysis Time">
                        <MetricValue>{(item.check_duration_ms / 1000).toFixed(2)}s</MetricValue>
                        <MetricLabel>Time taken to check quality</MetricLabel>
                      </MetricCard>
                    </MetricsGrid>

                    {item.type_mismatch_details && (
                      <>
                        <MetricLabel style={{ margin: '0 15px' }}>Type Mismatch Details:</MetricLabel>
                        <ErrorDetails>
                          {Object.entries(item.type_mismatch_details).map(([column, details]) => (
                            `Column: ${column}\n${JSON.stringify(details, null, 2)}\n\n`
                          ))}
                        </ErrorDetails>
                      </>
                    )}

                    {item.integrity_check_details && (
                      <>
                        <MetricLabel style={{ margin: '0 15px' }}>Integrity Check Details:</MetricLabel>
                        <ErrorDetails>
                          {Object.entries(item.integrity_check_details).map(([column, details]) => (
                            `Column: ${column}\n${JSON.stringify(details, null, 2)}\n\n`
                          ))}
                        </ErrorDetails>
                      </>
                    )}

                    {item.error_details && (
                      <>
                        <MetricLabel style={{ margin: '0 15px' }}>Error Details:</MetricLabel>
                        <ErrorDetails>{item.error_details}</ErrorDetails>
                      </>
                    )}
                  </QualityDetails>
                </QualityItem>
              ))}
              </>
            )}
          </QualityList>

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
        </>
      )}
    </Container>
  );
};

export default Quality;