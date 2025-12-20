import { useState, useEffect, useCallback, useRef } from 'react';
import styled from 'styled-components';
import {
  Container,
  Header,
  ErrorMessage,
  LoadingOverlay,
  Select,
  FiltersContainer,
  Button,
} from './shared/BaseComponents';
import { useTableFilters } from '../hooks/useTableFilters';
import { qualityApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { theme } from '../theme/theme';
import QualityTreeView from './QualityTreeView';

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
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  
  ${props => {
    const badgeType = props.$status || '';
    if (badgeType === 'PASSED') {
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
    if (badgeType === 'FAILED') {
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
  
  &:hover {
    transform: translateY(-2px) scale(1.08);
    box-shadow: ${theme.shadows.lg};
  }
`;

const QualityScore = styled.span<{ $score: number }>`
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
  
  &:hover {
    transform: translateY(-2px) scale(1.08);
    box-shadow: ${theme.shadows.lg};
  }
`;

const Quality = () => {
  const isMountedRef = useRef(true);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [qualityData, setQualityData] = useState<any[]>([]);
  const [selectedItem, setSelectedItem] = useState<any | null>(null);

  const { filters, setFilter, clearFilters } = useTableFilters({
    engine: '',
    status: ''
  });

  const formatNumber = useCallback((num: number) => num.toLocaleString(), []);

  const formatDate = useCallback((date: string) => {
    if (!date) return '-';
    return new Date(date).toLocaleString();
  }, []);

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
        page: 1,
        limit: 1000,
        search: filters.engine as string ? `engine:${filters.engine}` : undefined,
        status: filters.status as string || undefined
      });
      
      if (isMountedRef.current) {
        setQualityData(response.data || []);
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
  }, [filters.engine, filters.status]);

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

  const handleItemClick = useCallback((item: any) => {
    setSelectedItem((prev: any) => prev?.id === item.id ? null : item);
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
          }}
        >
          Reset All
        </Button>
      </FiltersContainer>

      <div style={{ display: 'grid', gridTemplateColumns: selectedItem ? '1fr 400px' : '1fr', gap: theme.spacing.lg }}>
        <QualityTreeView 
          items={qualityData} 
          onItemClick={handleItemClick}
        />
        
        {selectedItem && (
          <div style={{
            background: theme.colors.background.secondary,
            border: `1px solid ${theme.colors.border.light}`,
            borderRadius: theme.borderRadius.md,
            padding: theme.spacing.lg,
            position: 'sticky',
            top: theme.spacing.md,
            maxHeight: 'calc(100vh - 200px)',
            overflowY: 'auto'
          }}>
            <h3 style={{ marginTop: 0, marginBottom: theme.spacing.md, color: theme.colors.text.primary }}>
              Quality Details
            </h3>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: theme.spacing.md }}>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Schema:</strong>
                <div style={{ color: theme.colors.text.primary }}>{selectedItem.schema_name || 'N/A'}</div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Table:</strong>
                <div style={{ color: theme.colors.text.primary }}>{selectedItem.table_name || 'N/A'}</div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Total Rows:</strong>
                <div style={{ color: theme.colors.text.primary }}>{formatNumber(selectedItem.total_rows || 0)}</div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Status:</strong>
                <div>
                  <Badge $status={selectedItem.validation_status}>{selectedItem.validation_status}</Badge>
                </div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Quality Score:</strong>
                <div>
                  <QualityScore $score={selectedItem.quality_score || 0}>
                    {selectedItem.quality_score || 0}%
                  </QualityScore>
                </div>
              </div>
              <div>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Last Check:</strong>
                <div style={{ color: theme.colors.text.primary, fontSize: '0.9em' }}>
                  {formatDate(selectedItem.check_timestamp)}
                </div>
              </div>
              <div style={{ borderTop: `1px solid ${theme.colors.border.light}`, paddingTop: theme.spacing.md, marginTop: theme.spacing.sm }}>
                <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Metrics:</strong>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: theme.spacing.sm, marginTop: theme.spacing.sm }}>
                  <div>
                    <div style={{ fontSize: '0.85em', color: theme.colors.text.secondary }}>Missing Values</div>
                    <div style={{ fontWeight: 600 }}>{formatNumber(selectedItem.null_count || 0)}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.85em', color: theme.colors.text.secondary }}>Duplicates</div>
                    <div style={{ fontWeight: 600 }}>{formatNumber(selectedItem.duplicate_count || 0)}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.85em', color: theme.colors.text.secondary }}>Type Mismatches</div>
                    <div style={{ fontWeight: 600 }}>{formatNumber(selectedItem.invalid_type_count || 0)}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.85em', color: theme.colors.text.secondary }}>Range Violations</div>
                    <div style={{ fontWeight: 600 }}>{formatNumber(selectedItem.out_of_range_count || 0)}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.85em', color: theme.colors.text.secondary }}>Referential Issues</div>
                    <div style={{ fontWeight: 600 }}>{formatNumber(selectedItem.referential_integrity_errors || 0)}</div>
                  </div>
                  <div>
                    <div style={{ fontSize: '0.85em', color: theme.colors.text.secondary }}>Constraint Issues</div>
                    <div style={{ fontWeight: 600 }}>{formatNumber(selectedItem.constraint_violation_count || 0)}</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </Container>
  );
};

export default Quality;
