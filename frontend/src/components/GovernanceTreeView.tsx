import React, { useState, useMemo } from 'react';
import styled, { keyframes } from 'styled-components';
import { theme } from '../theme/theme';

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

const TreeContainer = styled.div`
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  font-size: 0.95em;
  background: ${theme.colors.background.main};
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.lg};
  padding: ${theme.spacing.lg};
  max-height: 800px;
  overflow-y: auto;
  overflow-x: hidden;
  box-shadow: ${theme.shadows.md};
  position: relative;
  
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
    transition: background ${theme.transitions.normal};
    
    &:hover {
      background: ${theme.colors.primary.main};
    }
  }
`;

const TreeNode = styled.div`
  user-select: none;
  margin: 4px 0;
`;

const TreeContent = styled.div`
  display: flex;
  align-items: center;
  padding: 8px 4px;
  border-radius: ${theme.borderRadius.md};
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  
  &:hover {
    background: ${theme.colors.background.secondary};
  }
`;

const ExpandIconContainer = styled.div<{ $isExpanded: boolean }>`
  width: 20px;
  height: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin-right: 8px;
  transition: transform ${theme.transitions.normal};
  transform: ${props => props.$isExpanded ? 'rotate(90deg)' : 'rotate(0deg)'};
  color: ${theme.colors.text.secondary};
`;

const NodeLabel = styled.span`
  font-weight: 500;
  color: ${theme.colors.text.primary};
  flex: 1;
  display: flex;
  align-items: center;
  gap: 8px;
`;

const CountBadge = styled.span`
  background: ${theme.colors.background.secondary};
  color: ${theme.colors.text.secondary};
  padding: 2px 8px;
  border-radius: ${theme.borderRadius.sm};
  font-size: 0.85em;
  font-weight: 500;
`;

const ExpandableContent = styled.div<{ $isExpanded: boolean }>`
  max-height: ${props => props.$isExpanded ? '10000px' : '0'};
  overflow: hidden;
  transition: max-height 0.3s ease-out;
  margin-left: 28px;
`;

const TableDetailsRow = styled.div<{ $level?: number }>`
  padding: 12px 8px;
  padding-left: ${props => (props.$level || 0) * 24 + 36}px;
  margin: 2px 0;
  border-radius: ${theme.borderRadius.md};
  background: ${theme.colors.background.main};
  border: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  animation: ${fadeIn} 0.3s ease-out;
  
  &:hover {
    background: ${theme.colors.background.secondary};
    border-color: ${theme.colors.primary.main};
    transform: translateX(4px);
  }
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

const EmptyStateIcon = styled.div`
  font-size: 3em;
  margin-bottom: ${theme.spacing.md};
  animation: ${fadeIn} 0.5s ease-out;
  font-family: 'Courier New', monospace;
  opacity: 0.5;
`;

const EmptyStateTitle = styled.div`
  font-size: 1.1em;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  font-weight: 500;
  margin-bottom: ${theme.spacing.sm};
`;

const EmptyStateText = styled.div`
  font-size: 0.9em;
  opacity: 0.7;
`;

interface GovernanceItem {
  id: number;
  schema_name?: string;
  table_name?: string;
  inferred_source_engine?: string;
  data_category?: string;
  business_domain?: string;
  health_status?: string;
  sensitivity_level?: string;
  data_quality_score?: number;
  table_size_mb?: number;
  total_rows?: number;
  access_frequency?: string;
}

interface SchemaNode {
  name: string;
  tables: GovernanceItem[];
}

interface GovernanceTreeViewProps {
  items: GovernanceItem[];
  onItemClick?: (item: GovernanceItem) => void;
}

const GovernanceTreeView: React.FC<GovernanceTreeViewProps> = ({ items, onItemClick }) => {
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());

  const treeData = useMemo(() => {
    const schemas = new Map<string, SchemaNode>();

    items.forEach(item => {
      const schemaKey = item.schema_name || 'default';
      if (!schemas.has(schemaKey)) {
        schemas.set(schemaKey, {
          name: schemaKey,
          tables: []
        });
      }
      schemas.get(schemaKey)!.tables.push(item);
    });

    return Array.from(schemas.values()).sort((a, b) => a.name.localeCompare(b.name));
  }, [items]);

  const toggleSchema = (schemaName: string) => {
    setExpandedSchemas(prev => {
      const next = new Set(prev);
      if (next.has(schemaName)) {
        next.delete(schemaName);
      } else {
        next.add(schemaName);
      }
      return next;
    });
  };

  const renderTreeLine = (level: number, isLast: boolean) => {
    if (level === 0) return null;
    const lines: string[] = [];
    for (let i = 0; i < level - 1; i++) {
      lines.push('│  ');
    }
    lines.push(isLast ? '└─ ' : '├─ ');
    return <span style={{ color: theme.colors.text.secondary, fontFamily: 'monospace' }}>{lines.join('')}</span>;
  };

  const formatNumber = (value: number | string | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    const num = Number(value);
    if (isNaN(num)) return 'N/A';
    return num.toLocaleString();
  };

  const formatBytes = (mb: number | string | null | undefined) => {
    if (mb === null || mb === undefined) return 'N/A';
    const num = Number(mb);
    if (isNaN(num)) return 'N/A';
    if (num < 1) return `${(num * 1024).toFixed(2)} KB`;
    return `${num.toFixed(2)} MB`;
  };

  const renderSchema = (schema: SchemaNode, level: number) => {
    const isExpanded = expandedSchemas.has(schema.name);
    const isLast = false;

    return (
      <TreeNode key={schema.name}>
        <TreeContent onClick={() => toggleSchema(schema.name)}>
          {renderTreeLine(level, isLast)}
          <ExpandIconContainer $isExpanded={isExpanded}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <polyline points="9 18 15 12 9 6"/>
            </svg>
          </ExpandIconContainer>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={theme.colors.primary.main} strokeWidth="2" style={{ marginRight: '8px' }}>
            <ellipse cx="12" cy="5" rx="9" ry="3"/>
            <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>
            <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
          </svg>
          <NodeLabel>
            {schema.name}
            <CountBadge>{schema.tables.length}</CountBadge>
          </NodeLabel>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded}>
          {isExpanded && schema.tables.map((item, index) => 
            renderTable(item, schema.name, level + 1, index === schema.tables.length - 1)
          )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  const renderTable = (item: GovernanceItem, _schemaName: string, level: number, isLast: boolean) => {
    return (
      <TableDetailsRow
        key={item.id}
        $level={level}
        onClick={() => onItemClick?.(item)}
      >
        {renderTreeLine(level, isLast)}
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke={theme.colors.text.secondary} strokeWidth="2" style={{ marginRight: '8px' }}>
          <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>
          <polyline points="9 22 9 12 15 12 15 22"/>
        </svg>
        <span style={{ marginRight: '8px', fontWeight: 500, color: theme.colors.text.primary }}>
          {item.table_name || 'N/A'}
        </span>
        {item.inferred_source_engine && (
          <Badge $type={item.inferred_source_engine}>{item.inferred_source_engine}</Badge>
        )}
        {item.data_category && (
          <Badge type={item.data_category}>{item.data_category}</Badge>
        )}
        {item.health_status && (
          <Badge $status={item.health_status}>{item.health_status}</Badge>
        )}
        {item.sensitivity_level && (
          <Badge type={`${item.sensitivity_level}_SENSITIVITY`}>{item.sensitivity_level}</Badge>
        )}
        {item.data_quality_score !== undefined && (
          <QualityScore $score={item.data_quality_score}>{item.data_quality_score}%</QualityScore>
        )}
        <span style={{ marginLeft: 'auto', color: theme.colors.text.secondary, fontSize: '0.85em' }}>
          {formatNumber(item.total_rows)} rows • {formatBytes(item.table_size_mb)}
        </span>
      </TableDetailsRow>
    );
  };

  if (treeData.length === 0) {
    return (
      <TreeContainer>
        <div style={{ 
          padding: '60px 40px', 
          textAlign: 'center', 
          color: theme.colors.text.secondary 
        }}>
          <EmptyStateIcon>■</EmptyStateIcon>
          <EmptyStateTitle>No governance data available</EmptyStateTitle>
          <EmptyStateText>Data will appear here once collected.</EmptyStateText>
        </div>
      </TreeContainer>
    );
  }

  return (
    <TreeContainer>
      {treeData.map((schema) => 
        renderSchema(schema, 0)
      )}
    </TreeContainer>
  );
};

export default GovernanceTreeView;

