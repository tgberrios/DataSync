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
  animation: ${fadeIn} 0.3s ease-out;
  margin-bottom: 2px;
`;

const TreeLine = styled.span<{ $isLast?: boolean }>`
  color: ${theme.colors.border.medium};
  margin-right: 6px;
  font-family: 'Courier New', monospace;
  font-size: 0.9em;
  transition: color ${theme.transitions.normal};
`;

const TreeContent = styled.div<{ $level: number; $isExpanded?: boolean; $nodeType?: 'schema' | 'object' }>`
  display: flex;
  align-items: center;
  padding: ${props => props.$level === 0 ? '12px 8px' : '10px 8px'};
  padding-left: ${props => props.$level * 24 + 8}px;
  cursor: pointer;
  border-radius: ${theme.borderRadius.md};
  transition: all ${theme.transitions.normal};
  position: relative;
  margin: 2px 0;
  
  ${props => {
    if (props.$nodeType === 'schema') {
      return `
        background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
        border: 1px solid ${theme.colors.border.light};
        font-weight: 600;
        
        &:hover {
          background: linear-gradient(135deg, ${theme.colors.primary.light}15 0%, ${theme.colors.primary.main}15 100%);
          border-color: ${theme.colors.primary.main};
          transform: translateX(4px);
        }
      `;
    }
    return `
      &:hover {
        background: ${theme.colors.background.secondary};
        transform: translateX(4px);
      }
    `;
  }}
`;

const ExpandIconContainer = styled.div<{ $isExpanded: boolean }>`
  width: 20px;
  height: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin-right: 8px;
  color: ${theme.colors.primary.main};
  transition: transform ${theme.transitions.normal};
  transform: ${props => props.$isExpanded ? 'rotate(90deg)' : 'rotate(0deg)'};
  
  svg {
    width: 14px;
    height: 14px;
  }
`;

const NodeLabel = styled.span<{ $isSchema?: boolean }>`
  flex: 1;
  color: ${props => props.$isSchema ? theme.colors.primary.main : theme.colors.text.primary};
  font-weight: ${props => props.$isSchema ? 600 : 500};
  margin-right: 8px;
`;

const CountBadge = styled.span`
  padding: 2px 8px;
  border-radius: ${theme.borderRadius.sm};
  background: ${theme.colors.background.tertiary};
  color: ${theme.colors.text.secondary};
  font-size: 0.8em;
  font-weight: 500;
  margin-left: auto;
`;

const ExpandableContent = styled.div<{ $isExpanded: boolean; $level: number }>`
  max-height: ${props => props.$isExpanded ? '10000px' : '0'};
  overflow: hidden;
  transition: max-height 0.3s cubic-bezier(0.4, 0, 0.2, 1);
`;

const ObjectDetailsRow = styled.div<{ $level?: number }>`
  padding: 12px 8px;
  padding-left: ${props => (props.$level || 0) * 24 + 36}px;
  margin: 2px 0;
  border-radius: ${theme.borderRadius.md};
  background: ${theme.colors.background.main};
  border: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  
  &:hover {
    background: ${theme.colors.background.secondary};
    border-color: ${theme.colors.primary.main};
    transform: translateX(4px);
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

const Badge = styled.span<{ $status?: string; $type?: string }>`
  padding: 4px 8px;
  border-radius: ${theme.borderRadius.sm};
  font-size: 0.75em;
  font-weight: 500;
  margin-left: 8px;
  display: inline-block;
  
  ${props => {
    if (props.$status) {
      const statusColors: Record<string, { bg: string; text: string }> = {
        'HEALTHY': { bg: theme.colors.status.success.bg, text: theme.colors.status.success.text },
        'WARNING': { bg: theme.colors.status.warning.bg, text: theme.colors.status.warning.text },
        'CRITICAL': { bg: theme.colors.status.error.bg, text: theme.colors.status.error.text },
        'REAL_TIME': { bg: theme.colors.primary.light, text: theme.colors.primary.dark },
        'HIGH': { bg: theme.colors.primary.light, text: theme.colors.primary.dark },
        'MEDIUM': { bg: theme.colors.status.info.bg, text: theme.colors.status.info.text },
        'LOW': { bg: theme.colors.background.secondary, text: theme.colors.text.secondary },
        'RARE': { bg: theme.colors.background.secondary, text: theme.colors.text.secondary },
      };
      const colors = statusColors[props.$status] || { bg: theme.colors.background.secondary, text: theme.colors.text.secondary };
      return `background-color: ${colors.bg}; color: ${colors.text};`;
    }
    if (props.$type) {
      return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.primary};`;
    }
    return `background-color: ${theme.colors.background.secondary}; color: ${theme.colors.text.secondary};`;
  }}
`;

interface GovernanceItem {
  id: number;
  server_name?: string;
  database_name?: string;
  schema_name?: string;
  object_name?: string;
  object_type?: string;
  health_status?: string;
  access_frequency?: string;
  row_count?: number;
  table_size_mb?: number;
  [key: string]: any;
}

interface SchemaNode {
  name: string;
  objects: GovernanceItem[];
}

interface GovernanceCatalogOracleTreeViewProps {
  items: GovernanceItem[];
  onItemClick?: (item: GovernanceItem) => void;
}

const GovernanceCatalogOracleTreeView: React.FC<GovernanceCatalogOracleTreeViewProps> = ({ items, onItemClick }) => {
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());

  const treeData = useMemo(() => {
    const schemas = new Map<string, SchemaNode>();

    items.forEach(item => {
      const schemaKey = item.schema_name || 'default';
      if (!schemas.has(schemaKey)) {
        schemas.set(schemaKey, {
          name: schemaKey,
          objects: []
        });
      }
      schemas.get(schemaKey)!.objects.push(item);
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
    return <TreeLine $isLast={isLast}>{lines.join('')}</TreeLine>;
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
    const schemaIndex = treeData.findIndex(s => s.name === schema.name);
    const isLastSchema = schemaIndex === treeData.length - 1;

    return (
      <TreeNode key={schema.name}>
        <TreeContent 
          $level={level} 
          $isExpanded={isExpanded}
          $nodeType="schema"
          onClick={() => toggleSchema(schema.name)}
        >
          {renderTreeLine(level, isLastSchema)}
          <ExpandIconContainer $isExpanded={isExpanded}>
            {isExpanded ? (
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
                <polyline points="18 15 12 9 6 15"/>
              </svg>
            ) : (
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
                <polyline points="9 18 15 12 9 6"/>
              </svg>
            )}
          </ExpandIconContainer>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={theme.colors.primary.main} strokeWidth="2" style={{ marginRight: '8px' }}>
            <ellipse cx="12" cy="5" rx="9" ry="3"/>
            <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>
            <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
          </svg>
          <NodeLabel $isSchema>{schema.name}</NodeLabel>
          <CountBadge>{schema.objects.length} {schema.objects.length === 1 ? 'object' : 'objects'}</CountBadge>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && schema.objects.map((item, index) => 
            renderObject(item, schema.name, level + 1, index === schema.objects.length - 1)
          )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  const renderObject = (item: GovernanceItem, _schemaName: string, level: number, isLast: boolean) => {
    return (
      <ObjectDetailsRow
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
          {item.object_name || 'N/A'}
        </span>
        {item.object_type && (
          <Badge $type={item.object_type}>{item.object_type}</Badge>
        )}
        {item.health_status && (
          <Badge $status={item.health_status}>{item.health_status}</Badge>
        )}
        {item.access_frequency && (
          <Badge $status={item.access_frequency}>{item.access_frequency}</Badge>
        )}
        <span style={{ marginLeft: 'auto', color: theme.colors.text.secondary, fontSize: '0.85em' }}>
          {formatNumber(item.row_count)} rows • {formatBytes(item.table_size_mb)}
        </span>
      </ObjectDetailsRow>
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

export default GovernanceCatalogOracleTreeView;

