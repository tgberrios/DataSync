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

const slideDown = keyframes`
  from {
    opacity: 0;
    max-height: 0;
    transform: translateY(-10px);
  }
  to {
    opacity: 1;
    max-height: 10000px;
    transform: translateY(0);
  }
`;

const slideUp = keyframes`
  from {
    opacity: 1;
    max-height: 10000px;
    transform: translateY(0);
  }
  to {
    opacity: 0;
    max-height: 0;
    transform: translateY(-10px);
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
  animation: ${fadeIn} 0.3s ease-out;
  
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
  animation: ${fadeIn} 0.3s ease-out;
  animation-fill-mode: both;
  
  &:nth-child(1) { animation-delay: 0.05s; }
  &:nth-child(2) { animation-delay: 0.1s; }
  &:nth-child(3) { animation-delay: 0.15s; }
  &:nth-child(4) { animation-delay: 0.2s; }
  &:nth-child(5) { animation-delay: 0.25s; }
`;

const TreeContent = styled.div<{ $level: number; $isExpanded?: boolean; $nodeType?: 'database' | 'schema' | 'item' }>`
  display: flex;
  align-items: center;
  padding: ${props => props.$level === 0 ? '12px 8px' : '10px 8px'};
  padding-left: ${props => props.$level * 24 + 8}px;
  border-radius: ${theme.borderRadius.md};
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  position: relative;
  margin: 2px 0;
  
  ${props => {
    if (props.$nodeType === 'database') {
      return `
        background: linear-gradient(135deg, ${theme.colors.primary.light}08 0%, ${theme.colors.primary.main}05 100%);
        border-left: 3px solid ${theme.colors.primary.main};
        font-weight: 600;
      `;
    }
    if (props.$nodeType === 'schema') {
      return `
        background: ${theme.colors.background.secondary};
        border-left: 2px solid ${theme.colors.border.medium};
      `;
    }
    return `
      border-left: 1px solid ${theme.colors.border.light};
    `;
  }}
  
  &:hover {
    background: ${props => {
      if (props.$nodeType === 'database') {
        return `linear-gradient(135deg, ${theme.colors.primary.light}15 0%, ${theme.colors.primary.main}10 100%)`;
      }
      return theme.colors.background.secondary;
    }};
    transform: translateX(2px);
    box-shadow: ${theme.shadows.sm};
  }
`;

const ExpandIconContainer = styled.div<{ $isExpanded: boolean }>`
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 20px;
  height: 20px;
  margin-right: 8px;
  border-radius: ${theme.borderRadius.sm};
  background: ${props => props.$isExpanded 
    ? `linear-gradient(135deg, ${theme.colors.primary.main} 0%, ${theme.colors.primary.light} 100%)`
    : theme.colors.background.secondary
  };
  color: ${props => props.$isExpanded ? theme.colors.text.white : theme.colors.primary.main};
  font-size: 0.7em;
  font-weight: bold;
  transition: all ${theme.transitions.normal};
  flex-shrink: 0;
  
  &:hover {
    transform: scale(1.1);
    box-shadow: ${theme.shadows.sm};
  }
  
  svg {
    transition: transform ${theme.transitions.normal};
    transform: ${props => props.$isExpanded ? 'rotate(0deg)' : 'rotate(-90deg)'};
  }
`;

const NodeLabel = styled.span<{ $isDatabase?: boolean; $isSchema?: boolean }>`
  font-weight: ${props => props.$isDatabase ? '700' : props.$isSchema ? '600' : '500'};
  font-size: ${props => props.$isDatabase ? '1.05em' : props.$isSchema ? '0.98em' : '0.92em'};
  color: ${props => {
    if (props.$isDatabase) return theme.colors.primary.main;
    if (props.$isSchema) return theme.colors.text.primary;
    return theme.colors.text.secondary;
  }};
  margin-right: 12px;
  transition: color ${theme.transitions.normal};
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`;

const CountBadge = styled.span`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.8em;
  font-weight: 500;
  background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
  color: ${theme.colors.text.secondary};
  border: 1px solid ${theme.colors.border.light};
  margin-left: auto;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    background: linear-gradient(135deg, ${theme.colors.primary.light}10 0%, ${theme.colors.primary.main}08 100%);
    border-color: ${theme.colors.primary.main};
    color: ${theme.colors.primary.main};
    transform: translateY(-1px);
  }
`;

const ExpandableContent = styled.div<{ $isExpanded: boolean; $level: number }>`
  overflow: hidden;
  animation: ${props => props.$isExpanded ? slideDown : slideUp} 0.3s ease-out;
  padding-left: ${props => props.$level * 24 + 36}px;
`;

const TreeLine = styled.span`
  color: ${theme.colors.text.secondary};
  font-family: 'Courier New', monospace;
  margin-right: 4px;
  font-size: 0.9em;
`;

const ObjectDetailsRow = styled.div<{ $level: number }>`
  padding: 12px 8px;
  padding-left: ${props => props.$level * 24 + 36}px;
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
    box-shadow: ${theme.shadows.sm};
  }
`;

const Badge = styled.span<{ $status?: string; $type?: string }>`
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
    if (props.$status) {
      switch (props.$status) {
        case 'PENDING':
          return `
            background: linear-gradient(135deg, ${theme.colors.status.warning.bg} 0%, ${theme.colors.status.warning.text}15 100%);
            color: ${theme.colors.status.warning.text};
            border-color: ${theme.colors.status.warning.text}40;
          `;
        case 'RUNNING':
          return `
            background: linear-gradient(135deg, #e3f2fd 0%, #1565c015 100%);
            color: #1565c0;
            border-color: #1565c040;
          `;
        case 'COMPLETED':
          return `
            background: linear-gradient(135deg, ${theme.colors.status.success.bg} 0%, ${theme.colors.status.success.text}15 100%);
            color: ${theme.colors.status.success.text};
            border-color: ${theme.colors.status.success.text}40;
          `;
        case 'FAILED':
          return `
            background: linear-gradient(135deg, ${theme.colors.status.error.bg} 0%, ${theme.colors.status.error.text}15 100%);
            color: ${theme.colors.status.error.text};
            border-color: ${theme.colors.status.error.text}40;
          `;
        case 'SKIPPED':
          return `
            background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
            color: ${theme.colors.text.secondary};
            border-color: ${theme.colors.border.medium};
          `;
        default:
          return `
            background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
            color: ${theme.colors.text.primary};
            border-color: ${theme.colors.border.medium};
          `;
      }
    }
    if (props.$type) {
      return `
        background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
        color: ${theme.colors.text.primary};
        border-color: ${theme.colors.border.medium};
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

const EmptyState = styled.div`
  padding: 60px 40px;
  text-align: center;
  color: ${theme.colors.text.secondary};
  animation: ${fadeIn} 0.5s ease-out;
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

interface MaintenanceItem {
  id: number;
  maintenance_type?: string;
  db_engine?: string;
  schema_name?: string;
  object_name?: string;
  object_type?: string;
  status?: string;
  priority?: string;
  impact_score?: number;
  space_reclaimed_mb?: number;
  maintenance_duration_seconds?: number;
  last_maintenance_date?: string;
  next_maintenance_date?: string;
  auto_execute?: boolean;
  enabled?: boolean;
  maintenance_count?: number;
  performance_improvement_pct?: number;
  first_detected_date?: string;
  last_checked_date?: string;
  result_message?: string;
  error_details?: string;
  fragmentation_before?: number;
  dead_tuples_before?: number;
  table_size_before_mb?: number;
  index_size_before_mb?: number;
  fragmentation_after?: number;
  dead_tuples_after?: number;
  table_size_after_mb?: number;
  index_size_after_mb?: number;
}

interface SchemaNode {
  name: string;
  items: MaintenanceItem[];
}

interface DatabaseNode {
  name: string;
  schemas: Map<string, SchemaNode>;
}

interface MaintenanceTreeViewProps {
  items: MaintenanceItem[];
  onItemClick?: (item: MaintenanceItem) => void;
}

const MaintenanceTreeView: React.FC<MaintenanceTreeViewProps> = ({ items, onItemClick }) => {
  const [expandedDatabases, setExpandedDatabases] = useState<Set<string>>(new Set());
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());

  const treeData = useMemo(() => {
    const databases = new Map<string, DatabaseNode>();

    items.forEach(item => {
      const dbKey = item.db_engine || 'Unknown';
      const schemaKey = item.schema_name || 'default';
      
      if (!databases.has(dbKey)) {
        databases.set(dbKey, {
          name: dbKey,
          schemas: new Map()
        });
      }
      
      const db = databases.get(dbKey)!;
      if (!db.schemas.has(schemaKey)) {
        db.schemas.set(schemaKey, {
          name: schemaKey,
          items: []
        });
      }
      
      db.schemas.get(schemaKey)!.items.push(item);
    });

    return Array.from(databases.values()).sort((a, b) => a.name.localeCompare(b.name));
  }, [items]);

  const toggleDatabase = (dbName: string) => {
    setExpandedDatabases(prev => {
      const next = new Set(prev);
      if (next.has(dbName)) {
        next.delete(dbName);
      } else {
        next.add(dbName);
      }
      return next;
    });
  };

  const toggleSchema = (dbName: string, schemaName: string) => {
    const key = `${dbName}:${schemaName}`;
    setExpandedSchemas(prev => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
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
    return <TreeLine>{lines.join('')}</TreeLine>;
  };

  const formatBytes = (mb: number | string | null | undefined) => {
    if (mb === null || mb === undefined) return 'N/A';
    const numMb = Number(mb);
    if (isNaN(numMb)) return 'N/A';
    if (numMb < 1) return `${(numMb * 1024).toFixed(2)} KB`;
    if (numMb < 1024) return `${numMb.toFixed(2)} MB`;
    return `${(numMb / 1024).toFixed(2)} GB`;
  };

  const formatDuration = (seconds: number | string | null | undefined) => {
    if (seconds === null || seconds === undefined) return 'N/A';
    const numSeconds = Number(seconds);
    if (isNaN(numSeconds)) return 'N/A';
    if (numSeconds < 60) return `${numSeconds.toFixed(2)}s`;
    if (numSeconds < 3600) return `${(numSeconds / 60).toFixed(2)}m`;
    return `${(numSeconds / 3600).toFixed(2)}h`;
  };

  const renderDatabase = (database: DatabaseNode, level: number) => {
    const isExpanded = expandedDatabases.has(database.name);
    const totalItems = Array.from(database.schemas.values()).reduce((sum, schema) => sum + schema.items.length, 0);

    return (
      <TreeNode key={database.name}>
        <TreeContent 
          $level={level} 
          $isExpanded={isExpanded}
          $nodeType="database"
          onClick={() => toggleDatabase(database.name)}
        >
          {renderTreeLine(level, false)}
          <ExpandIconContainer $isExpanded={isExpanded}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
              <polyline points="9 18 15 12 9 6"/>
            </svg>
          </ExpandIconContainer>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={theme.colors.primary.main} strokeWidth="2" style={{ marginRight: '8px' }}>
            <rect x="3" y="3" width="18" height="18" rx="2"/>
            <path d="M3 9h18M9 3v18"/>
          </svg>
          <NodeLabel $isDatabase>
            {database.name}
          </NodeLabel>
          <CountBadge>{totalItems}</CountBadge>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && Array.from(database.schemas.values())
            .sort((a, b) => a.name.localeCompare(b.name))
            .map((schema, idx, arr) => 
              renderSchema(schema, database.name, level + 1, idx === arr.length - 1)
            )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  const renderSchema = (schema: SchemaNode, dbName: string, level: number, isLast: boolean) => {
    const key = `${dbName}:${schema.name}`;
    const isExpanded = expandedSchemas.has(key);

    return (
      <TreeNode key={key}>
        <TreeContent 
          $level={level} 
          $isExpanded={isExpanded}
          $nodeType="schema"
          onClick={() => toggleSchema(dbName, schema.name)}
        >
          {renderTreeLine(level, isLast)}
          <ExpandIconContainer $isExpanded={isExpanded}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
              <polyline points="9 18 15 12 9 6"/>
            </svg>
          </ExpandIconContainer>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={theme.colors.primary.main} strokeWidth="2" style={{ marginRight: '8px' }}>
            <ellipse cx="12" cy="5" rx="9" ry="3"/>
            <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>
            <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
          </svg>
          <NodeLabel $isSchema>
            {schema.name}
          </NodeLabel>
          <CountBadge>{schema.items.length}</CountBadge>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && schema.items.map((item, index) => 
            renderItem(item, level + 1, index === schema.items.length - 1)
          )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  const renderItem = (item: MaintenanceItem, level: number, isLast: boolean) => {
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
        {item.maintenance_type && (
          <Badge $type={item.maintenance_type}>{item.maintenance_type}</Badge>
        )}
        {item.status && (
          <Badge $status={item.status}>{item.status}</Badge>
        )}
        {item.error_details && (
          <span 
            title={item.error_details}
            style={{ 
              marginLeft: '8px', 
              color: theme.colors.status.error.text, 
              fontSize: '0.75em',
              padding: '2px 6px',
              background: theme.colors.status.error.bg,
              borderRadius: theme.borderRadius.sm,
              border: `1px solid ${theme.colors.status.error.text}40`,
              maxWidth: '200px',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap'
            }}
          >
            ⚠ {item.error_details}
          </span>
        )}
        <span style={{ marginLeft: 'auto', color: theme.colors.text.secondary, fontSize: '0.85em', display: 'flex', gap: '8px', alignItems: 'center' }}>
          {item.impact_score && (
            <span>Impact: {Number(item.impact_score).toFixed(1)}</span>
          )}
          {item.space_reclaimed_mb && (
            <span>• {formatBytes(item.space_reclaimed_mb)}</span>
          )}
          {item.maintenance_duration_seconds && (
            <span>• {formatDuration(item.maintenance_duration_seconds)}</span>
          )}
        </span>
      </ObjectDetailsRow>
    );
  };

  if (treeData.length === 0) {
    return (
      <TreeContainer>
        <EmptyState>
          <EmptyStateIcon>🔧</EmptyStateIcon>
          <EmptyStateTitle>No maintenance data available</EmptyStateTitle>
          <EmptyStateText>Maintenance operations will appear here once detected.</EmptyStateText>
        </EmptyState>
      </TreeContainer>
    );
  }

  return (
    <TreeContainer>
      {treeData.map((database) => 
        renderDatabase(database, 0)
      )}
    </TreeContainer>
  );
};

export default MaintenanceTreeView;

