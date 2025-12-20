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
    max-height: 0;
    opacity: 0;
  }
  to {
    max-height: 1000px;
    opacity: 1;
  }
`;

const slideUp = keyframes`
  from {
    max-height: 1000px;
    opacity: 1;
  }
  to {
    max-height: 0;
    opacity: 0;
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

const TreeContent = styled.div<{ $level: number; $isExpanded?: boolean; $nodeType?: 'schema' | 'table' | 'column' }>`
  display: flex;
  align-items: center;
  padding: ${props => props.$level === 0 ? '12px 8px' : props.$level === 1 ? '10px 8px' : '8px 8px'};
  padding-left: ${props => props.$level * 24 + 8}px;
  cursor: pointer;
  border-radius: ${theme.borderRadius.md};
  transition: all ${theme.transitions.normal};
  position: relative;
  margin: 2px 0;
  
  ${props => {
    if (props.$nodeType === 'schema') {
      return `
        background: linear-gradient(135deg, ${theme.colors.primary.light}08 0%, ${theme.colors.primary.main}05 100%);
        border-left: 3px solid ${theme.colors.primary.main};
        font-weight: 600;
      `;
    }
    if (props.$nodeType === 'table') {
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
      if (props.$nodeType === 'schema') {
        return `linear-gradient(135deg, ${theme.colors.primary.light}15 0%, ${theme.colors.primary.main}10 100%)`;
      }
      return theme.colors.background.secondary;
    }};
    transform: translateX(2px);
    box-shadow: ${theme.shadows.sm};
  }
  
  &:active {
    transform: translateX(1px) scale(0.99);
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

const NodeLabel = styled.span<{ $isSchema?: boolean; $isTable?: boolean; $isColumn?: boolean }>`
  font-weight: ${props => props.$isSchema ? '700' : props.$isTable ? '600' : '500'};
  font-size: ${props => props.$isSchema ? '1.05em' : props.$isTable ? '0.98em' : '0.92em'};
  color: ${props => {
    if (props.$isSchema) return theme.colors.primary.main;
    if (props.$isTable) return theme.colors.text.primary;
    return theme.colors.text.secondary;
  }};
  margin-right: 12px;
  transition: color ${theme.transitions.normal};
  letter-spacing: ${props => props.$isSchema ? '0.3px' : '0'};
  
  ${props => props.$isSchema && `
    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
  `}
`;

const ColumnInfo = styled.div`
  display: inline-flex;
  gap: 6px;
  align-items: center;
  margin-left: auto;
  flex-wrap: wrap;
  font-size: 0.85em;
`;

const CountBadge = styled.span`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.8em;
  font-weight: 500;
  background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
  color: ${theme.colors.text.secondary};
  border: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  
  &:hover {
    background: linear-gradient(135deg, ${theme.colors.primary.light}10 0%, ${theme.colors.primary.main}08 100%);
    border-color: ${theme.colors.primary.main};
    color: ${theme.colors.primary.main};
    transform: translateY(-1px);
  }
`;

const Badge = styled.span<{ $type?: string; $level?: string; $flag?: boolean }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.75em;
  font-weight: 600;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  transition: all ${theme.transitions.normal};
  white-space: nowrap;
  
  ${props => {
    if (props.$type) {
      const typeColors: Record<string, { bg: string; text: string }> = {
        'PK': { bg: theme.colors.status.info.bg, text: theme.colors.status.info.text },
        'FK': { bg: theme.colors.status.warning.bg, text: theme.colors.status.warning.text },
        'UQ': { bg: theme.colors.status.success.bg, text: theme.colors.status.success.text },
        'IDX': { bg: '#e8eaf6', text: '#3f51b5' },
      };
      const colors = typeColors[props.$type] || { bg: theme.colors.background.secondary, text: theme.colors.text.primary };
      return `
        background-color: ${colors.bg};
        color: ${colors.text};
        border: 1px solid ${colors.text}20;
      `;
    }
    if (props.$level) {
      switch (props.$level) {
        case 'HIGH': return `
          background-color: ${theme.colors.status.error.bg};
          color: ${theme.colors.status.error.text};
          border: 1px solid ${theme.colors.status.error.text}30;
        `;
        case 'MEDIUM': return `
          background-color: ${theme.colors.status.warning.bg};
          color: ${theme.colors.status.warning.text};
          border: 1px solid ${theme.colors.status.warning.text}30;
        `;
        case 'LOW': return `
          background-color: ${theme.colors.status.success.bg};
          color: ${theme.colors.status.success.text};
          border: 1px solid ${theme.colors.status.success.text}30;
        `;
        default: return `
          background-color: ${theme.colors.background.secondary};
          color: ${theme.colors.text.secondary};
        `;
      }
    }
    if (props.$flag !== undefined) {
      return props.$flag 
        ? `
          background-color: ${theme.colors.status.error.bg};
          color: ${theme.colors.status.error.text};
          border: 1px solid ${theme.colors.status.error.text}30;
        `
        : `
          background-color: ${theme.colors.status.success.bg};
          color: ${theme.colors.status.success.text};
        `;
    }
    return `
      background-color: ${theme.colors.background.secondary};
      color: ${theme.colors.text.primary};
    `;
  }}
  
  &:hover {
    transform: translateY(-2px) scale(1.05);
    box-shadow: ${theme.shadows.md};
  }
`;

const ExpandableContent = styled.div<{ $isExpanded: boolean; $level: number }>`
  overflow: hidden;
  animation: ${props => props.$isExpanded ? slideDown : slideUp} 0.3s ease-out;
  padding-left: ${props => props.$level * 24 + 36}px;
`;

const ColumnDetailsRow = styled.div<{ $level: number }>`
  padding: 12px 16px;
  margin: 4px 0 8px 0;
  font-size: 0.88em;
  color: ${theme.colors.text.secondary};
  background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
  border-left: 3px solid ${theme.colors.primary.main};
  border-radius: ${theme.borderRadius.md};
  box-shadow: ${theme.shadows.sm};
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 12px;
  animation: ${fadeIn} 0.3s ease-out;
  
  div {
    display: flex;
    align-items: center;
    gap: 6px;
    
    strong {
      color: ${theme.colors.text.primary};
      font-weight: 600;
      min-width: 80px;
    }
  }
`;

const EmptyState = styled.div`
  padding: 60px 40px;
  text-align: center;
  color: ${theme.colors.text.secondary};
  animation: ${fadeIn} 0.5s ease-out;
  
  &::before {
    content: '📊';
    font-size: 3em;
    display: block;
    margin-bottom: ${theme.spacing.md};
    opacity: 0.5;
  }
`;

const IconSchema = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
    <line x1="9" y1="3" x2="9" y2="21"/>
    <line x1="3" y1="9" x2="21" y2="9"/>
  </svg>
);

const IconTable = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M9 3H5a2 2 0 0 0-2 2v4m6-6h10a2 2 0 0 1 2 2v4M9 3v18m0 0h10a2 2 0 0 0 2-2V9M9 21H5a2 2 0 0 1-2-2V9m0 0h18"/>
  </svg>
);

const IconColumn = () => (
  <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <line x1="12" y1="5" x2="12" y2="19"/>
    <line x1="5" y1="12" x2="19" y2="12"/>
  </svg>
);

interface Column {
  id: number;
  schema_name: string;
  table_name: string;
  column_name: string;
  db_engine?: string;
  data_type?: string;
  ordinal_position?: number;
  is_nullable?: boolean;
  sensitivity_level?: string;
  contains_pii?: boolean;
  contains_phi?: boolean;
  is_primary_key?: boolean;
  is_foreign_key?: boolean;
  is_unique?: boolean;
  is_indexed?: boolean;
  [key: string]: any;
}

interface SchemaNode {
  name: string;
  tables: Map<string, TableNode>;
}

interface TableNode {
  name: string;
  columns: Column[];
}

interface TreeViewProps {
  columns: Column[];
  onColumnClick?: (column: Column) => void;
}

const ColumnCatalogTreeView: React.FC<TreeViewProps> = ({ columns, onColumnClick }) => {
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());
  const [expandedColumns, setExpandedColumns] = useState<Set<number>>(new Set());

  const treeData = useMemo(() => {
    const schemas = new Map<string, SchemaNode>();

    columns.forEach(column => {
      const schemaName = column.schema_name;
      const tableName = column.table_name;

      if (!schemas.has(schemaName)) {
        schemas.set(schemaName, {
          name: schemaName,
          tables: new Map()
        });
      }

      const schema = schemas.get(schemaName)!;

      if (!schema.tables.has(tableName)) {
        schema.tables.set(tableName, {
          name: tableName,
          columns: []
        });
      }

      const table = schema.tables.get(tableName)!;
      table.columns.push(column);
    });

    schemas.forEach(schema => {
      schema.tables.forEach(table => {
        table.columns.sort((a, b) => (a.ordinal_position || 0) - (b.ordinal_position || 0));
      });
    });

    return Array.from(schemas.values()).sort((a, b) => a.name.localeCompare(b.name));
  }, [columns]);

  const toggleSchema = (schemaName: string) => {
    setExpandedSchemas(prev => {
      const next = new Set(prev);
      if (next.has(schemaName)) {
        next.delete(schemaName);
        const schema = treeData.find(s => s.name === schemaName);
        if (schema) {
          schema.tables.forEach(table => {
            const key = `${schemaName}.${table.name}`;
            setExpandedTables(prevTables => {
              const nextTables = new Set(prevTables);
              nextTables.delete(key);
              return nextTables;
            });
          });
        }
      } else {
        next.add(schemaName);
      }
      return next;
    });
  };

  const toggleTable = (schemaName: string, tableName: string) => {
    const key = `${schemaName}.${tableName}`;
    setExpandedTables(prev => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };

  const toggleColumn = (columnId: number) => {
    setExpandedColumns(prev => {
      const next = new Set(prev);
      if (next.has(columnId)) {
        next.delete(columnId);
      } else {
        next.add(columnId);
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
    
    if (isLast) {
      lines.push('└── ');
    } else {
      lines.push('├── ');
    }
    
    return <TreeLine>{lines.join('')}</TreeLine>;
  };

  const renderColumn = (column: Column, schemaName: string, tableName: string, level: number) => {
    const columnId = column.id;
    const isExpanded = expandedColumns.has(columnId);
    const table = treeData.find(s => s.name === schemaName)?.tables.get(tableName);
    const columnIndex = table?.columns.findIndex(c => c.id === columnId) ?? -1;
    const isLastColumn = columnIndex === (table?.columns.length ?? 0) - 1;

    return (
      <TreeNode key={columnId}>
        <TreeContent 
          $level={level} 
          $isExpanded={isExpanded}
          $nodeType="column"
          onClick={() => {
            toggleColumn(columnId);
            onColumnClick?.(column);
          }}
        >
          {renderTreeLine(level, isLastColumn)}
          <ExpandIconContainer $isExpanded={isExpanded}>
            {isExpanded ? (
              <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
                <polyline points="18 15 12 9 6 15"/>
              </svg>
            ) : (
              <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
                <polyline points="9 18 15 12 9 6"/>
              </svg>
            )}
          </ExpandIconContainer>
          <IconColumn />
          <NodeLabel $isColumn>{column.column_name}</NodeLabel>
          <ColumnInfo>
            {column.data_type && (
              <Badge $type={column.data_type}>
                {column.data_type}
              </Badge>
            )}
            {column.sensitivity_level && (
              <Badge $level={column.sensitivity_level}>
                {column.sensitivity_level}
              </Badge>
            )}
            {column.contains_pii && <Badge $flag={true}>PII</Badge>}
            {column.contains_phi && <Badge $flag={true}>PHI</Badge>}
            {column.is_primary_key && <Badge $type="PK">PK</Badge>}
            {column.is_foreign_key && <Badge $type="FK">FK</Badge>}
            {column.is_unique && <Badge $type="UQ">UQ</Badge>}
            {column.is_indexed && <Badge $type="IDX">IDX</Badge>}
          </ColumnInfo>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && (
            <ColumnDetailsRow $level={level}>
              <div><strong>Engine:</strong> <span>{column.db_engine || 'N/A'}</span></div>
              <div><strong>Position:</strong> <span>{column.ordinal_position || 'N/A'}</span></div>
              <div><strong>Nullable:</strong> <span>{column.is_nullable ? 'Yes' : 'No'}</span></div>
              {column.character_maximum_length && (
                <div><strong>Max Length:</strong> <span>{column.character_maximum_length}</span></div>
              )}
              {column.numeric_precision && (
                <div><strong>Precision:</strong> <span>{column.numeric_precision}</span></div>
              )}
              {column.column_default && (
                <div><strong>Default:</strong> <span>{column.column_default}</span></div>
              )}
            </ColumnDetailsRow>
          )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  const renderTable = (table: TableNode, schemaName: string, level: number) => {
    const tableKey = `${schemaName}.${table.name}`;
    const isExpanded = expandedTables.has(tableKey);
    const schema = treeData.find(s => s.name === schemaName);
    const tableIndex = schema ? Array.from(schema.tables.keys()).indexOf(table.name) : -1;
    const isLastTable = tableIndex === (schema?.tables.size ?? 0) - 1;

    return (
      <TreeNode key={tableKey}>
        <TreeContent 
          $level={level} 
          $isExpanded={isExpanded}
          $nodeType="table"
          onClick={() => toggleTable(schemaName, table.name)}
        >
          {renderTreeLine(level, isLastTable)}
          <ExpandIconContainer $isExpanded={isExpanded}>
            {isExpanded ? (
              <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
                <polyline points="18 15 12 9 6 15"/>
              </svg>
            ) : (
              <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
                <polyline points="9 18 15 12 9 6"/>
              </svg>
            )}
          </ExpandIconContainer>
          <IconTable />
          <NodeLabel $isTable>{table.name}</NodeLabel>
          <ColumnInfo>
            <CountBadge>{table.columns.length} {table.columns.length === 1 ? 'column' : 'columns'}</CountBadge>
          </ColumnInfo>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && table.columns.map((column) => 
            renderColumn(column, schemaName, table.name, level + 1)
          )}
        </ExpandableContent>
      </TreeNode>
    );
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
          <IconSchema />
          <NodeLabel $isSchema>{schema.name}</NodeLabel>
          <ColumnInfo>
            <CountBadge>{schema.tables.size} {schema.tables.size === 1 ? 'table' : 'tables'}</CountBadge>
          </ColumnInfo>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && Array.from(schema.tables.values()).map((table) => 
            renderTable(table, schema.name, level + 1)
          )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  if (treeData.length === 0) {
    return (
      <TreeContainer>
        <EmptyState>
          No column data available. Columns will appear here once cataloged.
        </EmptyState>
      </TreeContainer>
    );
  }

  return (
    <TreeContainer>
      {treeData.map((schema, index) => (
        <div key={schema.name} style={{ animationDelay: `${index * 0.05}s` }}>
          {renderSchema(schema, 0)}
        </div>
      ))}
    </TreeContainer>
  );
};

export default ColumnCatalogTreeView;
