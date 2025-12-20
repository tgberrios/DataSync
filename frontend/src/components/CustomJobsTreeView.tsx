import React, { useState, useMemo } from 'react';
import styled, { keyframes } from 'styled-components';
import { theme } from '../theme/theme';
import { ActiveBadge, ActionButton, PlayButton } from './shared/BaseComponents';
import { format } from 'date-fns';
import type { CustomJobEntry } from '../services/api';

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
  margin: 4px 0;
`;

const TreeContent = styled.div<{ $level: number; $isExpanded: boolean; $nodeType: 'schema' | 'table' | 'job' }>`
  display: flex;
  align-items: center;
  padding: 8px 4px;
  padding-left: ${props => props.$level * 24 + 8}px;
  border-radius: ${theme.borderRadius.md};
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  background: ${props => props.$nodeType === 'schema' ? 'transparent' : 'transparent'};
  
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

const NodeLabel = styled.span<{ $isSchema?: boolean }>`
  font-weight: ${props => props.$isSchema ? 600 : 500};
  color: ${props => props.$isSchema ? theme.colors.primary.main : theme.colors.text.primary};
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

const DuplicateButton = styled.button`
  padding: 4px 8px;
  border: none;
  border-radius: ${theme.borderRadius.sm};
  background: ${theme.colors.primary.main};
  color: ${theme.colors.text.white};
  cursor: pointer;
  font-size: 0.75em;
  transition: all ${theme.transitions.normal};
  display: flex;
  align-items: center;
  gap: 4px;
  
  &:hover {
    background: ${theme.colors.primary.dark};
    transform: translateY(-1px);
    box-shadow: ${theme.shadows.sm};
  }
  
  &:active {
    transform: translateY(0);
  }
  
  svg {
    width: 12px;
    height: 12px;
  }
`;

const ExpandableContent = styled.div<{ $isExpanded: boolean; $level: number }>`
  overflow: hidden;
  animation: ${props => props.$isExpanded ? slideDown : slideUp} 0.3s ease-out;
  padding-left: ${props => props.$level * 24 + 36}px;
`;

const JobDetailsRow = styled.div<{ $level: number }>`
  padding: 12px 8px;
  padding-left: ${props => props.$level * 24 + 36}px;
  margin: 2px 0;
  border-radius: ${theme.borderRadius.md};
  background: ${theme.colors.background.main};
  border: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  animation: ${fadeIn} 0.3s ease-out;
  display: flex;
  align-items: center;
  gap: 12px;
  
  &:hover {
    background: ${theme.colors.background.secondary};
    border-color: ${theme.colors.primary.main};
    transform: translateX(4px);
  }
`;

const EmptyState = styled.div`
  padding: 60px 40px;
  text-align: center;
  color: ${theme.colors.text.secondary};
  animation: ${fadeIn} 0.5s ease-out;
  
  &::before {
    content: '■';
    font-size: 3em;
    display: block;
    margin-bottom: ${theme.spacing.md};
    opacity: 0.5;
    font-family: 'Courier New', monospace;
  }
`;

const IconSchema = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={theme.colors.primary.main} strokeWidth="2" style={{ marginRight: '8px' }}>
    <ellipse cx="12" cy="5" rx="9" ry="3"/>
    <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>
    <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>
  </svg>
);

const IconTable = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke={theme.colors.text.secondary} strokeWidth="2" style={{ marginRight: '8px' }}>
    <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>
    <polyline points="9 22 9 12 15 12 15 22"/>
  </svg>
);

const IconJob = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke={theme.colors.text.secondary} strokeWidth="2" style={{ marginRight: '8px' }}>
    <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
    <line x1="9" y1="3" x2="9" y2="21"/>
    <line x1="3" y1="9" x2="21" y2="9"/>
  </svg>
);

interface SchemaNode {
  name: string;
  tables: Map<string, TableNode>;
}

interface TableNode {
  name: string;
  jobs: CustomJobEntry[];
}

interface TreeViewProps {
  jobs: CustomJobEntry[];
  onJobClick?: (job: CustomJobEntry) => void;
  onJobEdit?: (job: CustomJobEntry) => void;
  onJobExecute?: (jobName: string) => void;
  onJobToggleActive?: (jobName: string, currentActive: boolean) => void;
  onJobDelete?: (jobName: string) => void;
  onJobDuplicate?: (job: CustomJobEntry) => void;
}

const CustomJobsTreeView: React.FC<TreeViewProps> = ({ 
  jobs, 
  onJobClick, 
  onJobEdit, 
  onJobExecute, 
  onJobToggleActive, 
  onJobDelete,
  onJobDuplicate
}) => {
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());

  const treeData = useMemo(() => {
    const schemas = new Map<string, SchemaNode>();

    jobs.forEach(job => {
      const schemaName = job.target_schema || 'Other';
      const tableName = job.target_table || 'Other';

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
          jobs: []
        });
      }

      schema.tables.get(tableName)!.jobs.push(job);
    });

    return Array.from(schemas.values()).sort((a, b) => a.name.localeCompare(b.name));
  }, [jobs]);

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

  const renderTreeLine = (level: number, isLast: boolean) => {
    if (level === 0) return null;
    
    const lines: string[] = [];
    for (let i = 0; i < level - 1; i++) {
      lines.push('│  ');
    }
    lines.push(isLast ? '└─ ' : '├─ ');
    return <span style={{ color: theme.colors.text.secondary, fontFamily: 'monospace' }}>{lines.join('')}</span>;
  };

  const renderSchema = (schema: SchemaNode, level: number, isLast: boolean) => {
    const isExpanded = expandedSchemas.has(schema.name);
    const totalJobs = Array.from(schema.tables.values()).reduce((sum, table) => sum + table.jobs.length, 0);

    return (
      <TreeNode key={schema.name}>
        <TreeContent 
          $level={level} 
          $isExpanded={isExpanded}
          $nodeType="schema"
          onClick={() => toggleSchema(schema.name)}
        >
          {renderTreeLine(level, isLast)}
          <ExpandIconContainer $isExpanded={isExpanded}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <polyline points="9 18 15 12 9 6"/>
            </svg>
          </ExpandIconContainer>
          <IconSchema />
          <NodeLabel $isSchema>{schema.name}</NodeLabel>
          <CountBadge>{totalJobs} {totalJobs === 1 ? 'job' : 'jobs'}</CountBadge>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && Array.from(schema.tables.values())
            .sort((a, b) => a.name.localeCompare(b.name))
            .map((table, index, arr) => 
              renderTable(table, schema.name, level + 1, index === arr.length - 1)
            )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  const renderTable = (table: TableNode, schemaName: string, level: number, isLast: boolean) => {
    const key = `${schemaName}.${table.name}`;
    const isExpanded = expandedTables.has(key);

    return (
      <TreeNode key={key}>
        <TreeContent 
          $level={level} 
          $isExpanded={isExpanded}
          $nodeType="table"
          onClick={() => toggleTable(schemaName, table.name)}
        >
          {renderTreeLine(level, isLast)}
          <ExpandIconContainer $isExpanded={isExpanded}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <polyline points="9 18 15 12 9 6"/>
            </svg>
          </ExpandIconContainer>
          <IconTable />
          <NodeLabel>{table.name}</NodeLabel>
          <CountBadge>{table.jobs.length} {table.jobs.length === 1 ? 'job' : 'jobs'}</CountBadge>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && table.jobs.map((job, index) => 
            renderJob(job, level + 1, index === table.jobs.length - 1)
          )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  const renderJob = (job: CustomJobEntry, level: number, isLast: boolean) => {
    return (
      <JobDetailsRow
        key={job.id}
        $level={level}
        onClick={() => onJobClick?.(job)}
      >
        {renderTreeLine(level, isLast)}
        <IconJob />
        <span style={{ marginRight: '8px', fontWeight: 500, color: theme.colors.text.primary, flex: 1 }}>
          {job.job_name}
        </span>
        {job.description && (
          <span style={{ color: theme.colors.text.secondary, fontSize: '0.85em', flex: 1 }}>
            {job.description.length > 50 ? job.description.substring(0, 50) + '...' : job.description}
          </span>
        )}
        <span style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>
          {job.source_db_engine} → {job.target_db_engine}
        </span>
        <span style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>
          {job.schedule_cron || 'Manual'}
        </span>
        <ActiveBadge $active={job.active}>
          {job.active ? 'Active' : 'Inactive'}
        </ActiveBadge>
        <ActiveBadge $active={job.enabled}>
          {job.enabled ? 'Enabled' : 'Disabled'}
        </ActiveBadge>
        <div style={{ display: 'flex', gap: '4px', marginLeft: 'auto' }}>
          {onJobExecute && (
            <PlayButton
              onClick={(e) => {
                e.stopPropagation();
                onJobExecute(job.job_name);
              }}
              title={`Execute job: ${job.job_name}`}
            >
              ▶
            </PlayButton>
          )}
          {onJobDuplicate && (
            <DuplicateButton
              onClick={(e: React.MouseEvent) => {
                e.stopPropagation();
                onJobDuplicate(job);
              }}
              title="Duplicate Job"
            >
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
                <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
              </svg>
              Duplicate
            </DuplicateButton>
          )}
          {onJobEdit && (
            <ActionButton
              onClick={(e) => {
                e.stopPropagation();
                onJobEdit(job);
              }}
            >
              Edit
            </ActionButton>
          )}
          {onJobToggleActive && (
            <ActionButton
              onClick={(e) => {
                e.stopPropagation();
                onJobToggleActive(job.job_name, job.active);
              }}
            >
              {job.active ? 'Deactivate' : 'Activate'}
            </ActionButton>
          )}
          {onJobDelete && (
            <ActionButton
              onClick={(e) => {
                e.stopPropagation();
                onJobDelete(job.job_name);
              }}
              $variant="danger"
            >
              Delete
            </ActionButton>
          )}
        </div>
      </JobDetailsRow>
    );
  };

  if (treeData.length === 0) {
    return (
      <TreeContainer>
        <EmptyState>
          <div style={{ fontSize: '1.1em', fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", "Roboto", "Oxygen", "Ubuntu", "Cantarell", sans-serif', fontWeight: 500, marginBottom: '8px' }}>
            No custom jobs available
          </div>
          <div style={{ fontSize: '0.9em', opacity: 0.7 }}>
            Create a new job to get started.
          </div>
        </EmptyState>
      </TreeContainer>
    );
  }

  return (
    <TreeContainer>
      {treeData.map((schema, index) => 
        renderSchema(schema, 0, index === treeData.length - 1)
      )}
    </TreeContainer>
  );
};

export default CustomJobsTreeView;

