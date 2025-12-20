import React, { useState, useMemo } from 'react';
import styled, { keyframes } from 'styled-components';
import { theme } from '../theme/theme';
import type { LogEntry } from '../services/api';

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

const TreeContent = styled.div<{ $level: number; $isExpanded?: boolean; $nodeType?: 'level' | 'category' | 'log' }>`
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
    if (props.$nodeType === 'level') {
      return `
        background: linear-gradient(135deg, ${theme.colors.primary.light}08 0%, ${theme.colors.primary.main}05 100%);
        border-left: 3px solid ${theme.colors.primary.main};
        font-weight: 600;
      `;
    }
    if (props.$nodeType === 'category') {
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
      if (props.$nodeType === 'level') {
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

const NodeLabel = styled.span<{ $isLevel?: boolean; $isCategory?: boolean }>`
  font-weight: ${props => props.$isLevel ? '700' : props.$isCategory ? '600' : '500'};
  font-size: ${props => props.$isLevel ? '1.05em' : props.$isCategory ? '0.98em' : '0.92em'};
  color: ${props => {
    if (props.$isLevel) return theme.colors.primary.main;
    if (props.$isCategory) return theme.colors.text.primary;
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

const LogItemRow = styled.div<{ $level: number; $logLevel: string; $isSelected?: boolean }>`
  padding: 12px 8px;
  padding-left: ${props => props.$level * 24 + 36}px;
  margin: 2px 0;
  border-radius: ${theme.borderRadius.md};
  background: ${props => props.$isSelected ? theme.colors.primary.light + '15' : theme.colors.background.main};
  border-left: 2px solid ${props => {
    switch (props.$logLevel?.toUpperCase()) {
      case 'ERROR': return theme.colors.status.error.text;
      case 'WARNING': return theme.colors.status.warning.text;
      case 'INFO': return theme.colors.status.info.text;
      case 'DEBUG': return theme.colors.text.secondary;
      default: return theme.colors.border.light;
    }
  }};
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  animation: ${fadeIn} 0.3s ease-out;
  display: flex;
  align-items: center;
  gap: 8px;
  
  &:hover {
    background: ${props => props.$isSelected ? theme.colors.primary.light + '25' : theme.colors.background.secondary};
    border-left-width: 3px;
    transform: translateX(4px);
    box-shadow: ${theme.shadows.sm};
  }
`;

const Checkbox = styled.input`
  width: 18px;
  height: 18px;
  cursor: pointer;
  accent-color: ${theme.colors.primary.main};
  flex-shrink: 0;
`;

const LogMessage = styled.span`
  margin-left: auto;
  color: ${theme.colors.text.secondary};
  font-size: 0.85em;
  max-width: 40%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  user-select: text;
  cursor: text;
  
  &:hover {
    white-space: normal;
    word-break: break-word;
    max-width: 100%;
  }
`;

const BulkActionsBar = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: ${theme.spacing.md};
  background: ${theme.colors.background.secondary};
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.md};
  margin-bottom: ${theme.spacing.md};
  animation: ${fadeIn} 0.3s ease-out;
`;

const BulkActionsButton = styled.button`
  padding: 8px 16px;
  border: none;
  border-radius: ${theme.borderRadius.md};
  background: ${theme.colors.primary.main};
  color: ${theme.colors.text.white};
  font-family: ${theme.fonts.primary};
  font-size: 0.9em;
  font-weight: 500;
  cursor: pointer;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    background: ${theme.colors.primary.dark};
    transform: translateY(-1px);
    box-shadow: ${theme.shadows.md};
  }
  
  &:disabled {
    background: ${theme.colors.background.tertiary};
    color: ${theme.colors.text.secondary};
    cursor: not-allowed;
    transform: none;
  }
`;

const SelectAllButton = styled.button`
  padding: 6px 12px;
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  background: transparent;
  color: ${theme.colors.text.primary};
  font-family: ${theme.fonts.primary};
  font-size: 0.85em;
  cursor: pointer;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    background: ${theme.colors.background.secondary};
    border-color: ${theme.colors.primary.main};
  }
`;

const Badge = styled.span<{ $level?: string; $category?: string }>`
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
    if (props.$level) {
      const level = props.$level.toUpperCase();
      switch (level) {
        case 'ERROR':
          return `
            background: linear-gradient(135deg, ${theme.colors.status.error.bg} 0%, ${theme.colors.status.error.text}15 100%);
            color: ${theme.colors.status.error.text};
            border-color: ${theme.colors.status.error.text}40;
          `;
        case 'WARNING':
          return `
            background: linear-gradient(135deg, ${theme.colors.status.warning.bg} 0%, ${theme.colors.status.warning.text}15 100%);
            color: ${theme.colors.status.warning.text};
            border-color: ${theme.colors.status.warning.text}40;
          `;
        case 'INFO':
          return `
            background: linear-gradient(135deg, ${theme.colors.status.info.bg} 0%, ${theme.colors.status.info.text}15 100%);
            color: ${theme.colors.status.info.text};
            border-color: ${theme.colors.status.info.text}40;
          `;
        case 'DEBUG':
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
    if (props.$category) {
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

interface CategoryNode {
  name: string;
  logs: LogEntry[];
}

interface LevelNode {
  name: string;
  categories: Map<string, CategoryNode>;
}

interface LogsTreeViewProps {
  logs: LogEntry[];
  onLogClick?: (log: LogEntry) => void;
  onSelectedLogsChange?: (selectedLogs: LogEntry[]) => void;
  onCopySelected?: (selectedLogs: LogEntry[]) => void;
}

const LogsTreeView: React.FC<LogsTreeViewProps> = ({ logs, onLogClick, onSelectedLogsChange, onCopySelected }) => {
  const [expandedLevels, setExpandedLevels] = useState<Set<string>>(new Set());
  const [expandedCategories, setExpandedCategories] = useState<Set<string>>(new Set());
  const [selectedLogIds, setSelectedLogIds] = useState<Set<string | number>>(new Set());

  const treeData = useMemo(() => {
    const levels = new Map<string, LevelNode>();
    const levelOrder = ['ERROR', 'WARNING', 'INFO', 'DEBUG', 'TRACE'];

    logs.forEach(log => {
      const levelKey = (log.level || 'UNKNOWN').toUpperCase();
      const categoryKey = log.category || 'SYSTEM';
      
      if (!levels.has(levelKey)) {
        levels.set(levelKey, {
          name: levelKey,
          categories: new Map()
        });
      }
      
      const level = levels.get(levelKey)!;
      if (!level.categories.has(categoryKey)) {
        level.categories.set(categoryKey, {
          name: categoryKey,
          logs: []
        });
      }
      
      level.categories.get(categoryKey)!.logs.push(log);
    });

    // Sort by level order, then alphabetically
    return Array.from(levels.values()).sort((a, b) => {
      const aIndex = levelOrder.indexOf(a.name);
      const bIndex = levelOrder.indexOf(b.name);
      if (aIndex !== -1 && bIndex !== -1) return aIndex - bIndex;
      if (aIndex !== -1) return -1;
      if (bIndex !== -1) return 1;
      return a.name.localeCompare(b.name);
    });
  }, [logs]);

  const toggleLevel = (levelName: string) => {
    setExpandedLevels(prev => {
      const next = new Set(prev);
      if (next.has(levelName)) {
        next.delete(levelName);
      } else {
        next.add(levelName);
      }
      return next;
    });
  };

  const toggleCategory = (levelName: string, categoryName: string) => {
    const key = `${levelName}:${categoryName}`;
    setExpandedCategories(prev => {
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

  const formatDate = (date: string | null | undefined) => {
    if (!date) return 'N/A';
    return new Date(date).toLocaleString();
  };

  const toggleLogSelection = (log: LogEntry, event: React.MouseEvent, index: number) => {
    event.stopPropagation();
    const logId = log.id || `${log.timestamp}-${log.message}-${index}`;
    setSelectedLogIds(prev => {
      const next = new Set(prev);
      if (next.has(logId)) {
        next.delete(logId);
      } else {
        next.add(logId);
      }
      
      if (onSelectedLogsChange) {
        const selectedLogs: LogEntry[] = [];
        logs.forEach((l, idx) => {
          const id = l.id || `${l.timestamp}-${l.message}-${idx}`;
          if (next.has(id)) {
            selectedLogs.push(l);
          }
        });
        onSelectedLogsChange(selectedLogs);
      }
      
      return next;
    });
  };

  const selectAllLogs = () => {
    const allIds = new Set(logs.map((log, index) => log.id || `${log.timestamp}-${log.message}-${index}`));
    setSelectedLogIds(allIds);
    if (onSelectedLogsChange) {
      onSelectedLogsChange(logs);
    }
  };

  const deselectAllLogs = () => {
    setSelectedLogIds(new Set());
    if (onSelectedLogsChange) {
      onSelectedLogsChange([]);
    }
  };

  const getSelectedLogs = () => {
    const selectedLogs: LogEntry[] = [];
    logs.forEach((log, index) => {
      const id = log.id || `${log.timestamp}-${log.message}-${index}`;
      if (selectedLogIds.has(id)) {
        selectedLogs.push(log);
      }
    });
    return selectedLogs;
  };

  const renderLevel = (level: LevelNode) => {
    const isExpanded = expandedLevels.has(level.name);
    const totalLogs = Array.from(level.categories.values()).reduce((sum, cat) => sum + cat.logs.length, 0);

    return (
      <TreeNode key={level.name}>
        <TreeContent 
          $level={0} 
          $isExpanded={isExpanded}
          $nodeType="level"
          onClick={() => toggleLevel(level.name)}
        >
          {renderTreeLine(0, false)}
          <ExpandIconContainer $isExpanded={isExpanded}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
              <polyline points="9 18 15 12 9 6"/>
            </svg>
          </ExpandIconContainer>
          <Badge $level={level.name}>{level.name}</Badge>
          <NodeLabel $isLevel>
            {level.name}
          </NodeLabel>
          <CountBadge>{totalLogs}</CountBadge>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={0}>
          {isExpanded && Array.from(level.categories.values())
            .sort((a, b) => a.name.localeCompare(b.name))
            .map((category, idx, arr) => 
              renderCategory(category, level.name, 1, idx === arr.length - 1)
            )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  const renderCategory = (category: CategoryNode, levelName: string, level: number, isLast: boolean) => {
    const key = `${levelName}:${category.name}`;
    const isExpanded = expandedCategories.has(key);

    return (
      <TreeNode key={key}>
        <TreeContent 
          $level={level} 
          $isExpanded={isExpanded}
          $nodeType="category"
          onClick={() => toggleCategory(levelName, category.name)}
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
          <NodeLabel $isCategory>
            {category.name}
          </NodeLabel>
          <CountBadge>{category.logs.length}</CountBadge>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && category.logs.map((log, index) => 
            renderLog(log, level + 1, index === category.logs.length - 1, index)
          )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  const renderLog = (log: LogEntry, level: number, isLast: boolean, index: number) => {
    const logId = log.id || `${log.timestamp}-${log.message}-${index}`;
    const isSelected = selectedLogIds.has(logId);
    
    return (
      <LogItemRow
        key={logId}
        $level={level}
        $logLevel={log.level || 'INFO'}
        $isSelected={isSelected}
        onClick={(e) => {
          const target = e.target as HTMLElement;
          if (target.tagName === 'INPUT' || target.closest('input')) {
            return;
          }
          if (target.closest('[data-selectable="true"]')) {
            return;
          }
          e.stopPropagation();
          onLogClick?.(log);
        }}
      >
        {renderTreeLine(level, isLast)}
        <Checkbox
          type="checkbox"
          checked={isSelected}
          onChange={(e) => {
            e.stopPropagation();
            toggleLogSelection(log, e as any, index);
          }}
          onClick={(e) => e.stopPropagation()}
        />
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke={theme.colors.text.secondary} strokeWidth="2" style={{ marginRight: '8px', flexShrink: 0 }}>
          <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
          <polyline points="14 2 14 8 20 8"/>
          <line x1="16" y1="13" x2="8" y2="13"/>
          <line x1="16" y1="17" x2="8" y2="17"/>
          <polyline points="10 9 9 9 8 9"/>
        </svg>
        <span style={{ fontWeight: 500, color: theme.colors.text.primary, fontSize: '0.9em', whiteSpace: 'nowrap' }}>
          {formatDate(log.timestamp)}
        </span>
        {log.category && (
          <Badge $category={log.category}>{log.category}</Badge>
        )}
        {log.function && (
          <span style={{ color: theme.colors.text.secondary, fontSize: '0.85em', whiteSpace: 'nowrap' }}>
            [{log.function}]
          </span>
        )}
        <LogMessage data-selectable="true" onClick={(e) => e.stopPropagation()}>
          {log.message}
        </LogMessage>
      </LogItemRow>
    );
  };

  if (treeData.length === 0) {
    return (
      <TreeContainer>
        <EmptyState>
          <EmptyStateIcon>📋</EmptyStateIcon>
          <EmptyStateTitle>No logs available</EmptyStateTitle>
          <EmptyStateText>Logs will appear here once generated.</EmptyStateText>
        </EmptyState>
      </TreeContainer>
    );
  }

  const selectedCount = selectedLogIds.size;
  const hasSelection = selectedCount > 0;

  return (
    <>
      {hasSelection && (
        <BulkActionsBar>
          <div style={{ display: 'flex', alignItems: 'center', gap: theme.spacing.md }}>
            <span style={{ color: theme.colors.text.primary, fontWeight: 500 }}>
              {selectedCount} log{selectedCount !== 1 ? 's' : ''} selected
            </span>
            <SelectAllButton onClick={deselectAllLogs}>
              Deselect All
            </SelectAllButton>
          </div>
          <BulkActionsButton
            onClick={() => {
              const selectedLogs = getSelectedLogs();
              if (onCopySelected) {
                onCopySelected(selectedLogs);
              } else if (onSelectedLogsChange) {
                onSelectedLogsChange(selectedLogs);
              }
            }}
          >
            Copy Selected ({selectedCount})
          </BulkActionsButton>
        </BulkActionsBar>
      )}
      {!hasSelection && logs.length > 0 && (
        <div style={{ marginBottom: theme.spacing.md }}>
          <SelectAllButton onClick={selectAllLogs}>
            Select All Logs
          </SelectAllButton>
        </div>
      )}
      <TreeContainer>
        {treeData.map((level) => 
          renderLevel(level)
        )}
      </TreeContainer>
    </>
  );
};

export default LogsTreeView;

