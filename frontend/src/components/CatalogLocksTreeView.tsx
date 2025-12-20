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

const TreeContent = styled.div<{ $level: number; $isExpanded?: boolean; $nodeType?: 'group' | 'lock' }>`
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
    if (props.$nodeType === 'group') {
      return `
        background: linear-gradient(135deg, ${theme.colors.primary.light}08 0%, ${theme.colors.primary.main}05 100%);
        border-left: 3px solid ${theme.colors.primary.main};
        font-weight: 600;
      `;
    }
    return `
      background: ${theme.colors.background.secondary};
      border-left: 2px solid ${theme.colors.border.medium};
    `;
  }}
  
  &:hover {
    background: ${props => {
      if (props.$nodeType === 'group') {
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

const NodeLabel = styled.span<{ $isGroup?: boolean }>`
  font-weight: ${props => props.$isGroup ? '700' : '600'};
  font-size: ${props => props.$isGroup ? '1.05em' : '0.98em'};
  color: ${props => {
    if (props.$isGroup) return theme.colors.primary.main;
    return theme.colors.text.primary;
  }};
  margin-right: 12px;
  transition: color ${theme.transitions.normal};
  letter-spacing: ${props => props.$isGroup ? '0.3px' : '0'};
  
  ${props => props.$isGroup && `
    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
  `}
`;

const LockInfo = styled.div`
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

const ExpandableContent = styled.div<{ $isExpanded: boolean; $level: number }>`
  overflow: hidden;
  animation: ${props => props.$isExpanded ? slideDown : slideUp} 0.3s ease-out;
  padding-left: ${props => props.$level * 24 + 36}px;
`;

const EmptyState = styled.div`
  padding: 60px 40px;
  text-align: center;
  color: ${theme.colors.text.secondary};
  animation: ${fadeIn} 0.5s ease-out;
  
  &::before {
    content: '█';
    font-size: 3em;
    display: block;
    margin-bottom: ${theme.spacing.md};
    opacity: 0.5;
    font-family: 'Courier New', monospace;
  }
`;

const IconGroup = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <rect x="3" y="3" width="18" height="18" rx="2" ry="2"/>
    <line x1="9" y1="3" x2="9" y2="21"/>
    <line x1="3" y1="9" x2="21" y2="9"/>
  </svg>
);

const IconLock = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/>
    <path d="M7 11V7a5 5 0 0 1 10 0v4"/>
  </svg>
);

interface Lock {
  lock_name: string;
  acquired_by?: string;
  acquired_at?: string;
  expires_at?: string;
  [key: string]: any;
}

interface GroupNode {
  name: string;
  locks: Lock[];
}

interface TreeViewProps {
  locks: Lock[];
  onLockClick?: (lock: Lock) => void;
}

const CatalogLocksTreeView: React.FC<TreeViewProps> = ({ locks, onLockClick }) => {
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());

  const treeData = useMemo(() => {
    // Intentar agrupar por prefijo común (ej: "schema.table" -> agrupar por "schema")
    const groups = new Map<string, GroupNode>();
    const ungrouped: Lock[] = [];

    locks.forEach(lock => {
      const parts = lock.lock_name.split('.');
      if (parts.length >= 2) {
        // Si tiene formato schema.table, agrupar por schema
        const groupName = parts[0];
        if (!groups.has(groupName)) {
          groups.set(groupName, {
            name: groupName,
            locks: []
          });
        }
        groups.get(groupName)!.locks.push(lock);
      } else {
        // Si no tiene formato claro, poner en "ungrouped"
        ungrouped.push(lock);
      }
    });

    const result: GroupNode[] = Array.from(groups.values()).sort((a, b) => a.name.localeCompare(b.name));
    if (ungrouped.length > 0) {
      result.push({
        name: 'Other',
        locks: ungrouped
      });
    }

    return result;
  }, [locks]);

  const toggleGroup = (groupName: string) => {
    setExpandedGroups(prev => {
      const next = new Set(prev);
      if (next.has(groupName)) {
        next.delete(groupName);
      } else {
        next.add(groupName);
      }
      return next;
    });
  };

  const getLockStatus = (expiresAt?: string) => {
    if (!expiresAt) return { status: 'unknown', label: 'Unknown' };
    const expires = new Date(expiresAt);
    const now = new Date();
    const diffMs = expires.getTime() - now.getTime();
    const diffMins = diffMs / (1000 * 60);

    if (diffMs < 0) {
      return { status: 'expired', label: 'Expired' };
    } else if (diffMins < 5) {
      return { status: 'warning', label: 'Expiring Soon' };
    } else {
      return { status: 'active', label: 'Active' };
    }
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
    
    return <TreeLine $isLast={isLast}>{lines.join('')}</TreeLine>;
  };

  const renderLock = (lock: Lock, level: number, isLast: boolean) => {
    const status = getLockStatus(lock.expires_at);

    return (
      <TreeNode key={lock.lock_name}>
        <TreeContent 
          $level={level} 
          $nodeType="lock"
          onClick={() => onLockClick?.(lock)}
        >
          {renderTreeLine(level, isLast)}
          <IconLock />
          <NodeLabel>{lock.lock_name}</NodeLabel>
          <LockInfo>
            <CountBadge style={{
              background: status.status === 'expired' ? theme.colors.status.error.bg : 
                         status.status === 'warning' ? theme.colors.status.warning.bg : 
                         theme.colors.status.success.bg,
              color: status.status === 'expired' ? theme.colors.status.error.text : 
                     status.status === 'warning' ? theme.colors.status.warning.text : 
                     theme.colors.status.success.text
            }}>
              {status.label}
            </CountBadge>
            {lock.acquired_by && (
              <CountBadge>{lock.acquired_by}</CountBadge>
            )}
          </LockInfo>
        </TreeContent>
      </TreeNode>
    );
  };

  const renderGroup = (group: GroupNode, level: number) => {
    const isExpanded = expandedGroups.has(group.name);
    const groupIndex = treeData.findIndex(g => g.name === group.name);
    const isLastGroup = groupIndex === treeData.length - 1;

    return (
      <TreeNode key={group.name}>
        <TreeContent 
          $level={level} 
          $isExpanded={isExpanded}
          $nodeType="group"
          onClick={() => toggleGroup(group.name)}
        >
          {renderTreeLine(level, isLastGroup)}
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
          <IconGroup />
          <NodeLabel $isGroup>{group.name}</NodeLabel>
          <LockInfo>
            <CountBadge>{group.locks.length} {group.locks.length === 1 ? 'lock' : 'locks'}</CountBadge>
          </LockInfo>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && group.locks.map((lock, index) => {
            const isLast = index === group.locks.length - 1;
            return renderLock(lock, level + 1, isLast);
          })}
        </ExpandableContent>
      </TreeNode>
    );
  };

  if (treeData.length === 0) {
    return (
      <TreeContainer>
        <EmptyState>
          No active locks. Locks will appear here when catalog operations are running.
        </EmptyState>
      </TreeContainer>
    );
  }

  return (
    <TreeContainer>
      {treeData.map((group, index) => (
        <div key={group.name} style={{ animationDelay: `${index * 0.05}s` }}>
          {renderGroup(group, 0)}
        </div>
      ))}
    </TreeContainer>
  );
};

export default CatalogLocksTreeView;

