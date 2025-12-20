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

const TreeContent = styled.div<{ $level: number; $isExpanded?: boolean; $nodeType?: 'role' | 'user' }>`
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
    if (props.$nodeType === 'role') {
      return `
        background: linear-gradient(135deg, ${theme.colors.primary.light}08 0%, ${theme.colors.primary.main}05 100%);
        border-left: 3px solid ${theme.colors.primary.main};
        font-weight: 600;
      `;
    }
    return `
      border-left: 1px solid ${theme.colors.border.light};
    `;
  }}
  
  &:hover {
    background: ${props => {
      if (props.$nodeType === 'role') {
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

const NodeLabel = styled.span<{ $isRole?: boolean }>`
  font-weight: ${props => props.$isRole ? '700' : '500'};
  font-size: ${props => props.$isRole ? '1.05em' : '0.92em'};
  color: ${props => props.$isRole ? theme.colors.primary.main : theme.colors.text.primary};
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

const UserItemRow = styled.div<{ $level: number; $isSelected?: boolean; $isActive?: boolean }>`
  padding: 12px 8px;
  padding-left: ${props => props.$level * 24 + 36}px;
  margin: 2px 0;
  border-radius: ${theme.borderRadius.md};
  background: ${props => props.$isSelected ? theme.colors.primary.light + '15' : theme.colors.background.main};
  border-left: 2px solid ${props => props.$isActive ? theme.colors.status.success.text : theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  animation: ${fadeIn} 0.3s ease-out;
  
  &:hover {
    background: ${theme.colors.background.secondary};
    border-left-width: 3px;
    transform: translateX(4px);
    box-shadow: ${theme.shadows.sm};
  }
`;

const Badge = styled.span<{ $role?: string; $active?: boolean }>`
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
    if (props.$role) {
      switch (props.$role) {
        case 'admin':
          return `
            background: linear-gradient(135deg, #ffebee 0%, #c6282815 100%);
            color: #c62828;
            border-color: #c6282840;
          `;
        case 'user':
          return `
            background: linear-gradient(135deg, #e3f2fd 0%, #1565c015 100%);
            color: #1565c0;
            border-color: #1565c040;
          `;
        case 'viewer':
          return `
            background: linear-gradient(135deg, #f3e5f5 0%, #6a1b9a15 100%);
            color: #6a1b9a;
            border-color: #6a1b9a40;
          `;
        default:
          return `
            background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
            color: ${theme.colors.text.primary};
            border-color: ${theme.colors.border.medium};
          `;
      }
    }
    if (props.$active !== undefined) {
      return props.$active
        ? `
            background: linear-gradient(135deg, ${theme.colors.status.success.bg} 0%, ${theme.colors.status.success.text}15 100%);
            color: ${theme.colors.status.success.text};
            border-color: ${theme.colors.status.success.text}40;
          `
        : `
            background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
            color: ${theme.colors.text.secondary};
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

interface User {
  id: number;
  username: string;
  email: string;
  role: 'admin' | 'user' | 'viewer';
  active: boolean;
  created_at: string;
  updated_at: string;
  last_login: string | null;
}

interface RoleNode {
  name: string;
  users: User[];
}

interface UserManagementTreeViewProps {
  users: User[];
  onUserClick?: (user: User) => void;
}

const UserManagementTreeView: React.FC<UserManagementTreeViewProps> = ({ users, onUserClick }) => {
  const [expandedRoles, setExpandedRoles] = useState<Set<string>>(new Set());

  const treeData = useMemo(() => {
    const roles = new Map<string, RoleNode>();
    const roleOrder = ['admin', 'user', 'viewer'];

    users.forEach(user => {
      const roleKey = user.role || 'user';
      
      if (!roles.has(roleKey)) {
        roles.set(roleKey, {
          name: roleKey,
          users: []
        });
      }
      
      roles.get(roleKey)!.users.push(user);
    });

    return Array.from(roles.values()).sort((a, b) => {
      const aIndex = roleOrder.indexOf(a.name);
      const bIndex = roleOrder.indexOf(b.name);
      if (aIndex !== -1 && bIndex !== -1) return aIndex - bIndex;
      if (aIndex !== -1) return -1;
      if (bIndex !== -1) return 1;
      return a.name.localeCompare(b.name);
    });
  }, [users]);

  const toggleRole = (roleName: string) => {
    setExpandedRoles(prev => {
      const next = new Set(prev);
      if (next.has(roleName)) {
        next.delete(roleName);
      } else {
        next.add(roleName);
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
    if (!date) return 'Never';
    return new Date(date).toLocaleString();
  };

  const renderRole = (role: RoleNode, level: number, isLast: boolean) => {
    const isExpanded = expandedRoles.has(role.name);

    return (
      <TreeNode key={role.name}>
        <TreeContent 
          $level={level} 
          $isExpanded={isExpanded}
          $nodeType="role"
          onClick={() => toggleRole(role.name)}
        >
          {renderTreeLine(level, isLast)}
          <ExpandIconContainer $isExpanded={isExpanded}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
              <polyline points="9 18 15 12 9 6"/>
            </svg>
          </ExpandIconContainer>
          <Badge $role={role.name}>{role.name.toUpperCase()}</Badge>
          <NodeLabel $isRole>
            {role.name}
          </NodeLabel>
          <CountBadge>{role.users.length}</CountBadge>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && role.users
            .sort((a, b) => a.username.localeCompare(b.username))
            .map((user, index) => 
              renderUser(user, level + 1, index === role.users.length - 1)
            )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  const renderUser = (user: User, level: number, isLast: boolean) => {
    return (
      <UserItemRow
        key={user.id}
        $level={level}
        $isActive={user.active}
        onClick={() => onUserClick?.(user)}
      >
        {renderTreeLine(level, isLast)}
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke={theme.colors.text.secondary} strokeWidth="2" style={{ marginRight: '8px', flexShrink: 0 }}>
          <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/>
          <circle cx="12" cy="7" r="4"/>
        </svg>
        <span style={{ marginRight: '8px', fontWeight: 600, color: theme.colors.text.primary }}>
          {user.username}
        </span>
        <span style={{ marginRight: '8px', color: theme.colors.text.secondary, fontSize: '0.85em' }}>
          {user.email}
        </span>
        <Badge $active={user.active}>
          {user.active ? 'Active' : 'Inactive'}
        </Badge>
        <span style={{ marginLeft: 'auto', color: theme.colors.text.secondary, fontSize: '0.75em', display: 'flex', gap: '8px', alignItems: 'center' }}>
          <span>Last login: {formatDate(user.last_login)}</span>
        </span>
      </UserItemRow>
    );
  };

  if (treeData.length === 0) {
    return (
      <TreeContainer>
        <EmptyState>
          <EmptyStateIcon>👥</EmptyStateIcon>
          <EmptyStateTitle>No users available</EmptyStateTitle>
          <EmptyStateText>Users will appear here once added.</EmptyStateText>
        </EmptyState>
      </TreeContainer>
    );
  }

  return (
    <TreeContainer>
      {treeData.map((role, index) => 
        renderRole(role, 0, index === treeData.length - 1)
      )}
    </TreeContainer>
  );
};

export default UserManagementTreeView;

