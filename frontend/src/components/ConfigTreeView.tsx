import React, { useState, useMemo } from 'react';
import styled, { keyframes } from 'styled-components';
import { theme } from '../theme/theme';
import type { ConfigEntry } from '../services/api';

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

const TreeContent = styled.div<{ $level: number; $isExpanded?: boolean; $nodeType?: 'category' | 'config' }>`
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
    if (props.$nodeType === 'category') {
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
      if (props.$nodeType === 'category') {
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

const NodeLabel = styled.span<{ $isCategory?: boolean }>`
  font-weight: ${props => props.$isCategory ? '700' : '500'};
  font-size: ${props => props.$isCategory ? '1.05em' : '0.92em'};
  color: ${props => props.$isCategory ? theme.colors.primary.main : theme.colors.text.primary};
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

const ConfigItemRow = styled.div<{ $level: number; $isSelected?: boolean }>`
  padding: 12px 8px;
  padding-left: ${props => props.$level * 24 + 36}px;
  margin: 2px 0;
  border-radius: ${theme.borderRadius.md};
  background: ${props => props.$isSelected ? theme.colors.primary.light + '15' : theme.colors.background.main};
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

const ConfigKey = styled.span`
  font-weight: 600;
  color: ${theme.colors.text.primary};
  font-family: monospace;
  font-size: 0.9em;
  margin-right: 12px;
`;

const ConfigValue = styled.span`
  color: ${theme.colors.text.secondary};
  font-size: 0.85em;
  font-family: monospace;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 300px;
  user-select: text;
  cursor: text;
  
  &:hover {
    white-space: normal;
    word-break: break-word;
    max-width: 100%;
  }
`;

const ConfigMeta = styled.div`
  display: flex;
  gap: 12px;
  margin-top: 4px;
  font-size: 0.75em;
  color: ${theme.colors.text.secondary};
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
  configs: ConfigEntry[];
}

interface ConfigTreeViewProps {
  configs: ConfigEntry[];
  onConfigClick?: (config: ConfigEntry) => void;
}

const ConfigTreeView: React.FC<ConfigTreeViewProps> = ({ configs, onConfigClick }) => {
  const [expandedCategories, setExpandedCategories] = useState<Set<string>>(new Set());

  const treeData = useMemo(() => {
    const categories = new Map<string, CategoryNode>();
    const uncategorized: ConfigEntry[] = [];

    configs.forEach(config => {
      const keyParts = config.key.split('.');
      if (keyParts.length > 1) {
        const categoryName = keyParts[0];
        
        if (!categories.has(categoryName)) {
          categories.set(categoryName, {
            name: categoryName,
            configs: []
          });
        }
        
        categories.get(categoryName)!.configs.push(config);
      } else {
        uncategorized.push(config);
      }
    });

    const sortedCategories = Array.from(categories.values()).sort((a, b) => a.name.localeCompare(b.name));
    return { categories: sortedCategories, uncategorized };
  }, [configs]);

  const toggleCategory = (categoryName: string) => {
    setExpandedCategories(prev => {
      const next = new Set(prev);
      if (next.has(categoryName)) {
        next.delete(categoryName);
      } else {
        next.add(categoryName);
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

  const getValuePreview = (value: string): string => {
    if (!value) return '(empty)';
    if (value.length > 60) {
      return value.substring(0, 60) + '...';
    }
    return value;
  };

  const renderCategory = (category: CategoryNode, level: number, isLast: boolean) => {
    const isExpanded = expandedCategories.has(category.name);

    return (
      <TreeNode key={category.name}>
        <TreeContent 
          $level={level} 
          $isExpanded={isExpanded}
          $nodeType="category"
          onClick={() => toggleCategory(category.name)}
        >
          {renderTreeLine(level, isLast)}
          <ExpandIconContainer $isExpanded={isExpanded}>
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
              <polyline points="9 18 15 12 9 6"/>
            </svg>
          </ExpandIconContainer>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={theme.colors.primary.main} strokeWidth="2" style={{ marginRight: '8px' }}>
            <rect x="3" y="3" width="18" height="18" rx="2"/>
            <path d="M3 9h18M9 3v18"/>
          </svg>
          <NodeLabel $isCategory>
            {category.name}
          </NodeLabel>
          <CountBadge>{category.configs.length}</CountBadge>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && category.configs
            .sort((a, b) => a.key.localeCompare(b.key))
            .map((config, index) => 
              renderConfig(config, level + 1, index === category.configs.length - 1)
            )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  const renderConfig = (config: ConfigEntry, level: number, isLast: boolean) => {
    return (
      <ConfigItemRow
        key={config.key}
        $level={level}
        onClick={() => onConfigClick?.(config)}
      >
        {renderTreeLine(level, isLast)}
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke={theme.colors.text.secondary} strokeWidth="2" style={{ marginRight: '8px', flexShrink: 0 }}>
          <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
          <polyline points="14 2 14 8 20 8"/>
          <line x1="16" y1="13" x2="8" y2="13"/>
          <line x1="16" y1="17" x2="8" y2="17"/>
          <polyline points="10 9 9 9 8 9"/>
        </svg>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
            <ConfigKey>{config.key}</ConfigKey>
            <ConfigValue onClick={(e) => e.stopPropagation()}>
              {getValuePreview(config.value)}
            </ConfigValue>
          </div>
          <ConfigMeta>
            <span>Updated: {formatDate(config.updated_at)}</span>
            {config.description && (
              <span>• {config.description}</span>
            )}
          </ConfigMeta>
        </div>
      </ConfigItemRow>
    );
  };

  if (treeData.categories.length === 0 && treeData.uncategorized.length === 0) {
    return (
      <TreeContainer>
        <EmptyState>
          <EmptyStateIcon>⚙️</EmptyStateIcon>
          <EmptyStateTitle>No configuration available</EmptyStateTitle>
          <EmptyStateText>Configuration entries will appear here once added.</EmptyStateText>
        </EmptyState>
      </TreeContainer>
    );
  }

  return (
    <TreeContainer>
      {treeData.uncategorized.map((config, index) => 
        renderConfig(config, 0, index === treeData.uncategorized.length - 1 && treeData.categories.length === 0)
      )}
      {treeData.categories.map((category, index) => 
        renderCategory(category, 0, index === treeData.categories.length - 1)
      )}
    </TreeContainer>
  );
};

export default ConfigTreeView;

