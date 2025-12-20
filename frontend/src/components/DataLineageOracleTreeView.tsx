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

const TreeContent = styled.div<{ $level: number; $isExpanded?: boolean; $nodeType?: 'source' | 'relationship' | 'target' }>`
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
    if (props.$nodeType === 'source') {
      return `
        background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
        border: 2px solid ${theme.colors.border.light};
        border-left: 4px solid ${theme.colors.primary.main};
        box-shadow: ${theme.shadows.sm};
      `;
    }
    return `
      background: ${theme.colors.background.main};
      border: 1px solid transparent;
    `;
  }}
  
  &:hover {
    background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
    transform: translateX(4px) scale(1.01);
    box-shadow: ${theme.shadows.md};
    border-color: ${theme.colors.primary.main};
  }
  
  &:active {
    transform: translateX(2px) scale(0.99);
  }
`;

const ExpandIconContainer = styled.div<{ $isExpanded: boolean }>`
  width: 20px;
  height: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  margin-right: 8px;
  border-radius: ${theme.borderRadius.sm};
  background: linear-gradient(135deg, ${theme.colors.primary.light} 0%, ${theme.colors.primary.main} 100%);
  color: white;
  transition: all ${theme.transitions.normal};
  flex-shrink: 0;
  
  ${props => props.$isExpanded && `
    background: linear-gradient(135deg, ${theme.colors.primary.main} 0%, ${theme.colors.primary.dark} 100%);
    transform: rotate(90deg);
  `}
  
  &:hover {
    box-shadow: ${theme.shadows.md};
    transform: ${props => props.$isExpanded ? 'rotate(90deg) scale(1.1)' : 'scale(1.1)'};
  }
`;

const NodeLabel = styled.span<{ $isSource?: boolean; $isRelationship?: boolean; $isTarget?: boolean }>`
  font-weight: ${props => props.$isSource ? 600 : props.$isRelationship ? 500 : 400};
  color: ${props => props.$isSource ? theme.colors.primary.main : props.$isRelationship ? theme.colors.text.secondary : theme.colors.text.primary};
  font-size: ${props => props.$isSource ? '1em' : '0.9em'};
  flex: 1;
  transition: color ${theme.transitions.normal};
`;

const CountBadge = styled.span`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.75em;
  font-weight: 600;
  background: linear-gradient(135deg, ${theme.colors.primary.main} 0%, ${theme.colors.primary.light} 100%);
  color: white;
  margin-left: auto;
  box-shadow: ${theme.shadows.sm};
  transition: all ${theme.transitions.normal};
  
  &:hover {
    transform: scale(1.1);
    box-shadow: ${theme.shadows.md};
  }
`;

const RelationshipInfo = styled.div`
  display: flex;
  align-items: center;
  gap: ${theme.spacing.sm};
  margin-left: auto;
  flex-wrap: wrap;
`;

const Badge = styled.span<{ $type?: string; $level?: number }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.75em;
  font-weight: 600;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  transition: all ${theme.transitions.normal};
  border: 2px solid transparent;
  box-shadow: ${theme.shadows.sm};
  
  ${props => {
    if (props.$type) {
      return `
        background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
        color: ${theme.colors.text.primary};
        border-color: ${theme.colors.border.medium};
      `;
    }
    if (props.$level !== undefined) {
      if (props.$level === 0) return `
        background: linear-gradient(135deg, ${theme.colors.status.success.bg} 0%, ${theme.colors.status.success.text}15 100%);
        color: ${theme.colors.status.success.text};
        border-color: ${theme.colors.status.success.text}40;
      `;
      if (props.$level === 1) return `
        background: linear-gradient(135deg, ${theme.colors.status.warning.bg} 0%, ${theme.colors.status.warning.text}15 100%);
        color: ${theme.colors.status.warning.text};
        border-color: ${theme.colors.status.warning.text}40;
      `;
      if (props.$level === 2) return `
        background: linear-gradient(135deg, ${theme.colors.status.error.bg} 0%, ${theme.colors.status.error.text}15 100%);
        color: ${theme.colors.status.error.text};
        border-color: ${theme.colors.status.error.text}40;
      `;
    }
    return `
      background: ${theme.colors.background.secondary};
      color: ${theme.colors.text.secondary};
      border-color: ${theme.colors.border.medium};
    `;
  }}
  
  &:hover {
    transform: translateY(-2px) scale(1.08);
    box-shadow: ${theme.shadows.lg};
  }
`;

const ExpandableContent = styled.div<{ $isExpanded: boolean; $level: number }>`
  overflow: hidden;
  animation: ${props => props.$isExpanded ? slideDown : slideUp} 0.3s ease-out;
  padding-left: ${props => props.$level * 24 + 36}px;
`;

const TargetDetails = styled.div`
  padding: ${theme.spacing.sm};
  margin: ${theme.spacing.xs} 0;
  background: ${theme.colors.background.main};
  border-radius: ${theme.borderRadius.md};
  border: 1px solid ${theme.colors.border.light};
  font-size: 0.85em;
  animation: ${fadeIn} 0.3s ease-out;
`;

const EmptyState = styled.div`
  padding: 60px 40px;
  text-align: center;
  color: ${theme.colors.text.secondary};
  animation: ${fadeIn} 0.5s ease-out;
  
  &::before {
    content: '→';
    font-size: 3em;
    display: block;
    margin-bottom: ${theme.spacing.md};
    opacity: 0.5;
    font-family: 'Courier New', monospace;
  }
`;

const IconSource = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <ellipse cx="12" cy="5" rx="9" ry="3"/>
    <path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5"/>
    <path d="M3 12c0 1.66 4.03 3 9 3s9-1.34 9-3"/>
  </svg>
);

const IconRelationship = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M5 12h14M12 5l7 7-7 7"/>
  </svg>
);

const IconTarget = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M9 3H5a2 2 0 0 0-2 2v4m6-6h10a2 2 0 0 1 2 2v4M9 3v18m0 0h10a2 2 0 0 0 2-2V9M9 21H5a2 2 0 0 1-2-2V9m0 0h18"/>
  </svg>
);

interface LineageEdge {
  id: number;
  object_name: string;
  object_type: string;
  target_object_name: string;
  target_object_type: string;
  relationship_type: string;
  server_name?: string;
  schema_name?: string;
  confidence_score?: number;
  discovery_method?: string;
  column_name?: string;
  target_column_name?: string;
  [key: string]: any;
}

interface SourceNode {
  name: string;
  type: string;
  relationships: LineageEdge[];
}

interface TreeViewProps {
  edges: LineageEdge[];
  onEdgeClick?: (edge: LineageEdge) => void;
}

const DataLineageOracleTreeView: React.FC<TreeViewProps> = ({ edges, onEdgeClick }) => {
  const [expandedSources, setExpandedSources] = useState<Set<string>>(new Set());
  const [expandedRelationships, setExpandedRelationships] = useState<Set<number>>(new Set());

  const treeData = useMemo(() => {
    const sources = new Map<string, SourceNode>();

    edges.forEach(edge => {
      const sourceKey = `${edge.object_name}::${edge.object_type}`;
      if (!sources.has(sourceKey)) {
        sources.set(sourceKey, {
          name: edge.object_name || 'Unknown',
          type: edge.object_type || 'Unknown',
          relationships: []
        });
      }
      sources.get(sourceKey)!.relationships.push(edge);
    });

    return Array.from(sources.values()).sort((a, b) => a.name.localeCompare(b.name));
  }, [edges]);

  const toggleSource = (sourceKey: string) => {
    setExpandedSources(prev => {
      const next = new Set(prev);
      if (next.has(sourceKey)) {
        next.delete(sourceKey);
      } else {
        next.add(sourceKey);
      }
      return next;
    });
  };

  const toggleRelationship = (edgeId: number) => {
    setExpandedRelationships(prev => {
      const next = new Set(prev);
      if (next.has(edgeId)) {
        next.delete(edgeId);
      } else {
        next.add(edgeId);
      }
      return next;
    });
  };

  const formatConfidence = (score: number | string | null | undefined) => {
    if (score === null || score === undefined) return 'N/A';
    const numScore = Number(score);
    if (isNaN(numScore)) return 'N/A';
    return `${(numScore * 100).toFixed(1)}%`;
  };

  const getConfidenceLevel = (score: number | string | null | undefined): number => {
    if (score === null || score === undefined) return 2;
    const numScore = Number(score);
    if (isNaN(numScore)) return 2;
    if (numScore >= 0.8) return 0;
    if (numScore >= 0.5) return 1;
    return 2;
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

  const renderTarget = (edge: LineageEdge, level: number, isLast: boolean) => {
    const isExpanded = expandedRelationships.has(edge.id);

    return (
      <TreeNode key={edge.id}>
        <TreeContent 
          $level={level} 
          $isExpanded={isExpanded}
          $nodeType="target"
          onClick={() => {
            toggleRelationship(edge.id);
            onEdgeClick?.(edge);
          }}
        >
          {renderTreeLine(level, isLast)}
          <IconTarget />
          <span style={{ marginRight: '8px' }}></span>
          <NodeLabel $isTarget>{edge.target_object_name || 'Unknown'}</NodeLabel>
          <RelationshipInfo>
            <Badge $type={edge.target_object_type}>{edge.target_object_type || 'N/A'}</Badge>
            {edge.target_column_name && (
              <Badge>Column: {edge.target_column_name}</Badge>
            )}
          </RelationshipInfo>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && (
            <TargetDetails>
              <div style={{ marginBottom: theme.spacing.xs }}>
                <strong>Server:</strong> {edge.server_name || 'N/A'}
              </div>
              {edge.schema_name && (
                <div style={{ marginBottom: theme.spacing.xs }}>
                  <strong>Schema:</strong> {edge.schema_name}
                </div>
              )}
              <div style={{ marginBottom: theme.spacing.xs }}>
                <strong>Confidence:</strong>{' '}
                <Badge $level={getConfidenceLevel(edge.confidence_score)}>
                  {formatConfidence(edge.confidence_score)}
                </Badge>
              </div>
              {edge.discovery_method && (
                <div>
                  <strong>Method:</strong> {edge.discovery_method}
                </div>
              )}
            </TargetDetails>
          )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  const renderRelationship = (edge: LineageEdge, level: number, isLast: boolean) => {
    return (
      <TreeNode key={`rel-${edge.id}`}>
        <TreeContent 
          $level={level} 
          $nodeType="relationship"
        >
          {renderTreeLine(level, false)}
          <IconRelationship />
          <span style={{ marginRight: '8px' }}></span>
          <NodeLabel $isRelationship>{edge.relationship_type || 'Unknown'}</NodeLabel>
          <RelationshipInfo>
            <Badge $level={getConfidenceLevel(edge.confidence_score)}>
              {formatConfidence(edge.confidence_score)}
            </Badge>
          </RelationshipInfo>
        </TreeContent>
        {renderTarget(edge, level + 1, isLast)}
      </TreeNode>
    );
  };

  const renderSource = (source: SourceNode, level: number, isLast: boolean) => {
    const sourceKey = `${source.name}::${source.type}`;
    const isExpanded = expandedSources.has(sourceKey);

    return (
      <TreeNode key={sourceKey}>
        <TreeContent 
          $level={level} 
          $isExpanded={isExpanded}
          $nodeType="source"
          onClick={() => toggleSource(sourceKey)}
        >
          {renderTreeLine(level, isLast)}
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
          <IconSource />
          <span style={{ marginRight: '8px' }}></span>
          <NodeLabel $isSource>{source.name}</NodeLabel>
          <RelationshipInfo>
            <Badge $type={source.type}>{source.type}</Badge>
            <CountBadge>{source.relationships.length} {source.relationships.length === 1 ? 'relationship' : 'relationships'}</CountBadge>
          </RelationshipInfo>
        </TreeContent>
        <ExpandableContent $isExpanded={isExpanded} $level={level}>
          {isExpanded && source.relationships.map((edge, index) => 
            renderRelationship(edge, level + 1, index === source.relationships.length - 1)
          )}
        </ExpandableContent>
      </TreeNode>
    );
  };

  if (treeData.length === 0) {
    return (
      <TreeContainer>
        <EmptyState>
          <div style={{ 
            fontSize: '1.1em',
            fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", "Roboto", "Oxygen", "Ubuntu", "Cantarell", sans-serif',
            fontWeight: 500,
            marginBottom: theme.spacing.sm
          }}>
            No lineage data available
          </div>
          <div style={{ fontSize: '0.9em', opacity: 0.7 }}>
            Lineage relationships will appear here once extracted.
          </div>
        </EmptyState>
      </TreeContainer>
    );
  }

  return (
    <TreeContainer>
      {treeData.map((source, index) => 
        renderSource(source, 0, index === treeData.length - 1)
      )}
    </TreeContainer>
  );
};

export default DataLineageOracleTreeView;

