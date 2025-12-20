import { useState, useEffect, useCallback, useRef } from 'react';
import styled from 'styled-components';
import {
  Container,
  Header,
  ErrorMessage,
  LoadingOverlay,
  Button,
  Input,
} from './shared/BaseComponents';
import { configApi } from '../services/api';
import type { ConfigEntry } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { theme } from '../theme/theme';
import ConfigTreeView from './ConfigTreeView';

const ConfigTable = styled.table`
  width: 100%;
  border-collapse: collapse;
  margin-top: ${theme.spacing.lg};
  background: ${theme.colors.background.main};
  box-shadow: ${theme.shadows.md};
  border-radius: ${theme.borderRadius.md};
  overflow: hidden;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

const Th = styled.th`
  padding: ${theme.spacing.sm};
  text-align: left;
  border-bottom: 2px solid ${theme.colors.border.dark};
  background: ${theme.colors.gradient.primary};
  font-weight: bold;
  position: sticky;
  top: 0;
  z-index: 10;
`;

const Td = styled.td`
  padding: ${theme.spacing.sm};
  border-bottom: 1px solid ${theme.colors.border.light};
  font-family: ${theme.fonts.primary};
  transition: all ${theme.transitions.normal};
`;

const TableRow = styled.tr`
  transition: all ${theme.transitions.normal};
  
  &:hover {
    background: linear-gradient(90deg, ${theme.colors.background.main} 0%, ${theme.colors.background.tertiary} 100%);
    transform: scale(1.001);
    box-shadow: ${theme.shadows.sm};
    
    ${Td} {
      border-bottom-color: rgba(10, 25, 41, 0.1);
    }
  }
`;

const TextArea = styled.textarea`
  width: 100%;
  padding: 10px;
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  font-family: ${theme.fonts.primary};
  resize: vertical;
  min-height: 60px;
  background: ${theme.colors.background.main};
  transition: all ${theme.transitions.normal};

  &:hover:not(:disabled) {
    border-color: rgba(10, 25, 41, 0.3);
  }

  &:focus {
    outline: none;
    border-color: ${theme.colors.primary.main};
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
    transform: translateY(-1px);
  }

  &:disabled {
    background: ${theme.colors.background.secondary};
    cursor: not-allowed;
  }
`;

const DangerButton = styled(Button)`
  background-color: ${theme.colors.status.error.bg};
  color: ${theme.colors.status.error.text};
  border-color: ${theme.colors.status.error.text};
  
  &:hover:not(:disabled) {
    background-color: #ffcdd2;
    border-color: ${theme.colors.status.error.text};
  }
`;

const ActionCell = styled.td`
  padding: ${theme.spacing.sm};
  border-bottom: 1px solid ${theme.colors.border.light};
  text-align: right;
`;

const AddButton = styled(Button)`
  margin: ${theme.spacing.lg} 0;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const ExpandableValue = styled.div`
  position: relative;
`;

const ValuePreview = styled.div`
  font-family: monospace;
  font-size: 0.9em;
  color: ${theme.colors.text.secondary};
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 100%;
  padding-right: 30px;
`;

const ValueExpanded = styled.pre`
  margin: 0;
  white-space: pre-wrap;
  word-break: break-all;
  font-family: monospace;
  font-size: 0.9em;
  background: ${theme.colors.background.secondary};
  padding: ${theme.spacing.sm};
  border-radius: ${theme.borderRadius.sm};
  border: 1px solid ${theme.colors.border.light};
  max-height: 400px;
  overflow-y: auto;
`;

const ExpandButton = styled(Button)`
  padding: 6px 12px;
  margin-right: 5px;
  font-size: 0.9em;
`;

/**
 * Configuration component
 * Manages application configuration entries with CRUD operations
 */
const Config = () => {
  const [configs, setConfigs] = useState<ConfigEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [editingKey, setEditingKey] = useState<string | null>(null);
  const [editForm, setEditForm] = useState<ConfigEntry | null>(null);
  const [expandedKeys, setExpandedKeys] = useState<Set<string>>(new Set());
  const [selectedConfig, setSelectedConfig] = useState<ConfigEntry | null>(null);
  const [viewMode, setViewMode] = useState<'table' | 'tree'>('tree');
  const isMountedRef = useRef(true);

  const fetchConfigs = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      const data = await configApi.getConfigs();
      if (isMountedRef.current) {
        setConfigs(data || []);
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    } finally {
      if (isMountedRef.current) {
        setLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    isMountedRef.current = true;
    fetchConfigs();
    return () => {
      isMountedRef.current = false;
    };
  }, [fetchConfigs]);

  const handleEdit = useCallback((config: ConfigEntry) => {
    setEditingKey(config.key);
    setEditForm({ ...config });
  }, []);

  const handleCancel = useCallback(() => {
    setEditingKey(null);
    setEditForm(null);
  }, []);

  const handleSave = useCallback(async () => {
    if (!editForm) return;

    try {
      if (!isMountedRef.current) return;
      await configApi.updateConfig(editForm);
      if (isMountedRef.current) {
        await fetchConfigs();
        setEditingKey(null);
        setEditForm(null);
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [editForm, fetchConfigs]);

  const handleAdd = useCallback(() => {
    const newConfig: ConfigEntry = {
      key: '',
      value: '',
      description: null,
      updated_at: new Date().toISOString()
    };
    setEditingKey('new');
    setEditForm(newConfig);
  }, []);

  const handleCreate = useCallback(async () => {
    if (!editForm) return;

    try {
      if (!isMountedRef.current) return;
      await configApi.createConfig(editForm);
      if (isMountedRef.current) {
        await fetchConfigs();
        setEditingKey(null);
        setEditForm(null);
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [editForm, fetchConfigs]);

  const formatDate = useCallback((dateString: string) => {
    return new Date(dateString).toLocaleString();
  }, []);

  const isJsonValue = useCallback((value: string): boolean => {
    if (!value || value.trim().length === 0) return false;
    const trimmed = value.trim();
    if ((trimmed.startsWith('{') && trimmed.endsWith('}')) || 
        (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
      try {
        JSON.parse(value);
        return true;
      } catch {
        return false;
      }
    }
    return false;
  }, []);

  const isLongValue = useCallback((value: string): boolean => {
    return !!(value && value.length > 100);
  }, []);

  const shouldBeExpandable = useCallback((value: string): boolean => {
    return isJsonValue(value) || isLongValue(value);
  }, [isJsonValue, isLongValue]);

  const formatJsonValue = useCallback((value: string): string => {
    if (isJsonValue(value)) {
      try {
        return JSON.stringify(JSON.parse(value), null, 2);
      } catch {
        return value;
      }
    }
    return value;
  }, [isJsonValue]);

  const getValuePreview = useCallback((value: string): string => {
    if (isJsonValue(value)) {
      try {
        const parsed = JSON.parse(value);
        const keys = Object.keys(parsed);
        if (keys.length > 0) {
          return `{ ${keys.slice(0, 3).join(', ')}${keys.length > 3 ? '...' : ''} }`;
        }
        return '{ }';
      } catch {
        return value.substring(0, 50) + '...';
      }
    }
    if (isLongValue(value)) {
      return value.substring(0, 50) + '...';
    }
    return value;
  }, [isJsonValue, isLongValue]);

  const toggleExpand = useCallback((key: string) => {
    setExpandedKeys(prev => {
      const newSet = new Set(prev);
      if (newSet.has(key)) {
        newSet.delete(key);
      } else {
        newSet.add(key);
      }
      return newSet;
    });
  }, []);

  if (loading && configs.length === 0) {
    return (
      <Container>
        <Header>Configuration</Header>
        <LoadingOverlay>Loading configurations...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>Configuration</Header>

      {error && <ErrorMessage>{error}</ErrorMessage>}

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: theme.spacing.md }}>
        <AddButton onClick={handleAdd}>+ Add New Configuration</AddButton>
        <div style={{ display: 'flex', gap: theme.spacing.sm }}>
          <Button
            $variant={viewMode === 'tree' ? 'primary' : 'secondary'}
            onClick={() => setViewMode('tree')}
            style={{ padding: '8px 16px', fontSize: '0.9em' }}
          >
            Tree View
          </Button>
          <Button
            $variant={viewMode === 'table' ? 'primary' : 'secondary'}
            onClick={() => setViewMode('table')}
            style={{ padding: '8px 16px', fontSize: '0.9em' }}
          >
            Table View
          </Button>
        </div>
      </div>

      {viewMode === 'tree' ? (
        <div style={{ display: 'grid', gridTemplateColumns: selectedConfig ? '1fr 400px' : '1fr', gap: theme.spacing.lg }}>
          <ConfigTreeView 
            configs={configs} 
            onConfigClick={(config) => setSelectedConfig(prev => prev?.key === config.key ? null : config)}
          />
          
          {selectedConfig && (
            <div style={{
              background: theme.colors.background.secondary,
              border: `1px solid ${theme.colors.border.light}`,
              borderRadius: theme.borderRadius.md,
              padding: theme.spacing.lg,
              position: 'sticky',
              top: theme.spacing.md,
              maxHeight: 'calc(100vh - 200px)',
              overflowY: 'auto'
            }}>
              <h3 style={{ marginTop: 0, marginBottom: theme.spacing.md, color: theme.colors.text.primary }}>
                Configuration Details
              </h3>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: theme.spacing.md }}>
                <div>
                  <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Key:</strong>
                  <div style={{ color: theme.colors.text.primary, fontFamily: 'monospace', fontSize: '0.9em', marginTop: '4px' }}>
                    {selectedConfig.key}
                  </div>
                </div>
                <div>
                  <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Value:</strong>
                  <div style={{ 
                    color: theme.colors.text.primary, 
                    fontSize: '0.9em',
                    fontFamily: 'monospace',
                    padding: theme.spacing.sm,
                    background: theme.colors.background.main,
                    borderRadius: theme.borderRadius.sm,
                    border: `1px solid ${theme.colors.border.light}`,
                    whiteSpace: 'pre-wrap',
                    wordBreak: 'break-word',
                    maxHeight: '300px',
                    overflowY: 'auto'
                  }}>
                    {selectedConfig.value || '(empty)'}
                  </div>
                </div>
                {selectedConfig.description && (
                  <div>
                    <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Description:</strong>
                    <div style={{ color: theme.colors.text.primary, fontSize: '0.9em', marginTop: '4px' }}>
                      {selectedConfig.description}
                    </div>
                  </div>
                )}
                <div>
                  <strong style={{ color: theme.colors.text.secondary, fontSize: '0.85em' }}>Last Updated:</strong>
                  <div style={{ color: theme.colors.text.primary, fontSize: '0.9em', marginTop: '4px' }}>
                    {formatDate(selectedConfig.updated_at)}
                  </div>
                </div>
                <div style={{ marginTop: theme.spacing.md, paddingTop: theme.spacing.md, borderTop: `1px solid ${theme.colors.border.light}` }}>
                  <Button
                    onClick={() => {
                      handleEdit(selectedConfig);
                      setSelectedConfig(null);
                    }}
                    style={{ width: '100%', marginBottom: theme.spacing.sm }}
                  >
                    Edit Configuration
                  </Button>
                </div>
              </div>
            </div>
          )}
        </div>
      ) : (
        <ConfigTable>
        <thead>
          <tr>
            <Th>Key</Th>
            <Th>Value</Th>
            <Th>Current Batch</Th>
            <Th>Last Updated</Th>
            <Th>Actions</Th>
          </tr>
        </thead>
        <tbody>
          {editingKey === 'new' && editForm && (
            <TableRow>
              <Td>
                <Input
                  value={editForm.key}
                  onChange={e => setEditForm({ ...editForm, key: e.target.value })}
                  placeholder="Enter key..."
                />
              </Td>
              <Td>
                <TextArea
                  value={editForm.value}
                  onChange={e => setEditForm({ ...editForm, value: e.target.value })}
                  placeholder="Enter value..."
                />
              </Td>
              <Td>-</Td>
              <Td>-</Td>
              <ActionCell>
                <Button onClick={handleCreate}>Save</Button>
                <DangerButton onClick={handleCancel}>Cancel</DangerButton>
              </ActionCell>
            </TableRow>
          )}
          {configs.map(config => (
            <TableRow key={config.key}>
              <Td>
                {editingKey === config.key ? (
                  <Input
                    value={editForm?.key || ''}
                    onChange={e => setEditForm(prev => prev ? { ...prev, key: e.target.value } : null)}
                    disabled
                  />
                ) : (
                  config.key
                )}
              </Td>
              <Td>
                {editingKey === config.key ? (
                  <TextArea
                    value={editForm?.value || ''}
                    onChange={e => setEditForm(prev => prev ? { ...prev, value: e.target.value } : null)}
                  />
                ) : shouldBeExpandable(config.value) ? (
                  <ExpandableValue>
                    {expandedKeys.has(config.key) ? (
                      <ValueExpanded>
                        {formatJsonValue(config.value)}
                      </ValueExpanded>
                    ) : (
                      <ValuePreview>
                        {getValuePreview(config.value)}
                      </ValuePreview>
                    )}
                  </ExpandableValue>
                ) : (
                  <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                    {config.value}
                  </pre>
                )}
              </Td>
              <Td>
                {config.key === 'batch_size' ? config.value : '-'}
              </Td>
              <Td>{formatDate(config.updated_at)}</Td>
              <ActionCell>
                {editingKey === config.key ? (
                  <>
                    <Button onClick={handleSave}>Save</Button>
                    <DangerButton onClick={handleCancel}>Cancel</DangerButton>
                  </>
                ) : (
                  <>
                    {shouldBeExpandable(config.value) && (
                      <ExpandButton
                        $variant="secondary"
                        onClick={() => toggleExpand(config.key)}
                      >
                        {expandedKeys.has(config.key) ? 'Collapse' : 'Expand'}
                      </ExpandButton>
                    )}
                    <Button onClick={() => handleEdit(config)}>Edit</Button>
                  </>
                )}
              </ActionCell>
            </TableRow>
          ))}
        </tbody>
      </ConfigTable>
      )}
    </Container>
  );
};

export default Config;
