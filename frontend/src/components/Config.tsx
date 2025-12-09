import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { configApi } from '../services/api';
import type { ConfigEntry } from '../services/api';

const ConfigContainer = styled.div`
  background-color: white;
  color: #333;
  padding: 20px;
  font-family: monospace;
  animation: fadeIn 0.25s ease-in;
`;

const Header = styled.div`
  border: 2px solid #333;
  padding: 15px;
  text-align: center;
  margin-bottom: 30px;
  font-size: 1.5em;
  font-weight: bold;
  background: linear-gradient(135deg, #f5f5f5 0%, #ffffff 50%, #f5f5f5 100%);
  border-radius: 6px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  position: relative;
  overflow: hidden;
  animation: slideUp 0.3s ease-out;
  
  &::before {
    content: '';
    position: absolute;
    top: 0;
    left: -100%;
    width: 100%;
    height: 100%;
    background: linear-gradient(90deg, transparent, rgba(10, 25, 41, 0.1), transparent);
    animation: shimmer 3s infinite;
  }
`;

const ConfigTable = styled.table`
  width: 100%;
  border-collapse: collapse;
  margin-top: 20px;
  background: white;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  border-radius: 6px;
  overflow: hidden;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

const Th = styled.th`
  padding: 12px;
  text-align: left;
  border-bottom: 2px solid #333;
  background: linear-gradient(180deg, #f5f5f5 0%, #fafafa 100%);
  font-weight: bold;
  position: sticky;
  top: 0;
  z-index: 10;
`;

const Td = styled.td`
  padding: 12px;
  border-bottom: 1px solid #eee;
  font-family: 'Courier New', monospace;
  transition: all 0.2s ease;
`;

const TableRow = styled.tr`
  transition: all 0.2s ease;
  
  &:hover {
    background: linear-gradient(90deg, #ffffff 0%, #f8f9fa 100%);
    transform: scale(1.001);
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
    
    ${Td} {
      border-bottom-color: rgba(10, 25, 41, 0.1);
    }
  }
`;

const Input = styled.input`
  width: 100%;
  padding: 10px;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-family: 'Courier New', monospace;
  background: white;
  transition: all 0.2s ease;

  &:hover:not(:disabled) {
    border-color: rgba(10, 25, 41, 0.3);
  }

  &:focus {
    outline: none;
    border-color: #0d1b2a;
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
    transform: translateY(-1px);
  }

  &:disabled {
    background: #f5f5f5;
    cursor: not-allowed;
  }
`;

const TextArea = styled.textarea`
  width: 100%;
  padding: 10px;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-family: 'Courier New', monospace;
  resize: vertical;
  min-height: 60px;
  background: white;
  transition: all 0.2s ease;

  &:hover:not(:disabled) {
    border-color: rgba(10, 25, 41, 0.3);
  }

  &:focus {
    outline: none;
    border-color: #0d1b2a;
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
    transform: translateY(-1px);
  }

  &:disabled {
    background: #f5f5f5;
    cursor: not-allowed;
  }
`;

const Button = styled.button<{ $variant?: 'primary' | 'danger' }>`
  padding: 8px 16px;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-family: monospace;
  transition: all 0.2s ease;
  font-weight: 500;
  background-color: ${props => props.$variant === 'danger' ? '#ffebee' : '#0d1b2a'};
  color: ${props => props.$variant === 'danger' ? '#c62828' : 'white'};
  margin-right: 8px;

  &:hover:not(:disabled) {
    background-color: ${props => props.$variant === 'danger' ? '#ffcdd2' : '#1e3a5f'};
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  }

  &:active:not(:disabled) {
    transform: translateY(0);
  }

  &:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }
`;

const ActionCell = styled.td`
  padding: 12px;
  border-bottom: 1px solid #eee;
  text-align: right;
`;

const AddButton = styled(Button)`
  margin: 20px 0;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.2s;
  animation-fill-mode: both;
`;

const Config = () => {
  const [configs, setConfigs] = useState<ConfigEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [editingKey, setEditingKey] = useState<string | null>(null);
  const [editForm, setEditForm] = useState<ConfigEntry | null>(null);

  const fetchConfigs = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await configApi.getConfigs();
      setConfigs(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error loading configurations');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchConfigs();
  }, []);

  const handleEdit = (config: ConfigEntry) => {
    setEditingKey(config.key);
    setEditForm({ ...config });
  };

  const handleCancel = () => {
    setEditingKey(null);
    setEditForm(null);
  };

  const handleSave = async () => {
    if (!editForm) return;

    try {
      await configApi.updateConfig(editForm);
      await fetchConfigs();
      setEditingKey(null);
      setEditForm(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error updating configuration');
    }
  };

  const handleAdd = () => {
    const newConfig: ConfigEntry = {
      key: '',
      value: '',
      description: null,
      updated_at: new Date().toISOString()
    };
    setEditingKey('new');
    setEditForm(newConfig);
  };

  const handleCreate = async () => {
    if (!editForm) return;

    try {
      await configApi.createConfig(editForm);
      await fetchConfigs();
      setEditingKey(null);
      setEditForm(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error creating configuration');
    }
  };

  return (
    <ConfigContainer>
      <Header>Configuration</Header>

      {error && (
        <div style={{ color: 'red', padding: '20px', textAlign: 'center' }}>
          Error: {error}
        </div>
      )}

      <AddButton onClick={handleAdd}>+ Add New Configuration</AddButton>

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
                <Button $variant="danger" onClick={handleCancel}>Cancel</Button>
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
                ) : (
                  <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                    {config.value}
                  </pre>
                )}
              </Td>
              <Td>
                {config.key === 'batch_size' ? config.value : '-'}
              </Td>
              <Td>{new Date(config.updated_at).toLocaleString()}</Td>
              <ActionCell>
                {editingKey === config.key ? (
                  <>
                    <Button onClick={handleSave}>Save</Button>
                    <Button $variant="danger" onClick={handleCancel}>Cancel</Button>
                  </>
                ) : (
                  <Button onClick={() => handleEdit(config)}>Edit</Button>
                )}
              </ActionCell>
            </TableRow>
          ))}
        </tbody>
      </ConfigTable>

      {loading && (
        <div style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
          Loading configurations...
        </div>
      )}
    </ConfigContainer>
  );
};

export default Config;
