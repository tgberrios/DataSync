import { useState, useCallback, useMemo } from 'react';
import styled, { keyframes } from 'styled-components';
import {
  Button,
  Input,
  FormGroup,
  Label,
  Select,
} from './shared/BaseComponents';
import type { CatalogEntry } from '../services/api';
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

const BlurOverlay = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  backdrop-filter: blur(5px);
  background: rgba(0, 0, 0, 0.3);
  z-index: 999;
  animation: fadeIn 0.15s ease-in;
`;

const ModalOverlay = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
  animation: fadeIn 0.15s ease-in;
`;

const ModalContent = styled.div`
  background: ${theme.colors.background.main};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  min-width: 500px;
  max-width: 700px;
  max-height: 90vh;
  overflow-y: auto;
  font-family: ${theme.fonts.primary};
  box-shadow: ${theme.shadows.lg};
  animation: slideUp 0.2s ease-out;
  border: 1px solid ${theme.colors.border.light};
`;

const ModalHeader = styled.div`
  border-bottom: 2px solid ${theme.colors.border.dark};
  padding-bottom: ${theme.spacing.sm};
  margin-bottom: ${theme.spacing.lg};
  font-size: 1.2em;
  font-weight: bold;
  position: relative;
  
  &::after {
    content: '';
    position: absolute;
    bottom: -2px;
    left: 0;
    width: 60px;
    height: 2px;
    background: linear-gradient(90deg, ${theme.colors.primary.main}, ${theme.colors.primary.dark});
  }
`;

const ButtonGroup = styled.div`
  display: flex;
  justify-content: flex-end;
  gap: ${theme.spacing.sm};
  margin-top: ${theme.spacing.lg};
`;

const Textarea = styled.textarea`
  padding: 8px 12px;
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  font-family: ${theme.fonts.primary};
  background: ${theme.colors.background.main};
  color: ${theme.colors.text.primary};
  font-size: 14px;
  width: 100%;
  min-height: 100px;
  resize: vertical;
  
  &:focus {
    outline: none;
    border-color: ${theme.colors.primary.main};
    box-shadow: 0 0 0 2px ${theme.colors.primary.light}33;
  }
`;

const ConnectionStringExample = styled.div`
  margin-top: ${theme.spacing.xs};
  padding: ${theme.spacing.sm};
  background: ${theme.colors.background.secondary};
  border-radius: ${theme.borderRadius.sm};
  border-left: 3px solid ${theme.colors.primary.main};
  font-family: monospace;
  font-size: 0.85em;
  color: ${theme.colors.text.secondary};
  white-space: pre-wrap;
  word-break: break-all;
`;

const ErrorMessage = styled.div`
  color: ${theme.colors.status.error.text};
  background: ${theme.colors.status.error.bg};
  padding: ${theme.spacing.sm};
  border-radius: ${theme.borderRadius.sm};
  margin-top: ${theme.spacing.sm};
  font-size: 0.9em;
`;

const ConnectionTestResult = styled.div<{ $success: boolean }>`
  margin-top: 8px;
  padding: 8px 12px;
  border-radius: ${theme.borderRadius.sm};
  font-size: 0.9em;
  background-color: ${props => props.$success 
    ? theme.colors.status.success.bg 
    : theme.colors.status.error.bg};
  color: ${props => props.$success 
    ? theme.colors.status.success.text 
    : theme.colors.status.error.text};
  animation: ${fadeIn} 0.3s ease-out;
`;

interface AddTableModalProps {
  onClose: () => void;
  onSave: (entry: Omit<CatalogEntry, 'last_sync_time' | 'updated_at'>) => void;
}

const connectionStringExamples: Record<string, string> = {
  MariaDB: 'host=localhost;user=myuser;password=mypassword;db=mydatabase;port=3306',
  MSSQL: 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=mydatabase;UID=myuser;PWD=mypassword',
  Oracle: 'host=localhost;user=myuser;password=mypassword;db=mydatabase;port=1521',
  PostgreSQL: 'host=localhost;user=myuser;password=mypassword;db=mydatabase;port=5432',
  MongoDB: 'mongodb://myuser:mypassword@localhost:27017/mydatabase',
};

const connectionStringHelp: Record<string, string> = {
  MariaDB: 'Format: host=server;user=username;password=password;db=database;port=3306\n\nExample:\nhost=localhost;user=admin;password=secret123;db=production;port=3306',
  MSSQL: 'Format: DRIVER={ODBC Driver 17 for SQL Server};SERVER=server,port;DATABASE=database;UID=username;PWD=password\n\nExample:\nDRIVER={ODBC Driver 17 for SQL Server};SERVER=sqlserver.example.com,1433;DATABASE=MyDB;UID=sa;PWD=MyPassword123',
  Oracle: 'Format: host=server;user=username;password=password;db=database;port=1521\n\nExample:\nhost=oracle.example.com;user=system;password=oracle123;db=ORCL;port=1521',
  PostgreSQL: 'Format: host=server;user=username;password=password;db=database;port=5432\n\nExample:\nhost=postgres.example.com;user=postgres;password=postgres123;db=mydb;port=5432',
  MongoDB: 'Format: mongodb://username:password@host:port/database\n\nFor MongoDB Atlas (cloud): mongodb+srv://username:password@cluster.mongodb.net/database\n\nExample:\nmongodb://admin:secret123@localhost:27017/mydb\nmongodb+srv://admin:secret123@cluster0.xxxxx.mongodb.net/mydb',
};

const extractClusterName = (connectionString: string, dbEngine: string): string => {
  if (dbEngine === 'MongoDB') {
    const srvMatch = connectionString.match(/mongodb\+srv:\/\/[^@]+@([^.]+)/);
    if (srvMatch) {
      return srvMatch[1];
    }
    const standardMatchWithAuth = connectionString.match(/mongodb:\/\/[^@]+@([^:]+)/);
    if (standardMatchWithAuth) {
      return standardMatchWithAuth[1];
    }
    const standardMatchWithoutAuth = connectionString.match(/mongodb:\/\/([^:/]+)/);
    if (standardMatchWithoutAuth) {
      return standardMatchWithoutAuth[1];
    }
  } else {
    const parts = connectionString.split(';');
    for (const part of parts) {
      const [key, value] = part.split('=').map(s => s.trim());
      if (key && value) {
        const keyLower = key.toLowerCase();
        if (keyLower === 'host' || keyLower === 'hostname' || keyLower === 'server') {
          if (dbEngine === 'MSSQL' && value.includes(',')) {
            return value.split(',')[0].trim();
          }
          return value;
        }
      }
    }
    if (dbEngine === 'PostgreSQL' && (connectionString.includes('postgresql://') || connectionString.includes('postgres://'))) {
      const urlMatch = connectionString.match(/:\/\/(?:[^@]+@)?([^:]+)/);
      if (urlMatch) {
        return urlMatch[1];
      }
    }
  }
  return '';
};

const AddTableModal: React.FC<AddTableModalProps> = ({ onClose, onSave }) => {
  const [formData, setFormData] = useState({
    schema_name: '',
    table_name: '',
    db_engine: '',
    connection_string: '',
    active: true,
    status: 'FULL_LOAD',
    cluster_name: '',
    pk_strategy: 'CDC',
  });
  const [error, setError] = useState<string | null>(null);
  const [isClosing, setIsClosing] = useState(false);
  const [isTestingConnection, setIsTestingConnection] = useState(false);
  const [connectionTestResult, setConnectionTestResult] = useState<{ success: boolean; message: string; cluster_name?: string } | null>(null);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  const [isLoadingSchemas, setIsLoadingSchemas] = useState(false);
  const [isLoadingTables, setIsLoadingTables] = useState(false);

  const connectionExample = useMemo(() => {
    if (!formData.db_engine) return '';
    return connectionStringExamples[formData.db_engine] || '';
  }, [formData.db_engine]);

  const connectionHelp = useMemo(() => {
    if (!formData.db_engine) return '';
    return connectionStringHelp[formData.db_engine] || '';
  }, [formData.db_engine]);

  const handleClose = useCallback(() => {
    setIsClosing(true);
    setTimeout(() => {
      onClose();
    }, 150);
  }, [onClose]);

  const handleSave = useCallback(() => {
    setError(null);
    
    if (!formData.schema_name.trim()) {
      setError('Schema name is required');
      return;
    }
    
    if (!formData.table_name.trim()) {
      setError('Table name is required');
      return;
    }
    
    if (!formData.db_engine) {
      setError('Database engine is required');
      return;
    }
    
    if (!formData.connection_string.trim()) {
      setError('Connection string is required');
      return;
    }

    if (formData.db_engine === 'MongoDB') {
      if (!formData.connection_string.startsWith('mongodb://') && 
          !formData.connection_string.startsWith('mongodb+srv://')) {
        setError('MongoDB connection string must start with mongodb:// or mongodb+srv://');
        return;
      }
    } else if (formData.db_engine === 'MSSQL') {
      const requiredParams = ['server', 'uid', 'pwd', 'database'];
      const connStr = formData.connection_string.toLowerCase();
      const missing = requiredParams.filter(param => !connStr.includes(`${param}=`));
      if (missing.length > 0) {
        setError(`Connection string must include: ${missing.join(', ')}`);
        return;
      }
    } else {
      const requiredParams = ['host', 'user', 'db'];
      const connStr = formData.connection_string.toLowerCase();
      const missing = requiredParams.filter(param => !connStr.includes(`${param}=`));
      if (missing.length > 0) {
        setError(`Connection string must include: ${missing.join(', ')}`);
        return;
      }
    }

    onSave({
      schema_name: formData.schema_name.trim().toLowerCase(),
      table_name: formData.table_name.trim().toLowerCase(),
      db_engine: formData.db_engine,
      connection_string: formData.connection_string.trim(),
      active: formData.active,
      status: formData.status,
      cluster_name: formData.cluster_name.trim() || '',
      pk_strategy: formData.pk_strategy,
    });
    handleClose();
  }, [formData, onSave, handleClose]);

  const handleEngineChange = useCallback((engine: string) => {
    setFormData(prev => ({
      ...prev,
      db_engine: engine,
      connection_string: engine ? connectionStringExamples[engine] || '' : '',
    }));
    setConnectionTestResult(null);
  }, []);

  const handleTestConnection = useCallback(async () => {
    if (!formData.db_engine) {
      setConnectionTestResult({ success: false, message: 'Please select a database engine first' });
      return;
    }

    if (!formData.connection_string.trim()) {
      setConnectionTestResult({ success: false, message: 'Please enter a connection string' });
      return;
    }

    setIsTestingConnection(true);
    setConnectionTestResult(null);

    try {
      const token = localStorage.getItem('authToken');
      const response = await fetch('/api/test-connection', {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          ...(token && { 'Authorization': `Bearer ${token}` }),
        },
        body: JSON.stringify({
          db_engine: formData.db_engine,
          connection_string: formData.connection_string.trim(),
        }),
      });

      if (!response.ok) {
        if (response.status === 401) {
          setConnectionTestResult({ success: false, message: 'Authentication required. Please log in again.' });
          return;
        }
        if (response.status === 0 || response.status >= 500) {
          setConnectionTestResult({ success: false, message: 'Server error. Please check if the server is running.' });
          return;
        }
      }

      let data;
      try {
        data = await response.json();
      } catch (parseErr) {
        setConnectionTestResult({ success: false, message: 'Invalid response from server' });
        return;
      }

      if (response.ok && data.success) {
        const extractedClusterName = data.cluster_name || extractClusterName(formData.connection_string.trim(), formData.db_engine);
        if (extractedClusterName) {
          setFormData(prev => ({ ...prev, cluster_name: extractedClusterName }));
        }
        setConnectionTestResult({ 
          success: true, 
          message: data.message || 'Connection successful!',
          cluster_name: extractedClusterName 
        });
        
        await handleDiscoverSchemas();
      } else {
        setConnectionTestResult({ success: false, message: data.error || data.message || 'Connection failed' });
      }
    } catch (err: any) {
      if (err.name === 'TypeError' && err.message.includes('fetch')) {
        setConnectionTestResult({ success: false, message: 'Network error. Please check if the server is running and try again.' });
      } else {
        setConnectionTestResult({ success: false, message: err.message || 'Error testing connection' });
      }
    } finally {
      setIsTestingConnection(false);
    }
  }, [formData.db_engine, formData.connection_string]);

  const handleDiscoverSchemas = useCallback(async () => {
    if (!formData.db_engine || !formData.connection_string.trim()) {
      return;
    }

    setIsLoadingSchemas(true);
    setSchemas([]);
    setTables([]);
    setFormData(prev => ({ ...prev, schema_name: '', table_name: '' }));

    try {
      const token = localStorage.getItem('authToken');
      const response = await fetch('/api/discover-schemas', {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          ...(token && { 'Authorization': `Bearer ${token}` }),
        },
        body: JSON.stringify({
          db_engine: formData.db_engine,
          connection_string: formData.connection_string.trim(),
        }),
      });

      if (response.ok) {
        const data = await response.json();
        if (data.success && data.schemas) {
          setSchemas(data.schemas);
        }
      }
    } catch (err: any) {
      console.error('Error discovering schemas:', err);
    } finally {
      setIsLoadingSchemas(false);
    }
  }, [formData.db_engine, formData.connection_string]);

  const handleSchemaChange = useCallback(async (schema: string) => {
    setFormData(prev => ({ ...prev, schema_name: schema, table_name: '' }));
    setTables([]);

    if (!schema || !formData.db_engine || !formData.connection_string.trim()) {
      return;
    }

    setIsLoadingTables(true);
    try {
      const token = localStorage.getItem('authToken');
      const response = await fetch('/api/discover-tables', {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          ...(token && { 'Authorization': `Bearer ${token}` }),
        },
        body: JSON.stringify({
          db_engine: formData.db_engine,
          connection_string: formData.connection_string.trim(),
          schema_name: schema,
        }),
      });

      if (response.ok) {
        const data = await response.json();
        if (data.success && data.tables) {
          setTables(data.tables);
        }
      }
    } catch (err: any) {
      console.error('Error discovering tables:', err);
    } finally {
      setIsLoadingTables(false);
    }
  }, [formData.db_engine, formData.connection_string]);

  return (
    <>
      <BlurOverlay style={{ animation: isClosing ? 'fadeOut 0.15s ease-out' : 'fadeIn 0.15s ease-in' }} onClick={handleClose} />
      <ModalOverlay style={{ animation: isClosing ? 'fadeOut 0.15s ease-out' : 'fadeIn 0.15s ease-in' }}>
        <ModalContent onClick={(e) => e.stopPropagation()}>
          <ModalHeader>Add New Table to Catalog</ModalHeader>
          
          <FormGroup>
            <Label>Database Engine *</Label>
            <Select
              value={formData.db_engine}
              onChange={(e) => handleEngineChange(e.target.value)}
            >
              <option value="">Select Engine</option>
              <option value="MariaDB">MariaDB</option>
              <option value="MSSQL">MSSQL</option>
              <option value="MongoDB">MongoDB</option>
              <option value="Oracle">Oracle</option>
              <option value="PostgreSQL">PostgreSQL</option>
            </Select>
          </FormGroup>

          {formData.db_engine && (
            <>
              <FormGroup>
                <Label>Connection String Format</Label>
                <ConnectionStringExample>
                  {connectionHelp}
                </ConnectionStringExample>
              </FormGroup>
            </>
          )}

          <FormGroup>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
              <Label style={{ marginBottom: 0 }}>Connection String *</Label>
              <Button
                type="button"
                $variant="secondary"
                onClick={handleTestConnection}
                disabled={isTestingConnection || !formData.db_engine || !formData.connection_string.trim()}
                style={{ padding: '6px 12px', fontSize: '0.85em', minWidth: 'auto' }}
              >
                {isTestingConnection ? 'Testing...' : 'Test Connection'}
              </Button>
            </div>
            <Textarea
              value={formData.connection_string}
              onChange={(e) => {
                setFormData(prev => ({ 
                  ...prev, 
                  connection_string: e.target.value,
                  schema_name: '',
                  table_name: ''
                }));
                setConnectionTestResult(null);
                setSchemas([]);
                setTables([]);
              }}
              placeholder={connectionExample || "Enter connection string..."}
            />
            {connectionTestResult && (
              <ConnectionTestResult $success={connectionTestResult.success}>
                {connectionTestResult.success ? '✓ ' : '✗ '}
                {connectionTestResult.message}
                {connectionTestResult.success && connectionTestResult.cluster_name && (
                  <div style={{ marginTop: '4px', fontSize: '0.85em' }}>
                    Cluster name detected: {connectionTestResult.cluster_name}
                  </div>
                )}
              </ConnectionTestResult>
            )}
          </FormGroup>

          {connectionTestResult?.success && (
            <>
              <FormGroup>
                <Label>Schema Name *</Label>
                <Select
                  value={formData.schema_name}
                  onChange={(e) => handleSchemaChange(e.target.value)}
                  disabled={isLoadingSchemas}
                >
                  <option value="">
                    {isLoadingSchemas ? 'Loading schemas...' : 'Select Schema'}
                  </option>
                  {schemas.map((schema) => (
                    <option key={schema} value={schema}>
                      {schema}
                    </option>
                  ))}
                </Select>
              </FormGroup>

              {formData.schema_name && (
                <FormGroup>
                  <Label>Table Name *</Label>
                  <Select
                    value={formData.table_name}
                    onChange={(e) => setFormData(prev => ({ ...prev, table_name: e.target.value }))}
                    disabled={isLoadingTables}
                  >
                    <option value="">
                      {isLoadingTables ? 'Loading tables...' : 'Select Table'}
                    </option>
                    {tables.map((table) => (
                      <option key={table} value={table}>
                        {table}
                      </option>
                    ))}
                  </Select>
                </FormGroup>
              )}
            </>
          )}

          {!connectionTestResult?.success && (
            <>
              <FormGroup>
                <Label>Schema Name *</Label>
                <Input 
                  type="text" 
                  value={formData.schema_name}
                  onChange={(e) => setFormData(prev => ({ ...prev, schema_name: e.target.value }))}
                  placeholder="e.g., public, dbo, my_schema"
                />
              </FormGroup>

              <FormGroup>
                <Label>Table Name *</Label>
                <Input 
                  type="text" 
                  value={formData.table_name}
                  onChange={(e) => setFormData(prev => ({ ...prev, table_name: e.target.value }))}
                  placeholder="e.g., users, products, orders"
                />
              </FormGroup>
            </>
          )}

          <FormGroup>
            <Label>Cluster Name</Label>
            <Input 
              type="text" 
              value={formData.cluster_name}
              onChange={(e) => setFormData(prev => ({ ...prev, cluster_name: e.target.value }))}
              placeholder="Optional: cluster identifier"
            />
          </FormGroup>

          <FormGroup>
            <Label>PK Strategy</Label>
            <Select
              value={formData.pk_strategy}
              disabled
              style={{ opacity: 0.6, cursor: 'not-allowed' }}
            >
              <option value="CDC">CDC (Change Data Capture)</option>
            </Select>
          </FormGroup>


          <FormGroup>
            <Label>Status</Label>
            <Select
              value={formData.status}
              onChange={(e) => setFormData(prev => ({ ...prev, status: e.target.value }))}
            >
              <option value="FULL_LOAD">FULL_LOAD</option>
              <option value="IN_PROGRESS">IN_PROGRESS</option>
              <option value="LISTENING_CHANGES">LISTENING_CHANGES</option>
              <option value="NO_DATA">NO_DATA</option>
            </Select>
          </FormGroup>

          <FormGroup>
            <Label>Active</Label>
            <Select
              value={formData.active.toString()}
              onChange={(e) => setFormData(prev => ({ ...prev, active: e.target.value === 'true' }))}
            >
              <option value="true">Yes</option>
              <option value="false">No</option>
            </Select>
          </FormGroup>

          {error && <ErrorMessage>{error}</ErrorMessage>}

          <ButtonGroup>
            <Button $variant="secondary" onClick={handleClose}>Cancel</Button>
            <Button $variant="primary" onClick={handleSave}>Add Table</Button>
          </ButtonGroup>
        </ModalContent>
      </ModalOverlay>
    </>
  );
};

export default AddTableModal;
