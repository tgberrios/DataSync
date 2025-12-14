import { useState, useCallback, useMemo } from 'react';
import styled from 'styled-components';
import {
  Button,
  Input,
  FormGroup,
  Label,
  Select,
} from './shared/BaseComponents';
import type { CatalogEntry } from '../services/api';
import { theme } from '../theme/theme';

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

const AddTableModal: React.FC<AddTableModalProps> = ({ onClose, onSave }) => {
  const [formData, setFormData] = useState({
    schema_name: '',
    table_name: '',
    db_engine: '',
    connection_string: '',
    active: true,
    status: 'PENDING',
    cluster_name: '',
    pk_strategy: 'OFFSET',
    last_sync_column: '',
  });
  const [error, setError] = useState<string | null>(null);
  const [isClosing, setIsClosing] = useState(false);

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
      last_sync_column: formData.last_sync_column.trim() || '',
    });
    handleClose();
  }, [formData, onSave, handleClose]);

  const handleEngineChange = useCallback((engine: string) => {
    setFormData(prev => ({
      ...prev,
      db_engine: engine,
      connection_string: engine ? connectionStringExamples[engine] || '' : '',
    }));
  }, []);

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

          <FormGroup>
            <Label>Connection String *</Label>
            <Textarea
              value={formData.connection_string}
              onChange={(e) => setFormData(prev => ({ ...prev, connection_string: e.target.value }))}
              placeholder={connectionExample || "Enter connection string..."}
            />
          </FormGroup>

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
              onChange={(e) => setFormData(prev => ({ ...prev, pk_strategy: e.target.value }))}
            >
              <option value="OFFSET">OFFSET</option>
              <option value="PK">Primary Key</option>
            </Select>
          </FormGroup>

          <FormGroup>
            <Label>Sync Column</Label>
            <Input 
              type="text" 
              value={formData.last_sync_column}
              onChange={(e) => setFormData(prev => ({ ...prev, last_sync_column: e.target.value }))}
              placeholder="Optional: column name for incremental sync"
            />
          </FormGroup>

          <FormGroup>
            <Label>Status</Label>
            <Select
              value={formData.status}
              onChange={(e) => setFormData(prev => ({ ...prev, status: e.target.value }))}
            >
              <option value="PENDING">PENDING</option>
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
