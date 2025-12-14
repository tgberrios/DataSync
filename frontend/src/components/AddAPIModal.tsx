import { useState, useCallback, useMemo } from 'react';
import styled from 'styled-components';
import {
  Button,
  Input,
  FormGroup,
  Label,
  Select,
} from './shared/BaseComponents';
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
  min-width: 600px;
  max-width: 800px;
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
  min-height: 80px;
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

const TwoColumnGrid = styled.div`
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: ${theme.spacing.md};
  
  @media (max-width: 768px) {
    grid-template-columns: 1fr;
  }
`;

interface AddAPIModalProps {
  onClose: () => void;
  onSave: (entry: any) => void;
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

const AddAPIModal: React.FC<AddAPIModalProps> = ({ onClose, onSave }) => {
  const [formData, setFormData] = useState({
    api_name: '',
    api_type: 'REST',
    base_url: '',
    endpoint: '',
    http_method: 'GET',
    auth_type: 'NONE',
    auth_config: '{}',
    target_db_engine: '',
    target_connection_string: '',
    target_schema: '',
    target_table: '',
    request_body: '',
    request_headers: '{}',
    query_params: '{}',
    sync_interval: 3600,
    status: 'PENDING',
    active: true,
  });
  const [error, setError] = useState<string | null>(null);
  const [isClosing, setIsClosing] = useState(false);

  const connectionExample = useMemo(() => {
    if (!formData.target_db_engine) return '';
    return connectionStringExamples[formData.target_db_engine] || '';
  }, [formData.target_db_engine]);

  const connectionHelp = useMemo(() => {
    if (!formData.target_db_engine) return '';
    return connectionStringHelp[formData.target_db_engine] || '';
  }, [formData.target_db_engine]);

  const handleClose = useCallback(() => {
    setIsClosing(true);
    setTimeout(() => {
      onClose();
    }, 150);
  }, [onClose]);

  const handleSave = useCallback(() => {
    setError(null);
    
    if (!formData.api_name.trim()) {
      setError('API name is required');
      return;
    }
    
    if (!formData.base_url.trim()) {
      setError('Base URL is required');
      return;
    }
    
    if (!formData.endpoint.trim()) {
      setError('Endpoint is required');
      return;
    }
    
    if (!formData.target_db_engine) {
      setError('Target database engine is required');
      return;
    }
    
    if (!formData.target_connection_string.trim()) {
      setError('Target connection string is required');
      return;
    }

    if (!formData.target_schema.trim()) {
      setError('Target schema is required');
      return;
    }

    if (!formData.target_table.trim()) {
      setError('Target table is required');
      return;
    }

    if (formData.target_db_engine === 'MongoDB') {
      if (!formData.target_connection_string.startsWith('mongodb://') && 
          !formData.target_connection_string.startsWith('mongodb+srv://')) {
        setError('MongoDB connection string must start with mongodb:// or mongodb+srv://');
        return;
      }
    } else {
      const requiredParams = ['host', 'user', 'db'];
      const connStr = formData.target_connection_string.toLowerCase();
      const missing = requiredParams.filter(param => !connStr.includes(`${param}=`));
      if (missing.length > 0) {
        setError(`Connection string must include: ${missing.join(', ')}`);
        return;
      }
    }

    let authConfig, requestHeaders, queryParams;
    try {
      authConfig = JSON.parse(formData.auth_config || '{}');
    } catch {
      setError('Invalid JSON in auth_config');
      return;
    }

    try {
      requestHeaders = JSON.parse(formData.request_headers || '{}');
    } catch {
      setError('Invalid JSON in request_headers');
      return;
    }

    try {
      queryParams = JSON.parse(formData.query_params || '{}');
    } catch {
      setError('Invalid JSON in query_params');
      return;
    }

    onSave({
      api_name: formData.api_name.trim(),
      api_type: formData.api_type,
      base_url: formData.base_url.trim(),
      endpoint: formData.endpoint.trim(),
      http_method: formData.http_method,
      auth_type: formData.auth_type,
      auth_config: authConfig,
      target_db_engine: formData.target_db_engine,
      target_connection_string: formData.target_connection_string.trim(),
      target_schema: formData.target_schema.trim().toLowerCase(),
      target_table: formData.target_table.trim().toLowerCase(),
      request_body: formData.request_body.trim() || null,
      request_headers: requestHeaders,
      query_params: queryParams,
      sync_interval: formData.sync_interval,
      status: formData.status,
      active: formData.active,
    });
    handleClose();
  }, [formData, onSave, handleClose]);

  const handleEngineChange = useCallback((engine: string) => {
    setFormData(prev => ({
      ...prev,
      target_db_engine: engine,
      target_connection_string: engine ? connectionStringExamples[engine] || '' : '',
    }));
  }, []);

  return (
    <>
      <BlurOverlay style={{ animation: isClosing ? 'fadeOut 0.15s ease-out' : 'fadeIn 0.15s ease-in' }} onClick={handleClose} />
      <ModalOverlay style={{ animation: isClosing ? 'fadeOut 0.15s ease-out' : 'fadeIn 0.15s ease-in' }}>
        <ModalContent onClick={(e) => e.stopPropagation()}>
          <ModalHeader>Add New API to Catalog</ModalHeader>
          
          <FormGroup>
            <Label>API Name *</Label>
            <Input 
              type="text" 
              value={formData.api_name}
              onChange={(e) => setFormData(prev => ({ ...prev, api_name: e.target.value }))}
              placeholder="e.g., users_api, products_endpoint"
            />
          </FormGroup>

          <TwoColumnGrid>
            <FormGroup>
              <Label>API Type *</Label>
              <Select
                value={formData.api_type}
                onChange={(e) => setFormData(prev => ({ ...prev, api_type: e.target.value }))}
              >
                <option value="REST">REST</option>
                <option value="GraphQL">GraphQL</option>
                <option value="SOAP">SOAP</option>
              </Select>
            </FormGroup>

            <FormGroup>
              <Label>HTTP Method *</Label>
              <Select
                value={formData.http_method}
                onChange={(e) => setFormData(prev => ({ ...prev, http_method: e.target.value }))}
              >
                <option value="GET">GET</option>
                <option value="POST">POST</option>
                <option value="PUT">PUT</option>
                <option value="PATCH">PATCH</option>
                <option value="DELETE">DELETE</option>
              </Select>
            </FormGroup>
          </TwoColumnGrid>

          <FormGroup>
            <Label>Base URL *</Label>
            <Input 
              type="text" 
              value={formData.base_url}
              onChange={(e) => setFormData(prev => ({ ...prev, base_url: e.target.value }))}
              placeholder="e.g., https://api.example.com"
            />
          </FormGroup>

          <FormGroup>
            <Label>Endpoint *</Label>
            <Input 
              type="text" 
              value={formData.endpoint}
              onChange={(e) => setFormData(prev => ({ ...prev, endpoint: e.target.value }))}
              placeholder="e.g., /api/v1/users"
            />
          </FormGroup>

          <TwoColumnGrid>
            <FormGroup>
              <Label>Auth Type *</Label>
              <Select
                value={formData.auth_type}
                onChange={(e) => setFormData(prev => ({ ...prev, auth_type: e.target.value }))}
              >
                <option value="NONE">NONE</option>
                <option value="BASIC">BASIC</option>
                <option value="BEARER">BEARER</option>
                <option value="API_KEY">API_KEY</option>
                <option value="OAUTH2">OAUTH2</option>
              </Select>
            </FormGroup>

            <FormGroup>
              <Label>Sync Interval (seconds) *</Label>
              <Input 
                type="number" 
                value={formData.sync_interval}
                onChange={(e) => setFormData(prev => ({ ...prev, sync_interval: parseInt(e.target.value) || 3600 }))}
                min="1"
              />
            </FormGroup>
          </TwoColumnGrid>

          <FormGroup>
            <Label>Auth Config (JSON)</Label>
            <Textarea
              value={formData.auth_config}
              onChange={(e) => setFormData(prev => ({ ...prev, auth_config: e.target.value }))}
              placeholder='{"username": "user", "password": "pass"} or {"token": "your_token"}'
            />
          </FormGroup>

          <FormGroup>
            <Label>Request Headers (JSON)</Label>
            <Textarea
              value={formData.request_headers}
              onChange={(e) => setFormData(prev => ({ ...prev, request_headers: e.target.value }))}
              placeholder='{"Content-Type": "application/json", "Accept": "application/json"}'
            />
          </FormGroup>

          <FormGroup>
            <Label>Query Parameters (JSON)</Label>
            <Textarea
              value={formData.query_params}
              onChange={(e) => setFormData(prev => ({ ...prev, query_params: e.target.value }))}
              placeholder='{"page": 1, "limit": 100}'
            />
          </FormGroup>

          <FormGroup>
            <Label>Request Body</Label>
            <Textarea
              value={formData.request_body}
              onChange={(e) => setFormData(prev => ({ ...prev, request_body: e.target.value }))}
              placeholder='Optional: JSON body for POST/PUT requests'
            />
          </FormGroup>

          <FormGroup>
            <Label>Target Database Engine *</Label>
            <Select
              value={formData.target_db_engine}
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

          {formData.target_db_engine && (
            <FormGroup>
              <Label>Target Connection String Format</Label>
              <ConnectionStringExample>
                {connectionHelp}
              </ConnectionStringExample>
            </FormGroup>
          )}

          <FormGroup>
            <Label>Target Connection String *</Label>
            <Textarea
              value={formData.target_connection_string}
              onChange={(e) => setFormData(prev => ({ ...prev, target_connection_string: e.target.value }))}
              placeholder={connectionExample || "Enter connection string..."}
            />
          </FormGroup>

          <TwoColumnGrid>
            <FormGroup>
              <Label>Target Schema *</Label>
              <Input 
                type="text" 
                value={formData.target_schema}
                onChange={(e) => setFormData(prev => ({ ...prev, target_schema: e.target.value }))}
                placeholder="e.g., public, dbo"
              />
            </FormGroup>

            <FormGroup>
              <Label>Target Table *</Label>
              <Input 
                type="text" 
                value={formData.target_table}
                onChange={(e) => setFormData(prev => ({ ...prev, target_table: e.target.value }))}
                placeholder="e.g., api_data"
              />
            </FormGroup>
          </TwoColumnGrid>

          <TwoColumnGrid>
            <FormGroup>
              <Label>Status</Label>
              <Select
                value={formData.status}
                onChange={(e) => setFormData(prev => ({ ...prev, status: e.target.value }))}
              >
                <option value="PENDING">PENDING</option>
                <option value="IN_PROGRESS">IN_PROGRESS</option>
                <option value="SUCCESS">SUCCESS</option>
                <option value="ERROR">ERROR</option>
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
          </TwoColumnGrid>

          {error && <ErrorMessage>{error}</ErrorMessage>}

          <ButtonGroup>
            <Button $variant="secondary" onClick={handleClose}>Cancel</Button>
            <Button $variant="primary" onClick={handleSave}>Add API</Button>
          </ButtonGroup>
        </ModalContent>
      </ModalOverlay>
    </>
  );
};

export default AddAPIModal;
