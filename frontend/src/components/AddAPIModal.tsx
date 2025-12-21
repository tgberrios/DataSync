import { useState, useCallback, useRef, useMemo } from 'react';
import styled, { keyframes } from 'styled-components';
import {
  Button,
  Input,
  FormGroup,
  Label,
  Select,
} from './shared/BaseComponents';
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

const slideUp = keyframes`
  from {
    opacity: 0;
    transform: translateY(20px);
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
  animation: ${fadeIn} 0.15s ease-in;
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
  animation: ${fadeIn} 0.15s ease-in;
`;

const ModalContent = styled.div`
  background: ${theme.colors.background.main};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  min-width: 700px;
  max-width: 900px;
  max-height: 90vh;
  overflow-y: auto;
  font-family: ${theme.fonts.primary};
  box-shadow: ${theme.shadows.lg};
  animation: ${slideUp} 0.2s ease-out;
  border: 1px solid ${theme.colors.border.light};
  
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
    
    &:hover {
      background: ${theme.colors.primary.main};
    }
  }
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

const RefreshButton = styled.button`
  background: ${theme.colors.primary.main};
  color: ${theme.colors.background.main};
  border: none;
  border-radius: ${theme.borderRadius.md};
  padding: 8px 12px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all ${theme.transitions.normal};
  font-size: 0.9em;
  min-width: 40px;
  height: 40px;
  
  &:hover {
    background: ${theme.colors.primary.dark};
    transform: translateY(-1px);
  }
  
  &:disabled {
    background: ${theme.colors.border.medium};
    cursor: not-allowed;
    opacity: 0.6;
  }
  
  svg {
    width: 18px;
    height: 18px;
  }
`;

const UrlInputContainer = styled.div`
  display: flex;
  gap: ${theme.spacing.sm};
  align-items: flex-start;
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

const ErrorMessage = styled.div`
  color: ${theme.colors.status.error.text};
  background: ${theme.colors.status.error.bg};
  padding: ${theme.spacing.sm};
  border-radius: ${theme.borderRadius.sm};
  margin-top: ${theme.spacing.sm};
  font-size: 0.9em;
  animation: ${fadeIn} 0.3s ease-out;
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

const TwoColumnGrid = styled.div`
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: ${theme.spacing.md};
  
  @media (max-width: 768px) {
    grid-template-columns: 1fr;
  }
`;

const ScanningIndicator = styled.div`
  display: flex;
  align-items: center;
  gap: ${theme.spacing.sm};
  padding: ${theme.spacing.md};
  background: linear-gradient(135deg, ${theme.colors.primary.light}15 0%, ${theme.colors.primary.main}15 100%);
  border: 1px solid ${theme.colors.primary.main}40;
  border-radius: ${theme.borderRadius.md};
  margin-top: ${theme.spacing.sm};
  animation: ${fadeIn} 0.3s ease-out;
  font-size: 0.9em;
  color: ${theme.colors.text.primary};
`;

const Spinner = styled.div`
  width: 16px;
  height: 16px;
  border: 2px solid ${theme.colors.primary.light};
  border-top-color: ${theme.colors.primary.main};
  border-radius: 50%;
  animation: spin 1s linear infinite;
  
  @keyframes spin {
    to { transform: rotate(360deg); }
  }
`;

const EndpointsList = styled.div`
  margin-top: ${theme.spacing.md};
  max-height: 300px;
  overflow-y: auto;
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.md};
  background: ${theme.colors.background.secondary};
  animation: ${slideUp} 0.3s ease-out;
  
  &::-webkit-scrollbar {
    width: 6px;
  }
  
  &::-webkit-scrollbar-track {
    background: ${theme.colors.background.secondary};
  }
  
  &::-webkit-scrollbar-thumb {
    background: ${theme.colors.border.medium};
    border-radius: ${theme.borderRadius.sm};
    
    &:hover {
      background: ${theme.colors.primary.main};
    }
  }
`;

const EndpointItem = styled.div<{ $selected: boolean }>`
  padding: ${theme.spacing.md};
  border-bottom: 1px solid ${theme.colors.border.light};
  cursor: pointer;
  transition: all ${theme.transitions.normal};
  background: ${props => props.$selected ? `${theme.colors.primary.main}15` : 'transparent'};
  border-left: 3px solid ${props => props.$selected ? theme.colors.primary.main : 'transparent'};
  
  &:hover {
    background: ${props => props.$selected ? `${theme.colors.primary.main}20` : theme.colors.background.main};
  }
  
  &:last-child {
    border-bottom: none;
  }
`;

const EndpointMethod = styled.span<{ $method: string }>`
  display: inline-block;
  padding: 2px 8px;
  border-radius: ${theme.borderRadius.sm};
  font-size: 0.75em;
  font-weight: 600;
  margin-right: ${theme.spacing.sm};
  background: ${props => {
    if (props.$method === 'GET') return theme.colors.status.success.bg;
    if (props.$method === 'POST') return theme.colors.primary.light;
    return theme.colors.background.secondary;
  }};
  color: ${props => {
    if (props.$method === 'GET') return theme.colors.status.success.text;
    if (props.$method === 'POST') return theme.colors.primary.dark;
    return theme.colors.text.secondary;
  }};
`;

const EndpointPath = styled.span`
  font-family: monospace;
  color: ${theme.colors.text.primary};
  font-size: 0.9em;
`;

const CollapsibleSection = styled.div<{ $isOpen: boolean }>`
  margin-top: ${theme.spacing.md};
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.md};
  overflow: hidden;
  transition: all ${theme.transitions.normal};
`;

const CollapsibleHeader = styled.div`
  padding: ${theme.spacing.md};
  background: ${theme.colors.background.secondary};
  cursor: pointer;
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: 500;
  color: ${theme.colors.text.primary};
  transition: all ${theme.transitions.normal};
  
  &:hover {
    background: ${theme.colors.background.tertiary};
  }
`;

const CollapsibleContent = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '1000px' : '0'};
  overflow: hidden;
  transition: max-height 0.3s ease-out;
  padding: ${props => props.$isOpen ? theme.spacing.md : '0'} ${theme.spacing.md};
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

interface DetectedEndpoint {
  path: string;
  method: string;
  apiType: string;
  description?: string;
}

interface AddAPIModalProps {
  onClose: () => void;
  onSave: (entry: any) => void;
  initialData?: Partial<any>;
}

const connectionStringExamples: Record<string, string> = {
  MariaDB: 'host=localhost;user=myuser;password=mypassword;db=mydatabase;port=3306',
  MSSQL: 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=mydatabase;UID=myuser;PWD=mypassword',
  Oracle: 'host=localhost;user=myuser;password=mypassword;db=mydatabase;port=1521',
  PostgreSQL: 'postgresql://myuser:mypassword@localhost:5432/mydatabase',
  MongoDB: 'mongodb://myuser:mypassword@localhost:27017/mydatabase',
};

const connectionStringHelp: Record<string, string> = {
  MariaDB: 'Format: host=server;user=username;password=password;db=database;port=3306\n\nExample:\nhost=localhost;user=admin;password=secret123;db=production;port=3306',
  MSSQL: 'Format: DRIVER={ODBC Driver 17 for SQL Server};SERVER=server,port;DATABASE=database;UID=username;PWD=password\n\nExample:\nDRIVER={ODBC Driver 17 for SQL Server};SERVER=sqlserver.example.com,1433;DATABASE=MyDB;UID=sa;PWD=MyPassword123',
  Oracle: 'Format: host=server;user=username;password=password;db=database;port=1521\n\nExample:\nhost=oracle.example.com;user=system;password=oracle123;db=ORCL;port=1521',
  PostgreSQL: 'Format: postgresql://user:password@host:port/database\n\nExample:\npostgresql://postgres:postgres123@postgres.example.com:5432/mydb',
  MongoDB: 'Format: mongodb://username:password@host:port/database\n\nFor MongoDB Atlas (cloud): mongodb+srv://username:password@cluster.mongodb.net/database\n\nExample:\nmongodb://admin:secret123@localhost:27017/mydb\nmongodb+srv://admin:secret123@cluster0.xxxxx.mongodb.net/mydb',
};

const discoverEndpointsFromHTML = (html: string): string[] => {
  const endpoints: string[] = [];
  
  const anchorRegex = /<a[^>]+href=["']([^"']+)["'][^>]*>/gi;
  const apiPathRegex = /["'](\/api\/[^"']+)["']/gi;
  const pathRegex = /["'](\/[^"']+)["']/gi;
  const tablePathRegex = /<td[^>]*>([^<]*\/[^<]+)<\/td>/gi;
  const codeBlockRegex = /<code[^>]*>([^<]*\/[^<]+)<\/code>/gi;
  const markdownRegex = /\*\*(\/[^`\s]+)\*\*/g;
  const urlInTextRegex = /(https?:\/\/[^\s"']+|\/[^\s"']+)/g;
  
  let match;
  
  while ((match = anchorRegex.exec(html)) !== null) {
    const href = match[1];
    if (href.startsWith('/') && !href.startsWith('//') && !href.includes('mailto:') && !href.includes('tel:')) {
      const cleanPath = href.split('?')[0].split('#')[0];
      if (!endpoints.includes(cleanPath)) {
        endpoints.push(cleanPath);
      }
    }
  }
  
  while ((match = apiPathRegex.exec(html)) !== null) {
    const path = match[1];
    const cleanPath = path.split('?')[0].split('#')[0];
    if (!endpoints.includes(cleanPath)) {
      endpoints.push(cleanPath);
    }
  }
  
  while ((match = pathRegex.exec(html)) !== null) {
    const path = match[1];
    if (path.startsWith('/') && !path.startsWith('//') && path.length > 1) {
      const cleanPath = path.split('?')[0].split('#')[0];
      if (!endpoints.includes(cleanPath) && !cleanPath.match(/\.(css|js|png|jpg|jpeg|gif|svg|ico|woff|woff2|ttf|eot)$/i)) {
        endpoints.push(cleanPath);
      }
    }
  }
  
  while ((match = tablePathRegex.exec(html)) !== null) {
    const path = match[1].trim();
    if (path.startsWith('/') && !path.startsWith('//')) {
      const cleanPath = path.split('?')[0].split('#')[0].trim();
      if (!endpoints.includes(cleanPath) && cleanPath.length > 1) {
        endpoints.push(cleanPath);
      }
    }
  }
  
  while ((match = codeBlockRegex.exec(html)) !== null) {
    const path = match[1].trim();
    if (path.startsWith('/') && !path.startsWith('//')) {
      const cleanPath = path.split('?')[0].split('#')[0].trim();
      if (!endpoints.includes(cleanPath) && cleanPath.length > 1) {
        endpoints.push(cleanPath);
      }
    }
  }
  
  while ((match = markdownRegex.exec(html)) !== null) {
    const path = match[1];
    if (path.startsWith('/') && !path.startsWith('//')) {
      const cleanPath = path.split('?')[0].split('#')[0];
      if (!endpoints.includes(cleanPath) && cleanPath.length > 1) {
        endpoints.push(cleanPath);
      }
    }
  }
  
  while ((match = urlInTextRegex.exec(html)) !== null) {
    const url = match[1];
    if (url.startsWith('/') && !url.startsWith('//')) {
      const cleanPath = url.split('?')[0].split('#')[0];
      if (!endpoints.includes(cleanPath) && cleanPath.length > 1 && !cleanPath.match(/\.(css|js|png|jpg|jpeg|gif|svg|ico|woff|woff2|ttf|eot)$/i)) {
        endpoints.push(cleanPath);
      }
    }
  }
  
  return endpoints;
};

const discoverEndpointsFromOpenAPI = async (baseUrl: string, openApiPath: string): Promise<string[]> => {
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 3000);
    
    const response = await fetch(`${baseUrl}${openApiPath}`, {
      method: 'GET',
      headers: { 'Accept': 'application/json' },
      signal: controller.signal,
    });
    
    clearTimeout(timeoutId);
    
    if (response.ok) {
      const spec = await response.json();
      const endpoints: string[] = [];
      
      if (spec.paths) {
        Object.keys(spec.paths).forEach(path => {
          endpoints.push(path);
        });
      }
      
      return endpoints;
    }
  } catch (err) {
  }
  return [];
};

const AddAPIModal: React.FC<AddAPIModalProps> = ({ onClose, onSave, initialData }) => {
  const [formData, setFormData] = useState({
    api_name: initialData?.api_name ? `${initialData.api_name} (Copy)` : '',
    api_type: initialData?.api_type || 'REST',
    base_url: initialData?.base_url || '',
    endpoint: initialData?.endpoint || '',
    http_method: initialData?.http_method || 'GET',
    auth_type: initialData?.auth_type || 'NONE',
    auth_config: initialData?.auth_config ? (typeof initialData.auth_config === 'string' ? initialData.auth_config : JSON.stringify(initialData.auth_config)) : '{}',
    target_db_engine: initialData?.target_db_engine || '',
    target_connection_string: initialData?.target_connection_string || '',
    target_schema: '',
    target_table: '',
    request_headers: '{}',
    query_params: '{}',
    sync_interval: 3600,
    status: 'PENDING',
    active: true,
    operation_type: 'EXTRACT',
  });
  const [error, setError] = useState<string | null>(null);
  const [isClosing, setIsClosing] = useState(false);
  const [isScanning, setIsScanning] = useState(false);
  const [detectedEndpoints, setDetectedEndpoints] = useState<DetectedEndpoint[]>([]);
  const [selectedEndpoint, setSelectedEndpoint] = useState<DetectedEndpoint | null>(null);
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [isTestingConnection, setIsTestingConnection] = useState(false);
  const [connectionTestResult, setConnectionTestResult] = useState<{ success: boolean; message: string } | null>(null);
  const urlInputRef = useRef<HTMLInputElement>(null);
  const lastScannedUrl = useRef<string>('');


  const connectionExample = useMemo(() => {
    if (!formData.target_db_engine) return '';
    return connectionStringExamples[formData.target_db_engine] || '';
  }, [formData.target_db_engine]);

  const connectionHelp = useMemo(() => {
    if (!formData.target_db_engine) return '';
    return connectionStringHelp[formData.target_db_engine] || '';
  }, [formData.target_db_engine]);

  const scanAPI = useCallback(async (url: string) => {
    if (!url || url === lastScannedUrl.current) return;
    
    setIsScanning(true);
    setDetectedEndpoints([]);
    setSelectedEndpoint(null);
    lastScannedUrl.current = url;

    try {
      let baseUrl = url.trim();
      if (!baseUrl.startsWith('http://') && !baseUrl.startsWith('https://')) {
        throw new Error('URL must start with http:// or https://');
      }

      const urlObj = new URL(baseUrl);
      baseUrl = `${urlObj.protocol}//${urlObj.host}`;
      const originalPath = urlObj.pathname;

      const detected: DetectedEndpoint[] = [];
      const testedUrls = new Set<string>();
      
      const testEndpoint = async (path: string, method: string = 'GET', headers: Record<string, string> = {}): Promise<void> => {
        const testUrl = `${baseUrl}${path}`;
        const urlKey = `${testUrl}_${JSON.stringify(headers)}`;
        if (testedUrls.has(urlKey)) return;
        testedUrls.add(urlKey);

        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 3000);
          
          let response: Response | null = null;
          try {
            response = await fetch(testUrl, {
              method: method,
              mode: 'cors',
              signal: controller.signal,
              headers: {
                'Accept': 'application/json, */*',
                ...headers,
              },
            });
          } catch (fetchErr: any) {
            if (fetchErr.name === 'TypeError' || 
                fetchErr.message?.includes('CORS') || 
                fetchErr.message?.includes('Failed to fetch') ||
                fetchErr.message?.includes('NetworkError')) {
              return;
            }
            throw fetchErr;
          }

          if (!response) {
            return;
          }

          clearTimeout(timeoutId);

          if (response.ok || response.status === 200 || response.status === 201 || response.status === 204) {
            const contentType = response.headers.get('content-type') || '';
            let apiType = 'REST';
            let description = `Status: ${response.status}`;
            
            if (contentType.includes('application/json')) {
              apiType = 'REST';
              try {
                const json = await response.clone().json();
                if (Array.isArray(json)) {
                  description = `Returns array (${json.length} items)`;
                } else if (typeof json === 'object') {
                  const keys = Object.keys(json);
                  if (keys.length > 0) {
                    description = `Returns JSON (${keys.slice(0, 3).join(', ')}${keys.length > 3 ? '...' : ''})`;
                  } else {
                    description = `Returns JSON object`;
                  }
                }
              } catch {
                description = `Returns JSON`;
              }
            } else if (contentType.includes('image/')) {
              description = `Returns image`;
            } else if (contentType.includes('text/html')) {
              const text = await response.text();
              if (text.includes('graphql') || text.includes('GraphQL')) {
                apiType = 'GraphQL';
                description = 'GraphQL endpoint';
              } else if (text.includes('swagger') || text.includes('openapi')) {
                description = 'API documentation';
              } else {
                description = `Returns HTML`;
              }
            }

            detected.push({
              path: path,
              method: method,
              apiType,
              description,
            });
          }
        } catch (err: any) {
          if (err.name !== 'AbortError') {
          }
        }
      };

      const endpointsToTest = new Set<string>();
      
      endpointsToTest.add(originalPath || '/');
      
      const testRootAndDiscover = async () => {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), 8000);
          
          const rootResponse = await fetch(`${baseUrl}/`, {
            method: 'GET',
            mode: 'cors',
            signal: controller.signal,
            headers: { 
              'Accept': 'text/html, application/json, */*'
            },
          });

          clearTimeout(timeoutId);

          if (rootResponse.ok || rootResponse.status === 200) {
            const contentType = rootResponse.headers.get('content-type') || '';
            if (contentType.includes('text/html')) {
              const html = await rootResponse.text();
              console.log('HTML length:', html.length);
              const discovered = discoverEndpointsFromHTML(html);
              console.log('Discovered endpoints from HTML:', discovered);
              
              discovered.forEach(ep => {
                if (!ep.startsWith('//') && !ep.startsWith('http') && ep.length > 1) {
                  endpointsToTest.add(ep);
                  
                  const parts = ep.split('/').filter(p => p);
                  if (parts.length > 0) {
                    let currentPath = '';
                    parts.forEach(part => {
                      currentPath += '/' + part;
                      if (currentPath !== ep && !endpointsToTest.has(currentPath)) {
                        endpointsToTest.add(currentPath);
                      }
                    });
                  }
                }
              });
              
              if (discovered.length === 0) {
                console.log('No endpoints found in HTML, trying common patterns...');
                const commonPatterns = ['/api', '/api/cats', '/api/tags', '/cat', '/cat/gif', '/docs', '/swagger'];
                commonPatterns.forEach(pattern => endpointsToTest.add(pattern));
              }
            } else if (contentType.includes('application/json')) {
              const json = await rootResponse.json();
              if (Array.isArray(json)) {
                endpointsToTest.add('/');
              }
            }
          }
        } catch (err: any) {
          if (err.name !== 'AbortError') {
            console.log('Error fetching root:', err.message);
            console.log('Trying common endpoints as fallback...');
            const commonPatterns = ['/api', '/api/cats', '/api/tags', '/cat', '/cat/gif', '/docs', '/swagger'];
            commonPatterns.forEach(pattern => endpointsToTest.add(pattern));
          }
        }
      };
      
      await testRootAndDiscover();

      const openApiPaths = ['/openapi.json', '/swagger.json', '/api-docs', '/swagger', '/docs'];
      for (const openApiPath of openApiPaths) {
        const openApiEndpoints = await discoverEndpointsFromOpenAPI(baseUrl, openApiPath);
        openApiEndpoints.forEach(ep => endpointsToTest.add(ep));
      }

      const generateVariations = (path: string): string[] => {
        const variations: string[] = [];
        const parts = path.split('/').filter(p => p);
        
        if (parts.length > 0) {
          let currentPath = '';
          parts.forEach(part => {
            currentPath += '/' + part;
            if (currentPath !== path && !endpointsToTest.has(currentPath)) {
              variations.push(currentPath);
            }
          });
        }
        
        if (path.includes('/api/') && !path.includes('?')) {
          variations.push(path + '?limit=10');
          variations.push(path + '?limit=1');
        }
        
        if (path.includes('/cat') && !path.includes('?')) {
          variations.push(path + '?json=true');
        }
        
        return variations;
      };
      
      const allEndpoints = Array.from(endpointsToTest);
      allEndpoints.forEach(ep => {
        const variations = generateVariations(ep);
        variations.forEach(v => {
          if (!endpointsToTest.has(v)) {
            endpointsToTest.add(v);
          }
        });
      });

      const testPromises: Promise<void>[] = [];
      const endpointsArray = Array.from(endpointsToTest).sort((a, b) => {
        if (a.includes('/api/') && !b.includes('/api/')) return -1;
        if (!a.includes('/api/') && b.includes('/api/')) return 1;
        if (a.includes('/cat') && !b.includes('/cat')) return -1;
        if (!a.includes('/cat') && b.includes('/cat')) return 1;
        return a.localeCompare(b);
      });
      
      console.log(`Testing ${endpointsArray.length} endpoints...`);
      
      for (const path of endpointsArray) {
        testPromises.push(testEndpoint(path, 'GET'));
        
        if (path.includes('/api/') || path.includes('/cat') || path.includes('/tags')) {
          testPromises.push(testEndpoint(path, 'GET', { 'Accept': 'application/json' }));
        }
        
        if (path === '/cat' || (path.includes('/cat') && !path.includes('?'))) {
          testPromises.push(testEndpoint(path + '?json=true', 'GET'));
        }
        
        if ((path.includes('/api/cats') || path.includes('/cats')) && !path.includes('?')) {
          testPromises.push(testEndpoint(path + '?limit=1', 'GET'));
          testPromises.push(testEndpoint(path + '?limit=10', 'GET'));
        }
      }

      await Promise.allSettled(testPromises);
      
      console.log(`Found ${detected.length} working endpoints`);

      if (detected.length === 0) {
        detected.push({
          path: originalPath || '/',
          method: 'GET',
          apiType: 'REST',
          description: 'Detected from URL',
        });
      }

      detected.sort((a, b) => {
        if (a.path === '/api/cats' || a.path === '/api/tags') return -1;
        if (b.path === '/api/cats' || b.path === '/api/tags') return 1;
        if (a.path === '/cat') return -1;
        if (b.path === '/cat') return 1;
        return a.path.localeCompare(b.path);
      });

      setDetectedEndpoints(detected);
      if (detected.length > 0) {
        const bestEndpoint = detected.find(e => e.path.includes('/api/')) || detected[0];
        setSelectedEndpoint(bestEndpoint);
        const endpoint = bestEndpoint.path;
        setFormData(prev => ({
          ...prev,
          base_url: baseUrl,
          endpoint: endpoint,
          http_method: bestEndpoint.method,
          api_type: bestEndpoint.apiType,
        }));
      }
    } catch (err: any) {
      setError(err.message || 'Error scanning API');
    } finally {
      setIsScanning(false);
    }
  }, []);

  const handleUrlChange = useCallback((value: string) => {
    if (value && (value.startsWith('http://') || value.startsWith('https://'))) {
      try {
        const urlObj = new URL(value);
        const base = `${urlObj.protocol}//${urlObj.host}`;
        const endpoint = urlObj.pathname || '/';
        setFormData(prev => ({
          ...prev,
          base_url: base,
          endpoint: endpoint,
        }));
      } catch (err) {
        setFormData(prev => ({ ...prev, base_url: value }));
      }
    } else {
      setFormData(prev => ({ ...prev, base_url: value }));
    }
  }, []);

  const handleManualScan = useCallback(() => {
    const fullUrl = formData.base_url + (formData.endpoint === '/' ? '' : formData.endpoint);
    if (fullUrl && (fullUrl.startsWith('http://') || fullUrl.startsWith('https://'))) {
      scanAPI(fullUrl);
    } else if (formData.base_url && (formData.base_url.startsWith('http://') || formData.base_url.startsWith('https://'))) {
      scanAPI(formData.base_url);
    } else {
      setError('Please enter a valid URL');
    }
  }, [formData.base_url, formData.endpoint, scanAPI]);

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
    } else if (formData.target_db_engine === 'PostgreSQL') {
      const connStr = formData.target_connection_string.toLowerCase();
      if (!connStr.startsWith('postgresql://') && !connStr.startsWith('postgres://')) {
        const requiredParams = ['host', 'user', 'db'];
        const missing = requiredParams.filter(param => !connStr.includes(`${param}=`));
        if (missing.length > 0) {
          setError(`PostgreSQL connection string must be in URI format (postgresql://...) or include: ${missing.join(', ')}`);
          return;
        }
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
      setError('Invalid JSON in Auth Config');
      return;
    }

    try {
      requestHeaders = JSON.parse(formData.request_headers || '{}');
    } catch {
      setError('Invalid JSON in Request Headers');
      return;
    }

    try {
      queryParams = JSON.parse(formData.query_params || '{}');
    } catch {
      setError('Invalid JSON in Query Parameters');
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
      request_body: null,
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
    setConnectionTestResult(null);
  }, []);

  const handleTestConnection = useCallback(async () => {
    if (!formData.target_db_engine) {
      setConnectionTestResult({ success: false, message: 'Please select a database engine first' });
      return;
    }

    if (!formData.target_connection_string.trim()) {
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
          db_engine: formData.target_db_engine,
          connection_string: formData.target_connection_string.trim(),
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
        setConnectionTestResult({ success: true, message: data.message || 'Connection successful!' });
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
  }, [formData.target_db_engine, formData.target_connection_string]);

  const handleEndpointSelect = useCallback((endpoint: DetectedEndpoint) => {
    setSelectedEndpoint(endpoint);
    setFormData(prev => ({
      ...prev,
      endpoint: endpoint.path,
      http_method: endpoint.method,
      api_type: endpoint.apiType,
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
              placeholder="e.g., Cat API, Users API"
            />
          </FormGroup>

          <FormGroup>
            <Label>API URL *</Label>
            <UrlInputContainer>
              <Input 
                ref={urlInputRef}
                type="text" 
                value={formData.base_url + (formData.endpoint === '/' ? '' : formData.endpoint)}
                onChange={(e) => {
                  e.stopPropagation();
                  handleUrlChange(e.target.value);
                }}
                onFocus={(e) => e.stopPropagation()}
                placeholder="Enter URL here (e.g., https://cataas.com/cat)"
                style={{ flex: 1 }}
              />
              <RefreshButton
                type="button"
                onClick={handleManualScan}
                disabled={isScanning || !formData.base_url || (!formData.base_url.startsWith('http://') && !formData.base_url.startsWith('https://'))}
                title="Scan API for endpoints"
              >
                {isScanning ? (
                  <Spinner />
                ) : (
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0 1 14.85-3.48L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/>
                  </svg>
                )}
              </RefreshButton>
            </UrlInputContainer>
            {isScanning && (
              <ScanningIndicator>
                <Spinner />
                <span>Scanning API for endpoints...</span>
              </ScanningIndicator>
            )}
            {detectedEndpoints.length > 0 && !isScanning && (
              <EndpointsList>
                {detectedEndpoints.map((endpoint, index) => (
                  <EndpointItem
                    key={index}
                    $selected={selectedEndpoint?.path === endpoint.path && selectedEndpoint?.method === endpoint.method}
                    onClick={() => handleEndpointSelect(endpoint)}
                  >
                    <EndpointMethod $method={endpoint.method}>
                      {endpoint.method}
                    </EndpointMethod>
                    <EndpointPath>{endpoint.path}</EndpointPath>
                    {endpoint.description && (
                      <span style={{ marginLeft: '8px', color: theme.colors.text.secondary, fontSize: '0.85em' }}>
                        - {endpoint.description}
                      </span>
                    )}
                  </EndpointItem>
                ))}
              </EndpointsList>
            )}
          </FormGroup>

          <FormGroup>
            <Label>Operation Type *</Label>
            <Select
              value={formData.operation_type}
              onChange={(e) => setFormData(prev => ({ ...prev, operation_type: e.target.value }))}
            >
              <option value="EXTRACT">Extract (Get data from API)</option>
              <option value="SEND">Send (Send data to API)</option>
            </Select>
          </FormGroup>

          <TwoColumnGrid>
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
          </TwoColumnGrid>

          <TwoColumnGrid>
            <FormGroup>
              <Label>API Type</Label>
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
              </Select>
            </FormGroup>
          </TwoColumnGrid>

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

          <CollapsibleSection $isOpen={showAdvanced}>
            <CollapsibleHeader onClick={() => setShowAdvanced(!showAdvanced)}>
              <span>Advanced Options</span>
              <span>{showAdvanced ? '▼' : '▶'}</span>
            </CollapsibleHeader>
            <CollapsibleContent $isOpen={showAdvanced}>
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
            </CollapsibleContent>
          </CollapsibleSection>

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
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
              <Label style={{ marginBottom: 0 }}>Target Connection String *</Label>
              <Button
                type="button"
                $variant="secondary"
                onClick={handleTestConnection}
                disabled={isTestingConnection || !formData.target_db_engine || !formData.target_connection_string.trim()}
                style={{ padding: '6px 12px', fontSize: '0.85em', minWidth: 'auto' }}
              >
                {isTestingConnection ? 'Testing...' : 'Test Connection'}
              </Button>
            </div>
            <Textarea
              value={formData.target_connection_string}
              onChange={(e) => {
                setFormData(prev => ({ ...prev, target_connection_string: e.target.value }));
                setConnectionTestResult(null);
              }}
              placeholder={connectionExample || "Enter connection string..."}
            />
            {connectionTestResult && (
              <ConnectionTestResult $success={connectionTestResult.success}>
                {connectionTestResult.success ? '✓ ' : '✗ '}
                {connectionTestResult.message}
              </ConnectionTestResult>
            )}
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
