import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { governanceApi } from '../services/api';

const GovernanceContainer = styled.div`
  background-color: white;
  color: #333;
  padding: 20px;
  font-family: monospace;
`;

const Header = styled.div`
  border: 2px solid #333;
  padding: 15px;
  text-align: center;
  margin-bottom: 30px;
  font-size: 1.5em;
  font-weight: bold;
  background-color: #f5f5f5;
  border-radius: 4px;
`;

const FiltersContainer = styled.div`
  display: flex;
  gap: 15px;
  margin-bottom: 20px;
  padding: 15px;
  background: #f5f5f5;
  border-radius: 4px;
`;

const Select = styled.select`
  padding: 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-family: monospace;
`;

const Table = styled.table`
  width: 100%;
  border-collapse: collapse;
  margin-top: 20px;
  background: white;
`;

  const Th = styled.th<{ sortable?: boolean }>`
  padding: 12px;
  text-align: left;
  border-bottom: 2px solid #333;
  background: #f5f5f5;
  white-space: nowrap;
  cursor: ${props => props.sortable ? 'pointer' : 'default'};
  
  &:hover {
    background: ${props => props.sortable ? '#e0e0e0' : '#f5f5f5'};
  }
`;

const Td = styled.td`
  padding: 12px;
  border-bottom: 1px solid #ddd;
`;

const Badge = styled.span<{ type: string }>`
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 0.9em;
  font-weight: 500;
  background-color: ${props => {
    switch (props.type) {
      // Health Status
      case 'EXCELLENT': return '#e8f5e9';
      case 'HEALTHY': return '#e8f5e9';
      case 'WARNING': return '#fff3e0';
      case 'CRITICAL': return '#ffebee';
      case 'EMERGENCY': return '#fce4ec';
      // Access Frequency
      case 'REAL_TIME': return '#e1f5fe';
      case 'HIGH': return '#e3f2fd';
      case 'MEDIUM': return '#f3e5f5';
      case 'LOW': return '#fafafa';
      case 'RARE': return '#f5f5f5';
      case 'ARCHIVED': return '#eeeeee';
      // Sensitivity Level
      case 'PUBLIC_SENSITIVITY': return '#f1f8e9';
      case 'LOW_SENSITIVITY': return '#f1f8e9';
      case 'MEDIUM_SENSITIVITY': return '#fff3e0';
      case 'HIGH_SENSITIVITY': return '#ffebee';
      case 'CRITICAL_SENSITIVITY': return '#fce4ec';
      // Data Category
      case 'TRANSACTIONAL': return '#e8eaf6';
      case 'ANALYTICAL': return '#f3e5f5';
      case 'REFERENCE': return '#e0f2f1';
      case 'MASTER_DATA': return '#e8f5e9';
      case 'OPERATIONAL': return '#fff3e0';
      case 'TEMPORAL': return '#e1f5fe';
      case 'GEOSPATIAL': return '#f1f8e9';
      case 'FINANCIAL': return '#ffebee';
      case 'COMPLIANCE': return '#fce4ec';
      case 'TECHNICAL': return '#f5f5f5';
      case 'SPORTS': return '#e3f2fd';
      default: return '#f5f5f5';
    }
  }};
  color: ${props => {
    switch (props.type) {
      // Health Status
      case 'EXCELLENT': return '#1b5e20';
      case 'HEALTHY': return '#2e7d32';
      case 'WARNING': return '#ef6c00';
      case 'CRITICAL': return '#c62828';
      case 'EMERGENCY': return '#ad1457';
      // Access Frequency
      case 'REAL_TIME': return '#0277bd';
      case 'HIGH': return '#1565c0';
      case 'MEDIUM': return '#6a1b9a';
      case 'LOW': return '#616161';
      case 'RARE': return '#757575';
      case 'ARCHIVED': return '#9e9e9e';
      // Sensitivity Level
      case 'PUBLIC_SENSITIVITY': return '#388e3c';
      case 'LOW_SENSITIVITY': return '#558b2f';
      case 'MEDIUM_SENSITIVITY': return '#ef6c00';
      case 'HIGH_SENSITIVITY': return '#c62828';
      case 'CRITICAL_SENSITIVITY': return '#ad1457';
      // Data Category
      case 'TRANSACTIONAL': return '#3949ab';
      case 'ANALYTICAL': return '#7b1fa2';
      case 'REFERENCE': return '#00796b';
      case 'MASTER_DATA': return '#2e7d32';
      case 'OPERATIONAL': return '#f57c00';
      case 'TEMPORAL': return '#0277bd';
      case 'GEOSPATIAL': return '#388e3c';
      case 'FINANCIAL': return '#d32f2f';
      case 'COMPLIANCE': return '#c2185b';
      case 'TECHNICAL': return '#616161';
      case 'SPORTS': return '#1976d2';
      default: return '#757575';
    }
  }};
`;

const QualityScore = styled.span<{ score: number }>`
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 0.9em;
  font-weight: 500;
  background-color: ${props => {
    if (props.score >= 90) return '#e8f5e9';
    if (props.score >= 70) return '#f1f8e9';
    if (props.score >= 50) return '#fff3e0';
    return '#ffebee';
  }};
  color: ${props => {
    if (props.score >= 90) return '#2e7d32';
    if (props.score >= 70) return '#558b2f';
    if (props.score >= 50) return '#ef6c00';
    return '#c62828';
  }};
`;

const Pagination = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 10px;
  margin-top: 20px;
  padding: 15px;
`;

const PageButton = styled.button<{ active?: boolean }>`
  padding: 5px 10px;
  border: 1px solid #ddd;
  border-radius: 4px;
  background: ${props => props.active ? '#333' : 'white'};
  color: ${props => props.active ? 'white' : '#333'};
  cursor: pointer;
  font-family: monospace;
  
  &:hover {
    background: ${props => props.active ? '#333' : '#f5f5f5'};
  }
  
  &:disabled {
    cursor: not-allowed;
    opacity: 0.5;
  }
`;

const GovernanceList = styled.div`
  display: flex;
  flex-direction: column;
  gap: 8px;
`;

const GovernanceItem = styled.div`
  border: 1px solid #eee;
  border-radius: 4px;
  background-color: #fafafa;
  overflow: hidden;
  transition: all 0.2s ease;
  
  &:hover {
    border-color: #ddd;
    background-color: #f5f5f5;
  }
`;

  const GovernanceSummary = styled.div`
  display: grid;
  grid-template-columns: 200px 100px 120px 120px 100px 100px 80px 80px 80px 100px 150px;
  align-items: center;
  padding: 10px 15px;
  cursor: pointer;
  gap: 10px;
  font-size: 0.9em;
  overflow-x: auto;
  min-width: 100%;
  
  & > div {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
`;

const GovernanceDetails = styled.div<{ $isOpen: boolean }>`
  max-height: ${props => props.$isOpen ? '800px' : '0'};
  opacity: ${props => props.$isOpen ? '1' : '0'};
  transition: all 0.3s ease;
  border-top: ${props => props.$isOpen ? '1px solid #eee' : 'none'};
  background-color: white;
  overflow: hidden;
`;

const DetailsGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  padding: 15px;
  gap: 15px;
`;

const DetailCard = styled.div`
  background: white;
  border: 1px solid #eee;
  border-radius: 4px;
  padding: 12px;
`;

const DetailLabel = styled.div`
  color: #666;
  font-size: 0.85em;
  margin-bottom: 5px;
`;

const DetailValue = styled.div`
  font-size: 1.1em;
  font-weight: 500;
`;

const Tooltip = styled.div`
  position: relative;
  display: inline-block;
  
  &:hover .tooltip-content {
    visibility: visible;
    opacity: 1;
  }
`;

const TooltipContent = styled.div`
  visibility: hidden;
  opacity: 0;
  position: absolute;
  z-index: 1000;
  bottom: 125%;
  left: 50%;
  transform: translateX(-50%);
  background-color: #333;
  color: white;
  text-align: center;
  border-radius: 4px;
  padding: 8px 12px;
  font-size: 0.85em;
  white-space: nowrap;
  min-width: 200px;
  max-width: 300px;
  white-space: normal;
  box-shadow: 0 2px 8px rgba(0,0,0,0.2);
  transition: opacity 0.3s;
  
  &:after {
    content: "";
    position: absolute;
    top: 100%;
    left: 50%;
    margin-left: -5px;
    border-width: 5px;
    border-style: solid;
    border-color: #333 transparent transparent transparent;
  }
`;

const formatDate = (date: string) => {
  if (!date) return '-';
  return new Date(date).toLocaleString();
};

const formatSize = (mb: number | null | undefined) => {
  if (mb == null) return '-';
  const size = Number(mb);
  if (isNaN(size)) return '-';
  if (size >= 1024) {
    return `${(size / 1024).toFixed(2)} GB`;
  }
  return `${size.toFixed(2)} MB`;
};

const formatNumber = (num: number) => {
  return num?.toLocaleString() || '0';
};

const getCategoryDescription = (category: string) => {
  const descriptions: { [key: string]: string } = {
    'TRANSACTIONAL': 'Data that represents business transactions and events that occur in real-time',
    'ANALYTICAL': 'Data used for analysis, reporting, and business intelligence purposes',
    'REFERENCE': 'Lookup tables and master data used across multiple systems',
    'MASTER_DATA': 'Core business entities like customers, products, and suppliers',
    'OPERATIONAL': 'Data used for day-to-day operational processes and workflows',
    'TEMPORAL': 'Time-series data that tracks changes over time',
    'GEOSPATIAL': 'Location-based data including coordinates and geographic information',
    'FINANCIAL': 'Financial transactions, accounts, and monetary data',
    'COMPLIANCE': 'Data required for regulatory compliance and auditing',
    'TECHNICAL': 'System-generated data for technical monitoring and maintenance',
    'SPORTS': 'Sports-related data including scores, statistics, and player information'
  };
  return descriptions[category] || 'Data category classification';
};

const getHealthDescription = (health: string) => {
  const descriptions: { [key: string]: string } = {
    'EXCELLENT': 'Optimal data quality with no issues detected',
    'HEALTHY': 'Good data quality with minor or no issues',
    'WARNING': 'Some data quality issues detected that need attention',
    'CRITICAL': 'Significant data quality issues requiring immediate attention',
    'EMERGENCY': 'Severe data quality issues that may impact system functionality'
  };
  return descriptions[health] || 'Health status of the data';
};

const getSensitivityDescription = (sensitivity: string) => {
  const descriptions: { [key: string]: string } = {
    'PUBLIC': 'Data that can be freely shared and accessed by anyone',
    'LOW': 'Data with minimal sensitivity requirements',
    'MEDIUM': 'Data with moderate sensitivity requiring controlled access',
    'HIGH': 'Data with high sensitivity requiring strict access controls',
    'CRITICAL': 'Data with critical sensitivity requiring maximum security measures'
  };
  return descriptions[sensitivity] || 'Data sensitivity level';
};

const Governance = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<any[]>([]);
  const [openItemId, setOpenItemId] = useState<number | null>(null);
  const [page, setPage] = useState(1);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 10
  });

  const [filter, setFilter] = useState({
    engine: '',
    category: '',
    health: '',
    domain: '',
    sensitivity: ''
  });
  
  const [sort, setSort] = useState({
    field: 'health_status',
    direction: 'desc'
  });

  const toggleItem = (id: number) => {
    setOpenItemId(openItemId === id ? null : id);
  };

  const handleSort = (field: string) => {
    setSort(prev => ({
      field,
      direction: prev.field === field && prev.direction === 'asc' ? 'desc' : 'asc'
    }));
  };

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        const response = await governanceApi.getGovernanceData({
          page,
          limit: 10,
          ...filter,
          sort_field: sort.field,
          sort_direction: sort.direction
        });
        setData(response.data);
        setPagination(response.pagination);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading governance data');
      } finally {
        setLoading(false);
        }
      };

    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, [page, filter, sort]);

  return (
    <GovernanceContainer>
      <Header>
        Data Governance Catalog
      </Header>

      <FiltersContainer>
        <Select 
          value={filter.engine}
          onChange={(e) => setFilter({...filter, engine: e.target.value})}
        >
          <option value="">All Engines</option>
          <option value="PostgreSQL">PostgreSQL</option>
          <option value="MongoDB">MongoDB</option>
          <option value="MSSQL">MSSQL</option>
          <option value="MariaDB">MariaDB</option>
        </Select>

        <Select
          value={filter.category}
          onChange={(e) => setFilter({...filter, category: e.target.value})}
        >
          <option value="">All Categories</option>
          <option value="TRANSACTIONAL">Transactional</option>
          <option value="ANALYTICAL">Analytical</option>
          <option value="REFERENCE">Reference</option>
          <option value="MASTER_DATA">Master Data</option>
          <option value="OPERATIONAL">Operational</option>
          <option value="TEMPORAL">Temporal</option>
          <option value="GEOSPATIAL">Geospatial</option>
          <option value="FINANCIAL">Financial</option>
          <option value="COMPLIANCE">Compliance</option>
          <option value="TECHNICAL">Technical</option>
          <option value="SPORTS">Sports</option>
        </Select>

        <Select
          value={filter.health}
          onChange={(e) => setFilter({...filter, health: e.target.value})}
        >
          <option value="">All Health Status</option>
          <option value="EXCELLENT">Excellent</option>
          <option value="HEALTHY">Healthy</option>
          <option value="WARNING">Warning</option>
          <option value="CRITICAL">Critical</option>
          <option value="EMERGENCY">Emergency</option>
        </Select>

        <Select
          value={filter.sensitivity}
          onChange={(e) => setFilter({...filter, sensitivity: e.target.value})}
        >
          <option value="">All Sensitivity</option>
          <option value="PUBLIC">Public</option>
          <option value="LOW">Low</option>
          <option value="MEDIUM">Medium</option>
          <option value="HIGH">High</option>
          <option value="CRITICAL">Critical</option>
        </Select>

        <Select
          value={filter.domain}
          onChange={(e) => setFilter({...filter, domain: e.target.value})}
        >
          <option value="">All Domains</option>
          <option value="CUSTOMER">Customer</option>
          <option value="SALES">Sales</option>
          <option value="MARKETING">Marketing</option>
          <option value="HR">HR</option>
          <option value="FINANCE">Finance</option>
          <option value="INVENTORY">Inventory</option>
          <option value="OPERATIONS">Operations</option>
          <option value="SUPPORT">Support</option>
          <option value="SECURITY">Security</option>
          <option value="ANALYTICS">Analytics</option>
          <option value="COMMUNICATION">Communication</option>
          <option value="LEGAL">Legal</option>
          <option value="RESEARCH">Research</option>
          <option value="MANUFACTURING">Manufacturing</option>
          <option value="LOGISTICS">Logistics</option>
          <option value="HEALTHCARE">Healthcare</option>
          <option value="EDUCATION">Education</option>
          <option value="REAL_ESTATE">Real Estate</option>
          <option value="INSURANCE">Insurance</option>
          <option value="SPORTS">Sports</option>
          <option value="GENERAL">General</option>
        </Select>
      </FiltersContainer>

      {loading && (
        <div style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
          Loading governance data...
        </div>
      )}

      {error && (
        <div style={{ color: 'red', padding: '20px', textAlign: 'center' }}>
          {error}
        </div>
      )}

      {!loading && !error && (
        <>
          <GovernanceList>
            {data.length === 0 ? (
              <div style={{ textAlign: 'center', padding: '20px', color: '#666' }}>
                No governance data found
              </div>
            ) : (
              data.map((item) => (
                <GovernanceItem key={item.id}>
                  <GovernanceSummary onClick={() => toggleItem(item.id)}>
                    <div onClick={(e) => { e.stopPropagation(); handleSort('table_name'); }} style={{ cursor: 'pointer' }}>
                      {item.schema_name}.{item.table_name}
                      {sort.field === 'table_name' && (sort.direction === 'asc' ? ' ↑' : ' ↓')}
                    </div>
                    <div onClick={(e) => { e.stopPropagation(); handleSort('inferred_source_engine'); }} style={{ cursor: 'pointer' }}>
                      {item.inferred_source_engine}
                      {sort.field === 'inferred_source_engine' && (sort.direction === 'asc' ? ' ↑' : ' ↓')}
                    </div>
                    <div onClick={(e) => { e.stopPropagation(); handleSort('data_category'); }} style={{ cursor: 'pointer' }}>
                      <Tooltip>
                        <Badge type={item.data_category}>
                          {item.data_category}
                        </Badge>
                        <TooltipContent className="tooltip-content">
                          {getCategoryDescription(item.data_category)}
                        </TooltipContent>
                      </Tooltip>
                      {sort.field === 'data_category' && (sort.direction === 'asc' ? ' ↑' : ' ↓')}
                    </div>
                    <div onClick={(e) => { e.stopPropagation(); handleSort('business_domain'); }} style={{ cursor: 'pointer' }}>
                      {item.business_domain}
                      {sort.field === 'business_domain' && (sort.direction === 'asc' ? ' ↑' : ' ↓')}
                    </div>
                    <div onClick={(e) => { e.stopPropagation(); handleSort('health_status'); }} style={{ cursor: 'pointer' }}>
                      <Tooltip>
                        <Badge type={item.health_status}>
                          {item.health_status}
                        </Badge>
                        <TooltipContent className="tooltip-content">
                          {getHealthDescription(item.health_status)}
                        </TooltipContent>
                      </Tooltip>
                      {sort.field === 'health_status' && (sort.direction === 'asc' ? ' ↑' : ' ↓')}
                    </div>
                    <div onClick={(e) => { e.stopPropagation(); handleSort('sensitivity_level'); }} style={{ cursor: 'pointer' }}>
                      <Tooltip>
                        <Badge type={`${item.sensitivity_level}_SENSITIVITY`}>
                          {item.sensitivity_level}
                        </Badge>
                        <TooltipContent className="tooltip-content">
                          {getSensitivityDescription(item.sensitivity_level)}
                        </TooltipContent>
                      </Tooltip>
                      {sort.field === 'sensitivity_level' && (sort.direction === 'asc' ? ' ↑' : ' ↓')}
                    </div>
                    <div onClick={(e) => { e.stopPropagation(); handleSort('data_quality_score'); }} style={{ cursor: 'pointer' }}>
                      <QualityScore score={item.data_quality_score}>
                        {item.data_quality_score}%
                      </QualityScore>
                      {sort.field === 'data_quality_score' && (sort.direction === 'asc' ? ' ↑' : ' ↓')}
                    </div>
                    <div onClick={(e) => { e.stopPropagation(); handleSort('table_size_mb'); }} style={{ cursor: 'pointer' }}>
                      {formatSize(item.table_size_mb)}
                      {sort.field === 'table_size_mb' && (sort.direction === 'asc' ? ' ↑' : ' ↓')}
                    </div>
                    <div onClick={(e) => { e.stopPropagation(); handleSort('total_rows'); }} style={{ cursor: 'pointer' }}>
                      {formatNumber(item.total_rows)}
                      {sort.field === 'total_rows' && (sort.direction === 'asc' ? ' ↑' : ' ↓')}
                    </div>
                    <div onClick={(e) => { e.stopPropagation(); handleSort('access_frequency'); }} style={{ cursor: 'pointer' }}>
                      <Badge type={item.access_frequency}>
                        {item.access_frequency}
                      </Badge>
                      {sort.field === 'access_frequency' && (sort.direction === 'asc' ? ' ↑' : ' ↓')}
                    </div>
                    <div onClick={(e) => { e.stopPropagation(); handleSort('last_analyzed'); }} style={{ textAlign: 'right', color: '#666', fontSize: '0.85em', cursor: 'pointer' }}>
                      {formatDate(item.last_analyzed)}
                      {sort.field === 'last_analyzed' && (sort.direction === 'asc' ? ' ↑' : ' ↓')}
                    </div>
                  </GovernanceSummary>

                  <GovernanceDetails $isOpen={openItemId === item.id}>
                    <DetailsGrid>
                      <DetailCard>
                        <DetailLabel>Schema Name</DetailLabel>
                        <DetailValue>{item.schema_name}</DetailValue>
                      </DetailCard>
                      <DetailCard>
                        <DetailLabel>Table Name</DetailLabel>
                        <DetailValue>{item.table_name}</DetailValue>
                      </DetailCard>
                      <DetailCard>
                        <DetailLabel>Database Engine</DetailLabel>
                        <DetailValue>{item.inferred_source_engine}</DetailValue>
                      </DetailCard>
                      <DetailCard>
                        <DetailLabel>Data Category</DetailLabel>
                        <DetailValue>
                          <Tooltip>
                            <Badge type={item.data_category}>
                              {item.data_category}
                            </Badge>
                            <TooltipContent className="tooltip-content">
                              {getCategoryDescription(item.data_category)}
                            </TooltipContent>
                          </Tooltip>
                        </DetailValue>
                      </DetailCard>
                      <DetailCard>
                        <DetailLabel>Business Domain</DetailLabel>
                        <DetailValue>{item.business_domain}</DetailValue>
                      </DetailCard>
                      <DetailCard>
                        <DetailLabel>Health Status</DetailLabel>
                        <DetailValue>
                          <Tooltip>
                            <Badge type={item.health_status}>
                              {item.health_status}
                            </Badge>
                            <TooltipContent className="tooltip-content">
                              {getHealthDescription(item.health_status)}
                            </TooltipContent>
                          </Tooltip>
                        </DetailValue>
                      </DetailCard>
                      <DetailCard>
                        <DetailLabel>Sensitivity Level</DetailLabel>
                        <DetailValue>
                          <Tooltip>
                            <Badge type={`${item.sensitivity_level}_SENSITIVITY`}>
                              {item.sensitivity_level}
                            </Badge>
                            <TooltipContent className="tooltip-content">
                              {getSensitivityDescription(item.sensitivity_level)}
                            </TooltipContent>
                          </Tooltip>
                        </DetailValue>
                      </DetailCard>
                      <DetailCard>
                        <DetailLabel>Data Quality Score</DetailLabel>
                        <DetailValue>
                          <QualityScore score={item.data_quality_score}>
                            {item.data_quality_score}%
                          </QualityScore>
                        </DetailValue>
                      </DetailCard>
                      <DetailCard>
                        <DetailLabel>Table Size</DetailLabel>
                        <DetailValue>{formatSize(item.table_size_mb)}</DetailValue>
                      </DetailCard>
                      <DetailCard>
                        <DetailLabel>Total Rows</DetailLabel>
                        <DetailValue>{formatNumber(item.total_rows)}</DetailValue>
                      </DetailCard>
                      <DetailCard>
                        <DetailLabel>Access Frequency</DetailLabel>
                        <DetailValue>
                          <Badge type={item.access_frequency}>
                            {item.access_frequency}
                          </Badge>
                        </DetailValue>
                      </DetailCard>
                      <DetailCard>
                        <DetailLabel>Last Analyzed</DetailLabel>
                        <DetailValue>{formatDate(item.last_analyzed)}</DetailValue>
                      </DetailCard>
                      {item.data_classification && (
                        <DetailCard>
                          <DetailLabel>Data Classification</DetailLabel>
                          <DetailValue>{item.data_classification}</DetailValue>
                        </DetailCard>
                      )}
                      {item.retention_policy && (
                        <DetailCard>
                          <DetailLabel>Retention Policy</DetailLabel>
                          <DetailValue>{item.retention_policy}</DetailValue>
                        </DetailCard>
                      )}
                      {item.owner && (
                        <DetailCard>
                          <DetailLabel>Data Owner</DetailLabel>
                          <DetailValue>{item.owner}</DetailValue>
                        </DetailCard>
                      )}
                      {item.compliance_requirements && (
                        <DetailCard>
                          <DetailLabel>Compliance Requirements</DetailLabel>
                          <DetailValue>{item.compliance_requirements}</DetailValue>
                        </DetailCard>
                      )}
                      {item.null_percentage != null && (
                        <DetailCard>
                          <DetailLabel>Null Percentage</DetailLabel>
                          <DetailValue>{item.null_percentage}%</DetailValue>
                        </DetailCard>
                      )}
                      {item.duplicate_percentage != null && (
                        <DetailCard>
                          <DetailLabel>Duplicate Percentage</DetailLabel>
                          <DetailValue>{item.duplicate_percentage}%</DetailValue>
                        </DetailCard>
                      )}
                      {item.fragmentation_percentage != null && (
                        <DetailCard>
                          <DetailLabel>Fragmentation Percentage</DetailLabel>
                          <DetailValue>{item.fragmentation_percentage}%</DetailValue>
                        </DetailCard>
                      )}
                      {item.query_count_daily != null && (
                        <DetailCard>
                          <DetailLabel>Daily Query Count</DetailLabel>
                          <DetailValue>{formatNumber(item.query_count_daily)}</DetailValue>
                        </DetailCard>
                      )}
                      {item.last_vacuum && (
                        <DetailCard>
                          <DetailLabel>Last Vacuum</DetailLabel>
                          <DetailValue>{formatDate(item.last_vacuum)}</DetailValue>
                        </DetailCard>
                      )}
                      {item.total_columns != null && (
                        <DetailCard>
                          <DetailLabel>Total Columns</DetailLabel>
                          <DetailValue>{formatNumber(item.total_columns)}</DetailValue>
                        </DetailCard>
                      )}
                      {item.index_count != null && (
                        <DetailCard>
                          <DetailLabel>Index Count</DetailLabel>
                          <DetailValue>{formatNumber(item.index_count)}</DetailValue>
                        </DetailCard>
                      )}
                      {item.constraint_count != null && (
                        <DetailCard>
                          <DetailLabel>Constraint Count</DetailLabel>
                          <DetailValue>{formatNumber(item.constraint_count)}</DetailValue>
                        </DetailCard>
                      )}
                      {item.primary_key_columns && (
                        <DetailCard>
                          <DetailLabel>Primary Key Columns</DetailLabel>
                          <DetailValue>{item.primary_key_columns}</DetailValue>
                        </DetailCard>
                      )}
                      {item.first_discovered && (
                        <DetailCard>
                          <DetailLabel>First Discovered</DetailLabel>
                          <DetailValue>{formatDate(item.first_discovered)}</DetailValue>
                        </DetailCard>
                      )}
                      {item.last_accessed && (
                        <DetailCard>
                          <DetailLabel>Last Accessed</DetailLabel>
                          <DetailValue>{formatDate(item.last_accessed)}</DetailValue>
                        </DetailCard>
                      )}
                    </DetailsGrid>
                    
                    {/* Critical Status Explanation */}
                    {item.health_status === 'CRITICAL' && (
                      <div style={{ 
                        margin: '15px', 
                        padding: '15px', 
                        backgroundColor: '#ffebee', 
                        border: '1px solid #f44336', 
                        borderRadius: '4px' 
                      }}>
                        <div style={{ 
                          fontWeight: 'bold', 
                          color: '#c62828', 
                          marginBottom: '10px',
                          fontSize: '1.1em'
                        }}>
                          ■ CRITICAL STATUS REASONS:
                        </div>
                        <div style={{ color: '#333', lineHeight: '1.5' }}>
                          {item.data_quality_score < 50 && (
                            <div>• Low data quality score ({item.data_quality_score}%)</div>
                          )}
                          {item.null_percentage > 20 && (
                            <div>• High null percentage ({item.null_percentage}%)</div>
                          )}
                          {item.duplicate_percentage > 10 && (
                            <div>• High duplicate percentage ({item.duplicate_percentage}%)</div>
                          )}
                          {item.fragmentation_percentage > 30 && (
                            <div>• High fragmentation ({item.fragmentation_percentage}%)</div>
                          )}
                          {item.query_count_daily === 0 && (
                            <div>• No recent queries detected</div>
                          )}
                          {item.last_vacuum && (() => {
                            const lastVacuum = new Date(item.last_vacuum);
                            const daysSinceVacuum = Math.floor((Date.now() - lastVacuum.getTime()) / (1000 * 60 * 60 * 24));
                            return daysSinceVacuum > 7 && <div>• No vacuum in {daysSinceVacuum} days</div>;
                          })()}
                          {(!item.null_percentage || item.null_percentage <= 20) && 
                           (!item.duplicate_percentage || item.duplicate_percentage <= 10) && 
                           (!item.fragmentation_percentage || item.fragmentation_percentage <= 30) && 
                           item.data_quality_score >= 50 && (
                            <div>• Manual classification or business rule violation</div>
                          )}
                        </div>
                      </div>
                    )}
                  </GovernanceDetails>
                </GovernanceItem>
              ))
            )}
          </GovernanceList>

          <Pagination>
            <PageButton
              disabled={page === 1}
              onClick={() => setPage(page - 1)}
            >
              Previous
            </PageButton>
            
            {Array.from({ length: pagination.totalPages }, (_, i) => i + 1)
              .filter(p => Math.abs(p - page) <= 2 || p === 1 || p === pagination.totalPages)
              .map((p, i, arr) => (
                <React.Fragment key={p}>
                  {i > 0 && arr[i - 1] !== p - 1 && <span>...</span>}
                  <PageButton
                    active={p === page}
                    onClick={() => setPage(p)}
                  >
                    {p}
                  </PageButton>
                </React.Fragment>
              ))
            }
            
            <PageButton
              disabled={page === pagination.totalPages}
              onClick={() => setPage(page + 1)}
            >
              Next
            </PageButton>
          </Pagination>
        </>
      )}
    </GovernanceContainer>
  );
};

export default Governance;
