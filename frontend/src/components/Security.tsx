import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { securityApi } from '../services/api';

const SecurityContainer = styled.div`
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

const Section = styled.div`
  margin-bottom: 25px;
  padding: 20px;
  border: 1px solid #eee;
  border-radius: 6px;
  background-color: #fafafa;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.03);
  transition: all 0.2s ease;
  animation: slideUp 0.25s ease-out;
  animation-fill-mode: both;
  
  &:hover {
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
    transform: translateY(-2px);
    border-color: rgba(10, 25, 41, 0.2);
  }
  
  &:nth-child(1) { animation-delay: 0.1s; }
  &:nth-child(2) { animation-delay: 0.2s; }
  &:nth-child(3) { animation-delay: 0.15s; }
  &:nth-child(4) { animation-delay: 0.2s; }
`;

const SectionTitle = styled.h3`
  margin-bottom: 15px;
  font-size: 1.2em;
  color: #222;
  border-bottom: 2px solid #333;
  padding-bottom: 8px;
  position: relative;
  
  &::after {
    content: '';
    position: absolute;
    bottom: -2px;
    left: 0;
    width: 60px;
    height: 2px;
    background: linear-gradient(90deg, #0d1b2a, #1e3a5f);
    transition: width 0.3s ease;
  }
  
  &:hover::after {
    width: 100%;
  }
`;

const Grid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 15px;
  margin-top: 15px;
  align-items: stretch;
`;

const Value = styled.div`
  font-size: 1.1em;
  padding: 12px;
  background-color: #fff;
  border-radius: 6px;
  border: 1px solid #ddd;
  text-align: center;
  min-height: 50px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: 500;
  transition: all 0.2s ease;
  
  &:hover {
    transform: translateY(-3px);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
    border-color: rgba(10, 25, 41, 0.3);
    background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
  }
`;

const Table = styled.table`
  width: 100%;
  border-collapse: collapse;
  margin-top: 20px;
  background: white;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  border-radius: 6px;
  overflow: hidden;
`;

const Th = styled.th`
  padding: 15px 12px;
  text-align: left;
  border-bottom: 2px solid #333;
  background: linear-gradient(180deg, #f5f5f5 0%, #fafafa 100%);
  white-space: nowrap;
  font-weight: 600;
  font-size: 0.95em;
  position: sticky;
  top: 0;
  z-index: 10;
`;

const Td = styled.td`
  padding: 12px;
  border-bottom: 1px solid #eee;
  vertical-align: middle;
  transition: all 0.2s ease;
`;

const Badge = styled.span<{ type: string }>`
  padding: 4px 10px;
  border-radius: 6px;
  font-size: 0.9em;
  font-weight: 500;
  display: inline-block;
  transition: all 0.2s ease;
  background-color: ${props => {
    switch (props.type) {
      case 'SUPERUSER': return '#ffebee';
      case 'CREATEDB': return '#e3f2fd';
      case 'CREATEROLE': return '#f3e5f5';
      case 'LOGIN': return '#e8f5e9';
      case 'ACTIVE': return '#e8f5e9';
      case 'INACTIVE': return '#ffebee';
      case 'SELECT': return '#e8f5e9';
      case 'INSERT': return '#e3f2fd';
      case 'UPDATE': return '#fff3e0';
      case 'DELETE': return '#ffebee';
      case 'ALL': return '#f1f8e9';
      default: return '#f5f5f5';
    }
  }};
  color: ${props => {
    switch (props.type) {
      case 'SUPERUSER': return '#c62828';
      case 'CREATEDB': return '#1565c0';
      case 'CREATEROLE': return '#6a1b9a';
      case 'LOGIN': return '#2e7d32';
      case 'ACTIVE': return '#2e7d32';
      case 'INACTIVE': return '#c62828';
      case 'SELECT': return '#2e7d32';
      case 'INSERT': return '#1565c0';
      case 'UPDATE': return '#ef6c00';
      case 'DELETE': return '#c62828';
      case 'ALL': return '#558b2f';
      default: return '#757575';
    }
  }};
  
  &:hover {
    transform: scale(1.05);
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
  }
`;

const ExpandableRow = styled.tr<{ expanded: boolean }>`
  cursor: pointer;
  background-color: ${props => props.expanded ? '#f8f9fa' : 'white'};
  transition: all 0.2s ease;
  
  &:hover {
    background: linear-gradient(90deg, #f0f0f0 0%, #f8f9fa 100%);
    transform: scale(1.001);
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
    
    ${Td} {
      border-bottom-color: rgba(10, 25, 41, 0.1);
    }
  }
`;

const ExpandableTd = styled.td`
  padding: 12px;
  border-bottom: 1px solid #eee;
  vertical-align: middle;
  position: relative;
  transition: all 0.2s ease;
`;

const ExpandIcon = styled.span<{ expanded: boolean }>`
  margin-left: 8px;
  font-size: 0.8em;
  color: #666;
  transition: transform 0.3s ease;
  transform: ${props => props.expanded ? 'rotate(90deg)' : 'rotate(0deg)'};
`;

const ConnectionCount = styled.span`
  background: linear-gradient(135deg, #0d1b2a 0%, #1e3a5f 100%);
  color: white;
  padding: 3px 10px;
  border-radius: 12px;
  font-size: 0.8em;
  font-weight: 500;
  margin-left: 8px;
  transition: all 0.2s ease;
  
  &:hover {
    transform: scale(1.1);
    box-shadow: 0 2px 6px rgba(13, 27, 42, 0.3);
  }
`;

const StatusSummary = styled.div`
  display: flex;
  gap: 4px;
  flex-wrap: wrap;
  margin-top: 4px;
`;

const ExpandedRow = styled.tr`
  background-color: #fafafa;
  animation: slideUp 0.2s ease-out;
`;

const ExpandedTd = styled.td`
  padding: 0;
  border-bottom: 1px solid #eee;
  background-color: #fafafa;
`;

const ExpandedContent = styled.div`
  padding: 15px 20px;
  border-left: 3px solid #0d1b2a;
  margin-left: 20px;
  animation: fadeIn 0.2s ease-in;
`;

const DetailRow = styled.div`
  display: grid;
  grid-template-columns: 1fr 1fr 1fr 1fr;
  gap: 15px;
  padding: 8px 0;
  border-bottom: 1px solid #f0f0f0;
  font-size: 0.9em;
  transition: all 0.2s ease;
  
  &:hover {
    background-color: #f8f9fa;
    padding-left: 5px;
  }
  
  &:last-child {
    border-bottom: none;
  }
`;

const DetailLabel = styled.div`
  font-weight: 500;
  color: #666;
`;

const DetailValue = styled.div`
  color: #333;
`;

const formatDate = (date: string) => {
  if (!date) return '-';
  return new Date(date).toLocaleString();
};

const formatNumber = (num: number) => {
  return num?.toLocaleString() || '0';
};

const groupUsersByUsername = (users: any[]) => {
  const grouped = users.reduce((acc, user) => {
    const username = user.username;
    if (!acc[username]) {
      acc[username] = {
        username,
        role_type: user.role_type,
        connections: [],
        mostRecentActivity: user.last_activity,
        statusCounts: {},
        expanded: false
      };
    }
    
    acc[username].connections.push({
      status: user.status,
      last_activity: user.last_activity,
      client_addr: user.client_addr,
      application_name: user.application_name
    });
    
    // Update most recent activity
    if (user.last_activity && (!acc[username].mostRecentActivity || 
        new Date(user.last_activity) > new Date(acc[username].mostRecentActivity))) {
      acc[username].mostRecentActivity = user.last_activity;
    }
    
    // Count statuses
    acc[username].statusCounts[user.status] = (acc[username].statusCounts[user.status] || 0) + 1;
    
    return acc;
  }, {});
  
  return Object.values(grouped);
};

const Security = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [securityData, setSecurityData] = useState<any>({
    users: {
      total: 0,
      active: 0,
      superusers: 0,
      withLogin: 0
    },
    connections: {
      current: 0,
      max: 0,
      idle: 0,
      active: 0
    },
    permissions: {
      totalGrants: 0,
      schemasWithAccess: 0,
      tablesWithAccess: 0
    }
  });
  const [activeUsers, setActiveUsers] = useState<any[]>([]);
  const [expandedUsers, setExpandedUsers] = useState<Set<string>>(new Set());

  const toggleUserExpansion = (username: string) => {
    setExpandedUsers(prev => {
      const newSet = new Set(prev);
      if (newSet.has(username)) {
        newSet.delete(username);
      } else {
        newSet.add(username);
      }
      return newSet;
    });
  };

  useEffect(() => {
    const fetchSecurityData = async () => {
      try {
        setLoading(true);
        setError(null);
        const response = await securityApi.getSecurityData();
        setSecurityData(response.summary);
        setActiveUsers(response.activeUsers || []);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading security data');
      } finally {
        setLoading(false);
      }
    };

    fetchSecurityData();
    const interval = setInterval(fetchSecurityData, 30000);
    return () => clearInterval(interval);
  }, []);

  return (
    <SecurityContainer>
      <Header>
        Security & Compliance Monitor
      </Header>

      {loading && (
        <div style={{ textAlign: 'center', padding: '20px' }}>
          Loading security data...
        </div>
      )}

      {error && (
        <div style={{ 
          color: 'red', 
          padding: '20px', 
          textAlign: 'center',
          border: '1px solid red',
          borderRadius: '4px',
          margin: '20px',
          backgroundColor: '#fff5f5'
        }}>
          {error}
        </div>
      )}

      {!loading && !error && (
        <>
          <Section>
            <SectionTitle>■ USER MANAGEMENT</SectionTitle>
            <Grid>
              <Value>Total Users: {securityData.users.total}</Value>
              <Value>Active Users: {securityData.users.active}</Value>
              <Value>Superusers: {securityData.users.superusers}</Value>
              <Value>With Login: {securityData.users.withLogin}</Value>
            </Grid>
          </Section>

          <Section>
            <SectionTitle>● CONNECTION STATUS</SectionTitle>
            <Grid>
              <Value>Current: {securityData.connections.current}</Value>
              <Value>Max Allowed: {securityData.connections.max}</Value>
              <Value>Idle: {securityData.connections.idle}</Value>
              <Value>Active: {securityData.connections.active}</Value>
            </Grid>
          </Section>

          <Section>
            <SectionTitle>■ PERMISSIONS OVERVIEW</SectionTitle>
            <Grid>
              <Value>Total Grants: {securityData.permissions.totalGrants}</Value>
              <Value>Schemas: {securityData.permissions.schemasWithAccess}</Value>
              <Value>Tables: {securityData.permissions.tablesWithAccess}</Value>
            </Grid>
          </Section>

          <Section>
            <SectionTitle>● ACTIVE USERS</SectionTitle>
            <Table>
              <thead>
                <tr>
                  <Th>Username</Th>
                  <Th>Role</Th>
                  <Th>Status</Th>
                  <Th>Last Activity</Th>
                  <Th>Client IP</Th>
                  <Th>Application</Th>
                </tr>
              </thead>
              <tbody>
                {groupUsersByUsername(activeUsers).map((user: any, index) => {
                  const isExpanded = expandedUsers.has(user.username);
                  return (
                    <React.Fragment key={index}>
                      <ExpandableRow 
                        expanded={isExpanded}
                        onClick={() => toggleUserExpansion(user.username)}
                      >
                        <ExpandableTd>
                          {user.username}
                          <ConnectionCount>
                            {user.connections.length}
                          </ConnectionCount>
                        </ExpandableTd>
                        <ExpandableTd>
                          <Badge type={user.role_type}>
                            {user.role_type}
                          </Badge>
                        </ExpandableTd>
                        <ExpandableTd>
                          <StatusSummary>
                            {Object.entries(user.statusCounts).map(([status, count]) => (
                              <Badge key={status} type={status}>
                                {status}: {count}
                              </Badge>
                            ))}
                          </StatusSummary>
                        </ExpandableTd>
                        <ExpandableTd>{formatDate(user.mostRecentActivity)}</ExpandableTd>
                        <ExpandableTd>
                          {user.connections.length > 0 ? user.connections[0].client_addr || '-' : '-'}
                        </ExpandableTd>
                        <ExpandableTd>
                          {user.connections.length > 0 ? user.connections[0].application_name || '-' : '-'}
                        </ExpandableTd>
                      </ExpandableRow>
                      {isExpanded && (
                        <ExpandedRow>
                          <ExpandedTd colSpan={6}>
                            <ExpandedContent>
                              <div style={{ marginBottom: '15px', fontWeight: '500', color: '#666' }}>
                                All Connections for {user.username}
                              </div>
                              {user.connections.map((conn: any, connIndex: number) => (
                                <DetailRow key={connIndex}>
                                  <DetailLabel>Status:</DetailLabel>
                                  <DetailValue>
                                    <Badge type={conn.status}>{conn.status}</Badge>
                                  </DetailValue>
                                  <DetailLabel>IP:</DetailLabel>
                                  <DetailValue>{conn.client_addr || '-'}</DetailValue>
                                  <DetailLabel>App:</DetailLabel>
                                  <DetailValue>{conn.application_name || '-'}</DetailValue>
                                  <DetailLabel>Activity:</DetailLabel>
                                  <DetailValue>{formatDate(conn.last_activity)}</DetailValue>
                                </DetailRow>
                              ))}
                            </ExpandedContent>
                          </ExpandedTd>
                        </ExpandedRow>
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </Table>
          </Section>

        </>
      )}
    </SecurityContainer>
  );
};

export default Security;
