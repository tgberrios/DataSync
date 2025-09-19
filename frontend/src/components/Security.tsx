import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { securityApi } from '../services/api';

const SecurityContainer = styled.div`
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

const Section = styled.div`
  margin-bottom: 25px;
  padding: 20px;
  border: 1px solid #eee;
  border-radius: 4px;
  background-color: #fafafa;
`;

const SectionTitle = styled.h3`
  margin-bottom: 15px;
  font-size: 1.2em;
  color: #222;
  border-bottom: 2px solid #333;
  padding-bottom: 8px;
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
  border-radius: 4px;
  border: 1px solid #ddd;
  text-align: center;
  min-height: 50px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: 500;
`;

const Table = styled.table`
  width: 100%;
  border-collapse: collapse;
  margin-top: 20px;
  background: white;
`;

const Th = styled.th`
  padding: 15px 12px;
  text-align: left;
  border-bottom: 2px solid #333;
  background: #f5f5f5;
  white-space: nowrap;
  font-weight: 600;
  font-size: 0.95em;
`;

const Td = styled.td`
  padding: 12px;
  border-bottom: 1px solid #eee;
  vertical-align: middle;
`;

const Badge = styled.span<{ type: string }>`
  padding: 4px 8px;
  border-radius: 4px;
  font-size: 0.9em;
  font-weight: 500;
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
`;

const formatDate = (date: string) => {
  if (!date) return '-';
  return new Date(date).toLocaleString();
};

const formatNumber = (num: number) => {
  return num?.toLocaleString() || '0';
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
                {activeUsers.map((user, index) => (
                  <tr key={index}>
                    <Td>{user.username}</Td>
                    <Td>
                      <Badge type={user.role_type}>
                        {user.role_type}
                      </Badge>
                    </Td>
                    <Td>
                      <Badge type={user.status}>
                        {user.status}
                      </Badge>
                    </Td>
                    <Td>{formatDate(user.last_activity)}</Td>
                    <Td>{user.client_addr || '-'}</Td>
                    <Td>{user.application_name || '-'}</Td>
                  </tr>
                ))}
              </tbody>
            </Table>
          </Section>

        </>
      )}
    </SecurityContainer>
  );
};

export default Security;
