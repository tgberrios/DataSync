import { NavLink, Outlet, useNavigate } from 'react-router-dom';
import { useState } from 'react';
import styled from 'styled-components';
import { authApi, getCurrentUser } from '../services/api';

const LayoutContainer = styled.div`
  display: flex;
  min-height: 100vh;
`;

const Sidebar = styled.div`
  width: 250px;
  background: linear-gradient(180deg, #1a1a1a 0%, #1a1a1a 100%);
  color: white;
  padding: 20px 0;
  border-right: 1px solid #333;
  box-shadow: 2px 0 10px rgba(0, 0, 0, 0.1);
  overflow-y: auto;
  display: flex;
  flex-direction: column;
`;

const MainContent = styled.div`
  flex: 1;
  background-color: white;
  overflow-y: auto;
  animation: fadeIn 0.2s ease-in;
`;

const NavItem = styled(NavLink)`
  display: flex;
  align-items: center;
  padding: 15px 25px;
  color: #888;
  text-decoration: none;
  font-family: monospace;
  font-size: 1.1em;
  border-left: 3px solid transparent;
  transition: all 0.2s ease;
  position: relative;
  
  &:hover {
    background: linear-gradient(90deg, #252525 0%, rgba(10, 25, 41, 0.3) 100%);
    color: white;
    transform: translateX(3px);
    border-left-color: #1e3a5f;
  }
  
  &.active {
    background: linear-gradient(90deg, #252525 0%, rgba(10, 25, 41, 0.5) 100%);
    color: white;
    border-left-color: #0d1b2a;
    font-weight: bold;
    
    &::before {
      content: '';
      position: absolute;
      left: 0;
      top: 0;
      bottom: 0;
      width: 3px;
      background: linear-gradient(180deg, #0d1b2a 0%, #1e3a5f 50%, #2d4a6f 100%);
      box-shadow: 0 0 8px rgba(13, 27, 42, 0.6);
    }
  }
`;

const NavSubItem = styled(NavLink)`
  display: flex;
  align-items: center;
  padding: 12px 25px 12px 40px;
  color: #888;
  text-decoration: none;
  font-family: monospace;
  font-size: 0.95em;
  border-left: 3px solid transparent;
  transition: all 0.2s ease;
  position: relative;
  
  &:hover {
    background: linear-gradient(90deg, #252525 0%, rgba(10, 25, 41, 0.3) 100%);
    color: white;
    transform: translateX(3px);
    border-left-color: #1e3a5f;
  }
  
  &.active {
    background: linear-gradient(90deg, #252525 0%, rgba(10, 25, 41, 0.5) 100%);
    color: white;
    border-left-color: #0d1b2a;
    font-weight: bold;
    
    &::before {
      content: '';
      position: absolute;
      left: 0;
      top: 0;
      bottom: 0;
      width: 3px;
      background: linear-gradient(180deg, #0d1b2a 0%, #1e3a5f 50%, #2d4a6f 100%);
      box-shadow: 0 0 8px rgba(13, 27, 42, 0.6);
    }
  }
`;

const NavGroup = styled.div`
  margin-bottom: 5px;
`;

const NavGroupHeader = styled.div<{ $isOpen: boolean }>`
  display: flex;
  align-items: center;
  padding: 12px 25px;
  color: #aaa;
  font-family: monospace;
  font-size: 0.9em;
  font-weight: bold;
  cursor: pointer;
  user-select: none;
  transition: all 0.2s ease;
  text-transform: uppercase;
  letter-spacing: 1px;
  
  &:hover {
    background: linear-gradient(90deg, #252525 0%, rgba(10, 25, 41, 0.2) 100%);
    color: white;
  }
  
  &::before {
    content: '${props => props.$isOpen ? '▼' : '▶'}';
    margin-right: 10px;
    font-size: 0.8em;
    transition: transform 0.2s ease;
  }
`;

const NavGroupContent = styled.div<{ $isOpen: boolean }>`
  display: block;
  max-height: ${props => props.$isOpen ? '2000px' : '0'};
  overflow: hidden;
  transition: max-height 0.3s ease;
  visibility: ${props => props.$isOpen ? 'visible' : 'hidden'};
`;

const Logo = styled.div`
  padding: 20px 25px;
  font-size: 1.3em;
  color: white;
  font-weight: bold;
  border-bottom: 1px solid #333;
  margin-bottom: 20px;
  font-family: monospace;
  background: linear-gradient(90deg, transparent 0%, rgba(10, 25, 41, 0.2) 50%, transparent 100%);
  transition: all 0.2s ease;
  
  &:hover {
    background: linear-gradient(90deg, transparent 0%, rgba(10, 25, 41, 0.4) 50%, transparent 100%);
    transform: scale(1.02);
  }
`;

const UserInfo = styled.div`
  padding: 20px 25px;
  border-top: 1px solid #333;
  margin-top: auto;
`;

const UsernameDisplay = styled.div`
  font-size: 1.1em;
  font-weight: 600;
  color: #fff;
  margin-bottom: 8px;
  font-family: monospace;
  letter-spacing: 0.5px;
`;

const RoleDisplay = styled.div`
  font-size: 0.95em;
  font-weight: 500;
  color: #4a9eff;
  margin-bottom: 15px;
  text-transform: uppercase;
  letter-spacing: 1px;
  font-family: monospace;
`;

const LogoutButton = styled.button`
  width: 100%;
  padding: 10px 15px;
  margin-top: 10px;
  background: transparent;
  border: 1px solid #555;
  color: #aaa;
  cursor: pointer;
  border-radius: 4px;
  font-family: monospace;
  transition: all 0.2s ease;
  
  &:hover {
    background: rgba(255, 0, 0, 0.1);
    border-color: #ff4444;
    color: #ff6666;
  }
`;

const Layout = () => {
  const navigate = useNavigate();
  const currentUser = getCurrentUser();
  const [openGroups, setOpenGroups] = useState<{ [key: string]: boolean }>({
    catalog: false,
    lineage: false,
    governance: false,
    dataSources: false,
    quality: false,
    operations: false,
    system: false,
  });

  const toggleGroup = (group: string) => {
    setOpenGroups(prev => ({
      ...prev,
      [group]: !prev[group]
    }));
  };

  const handleLogout = async () => {
    await authApi.logout();
    navigate('/login');
  };

  return (
    <LayoutContainer>
      <Sidebar>
        <Logo>DataSync</Logo>
        
        <NavItem to="/" end>
          ■ Dashboard
        </NavItem>

        <NavItem to="/monitor">
          ■ Monitor
        </NavItem>

        <NavGroup>
          <NavGroupHeader 
            $isOpen={openGroups.catalog} 
            onClick={() => toggleGroup('catalog')}
          >
            Catalog
          </NavGroupHeader>
          <NavGroupContent $isOpen={openGroups.catalog}>
            <NavSubItem to="/catalog">
              ■ Catalog
            </NavSubItem>
            <NavSubItem to="/column-catalog">
              ■ Column Catalog
            </NavSubItem>
            <NavSubItem to="/catalog-locks">
              ■ Catalog Locks
            </NavSubItem>
          </NavGroupContent>
        </NavGroup>

        <NavGroup>
          <NavGroupHeader 
            $isOpen={openGroups.lineage} 
            onClick={() => toggleGroup('lineage')}
          >
            Lineage
          </NavGroupHeader>
          <NavGroupContent $isOpen={openGroups.lineage}>
            <NavSubItem to="/data-lineage-mariadb">
              ■ Lineage MariaDB
            </NavSubItem>
            <NavSubItem to="/data-lineage-mssql">
              ■ Lineage MSSQL
            </NavSubItem>
            <NavSubItem to="/data-lineage-mongodb">
              ■ Lineage MongoDB
            </NavSubItem>
            <NavSubItem to="/data-lineage-oracle">
              ■ Lineage Oracle
            </NavSubItem>
          </NavGroupContent>
        </NavGroup>

        <NavGroup>
          <NavGroupHeader 
            $isOpen={openGroups.governance} 
            onClick={() => toggleGroup('governance')}
          >
            Governance
          </NavGroupHeader>
          <NavGroupContent $isOpen={openGroups.governance}>
            <NavSubItem to="/governance-catalog-mariadb">
              ■ Gov Catalog MariaDB
            </NavSubItem>
            <NavSubItem to="/governance-catalog-mssql">
              ■ Gov Catalog MSSQL
            </NavSubItem>
            <NavSubItem to="/governance-catalog-mongodb">
              ■ Gov Catalog MongoDB
            </NavSubItem>
            <NavSubItem to="/governance-catalog-oracle">
              ■ Gov Catalog Oracle
            </NavSubItem>
            <NavSubItem to="/governance">
              ■ Governance
            </NavSubItem>
          </NavGroupContent>
        </NavGroup>

        <NavGroup>
          <NavGroupHeader 
            $isOpen={openGroups.quality} 
            onClick={() => toggleGroup('quality')}
          >
            Quality
          </NavGroupHeader>
          <NavGroupContent $isOpen={openGroups.quality}>
            <NavSubItem to="/quality">
              ■ Quality
            </NavSubItem>
          </NavGroupContent>
        </NavGroup>

        <NavGroup>
          <NavGroupHeader 
            $isOpen={openGroups.dataSources} 
            onClick={() => toggleGroup('dataSources')}
          >
            Data Sources
          </NavGroupHeader>
          <NavGroupContent $isOpen={openGroups.dataSources}>
            <NavSubItem to="/api-catalog">
              ■ API Catalog
            </NavSubItem>
            <NavSubItem to="/custom-jobs">
              ■ Custom Jobs
            </NavSubItem>
          </NavGroupContent>
        </NavGroup>

        <NavGroup>
          <NavGroupHeader 
            $isOpen={openGroups.operations} 
            onClick={() => toggleGroup('operations')}
          >
            Operations
          </NavGroupHeader>
          <NavGroupContent $isOpen={openGroups.operations}>
            <NavSubItem to="/maintenance">
              ■ Maintenance
            </NavSubItem>
            <NavSubItem to="/security">
              ■ Security
            </NavSubItem>
          </NavGroupContent>
        </NavGroup>

        <NavGroup>
          <NavGroupHeader 
            $isOpen={openGroups.system} 
            onClick={() => toggleGroup('system')}
          >
            System
          </NavGroupHeader>
          <NavGroupContent $isOpen={openGroups.system}>
            <NavSubItem to="/logs">
              ■ Logs
            </NavSubItem>
            <NavSubItem to="/config">
              ■ Config
            </NavSubItem>
            <NavSubItem to="/user-management">
              ■ User Management
            </NavSubItem>
          </NavGroupContent>
        </NavGroup>

        <UserInfo>
          {currentUser && (
            <>
              <UsernameDisplay>{currentUser.username}</UsernameDisplay>
              <RoleDisplay>{currentUser.role}</RoleDisplay>
              <LogoutButton onClick={handleLogout}>
                Logout
              </LogoutButton>
            </>
          )}
        </UserInfo>
      </Sidebar>
      <MainContent>
        <Outlet />
      </MainContent>
    </LayoutContainer>
  );
};

export default Layout;
