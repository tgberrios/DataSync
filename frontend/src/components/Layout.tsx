import { NavLink, Outlet } from 'react-router-dom';
import styled from 'styled-components';

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

const Layout = () => {
  return (
    <LayoutContainer>
      <Sidebar>
        <Logo>DataSync</Logo>
        <NavItem to="/" end>
          ■ Dashboard
        </NavItem>
        <NavItem to="/catalog">
          ■ Catalog
        </NavItem>
        <NavItem to="/column-catalog">
          ■ Column Catalog
        </NavItem>
        <NavItem to="/catalog-locks">
          ■ Catalog Locks
        </NavItem>
        <NavItem to="/data-lineage-mariadb">
          ■ Lineage MariaDB
        </NavItem>
        <NavItem to="/data-lineage-mssql">
          ■ Lineage MSSQL
        </NavItem>
        <NavItem to="/data-lineage-mongodb">
          ■ Lineage MongoDB
        </NavItem>
        <NavItem to="/governance-catalog-mariadb">
          ■ Gov Catalog MariaDB
        </NavItem>
        <NavItem to="/governance-catalog-mssql">
          ■ Gov Catalog MSSQL
        </NavItem>
        <NavItem to="/governance-catalog-mongodb">
          ■ Gov Catalog MongoDB
        </NavItem>
        <NavItem to="/monitor">
          ■ Monitor
        </NavItem>
        <NavItem to="/query-performance">
          ■ Query Performance
        </NavItem>
        <NavItem to="/maintenance">
          ■ Maintenance
        </NavItem>
        <NavItem to="/live-changes">
          ■ Live Changes
        </NavItem>
        <NavItem to="/quality">
          ■ Quality
        </NavItem>
        <NavItem to="/governance">
          ■ Governance
        </NavItem>
        <NavItem to="/security">
          ■ Security
        </NavItem>
        <NavItem to="/logs">
          ■ Logs
        </NavItem>
        <NavItem to="/config">
          ■ Config
        </NavItem>
      </Sidebar>
      <MainContent>
        <Outlet />
      </MainContent>
    </LayoutContainer>
  );
};

export default Layout;
