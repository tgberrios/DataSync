import styled, { keyframes } from 'styled-components';
import { theme } from '../theme/theme';
import DataSyncProduct from './products/DataSyncProduct';

const fadeIn = keyframes`
  from {
    opacity: 0;
    transform: translateY(20px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
`;

const LandingContainer = styled.div`
  min-height: 100vh;
  background: ${theme.colors.background.main};
  color: ${theme.colors.text.primary};
  font-family: ${theme.fonts.primary};
  overflow-x: hidden;
`;

const Nav = styled.nav`
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: ${theme.spacing.lg} ${theme.spacing.xxl};
  background: ${theme.colors.background.main};
  border-bottom: 1px solid ${theme.colors.border.light};
  position: sticky;
  top: 0;
  z-index: 100;
  backdrop-filter: blur(10px);
  background: rgba(255, 255, 255, 0.95);
  
  @media (max-width: ${theme.breakpoints.md}) {
    padding: ${theme.spacing.md} ${theme.spacing.lg};
  }
`;

const Logo = styled.div`
  font-size: 1.5em;
  font-weight: bold;
  color: ${theme.colors.primary.main};
  letter-spacing: 2px;
  
  &::before {
    content: '■';
    color: ${theme.colors.primary.main};
    margin-right: 8px;
  }
`;

const NavLinks = styled.div`
  display: flex;
  gap: ${theme.spacing.xl};
  
  @media (max-width: ${theme.breakpoints.md}) {
    display: none;
  }
`;

const NavLink = styled.a<{ $highlight?: boolean }>`
  color: ${props => props.$highlight ? theme.colors.text.primary : theme.colors.text.secondary};
  text-decoration: none;
  transition: all ${theme.transitions.normal};
  padding: ${theme.spacing.sm} ${theme.spacing.md};
  border-radius: ${theme.borderRadius.sm};
  font-weight: ${props => props.$highlight ? '600' : 'normal'};
  background: ${props => props.$highlight ? theme.colors.background.secondary : 'transparent'};
  border: ${props => props.$highlight ? `1px solid ${theme.colors.border.medium}` : '1px solid transparent'};
  
  &:hover {
    color: ${theme.colors.primary.main};
    background: ${props => props.$highlight ? theme.colors.background.secondary : 'transparent'};
    border-color: ${props => props.$highlight ? theme.colors.primary.main : 'transparent'};
  }
`;

const Hero = styled.section`
  padding: 80px ${theme.spacing.xxl};
  text-align: center;
  background: ${theme.colors.background.main};
  animation: ${fadeIn} 0.6s ease-out;
  
  @media (max-width: ${theme.breakpoints.md}) {
    padding: 60px ${theme.spacing.lg};
  }
`;

const HeroTitle = styled.h1`
  font-size: 3.5em;
  margin-bottom: ${theme.spacing.lg};
  color: ${theme.colors.text.primary};
  letter-spacing: 1px;
  line-height: 1.2;
  
  @media (max-width: ${theme.breakpoints.md}) {
    font-size: 2.5em;
  }
`;

const HeroSubtitle = styled.p`
  font-size: 1.25em;
  color: ${theme.colors.text.secondary};
  margin-bottom: ${theme.spacing.xxl};
  max-width: 800px;
  margin-left: auto;
  margin-right: auto;
  line-height: 1.6;
  
  @media (max-width: ${theme.breakpoints.md}) {
    font-size: 1.1em;
  }
`;

const ProductsSection = styled.section`
  padding: 60px ${theme.spacing.xxl};
  background: ${theme.colors.background.secondary};
  
  @media (max-width: ${theme.breakpoints.md}) {
    padding: 40px ${theme.spacing.lg};
  }
`;

const Footer = styled.footer`
  background: ${theme.colors.background.dark};
  color: ${theme.colors.text.white};
  padding: ${theme.spacing.xl} ${theme.spacing.xxl};
  text-align: center;
  border-top: 1px solid ${theme.colors.border.dark};
`;

const LandingPage = () => {
  return (
    <LandingContainer>
      <Nav>
        <Logo>IKS</Logo>
        <NavLinks>
          <NavLink href="#datasync" $highlight>DataSync</NavLink>
          <NavLink href="#contact">Contact</NavLink>
        </NavLinks>
      </Nav>

      <Hero>
        <HeroTitle>Enterprise Solutions</HeroTitle>
        <HeroSubtitle>
          Platforms and tools designed to transform the way you manage and synchronize your data
        </HeroSubtitle>
      </Hero>

      <ProductsSection id="datasync">
        <DataSyncProduct />
      </ProductsSection>

      <Footer>
        <p>&copy; 2024 IKS. All rights reserved.</p>
        <p style={{ marginTop: theme.spacing.sm, fontSize: '0.9em', opacity: 0.8 }}>
          Contact: <a href="mailto:tgbberrios@gmail.com" style={{ color: theme.colors.text.white }}>tgbberrios@gmail.com</a>
        </p>
      </Footer>
    </LandingContainer>
  );
};

export default LandingPage;
