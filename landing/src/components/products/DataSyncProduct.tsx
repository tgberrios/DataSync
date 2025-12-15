import { useState, useEffect } from 'react';
import styled, { keyframes } from 'styled-components';
import { theme } from '../../theme/theme';

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

const slideUp = keyframes`
  from {
    opacity: 0;
    transform: translateY(30px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
`;

const pulse = keyframes`
  0%, 100% {
    transform: scale(1);
  }
  50% {
    transform: scale(1.05);
  }
`;

const screenshots = [
  {
    src: '/screenshots/Screenshot 2025-12-15 at 10-20-08 DataSync Monitor.png',
    alt: 'DataSync Real-Time Dashboard',
    caption: 'Real-Time Dashboard: synchronization status, system resources, and database health.'
  },
  {
    src: '/screenshots/Screenshot 2025-12-15 at 10-20-25 DataSync Monitor.png',
    alt: 'DataLake Catalog Manager',
    caption: 'DataLake Catalog Manager: multi-engine table catalog with sync strategies and status.'
  },
  {
    src: '/screenshots/Screenshot 2025-12-15 at 10-22-12 DataSync Monitor.png',
    alt: 'Data Governance Catalog',
    caption: 'Data Governance Catalog: health, sensitivity, and quality metrics per table.'
  },
  {
    src: '/screenshots/Screenshot 2025-12-15 at 10-21-58 DataSync Monitor.png',
    alt: 'API Catalog',
    caption: 'API Catalog: unified management of REST integrations and sync status.'
  },
  {
    src: '/screenshots/Screenshot 2025-12-15 at 10-21-36 DataSync Monitor.png',
    alt: 'Custom Jobs',
    caption: 'Custom Jobs: Python and SQL jobs orchestrated across your data platform.'
  },
  {
    src: '/screenshots/Screenshot 2025-12-15 at 10-21-43 DataSync Monitor.png',
    alt: 'System Logs',
    caption: 'Enterprise Logging: detailed monitoring of parallel processing and transfers.'
  },
  {
    src: '/screenshots/Screenshot 2025-12-15 at 10-22-05 DataSync Monitor.png',
    alt: 'System Configuration',
    caption: 'Configuration: tuning chunk size, workers, and sync intervals for your environment.'
  }
];


const ProductContainer = styled.div`
  animation: ${fadeIn} 0.4s ease-out;
`;

const ProductHero = styled.div`
  text-align: center;
  margin-bottom: ${theme.spacing.xxl};
`;

const ProductTitle = styled.h3`
  font-size: 2em;
  margin-bottom: ${theme.spacing.md};
  color: ${theme.colors.text.primary};
  letter-spacing: 1px;
  animation: ${slideUp} 0.6s ease-out;
  
  @media (max-width: ${theme.breakpoints.md}) {
    font-size: 1.75em;
  }
`;

const ProductDescription = styled.p`
  font-size: 1.1em;
  color: ${theme.colors.text.secondary};
  max-width: 800px;
  margin: 0 auto ${theme.spacing.xl};
  line-height: 1.6;
  animation: ${slideUp} 0.6s ease-out 0.2s both;
`;

const SectionTitle = styled.h2`
  font-size: 2.5em;
  text-align: center;
  margin-bottom: ${theme.spacing.lg};
  color: ${theme.colors.text.primary};
  letter-spacing: 1px;
  
  @media (max-width: ${theme.breakpoints.md}) {
    font-size: 2em;
  }
`;

const SectionSubtitle = styled.p`
  text-align: center;
  color: ${theme.colors.text.secondary};
  font-size: 1.1em;
  margin-bottom: ${theme.spacing.xxl};
  max-width: 600px;
  margin-left: auto;
  margin-right: auto;
`;

const FeaturesGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: ${theme.spacing.xl};
  max-width: 1200px;
  margin: 0 auto ${theme.spacing.xxl};
  
  @media (max-width: ${theme.breakpoints.md}) {
    grid-template-columns: 1fr;
  }
`;

const FeatureCard = styled.div<{ $index?: number }>`
  background: ${theme.colors.background.main};
  padding: ${theme.spacing.xl};
  border-radius: ${theme.borderRadius.lg};
  border: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  animation: ${slideUp} 0.5s ease-out ${props => (props.$index || 0) * 0.1}s both;
  
  &:hover {
    transform: translateY(-8px) scale(1.02);
    box-shadow: ${theme.shadows.xl};
    border-color: ${theme.colors.primary.main};
    background: ${theme.colors.background.secondary};
  }
`;

const FeatureIcon = styled.div`
  font-size: 2.5em;
  margin-bottom: ${theme.spacing.md};
  font-family: monospace;
  font-weight: bold;
  color: ${theme.colors.primary.main};
  transition: all ${theme.transitions.normal};
  display: inline-block;
  
  ${FeatureCard}:hover & {
    animation: ${pulse} 1s ease-in-out infinite;
    transform: scale(1.1);
  }
`;

const FeatureTitle = styled.h4`
  font-size: 1.5em;
  margin-bottom: ${theme.spacing.md};
  color: ${theme.colors.primary.main};
`;

const FeatureDescription = styled.p`
  color: ${theme.colors.text.secondary};
  line-height: 1.6;
`;

const DemoSection = styled.div`
  text-align: center;
  margin-bottom: ${theme.spacing.xxl};
`;

const DemoContainer = styled.div`
  max-width: 1200px;
  margin: 0 auto;
`;

const TechnicalSection = styled.div`
  background: ${theme.colors.background.main};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  margin-bottom: ${theme.spacing.xxl};
`;

const TechGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
  gap: ${theme.spacing.xl};
  margin-bottom: ${theme.spacing.xxl};
  
  @media (max-width: ${theme.breakpoints.md}) {
    grid-template-columns: 1fr;
  }
`;

const TechCard = styled.div`
  background: ${theme.colors.background.secondary};
  padding: ${theme.spacing.xl};
  border-radius: ${theme.borderRadius.lg};
  border: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  
  &:hover {
    transform: translateY(-5px);
    box-shadow: ${theme.shadows.lg};
    border-color: ${theme.colors.primary.main};
  }
`;

const TechIcon = styled.div`
  font-size: 2.5em;
  margin-bottom: ${theme.spacing.md};
  font-family: monospace;
  font-weight: bold;
  color: ${theme.colors.primary.main};
`;

const TechTitle = styled.h4`
  font-size: 1.3em;
  margin-bottom: ${theme.spacing.md};
  color: ${theme.colors.primary.main};
`;

const TechDescription = styled.p`
  color: ${theme.colors.text.secondary};
  line-height: 1.6;
  font-size: 0.95em;
`;

const PerformanceSection = styled.div`
  background: ${theme.colors.background.secondary};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  margin-bottom: ${theme.spacing.xxl};
`;

const PerformanceGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: ${theme.spacing.xl};
  
  @media (max-width: ${theme.breakpoints.md}) {
    grid-template-columns: 1fr;
  }
`;

const PerformanceItem = styled.div`
  text-align: center;
  padding: ${theme.spacing.lg};
  background: ${theme.colors.background.main};
  border-radius: ${theme.borderRadius.md};
  border: 1px solid ${theme.colors.border.light};
`;

const PerformanceLabel = styled.div`
  font-size: 0.9em;
  color: ${theme.colors.text.secondary};
  margin-bottom: ${theme.spacing.sm};
  text-transform: uppercase;
  letter-spacing: 1px;
`;

const PerformanceValue = styled.div`
  font-size: 1.5em;
  font-weight: bold;
  color: ${theme.colors.primary.main};
  margin-bottom: ${theme.spacing.xs};
`;

const PerformanceDesc = styled.div`
  font-size: 0.85em;
  color: ${theme.colors.text.secondary};
`;

const StackSection = styled.div`
  background: ${theme.colors.background.secondary};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
`;

const StackGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: ${theme.spacing.lg};
  
  @media (max-width: ${theme.breakpoints.md}) {
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  }
`;

const StackItem = styled.div`
  text-align: center;
  padding: ${theme.spacing.md};
  background: ${theme.colors.background.main};
  border-radius: ${theme.borderRadius.md};
  border: 1px solid ${theme.colors.border.light};
`;

const StackLabel = styled.div`
  font-size: 0.85em;
  color: ${theme.colors.text.secondary};
  margin-bottom: ${theme.spacing.xs};
  text-transform: uppercase;
  letter-spacing: 0.5px;
`;

const StackValue = styled.div`
  font-size: 1.1em;
  font-weight: 600;
  color: ${theme.colors.text.primary};
  font-family: monospace;
`;

const AdvancedFeaturesSection = styled.div`
  background: ${theme.colors.background.secondary};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  margin-bottom: ${theme.spacing.xxl};
`;

const ImplementationDetailsSection = styled.div`
  background: ${theme.colors.background.main};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  margin-bottom: ${theme.spacing.xxl};
`;

const ImplementationGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
  gap: ${theme.spacing.xl};
  
  @media (max-width: ${theme.breakpoints.md}) {
    grid-template-columns: 1fr;
  }
`;

const ImplCard = styled.div`
  background: ${theme.colors.background.secondary};
  padding: ${theme.spacing.xl};
  border-radius: ${theme.borderRadius.lg};
  border: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  
  &:hover {
    transform: translateY(-5px);
    box-shadow: ${theme.shadows.lg};
    border-color: ${theme.colors.primary.main};
  }
`;

const ImplTitle = styled.h4`
  font-size: 1.3em;
  margin-bottom: ${theme.spacing.md};
  color: ${theme.colors.primary.main};
  border-bottom: 2px solid ${theme.colors.border.medium};
  padding-bottom: ${theme.spacing.sm};
`;

const ImplList = styled.ul`
  list-style: none;
  padding: 0;
  margin: 0;
  
  li {
    padding: ${theme.spacing.sm} 0;
    padding-left: ${theme.spacing.md};
    position: relative;
    color: ${theme.colors.text.secondary};
    line-height: 1.6;
    font-size: 0.95em;
    
    &::before {
      content: '→';
      position: absolute;
      left: 0;
      color: ${theme.colors.primary.main};
      font-weight: bold;
    }
  }
`;

const BusinessSection = styled.div`
  background: linear-gradient(135deg, ${theme.colors.background.main} 0%, ${theme.colors.background.secondary} 100%);
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  margin-bottom: ${theme.spacing.xxl};
  border: 2px solid ${theme.colors.border.light};
`;

const BusinessGrid = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
  gap: ${theme.spacing.xl};
  margin-bottom: ${theme.spacing.xxl};
  
  @media (max-width: ${theme.breakpoints.md}) {
    grid-template-columns: 1fr;
  }
`;

const BusinessCard = styled.div`
  background: ${theme.colors.background.main};
  padding: ${theme.spacing.xl};
  border-radius: ${theme.borderRadius.lg};
  border: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  text-align: center;
  
  &:hover {
    transform: translateY(-8px);
    box-shadow: ${theme.shadows.xl};
    border-color: ${theme.colors.primary.main};
  }
`;

const BusinessIcon = styled.div`
  font-size: 3em;
  margin-bottom: ${theme.spacing.md};
  font-family: monospace;
  font-weight: bold;
  color: ${theme.colors.primary.main};
`;

const BusinessTitle = styled.h4`
  font-size: 1.4em;
  margin-bottom: ${theme.spacing.md};
  color: ${theme.colors.primary.main};
`;

const BusinessDescription = styled.p`
  color: ${theme.colors.text.secondary};
  line-height: 1.7;
  font-size: 1em;
`;

const SimpleBenefitsSection = styled.div`
  background: ${theme.colors.background.main};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  border: 1px solid ${theme.colors.border.light};
  max-width: 800px;
  margin: 0 auto;
`;

const SimpleBenefitsTitle = styled.h3`
  font-size: 1.75em;
  text-align: center;
  margin-bottom: ${theme.spacing.xl};
  color: ${theme.colors.text.primary};
`;

const SimpleBenefitsList = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: ${theme.spacing.md};
  
  @media (max-width: ${theme.breakpoints.md}) {
    grid-template-columns: 1fr;
  }
`;

const BenefitItem = styled.div`
  display: flex;
  align-items: center;
  padding: ${theme.spacing.md};
  background: ${theme.colors.background.secondary};
  border-radius: ${theme.borderRadius.md};
  border: 1px solid ${theme.colors.border.light};
  transition: all ${theme.transitions.normal};
  
  &:hover {
    border-color: ${theme.colors.primary.main};
    transform: translateX(5px);
  }
`;

const BenefitCheckmark = styled.div`
  font-size: 1.5em;
  color: ${theme.colors.primary.main};
  margin-right: ${theme.spacing.md};
  font-weight: bold;
  min-width: 30px;
`;

const BenefitText = styled.div`
  color: ${theme.colors.text.primary};
  font-size: 1em;
  line-height: 1.5;
`;

const TechDivider = styled.div`
  background: ${theme.colors.primary.main};
  color: ${theme.colors.text.white};
  padding: ${theme.spacing.xxl};
  text-align: center;
  margin: ${theme.spacing.xxl} 0;
  border-radius: ${theme.borderRadius.lg};
  position: relative;
  overflow: hidden;
  
  &::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: linear-gradient(45deg, rgba(255,255,255,0.1) 0%, transparent 100%);
  }
`;

const TechDividerText = styled.h2`
  font-size: 2.5em;
  margin-bottom: ${theme.spacing.sm};
  position: relative;
  z-index: 1;
  
  @media (max-width: ${theme.breakpoints.md}) {
    font-size: 2em;
  }
`;

const TechDividerSubtext = styled.p`
  font-size: 1.2em;
  opacity: 0.9;
  position: relative;
  z-index: 1;
`;

const PricingContactContainer = styled.div`
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: ${theme.spacing.xxl};
  max-width: 1200px;
  margin: 0 auto ${theme.spacing.xxl};
  
  @media (max-width: ${theme.breakpoints.lg}) {
    grid-template-columns: 1fr;
  }
`;

const PricingSection = styled.div`
  background: ${theme.colors.background.main};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  animation: ${slideUp} 0.6s ease-out 0.3s both;
`;

const PricingCard = styled.div<{ $featured?: boolean }>`
  background: ${theme.colors.background.secondary};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  border: 2px solid ${props => props.$featured ? theme.colors.primary.main : theme.colors.border.light};
  text-align: center;
  transition: all ${theme.transitions.normal};
  position: relative;
  width: 100%;
  
  ${props => props.$featured && `
    box-shadow: ${theme.shadows.xl};
  `}
  
  &:hover {
    transform: translateY(-8px) scale(1.02);
    box-shadow: 0 8px 24px rgba(13, 27, 42, 0.2);
    border-color: ${theme.colors.primary.light};
  }
`;

const FeaturedBadge = styled.div`
  position: absolute;
  top: -15px;
  left: 50%;
  transform: translateX(-50%);
  background: ${theme.colors.primary.main};
  color: ${theme.colors.text.white};
  padding: ${theme.spacing.xs} ${theme.spacing.md};
  border-radius: ${theme.borderRadius.md};
  font-size: 0.9em;
  font-weight: bold;
`;

const Price = styled.div`
  font-size: 3em;
  font-weight: bold;
  color: ${theme.colors.primary.main};
  margin: ${theme.spacing.lg} 0;
  transition: all ${theme.transitions.normal};
  
  ${PricingCard}:hover & {
    animation: ${pulse} 1s ease-in-out;
  }
  
  span {
    font-size: 0.4em;
    color: ${theme.colors.text.secondary};
  }
`;

const PricingFeatures = styled.ul`
  list-style: none;
  padding: 0;
  margin: ${theme.spacing.xl} 0;
  text-align: left;
  
  li {
    padding: ${theme.spacing.sm} 0;
    color: ${theme.colors.text.secondary};
    
    &::before {
      content: '✓';
      color: ${theme.colors.status.success.text};
      margin-right: ${theme.spacing.sm};
      font-weight: bold;
    }
  }
`;

const CTAButton = styled.a`
  display: inline-block;
  padding: ${theme.spacing.md} ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.md};
  font-size: 1.1em;
  font-weight: 500;
  text-decoration: none;
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  background: ${theme.colors.primary.main};
  color: ${theme.colors.text.white};
  width: 100%;
  text-align: center;
  margin-top: ${theme.spacing.lg};
  position: relative;
  overflow: hidden;
  
  &::before {
    content: '';
    position: absolute;
    top: 0;
    left: -100%;
    width: 100%;
    height: 100%;
    background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.2), transparent);
    transition: left 0.5s;
  }
  
  &:hover {
    background: ${theme.colors.primary.light};
    transform: translateY(-3px) scale(1.02);
    box-shadow: 0 6px 20px rgba(13, 27, 42, 0.4);
    
    &::before {
      left: 100%;
    }
  }
  
  &:active {
    transform: translateY(-1px) scale(1);
  }
`;

const ContactSection = styled.div`
  background: ${theme.colors.background.main};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  text-align: center;
  animation: ${slideUp} 0.6s ease-out 0.4s both;
`;

const ContactForm = styled.form`
  max-width: 600px;
  margin: 0 auto;
  animation: ${fadeIn} 0.6s ease-out;
`;

const FormGroup = styled.div`
  margin-bottom: ${theme.spacing.lg};
`;

const Label = styled.label`
  display: block;
  margin-bottom: ${theme.spacing.sm};
  color: ${theme.colors.text.primary};
  font-weight: 500;
  text-align: left;
`;

const Input = styled.input`
  width: 100%;
  padding: ${theme.spacing.md};
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  font-family: ${theme.fonts.primary};
  font-size: 1em;
  transition: border-color ${theme.transitions.normal};
  
  &:focus {
    outline: none;
    border-color: ${theme.colors.primary.main};
  }
`;

const TextArea = styled.textarea`
  width: 100%;
  padding: ${theme.spacing.md};
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  font-family: ${theme.fonts.primary};
  font-size: 1em;
  min-height: 150px;
  resize: vertical;
  transition: border-color ${theme.transitions.normal};
  
  &:focus {
    outline: none;
    border-color: ${theme.colors.primary.main};
  }
`;

const SubmitButton = styled.button`
  width: 100%;
  padding: ${theme.spacing.md};
  background: ${theme.colors.primary.main};
  color: ${theme.colors.text.white};
  border: none;
  border-radius: ${theme.borderRadius.md};
  font-family: ${theme.fonts.primary};
  font-size: 1.1em;
  font-weight: 500;
  cursor: pointer;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    background: ${theme.colors.primary.light};
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(13, 27, 42, 0.3);
  }
  
  &:active {
    transform: translateY(0);
  }
`;

const DataSyncProduct = () => {
  const [activeScreenshot, setActiveScreenshot] = useState(0);
  const [isFading, setIsFading] = useState(false);

  useEffect(() => {
    const id = window.setInterval(() => {
      setActiveScreenshot(prev => (prev + 1) % screenshots.length);
    }, 6000);

    return () => {
      window.clearInterval(id);
    };
  }, []);

  useEffect(() => {
    setIsFading(true);
    const timeout = window.setTimeout(() => {
      setIsFading(false);
    }, 600);
    return () => {
      window.clearTimeout(timeout);
    };
  }, [activeScreenshot]);

  const [formData, setFormData] = useState({
    name: '',
    email: '',
    message: ''
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const subject = encodeURIComponent('DataSync - Contact Request');
    const body = encodeURIComponent(`Name: ${formData.name}\nEmail: ${formData.email}\n\nMessage:\n${formData.message}`);
    window.location.href = `mailto:tgbberrios@gmail.com?subject=${subject}&body=${body}`;
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value
    });
  };

  return (
    <ProductContainer>
      <ProductHero>
        <ProductTitle>DataSync</ProductTitle>
        <ProductDescription>
          Complete multi-database synchronization platform, data governance and data lineage 
          for modern enterprises. Unify all your data sources into a single, AI-ready platform. 
          Synchronize, manage and visualize your data from multiple sources in one unified solution 
          designed to accelerate machine learning and artificial intelligence initiatives.
        </ProductDescription>
      </ProductHero>

      <FeaturesGrid>
        <FeatureCard $index={0}>
          <FeatureIcon>[DB]</FeatureIcon>
          <FeatureTitle>Multi-Database Support</FeatureTitle>
          <FeatureDescription>
            Supports PostgreSQL, MariaDB, MSSQL, MongoDB, Oracle and REST APIs. 
            A single platform for all your data sources.
          </FeatureDescription>
        </FeatureCard>

        <FeatureCard $index={1}>
          <FeatureIcon>{'[<->]'}</FeatureIcon>
          <FeatureTitle>Real-Time Synchronization</FeatureTitle>
          <FeatureDescription>
            Bidirectional synchronization with granular control and flexible scheduling. 
            Keep your data always up to date.
          </FeatureDescription>
        </FeatureCard>

        <FeatureCard $index={2}>
          <FeatureIcon>[CH]</FeatureIcon>
          <FeatureTitle>Data Lineage</FeatureTitle>
          <FeatureDescription>
            Complete visualization of relationships and dependencies between data. 
            Understand the complete flow of your data.
          </FeatureDescription>
        </FeatureCard>

        <FeatureCard $index={3}>
          <FeatureIcon>[*]</FeatureIcon>
          <FeatureTitle>Enterprise Security</FeatureTitle>
          <FeatureDescription>
            JWT authentication, role-based authorization, rate limiting, HTTPS. 
            Meets the highest security standards.
          </FeatureDescription>
        </FeatureCard>

        <FeatureCard $index={4}>
          <FeatureIcon>[^]</FeatureIcon>
          <FeatureTitle>Data Quality</FeatureTitle>
          <FeatureDescription>
            Quality metrics, validation and real-time monitoring. 
            Ensure the integrity of your data.
          </FeatureDescription>
        </FeatureCard>

        <FeatureCard $index={5}>
          <FeatureIcon>[~]</FeatureIcon>
          <FeatureTitle>Custom Jobs</FeatureTitle>
          <FeatureDescription>
            Execute custom scripts for specific transformations and synchronizations. 
            Total flexibility for your needs.
          </FeatureDescription>
        </FeatureCard>
      </FeaturesGrid>

      <DemoSection>
        <DemoContainer>
          <SectionTitle style={{ fontSize: '1.9em', marginBottom: theme.spacing.md }}>
            Product Screenshots
          </SectionTitle>
          <SectionSubtitle style={{ marginBottom: theme.spacing.lg }}>
            Real interface from the DataSync platform, in carousel view
          </SectionSubtitle>

          <div
            style={{
              position: 'relative',
              width: '100%',
              maxWidth: '1200px',
              margin: '0 auto',
            }}
          >
            <img
              src={screenshots[activeScreenshot].src}
              alt={screenshots[activeScreenshot].alt}
              style={{
                width: '100%',
                borderRadius: theme.borderRadius.lg,
                boxShadow: theme.shadows.xl,
                display: 'block',
                opacity: isFading ? 0 : 1,
                transform: isFading ? 'translateY(10px)' : 'translateY(0)',
                transition: 'opacity 0.6s ease-out, transform 0.6s ease-out',
              }}
            />
            <p
              style={{
                marginTop: theme.spacing.md,
                fontSize: '0.95rem',
                color: theme.colors.text.secondary,
              }}
            >
              {screenshots[activeScreenshot].caption}
            </p>

            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                marginTop: theme.spacing.md,
                gap: theme.spacing.md,
              }}
            >
              <button
                type="button"
                onClick={() =>
                  setActiveScreenshot(
                    (activeScreenshot - 1 + screenshots.length) % screenshots.length
                  )
                }
                style={{
                  flex: 1,
                  padding: theme.spacing.sm,
                  borderRadius: theme.borderRadius.md,
                  border: `1px solid ${theme.colors.border.medium}`,
                  background: theme.colors.background.main,
                  color: theme.colors.text.primary,
                  cursor: 'pointer',
                  fontWeight: 600,
                }}
              >
                {'< Previous'}
              </button>

              <button
                type="button"
                onClick={() =>
                  setActiveScreenshot((activeScreenshot + 1) % screenshots.length)
                }
                style={{
                  flex: 1,
                  padding: theme.spacing.sm,
                  borderRadius: theme.borderRadius.md,
                  border: `1px solid ${theme.colors.primary.main}`,
                  background: theme.colors.primary.main,
                  color: theme.colors.text.white,
                  cursor: 'pointer',
                  fontWeight: 600,
                }}
              >
                {'Next >'}
              </button>
            </div>

            <div
              style={{
                display: 'flex',
                justifyContent: 'center',
                gap: theme.spacing.xs,
                marginTop: theme.spacing.md,
                flexWrap: 'wrap',
              }}
            >
              {screenshots.map((_, index) => (
                <button
                  key={index}
                  type="button"
                  onClick={() => setActiveScreenshot(index)}
                  style={{
                    width: 10,
                    height: 10,
                    borderRadius: '50%',
                    border: 'none',
                    padding: 0,
                    cursor: 'pointer',
                    background:
                      index === activeScreenshot
                        ? theme.colors.primary.main
                        : theme.colors.border.medium,
                  }}
                />
              ))}
            </div>
          </div>
        </DemoContainer>
      </DemoSection>

      <BusinessSection>
        <SectionTitle style={{ fontSize: '2em', marginBottom: theme.spacing.xl }}>For Business Users</SectionTitle>
        <SectionSubtitle style={{ marginBottom: theme.spacing.xxl }}>
          Everything you need to know about DataSync in simple terms
        </SectionSubtitle>

        <BusinessGrid>
          <BusinessCard>
            <BusinessIcon>[DB]</BusinessIcon>
            <BusinessTitle>Works with All Your Databases</BusinessTitle>
            <BusinessDescription>
              Connect to PostgreSQL, MariaDB, MSSQL, MongoDB, Oracle, and REST APIs. 
              One platform to manage all your data sources without learning multiple tools.
            </BusinessDescription>
          </BusinessCard>

          <BusinessCard>
            <BusinessIcon>{'[<->]'}</BusinessIcon>
            <BusinessTitle>Always Up-to-Date Data</BusinessTitle>
            <BusinessDescription>
              Automatic synchronization keeps your data current. Set it once and forget it. 
              Your data stays synchronized every few seconds or minutes, as you configure.
            </BusinessDescription>
          </BusinessCard>

          <BusinessCard>
            <BusinessIcon>[CH]</BusinessIcon>
            <BusinessTitle>See Where Your Data Comes From</BusinessTitle>
            <BusinessDescription>
              Visual maps show how your data flows through your systems. Understand 
              relationships and dependencies at a glance. Perfect for audits and compliance.
            </BusinessDescription>
          </BusinessCard>

          <BusinessCard>
            <BusinessIcon>[*]</BusinessIcon>
            <BusinessTitle>Enterprise-Grade Security</BusinessTitle>
            <BusinessDescription>
              Industry-standard security with encrypted connections, role-based access control, 
              and secure authentication. Your data is protected at every step.
            </BusinessDescription>
          </BusinessCard>

          <BusinessCard>
            <BusinessIcon>[^]</BusinessIcon>
            <BusinessTitle>Quality Assurance Built-In</BusinessTitle>
            <BusinessDescription>
              Automatic quality checks ensure your data is accurate, complete, and reliable. 
              Get alerts when issues are detected, so you can fix them quickly.
            </BusinessDescription>
          </BusinessCard>

          <BusinessCard>
            <BusinessIcon>[~]</BusinessIcon>
            <BusinessTitle>Customize to Your Needs</BusinessTitle>
            <BusinessDescription>
              Run custom scripts and transformations tailored to your specific business logic. 
              No limitations - build exactly what your organization requires.
            </BusinessDescription>
          </BusinessCard>

          <BusinessCard>
            <BusinessIcon>[ML]</BusinessIcon>
            <BusinessTitle>Unified Data for AI & Machine Learning</BusinessTitle>
            <BusinessDescription>
              All your data in one place, perfectly synchronized and ready for analytics, 
              machine learning, and AI initiatives. Enable data-driven decisions with a 
              unified view of your entire data ecosystem.
            </BusinessDescription>
          </BusinessCard>

          <BusinessCard>
            <BusinessIcon>[UNIFY]</BusinessIcon>
            <BusinessTitle>One Platform, All Your Data</BusinessTitle>
            <BusinessDescription>
              Break down data silos and unify all your databases, APIs, and data sources 
              into a single, coherent platform. Simplify your data infrastructure and 
              accelerate your digital transformation journey.
            </BusinessDescription>
          </BusinessCard>
        </BusinessGrid>

        <SimpleBenefitsSection>
          <SimpleBenefitsTitle>Key Benefits</SimpleBenefitsTitle>
          <SimpleBenefitsList>
            <BenefitItem>
              <BenefitCheckmark>✓</BenefitCheckmark>
              <BenefitText>Save time with automated data synchronization</BenefitText>
            </BenefitItem>
            <BenefitItem>
              <BenefitCheckmark>✓</BenefitCheckmark>
              <BenefitText>Reduce errors with automatic quality checks</BenefitText>
            </BenefitItem>
            <BenefitItem>
              <BenefitCheckmark>✓</BenefitCheckmark>
              <BenefitText>Improve decision-making with up-to-date data</BenefitText>
            </BenefitItem>
            <BenefitItem>
              <BenefitCheckmark>✓</BenefitCheckmark>
              <BenefitText>Meet compliance requirements with data lineage</BenefitText>
            </BenefitItem>
            <BenefitItem>
              <BenefitCheckmark>✓</BenefitCheckmark>
              <BenefitText>Scale as your business grows</BenefitText>
            </BenefitItem>
            <BenefitItem>
              <BenefitCheckmark>✓</BenefitCheckmark>
              <BenefitText>Centralize data management in one platform</BenefitText>
            </BenefitItem>
            <BenefitItem>
              <BenefitCheckmark>✓</BenefitCheckmark>
              <BenefitText>Unlock AI and ML potential with unified data</BenefitText>
            </BenefitItem>
            <BenefitItem>
              <BenefitCheckmark>✓</BenefitCheckmark>
              <BenefitText>Enable faster insights with real-time synchronization</BenefitText>
            </BenefitItem>
          </SimpleBenefitsList>
        </SimpleBenefitsSection>
      </BusinessSection>

      <TechDivider>
        <TechDividerText>Technical Documentation</TechDividerText>
        <TechDividerSubtext>For developers, architects, and technical decision makers</TechDividerSubtext>
      </TechDivider>

      <TechnicalSection>
        <SectionTitle style={{ fontSize: '2em', marginBottom: theme.spacing.xl }}>Technical Specifications</SectionTitle>
        <SectionSubtitle style={{ marginBottom: theme.spacing.xxl }}>
          Built for performance, reliability, and enterprise-scale data operations
        </SectionSubtitle>

        <TechGrid>
          <TechCard>
            <TechIcon>[ARCH]</TechIcon>
            <TechTitle>Multi-Threaded Architecture</TechTitle>
            <TechDescription>
              High-performance C++ core with 11+ dedicated threads for parallel processing:
              initialization, catalog sync, monitoring, quality checks, maintenance, 
              and separate transfer threads for each database engine (MariaDB, MSSQL, 
              MongoDB, Oracle, API, Custom Jobs).
            </TechDescription>
          </TechCard>

          <TechCard>
            <TechIcon>[PAR]</TechIcon>
            <TechTitle>Parallel Processing</TechTitle>
            <TechDescription>
              Thread pool architecture with configurable workers (1-32 threads). 
              Processes multiple tables simultaneously using data fetching, batch 
              preparation, and batch insertion pipelines. Supports up to 1,000 tables 
              per cycle with configurable chunk sizes (100-100,000+ records, scales with server capacity).
            </TechDescription>
          </TechCard>

          <TechCard>
            <TechIcon>[SYNC]</TechIcon>
            <TechTitle>Advanced Synchronization</TechTitle>
            <TechDescription>
              Multiple synchronization strategies: OFFSET-based, TIMESTAMP-based incremental 
              sync, and automatic PK detection. Configurable sync intervals (5-3600 seconds). 
              Supports full load, incremental sync, and real-time change listening modes.
            </TechDescription>
          </TechCard>

          <TechCard>
            <TechIcon>[QUAL]</TechIcon>
            <TechTitle>Data Quality Metrics</TechTitle>
            <TechDescription>
              Comprehensive quality monitoring: completeness, uniqueness, validity, 
              consistency, accuracy, timeliness, and integrity scores. Validates data types, 
              null counts, duplicates, constraints, and referential integrity. Quality 
              scores calculated automatically per table.
            </TechDescription>
          </TechCard>

          <TechCard>
            <TechIcon>[GOV]</TechIcon>
            <TechTitle>Data Governance</TechTitle>
            <TechDescription>
              Automatic data classification, sensitivity level detection, business domain 
              inference, and compliance tracking. Complete metadata catalog with lineage, 
              usage statistics, health monitoring, and retention policy recommendations.
            </TechDescription>
          </TechCard>

          <TechCard>
            <TechIcon>[API]</TechIcon>
            <TechTitle>REST API Integration</TechTitle>
            <TechDescription>
              Native API-to-database synchronization with support for REST, GraphQL, and SOAP. 
              Multiple authentication methods: API Key, Bearer Token, Basic Auth, OAuth2. 
              Configurable headers, query parameters, request bodies, and sync intervals.
            </TechDescription>
          </TechCard>

          <TechCard>
            <TechIcon>[UNIFY]</TechIcon>
            <TechTitle>Unified Data Architecture</TechTitle>
            <TechDescription>
              Centralized data hub architecture consolidating all sources into PostgreSQL. 
              Single source of truth enabling seamless data access for analytics, ML, and AI pipelines. 
              Eliminates data silos and provides consistent data models across all systems. 
              Optimized for enterprise-scale data unification and AI/ML workloads.
            </TechDescription>
          </TechCard>

          <TechCard>
            <TechIcon>[ML]</TechIcon>
            <TechTitle>AI & ML Ready Infrastructure</TechTitle>
            <TechDescription>
              Data unification platform optimized for machine learning and artificial intelligence. 
              Provides clean, synchronized, and quality-assured data ready for feature engineering. 
              Supports real-time data pipelines for streaming ML models. Integrated data quality 
              metrics ensure reliable training datasets. Enables rapid deployment of ML/AI initiatives 
              with unified access to all enterprise data sources.
            </TechDescription>
          </TechCard>
        </TechGrid>

        <PerformanceSection>
          <SectionTitle style={{ fontSize: '1.75em', marginBottom: theme.spacing.lg }}>Performance & Scalability</SectionTitle>
          <PerformanceGrid>
            <PerformanceItem>
              <PerformanceLabel>Chunk Size</PerformanceLabel>
              <PerformanceValue>100 - 100,000+ records</PerformanceValue>
              <PerformanceDesc>Configurable batch processing size (scales with server capacity)</PerformanceDesc>
            </PerformanceItem>
            <PerformanceItem>
              <PerformanceLabel>Max Workers</PerformanceLabel>
              <PerformanceValue>1 - 32 threads</PerformanceValue>
              <PerformanceDesc>Parallel processing capacity</PerformanceDesc>
            </PerformanceItem>
            <PerformanceItem>
              <PerformanceLabel>Tables per Cycle</PerformanceLabel>
              <PerformanceValue>Up to 1,000</PerformanceValue>
              <PerformanceDesc>Simultaneous table processing</PerformanceDesc>
            </PerformanceItem>
            <PerformanceItem>
              <PerformanceLabel>Sync Interval</PerformanceLabel>
              <PerformanceValue>5 - 3,600 seconds</PerformanceValue>
              <PerformanceDesc>Configurable synchronization frequency</PerformanceDesc>
            </PerformanceItem>
          </PerformanceGrid>
        </PerformanceSection>

        <StackSection>
          <SectionTitle style={{ fontSize: '1.75em', marginBottom: theme.spacing.lg }}>Technology Stack</SectionTitle>
          <StackGrid>
            <StackItem>
              <StackLabel>Backend Core</StackLabel>
              <StackValue>C++17</StackValue>
            </StackItem>
            <StackItem>
              <StackLabel>Frontend</StackLabel>
              <StackValue>React + TypeScript</StackValue>
            </StackItem>
            <StackItem>
              <StackLabel>Database</StackLabel>
              <StackValue>PostgreSQL</StackValue>
            </StackItem>
            <StackItem>
              <StackLabel>API Layer</StackLabel>
              <StackValue>Express.js + REST</StackValue>
            </StackItem>
            <StackItem>
              <StackLabel>Security</StackLabel>
              <StackValue>JWT + bcrypt</StackValue>
            </StackItem>
            <StackItem>
              <StackLabel>Threading</StackLabel>
              <StackValue>std::thread + atomic</StackValue>
            </StackItem>
          </StackGrid>
        </StackSection>

        <AdvancedFeaturesSection>
          <SectionTitle style={{ fontSize: '2em', marginBottom: theme.spacing.xl }}>Advanced Features</SectionTitle>
          <SectionSubtitle style={{ marginBottom: theme.spacing.xxl }}>
            Enterprise-grade capabilities for complex data operations
          </SectionSubtitle>

          <TechGrid>
            <TechCard>
              <TechIcon>[SCHEMA]</TechIcon>
              <TechTitle>Automatic Schema Synchronization</TechTitle>
              <TechDescription>
                Detects and applies schema changes automatically: adds missing columns, drops removed columns, 
                and updates column types with compatibility checks. Supports type conversion between database engines. 
                Validates schema consistency across all synchronized tables.
              </TechDescription>
            </TechCard>

            <TechCard>
              <TechIcon>[SCRIPT]</TechIcon>
              <TechTitle>Custom Jobs & Scripting</TechTitle>
              <TechDescription>
                Execute custom Python scripts and SQL queries across all supported databases. Supports data 
                transformation pipelines with JSON configuration. Schedule jobs using cron expressions or intervals. 
                Full logging and result tracking with sample data storage.
              </TechDescription>
            </TechCard>

            <TechCard>
              <TechIcon>[LINEAGE]</TechIcon>
              <TechTitle>Comprehensive Data Lineage</TechTitle>
              <TechDescription>
                Extract lineage from tables, views, stored procedures, triggers, and foreign keys. 
                Tracks dependencies across database objects with confidence scores and relationship types. 
                Supports MariaDB, MSSQL, MongoDB, and Oracle lineage extraction with multi-level dependency tracking.
              </TechDescription>
            </TechCard>

            <TechCard>
              <TechIcon>[LOG]</TechIcon>
              <TechTitle>Enterprise Logging System</TechTitle>
              <TechDescription>
                Categorized logging with 11 categories: SYSTEM, DATABASE, TRANSFER, CONFIG, VALIDATION, 
                MAINTENANCE, MONITORING, DDL_EXPORT, METRICS, GOVERNANCE, QUALITY. Five log levels from 
                DEBUG to CRITICAL. Database-backed logging with timestamps, thread IDs, and function context.
              </TechDescription>
            </TechCard>

            <TechCard>
              <TechIcon>[MAINT]</TechIcon>
              <TechTitle>Automated Maintenance</TechTitle>
              <TechDescription>
                Automatic detection and execution of maintenance tasks: VACUUM, ANALYZE, REINDEX for PostgreSQL; 
                OPTIMIZE and ANALYZE TABLE for MariaDB; UPDATE STATISTICS and index rebuild for MSSQL. 
                Tracks fragmentation, dead tuples, and performance improvements with before/after metrics.
              </TechDescription>
            </TechCard>

            <TechCard>
              <TechIcon>[QUERY]</TechIcon>
              <TechTitle>Query Performance Monitoring</TechTitle>
              <TechDescription>
                Collects query snapshots with execution statistics: calls, total time, mean time, cache hit ratio, 
                IO efficiency, and query efficiency scores. Tracks query fingerprints, execution plans, and categorizes 
                queries. Analyzes query patterns including joins, subqueries, CTEs, and window functions.
              </TechDescription>
            </TechCard>

            <TechCard>
              <TechIcon>[CATALOG]</TechIcon>
              <TechTitle>Intelligent Catalog Management</TechTitle>
              <TechDescription>
                Automatic catalog synchronization for all database engines. Validates schema consistency, 
                deactivates tables with no data, updates cluster names, and cleans orphaned entries. 
                Supports catalog statuses: PENDING, SKIP, NO_DATA, FULL_LOAD, LISTENING_CHANGES.
              </TechDescription>
            </TechCard>

            <TechCard>
              <TechIcon>[CONN]</TechIcon>
              <TechTitle>Robust Connection Management</TechTitle>
              <TechDescription>
                Connection retry logic with exponential backoff (up to 3 attempts). Connection pooling 
                with configurable timeouts. Automatic timeout configuration for MariaDB (wait_timeout, 
                net_read_timeout, etc.). Thread-safe connection handling with proper resource cleanup.
              </TechDescription>
            </TechCard>

            <TechCard>
              <TechIcon>[RETRY]</TechIcon>
              <TechTitle>Error Handling & Retry Logic</TechTitle>
              <TechDescription>
                Automatic retry for API requests with exponential backoff. Handles rate limiting (HTTP 429) 
                with adaptive delays. Connection retry with configurable backoff for all database engines. 
                Graceful error handling with detailed logging and status tracking.
              </TechDescription>
            </TechCard>

            <TechCard>
              <TechIcon>[METRICS]</TechIcon>
              <TechTitle>Transfer Metrics Collection</TechTitle>
              <TechDescription>
                Tracks records transferred, bytes transferred, memory usage, and IO operations per table. 
                Collects performance metrics, metadata metrics, and timestamp metrics. Generates comprehensive 
                metrics reports for monitoring and optimization.
              </TechDescription>
            </TechCard>

            <TechCard>
              <TechIcon>[GOV]</TechIcon>
              <TechTitle>Database-Specific Governance</TechTitle>
              <TechDescription>
                Engine-specific governance data collection: index statistics, fragmentation, access patterns, 
                health scores, and recommendations for MariaDB, MSSQL, MongoDB, and Oracle. Analyzes server 
                configuration, user permissions, and performance characteristics.
              </TechDescription>
            </TechCard>

            <TechCard>
              <TechIcon>[CLUSTER]</TechIcon>
              <TechTitle>Cluster Name Resolution</TechTitle>
              <TechDescription>
                Intelligent cluster name detection from connection strings using hostname pattern matching 
                and database-specific providers. Supports automatic cluster identification for high-availability 
                setups and distributed database architectures.
              </TechDescription>
            </TechCard>
          </TechGrid>
        </AdvancedFeaturesSection>

        <ImplementationDetailsSection>
          <SectionTitle style={{ fontSize: '2em', marginBottom: theme.spacing.xl }}>Implementation Details</SectionTitle>

          <ImplementationGrid>
            <ImplCard>
              <ImplTitle>Parallel Processing Pipeline</ImplTitle>
              <ImplList>
                <li>Data Fetcher Thread: Retrieves raw data from source databases</li>
                <li>4 Batch Preparer Threads: Transforms raw data into prepared batches</li>
                <li>4 Batch Inserter Threads: Executes batch inserts to target database</li>
                <li>Thread-safe queues with timeout support (MAX_QUEUE_SIZE: 10)</li>
                <li>Configurable batch sizes: 1,000 - 10,000 records per batch</li>
                <li>Maximum query size: 1,000,000 characters</li>
                <li>Statement timeout: 600 seconds per operation</li>
              </ImplList>
            </ImplCard>

            <ImplCard>
              <ImplTitle>Data Quality Validation</ImplTitle>
              <ImplList>
                <li>Null count validation per column and table</li>
                <li>Duplicate detection using primary keys and unique constraints</li>
                <li>Type validation: checks for invalid types and out-of-range values</li>
                <li>Constraint validation: referential integrity and constraint violations</li>
                <li>Quality score calculation: weighted combination of all metrics</li>
                <li>Validation status: PASSED, WARNING, FAILED based on thresholds</li>
                <li>Performance tracking: validation duration in milliseconds</li>
              </ImplList>
            </ImplCard>

            <ImplCard>
              <ImplTitle>Schema Synchronization</ImplTitle>
              <ImplList>
                <li>Column comparison: name, data type, nullability, default values</li>
                <li>Automatic column addition with proper type mapping</li>
                <li>Column removal with optional data preservation</li>
                <li>Type modification with compatibility checking</li>
                <li>PostgreSQL type mapping for all source database types</li>
                <li>Ordinal position tracking for column ordering</li>
                <li>Primary key detection and preservation</li>
              </ImplList>
            </ImplCard>

            <ImplCard>
              <ImplTitle>Custom Job Execution</ImplTitle>
              <ImplList>
                <li>Python 3 script execution with JSON output</li>
                <li>SQL execution for PostgreSQL, MariaDB, MSSQL, Oracle, MongoDB</li>
                <li>Data transformation using JSON configuration</li>
                <li>Result storage with sample data (first 100 rows)</li>
                <li>Cron expression parsing for scheduling</li>
                <li>Execution logging with process log IDs</li>
                <li>Error handling with detailed error messages</li>
                <li>Automatic table/collection creation for target databases</li>
              </ImplList>
            </ImplCard>

            <ImplCard>
              <ImplTitle>Data Lineage Extraction</ImplTitle>
              <ImplList>
                <li>Table dependencies: foreign keys and references</li>
                <li>View dependencies: extracts underlying table references</li>
                <li>Stored procedure dependencies (MSSQL)</li>
                <li>Trigger dependencies with action statement parsing</li>
                <li>MongoDB aggregation pipeline dependencies</li>
                <li>Multi-level dependency tracking with depth calculation</li>
                <li>Confidence scores based on discovery method</li>
                <li>Relationship type classification: DIRECT, INDIRECT, TRANSITIVE</li>
              </ImplList>
            </ImplCard>

            <ImplCard>
              <ImplTitle>Maintenance Management</ImplTitle>
              <ImplList>
                <li>Fragmentation detection: thresholds configurable per engine</li>
                <li>Dead tuple tracking for PostgreSQL (pg_stat_user_tables)</li>
                <li>Index size monitoring: before/after maintenance metrics</li>
                <li>Performance improvement calculation</li>
                <li>Space reclamation tracking in MB</li>
                <li>Priority-based task scheduling</li>
                <li>Automatic next maintenance date calculation</li>
                <li>Execution metrics: before/after comparison</li>
              </ImplList>
            </ImplCard>
          </ImplementationGrid>
        </ImplementationDetailsSection>
      </TechnicalSection>

      <PricingContactContainer>
        <PricingSection>
          <PricingCard $featured>
            <FeaturedBadge>Beta Testing</FeaturedBadge>
            <ProductTitle>DataSync Beta</ProductTitle>
            <Price>
              $99<span>/month</span>
            </Price>
            <PricingFeatures>
              <li>Up to 100 synchronized tables</li>
              <li>5 database connections</li>
              <li>Bidirectional synchronization</li>
              <li>API Catalog included</li>
              <li>Custom Jobs</li>
              <li>Complete Data Lineage</li>
              <li>Data Quality metrics</li>
              <li>Email support</li>
              <li>Updates included</li>
              <li>Special beta pricing</li>
            </PricingFeatures>
            <CTAButton href="#contact">Get Started</CTAButton>
          </PricingCard>
        </PricingSection>

        <ContactSection id="contact">
          <ProductTitle>Ready to Get Started?</ProductTitle>
          <ProductDescription>
            Request a demo or contact us for more information about DataSync
          </ProductDescription>
          <ContactForm onSubmit={handleSubmit}>
            <FormGroup>
              <Label htmlFor="name">Name</Label>
              <Input
                type="text"
                id="name"
                name="name"
                value={formData.name}
                onChange={handleChange}
                required
                placeholder="Your name"
              />
            </FormGroup>
            <FormGroup>
              <Label htmlFor="email">Email</Label>
              <Input
                type="email"
                id="email"
                name="email"
                value={formData.email}
                onChange={handleChange}
                required
                placeholder="your@email.com"
              />
            </FormGroup>
            <FormGroup>
              <Label htmlFor="message">Message</Label>
              <TextArea
                id="message"
                name="message"
                value={formData.message}
                onChange={handleChange}
                required
                placeholder="Tell us about your use case or request a demo..."
              />
            </FormGroup>
            <SubmitButton type="submit">Send</SubmitButton>
          </ContactForm>
        </ContactSection>
      </PricingContactContainer>
    </ProductContainer>
  );
};

export default DataSyncProduct;
