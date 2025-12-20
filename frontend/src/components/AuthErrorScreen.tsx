import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import styled, { keyframes } from 'styled-components';
import { theme } from '../theme/theme';
import { Button } from './shared/BaseComponents';

const fadeIn = keyframes`
  from {
    opacity: 0;
    transform: translateY(-10px);
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

const ErrorContainer = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: 100vh;
  background: linear-gradient(135deg, ${theme.colors.background.main} 0%, ${theme.colors.background.secondary} 100%);
  padding: ${theme.spacing.xl};
  position: relative;
  overflow: hidden;
`;

const ErrorBox = styled.div`
  background: ${theme.colors.background.secondary};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  box-shadow: ${theme.shadows.xl};
  max-width: 500px;
  width: 100%;
  text-align: center;
  animation: ${slideUp} 0.5s ease-out;
  position: relative;
  z-index: 10;
  border: 1px solid ${theme.colors.border.light};
  backdrop-filter: blur(10px);
`;

const ErrorIcon = styled.div`
  width: 80px;
  height: 80px;
  margin: 0 auto ${theme.spacing.lg};
  border-radius: 50%;
  background: linear-gradient(135deg, ${theme.colors.status.error.bg} 0%, ${theme.colors.status.error.text} 100%);
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 40px;
  color: ${theme.colors.text.white};
  animation: ${fadeIn} 0.5s ease-out;
  box-shadow: 0 4px 20px rgba(220, 38, 38, 0.3);
`;

const ErrorTitle = styled.h1`
  color: ${theme.colors.text.primary};
  font-size: 2em;
  margin-bottom: ${theme.spacing.md};
  font-family: monospace;
  letter-spacing: 1px;
`;

const ErrorMessage = styled.p`
  color: ${theme.colors.text.secondary};
  font-size: 1.1em;
  margin-bottom: ${theme.spacing.xl};
  line-height: 1.6;
`;

const ActionButton = styled(Button)`
  margin-top: ${theme.spacing.lg};
  padding: 12px 24px;
  font-size: 1em;
`;

const DecorativeCircle = styled.div<{ $size: number; $top: string; $left: string; $delay: number }>`
  position: absolute;
  width: ${props => props.$size}px;
  height: ${props => props.$size}px;
  border-radius: 50%;
  background: linear-gradient(135deg, ${theme.colors.primary.main}20, ${theme.colors.primary.dark}20);
  top: ${props => props.$top};
  left: ${props => props.$left};
  animation: ${fadeIn} ${props => 1 + props.$delay}s ease-out;
  z-index: 1;
`;

interface AuthErrorScreenProps {
  message?: string;
  onRetry?: () => void;
}

const AuthErrorScreen: React.FC<AuthErrorScreenProps> = ({ 
  message = "Your session has expired or you need to log in again.",
  onRetry 
}) => {
  const navigate = useNavigate();
  const [isRedirecting, setIsRedirecting] = useState(false);

  const handleLogin = () => {
    setIsRedirecting(true);
    localStorage.removeItem("authToken");
    localStorage.removeItem("authUser");
    setTimeout(() => {
      navigate("/login", { replace: true });
    }, 200);
  };

  return (
    <ErrorContainer>
      <DecorativeCircle $size={200} $top="10%" $left="10%" $delay={0} />
      <DecorativeCircle $size={150} $top="70%" $left="80%" $delay={0.3} />
      <DecorativeCircle $size={100} $top="40%" $left="5%" $delay={0.6} />
      
      <ErrorBox>
        <ErrorIcon>⚠</ErrorIcon>
        <ErrorTitle>Authentication Required</ErrorTitle>
        <ErrorMessage>{message}</ErrorMessage>
        <ActionButton $variant="primary" onClick={handleLogin} disabled={isRedirecting}>
          {isRedirecting ? 'Redirecting...' : 'Go to Login'}
        </ActionButton>
        {onRetry && (
          <ActionButton 
            $variant="secondary" 
            onClick={onRetry} 
            style={{ marginTop: theme.spacing.sm, marginLeft: theme.spacing.sm }}
          >
            Retry
          </ActionButton>
        )}
      </ErrorBox>
    </ErrorContainer>
  );
};

export default AuthErrorScreen;

