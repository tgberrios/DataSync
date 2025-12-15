import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import styled, { keyframes } from 'styled-components';
import { authApi, isAuthenticated } from '../services/api';
import { extractApiError } from '../utils/errorHandler';
import { theme } from '../theme/theme';
import { Button, Input, ErrorMessage } from './shared/BaseComponents';


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

const LoginContainer = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: 100vh;
  background: ${theme.colors.background.main};
  position: relative;
  overflow: hidden;
  cursor: default;
`;


const LoginBox = styled.div`
  background: ${theme.colors.background.secondary};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  box-shadow: ${theme.shadows.xl};
  width: 100%;
  max-width: 400px;
  animation: ${slideUp} 0.5s ease-out;
  position: relative;
  z-index: 10;
  backdrop-filter: blur(10px);
  border: 1px solid ${theme.colors.border.light};
`;

const LoginTitle = styled.h1`
  text-align: center;
  margin-bottom: ${theme.spacing.xl};
  color: ${theme.colors.text.primary};
  font-size: 2em;
  font-family: monospace;
  letter-spacing: 2px;
  position: relative;
  
  &::before {
    content: '■';
    color: ${theme.colors.primary.main};
    margin-right: 10px;
  }
`;

const FormGroup = styled.div`
  margin-bottom: ${theme.spacing.md};
`;

const Label = styled.label`
  display: block;
  margin-bottom: ${theme.spacing.sm};
  color: ${theme.colors.text.primary};
  font-weight: 500;
`;

const LoginButton = styled(Button)`
  width: 100%;
  padding: ${theme.spacing.md};
  font-size: 1.1em;
  margin-top: ${theme.spacing.md};
`;

const Login = () => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  // Si ya está autenticado, redirigir al dashboard
  useEffect(() => {
    if (isAuthenticated()) {
      navigate('/', { replace: true });
    }
  }, [navigate]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setLoading(true);

    try {
      const response = await authApi.login(username, password);
      if (response && response.token) {
        if (isAuthenticated()) {
          navigate('/', { replace: true });
        } else {
          setError('Failed to authenticate. Please try again.');
        }
      } else {
        setError('Invalid response from server. Please try again.');
      }
    } catch (err) {
      setError(extractApiError(err));
    } finally {
      setLoading(false);
    }
  };

  return (
    <LoginContainer>
      
      <LoginBox>
        <LoginTitle>DataSync</LoginTitle>
        <form onSubmit={handleSubmit}>
          <FormGroup>
            <Label htmlFor="username">Username</Label>
            <Input
              id="username"
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              required
              autoFocus
              disabled={loading}
            />
          </FormGroup>
          <FormGroup>
            <Label htmlFor="password">Password</Label>
            <Input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              disabled={loading}
            />
          </FormGroup>
          {error && <ErrorMessage>{error}</ErrorMessage>}
          <LoginButton type="submit" $variant="primary" disabled={loading}>
            {loading ? 'Logging in...' : 'Login'}
          </LoginButton>
        </form>
      </LoginBox>
    </LoginContainer>
  );
};

export default Login;
