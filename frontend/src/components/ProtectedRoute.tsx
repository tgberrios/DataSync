import { useState, useEffect } from 'react';
import { Navigate } from 'react-router-dom';
import { isAuthenticated, authApi } from '../services/api';
import AuthErrorScreen from './AuthErrorScreen';

interface ProtectedRouteProps {
  children: React.ReactNode;
}

const ProtectedRoute = ({ children }: ProtectedRouteProps) => {
  const [isValidating, setIsValidating] = useState(true);
  const [isValid, setIsValid] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const validateToken = async () => {
      if (!isAuthenticated()) {
        setIsValidating(false);
        setIsValid(false);
        return;
      }

      try {
        await authApi.getCurrentUser();
        setIsValid(true);
        setError(null);
      } catch (err: any) {
        setIsValid(false);
        if (err.response?.status === 401) {
          setError('Your session has expired. Please log in again.');
        } else {
          setError('Unable to verify authentication. Please try again.');
        }
      } finally {
        setIsValidating(false);
      }
    };

    validateToken();
  }, []);

  if (isValidating) {
    return null;
  }

  if (!isValid) {
    if (error) {
      return <AuthErrorScreen message={error} />;
    }
    return <Navigate to="/login" replace />;
  }

  return <>{children}</>;
};

export default ProtectedRoute;

