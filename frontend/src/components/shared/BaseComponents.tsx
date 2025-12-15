import styled from 'styled-components';
import { theme } from '../../theme/theme';

export const Container = styled.div`
  padding: ${theme.spacing.lg};
  font-family: ${theme.fonts.primary};
  animation: fadeIn 0.25s ease-in;
  background-color: ${theme.colors.background.main};
  color: ${theme.colors.text.primary};
  box-sizing: border-box;
`;

export const Header = styled.div`
  border: 2px solid ${theme.colors.border.dark};
  padding: ${theme.spacing.md};
  text-align: center;
  margin-bottom: ${theme.spacing.xxl};
  font-size: 1.5em;
  font-weight: bold;
  background: ${theme.colors.gradient.primary};
  border-radius: ${theme.borderRadius.md};
  box-shadow: ${theme.shadows.md};
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
    background: ${theme.colors.gradient.shimmer};
    animation: shimmer 3s infinite;
  }
`;

export const Section = styled.div`
  margin-bottom: ${theme.spacing.xxl};
  padding: ${theme.spacing.lg};
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.md};
  background-color: ${theme.colors.background.tertiary};
  box-shadow: ${theme.shadows.sm};
  transition: all ${theme.transitions.normal};
  animation: slideUp 0.25s ease-out;
  animation-fill-mode: both;
  
  &:hover {
    box-shadow: ${theme.shadows.lg};
    transform: translateY(-2px);
    border-color: rgba(10, 25, 41, 0.2);
  }
`;

export const SectionTitle = styled.h3`
  margin-bottom: ${theme.spacing.md};
  font-size: 1.2em;
  color: ${theme.colors.text.primary};
  border-bottom: 2px solid ${theme.colors.border.dark};
  padding-bottom: 8px;
  position: relative;
  
  &::after {
    content: '';
    position: absolute;
    bottom: -2px;
    left: 0;
    width: 60px;
    height: 2px;
    background: linear-gradient(90deg, ${theme.colors.primary.main}, ${theme.colors.primary.light});
    transition: width ${theme.transitions.slow};
  }
  
  &:hover::after {
    width: 100%;
  }
`;

export const FiltersContainer = styled.div`
  display: flex;
  gap: ${theme.spacing.md};
  margin-bottom: ${theme.spacing.lg};
  padding: ${theme.spacing.md};
  background: ${theme.colors.background.secondary};
  border-radius: ${theme.borderRadius.md};
  box-shadow: ${theme.shadows.sm};
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.05s;
  animation-fill-mode: both;
  flex-wrap: wrap;
`;

export const Select = styled.select`
  padding: 8px 12px;
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  font-family: ${theme.fonts.primary};
  background: ${theme.colors.background.main};
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.3);
    box-shadow: ${theme.shadows.sm};
  }
  
  &:focus {
    outline: none;
    border-color: ${theme.colors.primary.main};
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
  }
`;

export const Input = styled.input`
  padding: 8px 12px;
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  font-family: ${theme.fonts.primary};
  background: ${theme.colors.background.main};
  transition: all ${theme.transitions.normal};
  
  &:hover:not(:disabled) {
    border-color: rgba(10, 25, 41, 0.3);
    box-shadow: ${theme.shadows.sm};
  }
  
  &:focus:not(:disabled) {
    outline: none;
    border-color: ${theme.colors.primary.main};
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
  }
  
  &:disabled {
    background-color: ${theme.colors.background.secondary};
    cursor: not-allowed;
  }
`;

export const Button = styled.button<{ $variant?: 'primary' | 'secondary' | 'danger' | 'success' }>`
  padding: 10px 20px;
  border: none;
  border-radius: ${theme.borderRadius.md};
  cursor: pointer;
  font-family: ${theme.fonts.primary};
  transition: all ${theme.transitions.normal};
  font-weight: 500;
  
  ${props => {
    switch (props.$variant) {
      case 'primary':
        return `
          background: ${theme.colors.primary.main};
          color: ${theme.colors.text.white};
          
          &:hover {
            background: ${theme.colors.primary.light};
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(13, 27, 42, 0.3);
          }
        `;
      case 'danger':
        return `
          background: ${theme.colors.status.error.text};
          color: ${theme.colors.text.white};
          
          &:hover {
            background: #b71c1c;
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(198, 40, 40, 0.3);
          }
        `;
      case 'success':
        return `
          background: ${theme.colors.status.success.text};
          color: ${theme.colors.text.white};
          
          &:hover {
            background: #1b5e20;
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(46, 125, 50, 0.3);
          }
        `;
      default:
        return `
          background: ${theme.colors.background.secondary};
          color: ${theme.colors.text.primary};
          border: 1px solid ${theme.colors.border.medium};
          
          &:hover {
            background: #e0e0e0;
            border-color: rgba(10, 25, 41, 0.3);
            transform: translateY(-2px);
            box-shadow: ${theme.shadows.md};
          }
        `;
    }
  }}
  
  &:active {
    transform: translateY(0);
  }
  
  &:disabled {
    opacity: 0.6;
    cursor: not-allowed;
    transform: none;
  }
`;

export const TableContainer = styled.div`
  width: 100%;
  overflow-x: auto;
  margin-top: ${theme.spacing.lg};
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

export const Table = styled.table<{ $minWidth?: string }>`
  width: 100%;
  border-collapse: collapse;
  background: ${theme.colors.background.main};
  min-width: ${props => props.$minWidth || '1200px'};
  box-shadow: ${theme.shadows.md};
  border-radius: ${theme.borderRadius.md};
  overflow: hidden;
`;

export const Th = styled.th`
  padding: 12px;
  text-align: left;
  border-bottom: 2px solid ${theme.colors.border.dark};
  background: linear-gradient(180deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
  font-weight: bold;
  position: sticky;
  top: 0;
  z-index: 10;
`;

export const Td = styled.td`
  padding: 12px;
  border-bottom: 1px solid ${theme.colors.border.light};
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 200px;
  transition: all ${theme.transitions.normal};
`;

export const TableRow = styled.tr`
  transition: all 0.15s ease;
  
  &:hover {
    background: linear-gradient(90deg, ${theme.colors.background.main} 0%, ${theme.colors.background.tertiary} 100%);
    transform: scale(1.001);
    box-shadow: ${theme.shadows.sm};
    
    ${Td} {
      border-bottom-color: rgba(10, 25, 41, 0.1);
    }
  }
`;

export const StatusBadge = styled.span<{ $status: string }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.9em;
  font-weight: bold;
  display: inline-block;
  transition: all ${theme.transitions.normal};
  background: ${props => {
    switch (props.$status) {
      case 'ERROR': return theme.colors.status.error.bg;
      case 'LISTENING_CHANGES': return theme.colors.status.success.bg;
      case 'IN_PROGRESS': return theme.colors.status.warning.bg;
      case 'FULL_LOAD': return theme.colors.status.warning.bg;
      case 'NO_DATA': return theme.colors.status.warning.bg;
      case 'SKIP': return theme.colors.status.skip.bg;
      default: return theme.colors.background.secondary;
    }
  }};
  color: ${props => {
    switch (props.$status) {
      case 'ERROR': return theme.colors.status.error.text;
      case 'LISTENING_CHANGES': return theme.colors.status.success.text;
      case 'IN_PROGRESS': return theme.colors.status.warning.text;
      case 'FULL_LOAD': return theme.colors.status.warning.text;
      case 'NO_DATA': return theme.colors.status.warning.text;
      case 'SKIP': return theme.colors.status.skip.text;
      default: return theme.colors.text.primary;
    }
  }};
`;

export const Grid = styled.div<{ $columns?: string }>`
  display: grid;
  grid-template-columns: ${props => props.$columns || 'repeat(auto-fit, minmax(250px, 1fr))'};
  gap: ${theme.spacing.lg};
  margin-top: ${theme.spacing.md};
`;

export const Value = styled.div`
  font-size: 1.1em;
  padding: 12px;
  background-color: ${theme.colors.background.main};
  border-radius: ${theme.borderRadius.md};
  border: 1px solid ${theme.colors.border.medium};
  transition: all ${theme.transitions.normal};
  cursor: default;
  
  &:hover {
    transform: translateY(-3px);
    box-shadow: ${theme.shadows.lg};
    border-color: rgba(10, 25, 41, 0.3);
    background: linear-gradient(135deg, ${theme.colors.background.main} 0%, ${theme.colors.background.tertiary} 100%);
  }
`;

export const Pagination = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  gap: ${theme.spacing.sm};
  margin-top: ${theme.spacing.lg};
  padding: ${theme.spacing.md};
`;

export const PageButton = styled.button<{ $active?: boolean }>`
  padding: 8px 12px;
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  background: ${props => props.$active ? theme.colors.primary.main : theme.colors.background.main};
  color: ${props => props.$active ? theme.colors.text.white : theme.colors.text.primary};
  cursor: pointer;
  transition: all ${theme.transitions.normal};
  font-family: ${theme.fonts.primary};
  
  &:hover:not(:disabled) {
    background: ${props => props.$active ? theme.colors.primary.light : theme.colors.background.secondary};
    transform: translateY(-2px);
    box-shadow: ${theme.shadows.sm};
  }
  
  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
`;

export const ErrorMessage = styled.div`
  color: ${theme.colors.status.error.text};
  padding: ${theme.spacing.lg};
  text-align: center;
  border: 1px solid ${theme.colors.status.error.text};
  border-radius: ${theme.borderRadius.md};
  margin: ${theme.spacing.lg};
  background-color: ${theme.colors.status.error.bg};
  animation: slideUp 0.2s ease-out;
  box-shadow: 0 4px 12px rgba(220, 38, 38, 0.15);
`;

export const LoadingOverlay = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: ${theme.zIndex.modal};
  animation: fadeIn 0.2s ease-in;
`;

export const SearchContainer = styled.div`
  display: flex;
  gap: ${theme.spacing.sm};
  margin-bottom: ${theme.spacing.lg};
  align-items: center;
`;

export const PlayButton = styled.button`
  padding: 8px 14px;
  border: 1px solid ${theme.colors.status.success.text};
  border-radius: ${theme.borderRadius.md};
  background: linear-gradient(135deg, ${theme.colors.status.success.text} 0%, #45a049 100%);
  color: white;
  cursor: pointer;
  font-family: ${theme.fonts.primary};
  margin-right: 5px;
  transition: all ${theme.transitions.normal};
  font-weight: bold;
  font-size: 1.1em;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 40px;
  
  &:hover {
    background: linear-gradient(135deg, #45a049 0%, #3d8b40 100%);
    transform: translateY(-2px) scale(1.05);
    box-shadow: 0 4px 12px rgba(76, 175, 80, 0.4);
    border-color: #3d8b40;
  }
  
  &:active {
    transform: translateY(0) scale(1);
  }
`;

export const ActiveBadge = styled.span<{ $active: boolean }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.9em;
  font-weight: bold;
  display: inline-block;
  transition: all ${theme.transitions.normal};
  background: ${(props) => (props.$active ? theme.colors.status.success.bg : theme.colors.status.error.bg)};
  color: ${(props) => (props.$active ? theme.colors.status.success.text : theme.colors.status.error.text)};

  &:hover {
    transform: scale(1.05);
    box-shadow: ${theme.shadows.sm};
  }
`;

export const ActionButton = styled(Button)`
  padding: 6px 12px;
  margin-right: 5px;
  font-size: 0.9em;
`;

export const FormGroup = styled.div`
  margin-bottom: ${theme.spacing.md};
  animation: fadeIn 0.2s ease-in;
  animation-fill-mode: both;
  
  &:nth-child(1) { animation-delay: 0.03s; }
  &:nth-child(2) { animation-delay: 0.06s; }
  &:nth-child(3) { animation-delay: 0.09s; }
  &:nth-child(4) { animation-delay: 0.12s; }
  &:nth-child(5) { animation-delay: 0.15s; }
  &:nth-child(6) { animation-delay: 0.18s; }
`;

export const Label = styled.label`
  display: block;
  margin-bottom: 5px;
  font-weight: bold;
  color: ${theme.colors.text.primary};
  font-family: ${theme.fonts.primary};
`;

export const SearchInput = styled(Input)`
  flex: 1;
  font-size: 14px;
`;

export const SearchButton = styled(Button)`
  padding: 10px 20px;
  font-weight: bold;
`;

export const ClearSearchButton = styled(Button)`
  padding: 10px 15px;
`;

export const PaginationInfo = styled.div`
  text-align: center;
  margin-bottom: ${theme.spacing.sm};
  color: ${theme.colors.text.secondary};
  font-size: 0.9em;
  animation: fadeIn 0.25s ease-in;
`;

export const ModalOverlay = styled.div<{ $isOpen: boolean }>`
  display: ${props => props.$isOpen ? 'flex' : 'none'};
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  z-index: 1000;
  align-items: center;
  justify-content: center;
  animation: fadeIn 0.2s ease-in;
`;

export const ModalContent = styled.div`
  background: ${theme.colors.background.main};
  border-radius: ${theme.borderRadius.md};
  padding: ${theme.spacing.xl};
  max-width: 800px;
  width: 90%;
  max-height: 90vh;
  overflow-y: auto;
  box-shadow: ${theme.shadows.xl};
  animation: slideUp 0.3s ease-out;
`;

export const ModalHeader = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: ${theme.spacing.lg};
  padding-bottom: ${theme.spacing.md};
  border-bottom: 2px solid ${theme.colors.border.medium};
`;

export const ModalTitle = styled.h2`
  margin: 0;
  color: ${theme.colors.text.primary};
`;

export const TextArea = styled.textarea`
  padding: ${theme.spacing.sm};
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.md};
  font-family: ${theme.fonts.primary};
  background: ${theme.colors.background.main};
  color: ${theme.colors.text.primary};
  transition: all ${theme.transitions.normal};
  width: 100%;
  min-height: 120px;
  resize: vertical;
  
  &:hover:not(:disabled) {
    border-color: rgba(10, 25, 41, 0.3);
    box-shadow: ${theme.shadows.sm};
  }
  
  &:focus:not(:disabled) {
    outline: none;
    border-color: ${theme.colors.primary.main};
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
  }
  
  &:disabled {
    background-color: ${theme.colors.background.secondary};
    cursor: not-allowed;
  }
`;

