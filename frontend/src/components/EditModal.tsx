import { useState, useCallback } from 'react';
import styled from 'styled-components';
import {
  Button,
  Input,
  FormGroup,
  Label,
  Select,
} from './shared/BaseComponents';
import type { CatalogEntry } from '../services/api';
import { theme } from '../theme/theme';

const BlurOverlay = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  backdrop-filter: blur(5px);
  background: rgba(0, 0, 0, 0.3);
  z-index: 999;
  animation: fadeIn 0.15s ease-in;
`;

const ModalOverlay = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
  animation: fadeIn 0.15s ease-in;
`;

const ModalContent = styled.div`
  background: ${theme.colors.background.main};
  padding: ${theme.spacing.xxl};
  border-radius: ${theme.borderRadius.lg};
  min-width: 400px;
  font-family: ${theme.fonts.primary};
  box-shadow: ${theme.shadows.lg};
  animation: slideUp 0.2s ease-out;
  border: 1px solid ${theme.colors.border.light};
`;

const ModalHeader = styled.div`
  border-bottom: 2px solid ${theme.colors.border.dark};
  padding-bottom: ${theme.spacing.sm};
  margin-bottom: ${theme.spacing.lg};
  font-size: 1.2em;
  font-weight: bold;
  position: relative;
  
  &::after {
    content: '';
    position: absolute;
    bottom: -2px;
    left: 0;
    width: 60px;
    height: 2px;
    background: linear-gradient(90deg, ${theme.colors.primary.main}, ${theme.colors.primary.dark});
  }
`;

const ButtonGroup = styled.div`
  display: flex;
  justify-content: flex-end;
  gap: ${theme.spacing.sm};
  margin-top: ${theme.spacing.lg};
`;

interface EditModalProps {
  entry: CatalogEntry;
  onClose: () => void;
  onSave: (entry: CatalogEntry) => void;
}

/**
 * Edit Modal component
 * Modal dialog for editing catalog entry configurations
 */
const EditModal: React.FC<EditModalProps> = ({ entry, onClose, onSave }) => {
  const [editedEntry, setEditedEntry] = useState(entry);
  const [isClosing, setIsClosing] = useState(false);

  const handleClose = useCallback(() => {
    setIsClosing(true);
    setTimeout(() => {
      onClose();
    }, 150);
  }, [onClose]);

  const handleSave = useCallback(() => {
    onSave(editedEntry);
    handleClose();
  }, [editedEntry, onSave, handleClose]);

  return (
    <>
      <BlurOverlay style={{ animation: isClosing ? 'fadeOut 0.15s ease-out' : 'fadeIn 0.15s ease-in' }} />
      <ModalOverlay style={{ animation: isClosing ? 'fadeOut 0.15s ease-out' : 'fadeIn 0.15s ease-in' }}>
        <ModalContent style={{ animation: isClosing ? 'slideDown 0.15s ease-out' : 'slideUp 0.2s ease-out' }}>
          <ModalHeader>Edit Table Configuration</ModalHeader>
          
          <FormGroup>
            <Label>Table</Label>
            <Input 
              type="text" 
              value={`${editedEntry.schema_name}.${editedEntry.table_name}`} 
              disabled 
            />
          </FormGroup>

          <FormGroup>
            <Label>Engine</Label>
            <Input 
              type="text" 
              value={editedEntry.db_engine} 
              disabled 
            />
          </FormGroup>

          <FormGroup>
            <Label>Status</Label>
            <Input
              type="text"
              value={editedEntry.status}
              onChange={(e) => setEditedEntry({...editedEntry, status: e.target.value})}
            />
          </FormGroup>

          <FormGroup>
            <Label>Active</Label>
            <Select
              value={editedEntry.active.toString()}
              onChange={(e) => setEditedEntry({...editedEntry, active: e.target.value === 'true'})}
            >
              <option value="true">Yes</option>
              <option value="false">No</option>
            </Select>
          </FormGroup>

          <FormGroup>
            <Label>Offset</Label>
            <Input
              type="text"
            />
          </FormGroup>

          <FormGroup>
            <Label>Cluster</Label>
            <Input
              type="text"
              value={editedEntry.cluster_name}
              onChange={(e) => setEditedEntry({...editedEntry, cluster_name: e.target.value})}
            />
          </FormGroup>

          <ButtonGroup>
            <Button $variant="secondary" onClick={handleClose}>Cancel</Button>
            <Button $variant="primary" onClick={handleSave}>Save Changes</Button>
          </ButtonGroup>
        </ModalContent>
      </ModalOverlay>
    </>
  );
};

export default EditModal;
