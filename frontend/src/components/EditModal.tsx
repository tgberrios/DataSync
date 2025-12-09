import React from 'react';
import styled from 'styled-components';
import type { CatalogEntry } from '../services/api';

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
  background: white;
  padding: 25px;
  border-radius: 8px;
  min-width: 400px;
  font-family: monospace;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
  animation: slideUp 0.2s ease-out;
  border: 1px solid #eee;
`;

const ModalHeader = styled.div`
  border-bottom: 2px solid #333;
  padding-bottom: 10px;
  margin-bottom: 20px;
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
    background: linear-gradient(90deg, #0d1b2a, #1e3a5f);
  }
`;

const FormGroup = styled.div`
  margin-bottom: 15px;
  animation: fadeIn 0.2s ease-in;
  animation-fill-mode: both;
  
  &:nth-child(1) { animation-delay: 0.03s; }
  &:nth-child(2) { animation-delay: 0.06s; }
  &:nth-child(3) { animation-delay: 0.09s; }
  &:nth-child(4) { animation-delay: 0.12s; }
  &:nth-child(5) { animation-delay: 0.15s; }
  &:nth-child(6) { animation-delay: 0.18s; }
`;

const Label = styled.label`
  display: block;
  margin-bottom: 5px;
  font-weight: bold;
  color: #222;
`;

const Select = styled.select`
  width: 100%;
  padding: 10px;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-family: monospace;
  transition: all 0.2s ease;
  background: white;
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.3);
  }
  
  &:focus {
    outline: none;
    border-color: #0d1b2a;
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
  }
`;

const Input = styled.input`
  width: 100%;
  padding: 10px;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-family: monospace;
  transition: all 0.2s ease;
  
  &:hover:not(:disabled) {
    border-color: rgba(10, 25, 41, 0.3);
  }
  
  &:focus:not(:disabled) {
    outline: none;
    border-color: #0d1b2a;
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
    transform: translateY(-1px);
  }
  
  &:disabled {
    background-color: #f5f5f5;
    cursor: not-allowed;
  }
`;

const ButtonGroup = styled.div`
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  margin-top: 20px;
`;

const Button = styled.button`
  padding: 10px 20px;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-family: monospace;
  transition: all 0.2s ease;
  font-weight: 500;

  &.primary {
    background: #0d1b2a;
    color: white;
    
    &:hover {
      background: #1e3a5f;
      transform: translateY(-2px);
      box-shadow: 0 4px 12px rgba(13, 27, 42, 0.3);
    }
    
    &:active {
      transform: translateY(0);
    }
  }

  &.secondary {
    background: #f5f5f5;
    color: #333;
    border: 1px solid #ddd;
    
    &:hover {
      background: #e0e0e0;
      border-color: rgba(10, 25, 41, 0.3);
      transform: translateY(-2px);
      box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
    }
    
    &:active {
      transform: translateY(0);
    }
  }
`;

interface EditModalProps {
  entry: CatalogEntry;
  onClose: () => void;
  onSave: (entry: CatalogEntry) => void;
}

const EditModal: React.FC<EditModalProps> = ({ entry, onClose, onSave }) => {
  const [editedEntry, setEditedEntry] = React.useState(entry);
  const [isClosing, setIsClosing] = React.useState(false);

  const handleClose = () => {
    setIsClosing(true);
    setTimeout(() => {
      onClose();
    }, 150);
  };

  const handleSave = () => {
    onSave(editedEntry);
    handleClose();
  };

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
          <Button className="secondary" onClick={handleClose}>Cancel</Button>
          <Button className="primary" onClick={handleSave}>Save Changes</Button>
        </ButtonGroup>
      </ModalContent>
      </ModalOverlay>
    </>
  );
};

export default EditModal;
