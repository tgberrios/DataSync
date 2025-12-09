import React, { useState, useEffect } from 'react';
import EditModal from './EditModal';
import styled from 'styled-components';
import { format } from 'date-fns';
import { catalogApi } from '../services/api';
import type { CatalogEntry } from '../services/api';

const CatalogContainer = styled.div`
  padding: 20px;
  font-family: monospace;
  animation: fadeIn 0.25s ease-in;
`;

const Header = styled.div`
  border: 2px solid #333;
  padding: 15px;
  text-align: center;
  margin-bottom: 30px;
  font-size: 1.5em;
  font-weight: bold;
  background: linear-gradient(135deg, #f5f5f5 0%, #ffffff 50%, #f5f5f5 100%);
  border-radius: 6px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
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
    background: linear-gradient(90deg, transparent, rgba(10, 25, 41, 0.1), transparent);
    animation: shimmer 3s infinite;
  }
`;

const FiltersContainer = styled.div`
  display: flex;
  gap: 15px;
  margin-bottom: 20px;
  padding: 15px;
  background: #f5f5f5;
  border-radius: 6px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.03);
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.05s;
  animation-fill-mode: both;
`;

const Select = styled.select`
  padding: 8px 12px;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-family: monospace;
  background: white;
  transition: all 0.2s ease;
  cursor: pointer;
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.3);
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
  }
  
  &:focus {
    outline: none;
    border-color: #0d1b2a;
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
  }
`;

const TableContainer = styled.div`
  width: 100%;
  overflow-x: auto;
  margin-top: 20px;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.1s;
  animation-fill-mode: both;
`;

const Table = styled.table`
  width: 100%;
  border-collapse: collapse;
  background: white;
  min-width: 1200px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  border-radius: 6px;
  overflow: hidden;
`;

const Th = styled.th`
  padding: 12px;
  text-align: left;
  border-bottom: 2px solid #333;
  background: linear-gradient(180deg, #f5f5f5 0%, #fafafa 100%);
  font-weight: bold;
  position: sticky;
  top: 0;
  z-index: 10;
`;

const Td = styled.td`
  padding: 12px;
  border-bottom: 1px solid #eee;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 200px;
  transition: all 0.2s ease;
`;

const TableRow = styled.tr`
  transition: all 0.15s ease;
  
  &:hover {
    background: linear-gradient(90deg, #ffffff 0%, #f8f9fa 100%);
    transform: scale(1.001);
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
    
    ${Td} {
      border-bottom-color: rgba(10, 25, 41, 0.1);
    }
  }
`;

const StatusBadge = styled.span<{ $status: string }>`
  padding: 4px 10px;
  border-radius: 6px;
  font-size: 0.9em;
  font-weight: bold;
  display: inline-block;
  transition: all 0.2s ease;
  background: ${props => {
    switch (props.$status) {
      case 'ERROR': return '#ffebee';
      case 'LISTENING_CHANGES': return '#e8f5e9';
      case 'NO_DATA': return '#fff3e0';
      case 'SKIP': return '#f5f5f5';
      default: return '#f5f5f5';
    }
  }};
  color: ${props => {
    switch (props.$status) {
      case 'ERROR': return '#c62828';
      case 'LISTENING_CHANGES': return '#2e7d32';
      case 'NO_DATA': return '#ef6c00';
      case 'SKIP': return '#666666';
      default: return '#333';
    }
  }};
  
  &:hover {
    transform: scale(1.05);
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
  }
`;

const ActiveBadge = styled.span<{ $active: boolean }>`
  padding: 4px 10px;
  border-radius: 6px;
  font-size: 0.9em;
  font-weight: bold;
  display: inline-block;
  transition: all 0.2s ease;
  background: ${props => props.$active ? '#e8f5e9' : '#ffebee'};
  color: ${props => props.$active ? '#2e7d32' : '#c62828'};
  
  &:hover {
    transform: scale(1.05);
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.15);
  }
`;

const ActionButton = styled.button`
  padding: 6px 12px;
  border: 1px solid #ddd;
  border-radius: 6px;
  background: white;
  cursor: pointer;
  font-family: monospace;
  margin-right: 5px;
  transition: all 0.2s ease;
  font-weight: 500;
  
  &:hover {
    background: linear-gradient(135deg, #f5f5f5 0%, #ffffff 100%);
    border-color: rgba(10, 25, 41, 0.3);
    transform: translateY(-2px);
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
  }
  
  &:active {
    transform: translateY(0);
  }
`;


// Loading state indicator
const LoadingOverlay = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(255, 255, 255, 0.9);
  display: flex;
  justify-content: center;
  align-items: center;
  font-size: 1.2em;
  z-index: 1000;
  animation: fadeIn 0.3s ease-in;
  backdrop-filter: blur(2px);
`;

const ErrorMessage = styled.div`
  padding: 15px;
  margin: 20px 0;
  background-color: #ffebee;
  color: #c62828;
  border-radius: 6px;
  border: 1px solid #ef9a9a;
  animation: slideUp 0.2s ease-out;
  box-shadow: 0 4px 12px rgba(220, 38, 38, 0.15);
`;

const Pagination = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 10px;
  margin-top: 20px;
  padding: 15px;
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.15s;
  animation-fill-mode: both;
`;

const PageButton = styled.button<{ $active?: boolean }>`
  padding: 8px 14px;
  border: 1px solid #ddd;
  border-radius: 6px;
  background: ${props => props.$active ? '#0d1b2a' : 'white'};
  color: ${props => props.$active ? 'white' : '#333'};
  cursor: pointer;
  font-family: monospace;
  transition: all 0.2s ease;
  font-weight: ${props => props.$active ? 'bold' : 'normal'};
  
  &:hover:not(:disabled) {
    background: ${props => props.$active ? '#1e3a5f' : '#f5f5f5'};
    transform: translateY(-2px);
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
    border-color: ${props => props.$active ? '#0d1b2a' : 'rgba(10, 25, 41, 0.3)'};
  }
  
  &:disabled {
    cursor: not-allowed;
    opacity: 0.5;
  }
`;

const PaginationInfo = styled.div`
  text-align: center;
  margin-bottom: 10px;
  color: #666;
  font-size: 0.9em;
  animation: fadeIn 0.25s ease-in;
`;

const ResetButton = styled.button`
  padding: 8px 15px;
  border: 1px solid #ddd;
  border-radius: 6px;
  background: white;
  cursor: pointer;
  font-family: monospace;
  transition: all 0.2s ease;
  font-weight: 500;
  
  &:hover {
    background: linear-gradient(135deg, #f5f5f5 0%, #ffffff 100%);
    border-color: rgba(10, 25, 41, 0.3);
    transform: translateY(-2px);
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
  }
`;

const SearchContainer = styled.div`
  display: flex;
  gap: 10px;
  margin-bottom: 20px;
  padding: 15px;
  background: #f9f9f9;
  border-radius: 6px;
  border: 1px solid #e0e0e0;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.03);
  animation: slideUp 0.25s ease-out;
  animation-delay: 0.08s;
  animation-fill-mode: both;
`;

const SearchInput = styled.input`
  flex: 1;
  padding: 10px 14px;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-family: monospace;
  font-size: 14px;
  transition: all 0.2s ease;
  
  &:focus {
    outline: none;
    border-color: #0d1b2a;
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
    transform: translateY(-1px);
  }
  
  &:hover {
    border-color: rgba(10, 25, 41, 0.3);
  }
`;

const SearchButton = styled.button`
  padding: 10px 20px;
  border: 1px solid #0d1b2a;
  border-radius: 6px;
  background: #0d1b2a;
  color: white;
  cursor: pointer;
  font-family: monospace;
  transition: all 0.2s ease;
  font-weight: bold;
  
  &:hover {
    background: #1e3a5f;
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(13, 27, 42, 0.3);
  }
  
  &:active {
    transform: translateY(0);
  }
`;

const ClearSearchButton = styled.button`
  padding: 10px 15px;
  border: 1px solid #ddd;
  border-radius: 6px;
  background: white;
  cursor: pointer;
  font-family: monospace;
  transition: all 0.2s ease;
  
  &:hover {
    background: #f5f5f5;
    border-color: rgba(10, 25, 41, 0.3);
    transform: translateY(-2px);
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
  }
`;

const SchemaActionSelect = styled.select`
  padding: 8px 12px;
  border: 1px solid #ddd;
  border-radius: 6px;
  background: white;
  color: #333;
  font-family: monospace;
  cursor: pointer;
  transition: all 0.2s ease;
  
  &:hover {
    background: #f5f5f5;
    border-color: rgba(10, 25, 41, 0.3);
  }
  
  &:focus {
    outline: none;
    border-color: #0d1b2a;
    box-shadow: 0 0 0 3px rgba(10, 25, 41, 0.1);
  }
  
  option {
    background: white;
    color: #333;
  }
  
  option[value=""] {
    color: #666;
    font-style: italic;
  }
`;

const Catalog = () => {
  const [filter, setFilter] = useState({
    engine: '',
    status: '',
    active: '',
    strategy: ''
  });
  
  const [sort, setSort] = useState({
    field: 'active',
    direction: 'desc'
  });

  const [search, setSearch] = useState('');
  const [searchInput, setSearchInput] = useState('');
  const [availableSchemas, setAvailableSchemas] = useState<string[]>([]);

  const [data, setData] = useState<CatalogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedEntry, setSelectedEntry] = useState<CatalogEntry | null>(null);
  const [page, setPage] = useState(1);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 10
  });

  // Cargar schemas únicos
  useEffect(() => {
    const fetchSchemas = async () => {
      try {
        const schemas = await catalogApi.getSchemas();
        setAvailableSchemas(schemas);
      } catch (err) {
        console.error('Error loading schemas:', err);
      }
    };

    fetchSchemas();
  }, []);

  // Cargar datos del catálogo
  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        const response = await catalogApi.getCatalogEntries({
          page,
          limit: 10,
          ...filter,
          search,
          sort_field: sort.field,
          sort_direction: sort.direction
        });
        setData(response.data);
        setPagination(response.pagination);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error loading catalog data');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, [page, filter, search]);

  // Debounce para la búsqueda
  useEffect(() => {
    const timer = setTimeout(() => {
      setSearch(searchInput);
      setPage(1);
    }, 500);

    return () => clearTimeout(timer);
  }, [searchInput]);

  // Manejar cambio de estado activo
  const handleToggleActive = async (entry: CatalogEntry) => {
    try {
      setLoading(true);
      await catalogApi.updateEntryStatus(
        entry.schema_name,
        entry.table_name,
        entry.db_engine,
        !entry.active
      );
      // Recargar datos después de la actualización
      const response = await catalogApi.getCatalogEntries({
        page,
        limit: 10,
        ...filter,
        search
      });
      setData(response.data);
      setPagination(response.pagination);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error updating entry status');
    } finally {
      setLoading(false);
    }
  };

  // Forzar sincronización completa
  const handleEdit = async (updatedEntry: CatalogEntry) => {
    try {
      setLoading(true);
      await catalogApi.updateEntry(updatedEntry);
      // Recargar datos después de la actualización
      const response = await catalogApi.getCatalogEntries({
        page,
        limit: 10,
        ...filter,
        search
      });
      setData(response.data);
      setPagination(response.pagination);
      setSelectedEntry(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error updating entry');
    } finally {
      setLoading(false);
    }
  };

  const handleForceSync = async (entry: CatalogEntry) => {
    try {
      setLoading(true);
      await catalogApi.triggerFullSync(
        entry.schema_name,
        entry.table_name,
        entry.db_engine
      );
      // Recargar datos después de la sincronización
      const response = await catalogApi.getCatalogEntries({
        page,
        limit: 10,
        ...filter
      });
      setData(response.data);
      setPagination(response.pagination);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error triggering full sync');
    } finally {
      setLoading(false);
    }
  };

  const handleSkipTable = async (entry: CatalogEntry) => {
    if (!confirm(`Are you sure you want to mark table "${entry.schema_name}.${entry.table_name}" as SKIP?\n\nThis will:\n- Set status to 'SKIP'\n- Set active to false (table will not be processed)\n- Reset offset to 0\n\nThis action CANNOT be undone.`)) {
      return;
    }

    try {
      setLoading(true);
      const result = await catalogApi.skipTable(
        entry.schema_name,
        entry.table_name,
        entry.db_engine
      );
      
      // Recargar datos después del skip
      const response = await catalogApi.getCatalogEntries({
        page,
        limit: 10,
        ...filter,
        search
      });
      setData(response.data);
      setPagination(response.pagination);
      
      alert(`Table "${entry.schema_name}.${entry.table_name}" marked as SKIP successfully.`);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error skipping table');
    } finally {
      setLoading(false);
    }
  };

  const handleSchemaAction = async (value: string) => {
    if (!value || value === '') return;
    
    // Mostrar confirmación inmediatamente
    if (!confirm(`Are you sure you want to deactivate ALL tables in schema "${value}"?\n\nThis will change status to 'SKIPPED' and reset offsets to 0.\n\nThis action CANNOT be undone.`)) {
      // Si cancela, resetear el dropdown
      const select = document.querySelector('select[data-schema-action]') as HTMLSelectElement;
      if (select) select.value = '';
      return;
    }

    try {
      setLoading(true);
      const result = await catalogApi.deactivateSchema(value);
      
      // Recargar datos después de la desactivación
      const response = await catalogApi.getCatalogEntries({
        page,
        limit: 10,
        ...filter,
        search
      });
      setData(response.data);
      setPagination(response.pagination);
      
      alert(`Schema "${value}" deactivated successfully.\n${result.affectedRows} tables affected.`);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error deactivating schema');
    } finally {
      setLoading(false);
      // Resetear el dropdown después de la acción
      const select = document.querySelector('select[data-schema-action]') as HTMLSelectElement;
      if (select) select.value = '';
    }
  };

  return (
    <CatalogContainer>
      {loading && <LoadingOverlay>Loading...</LoadingOverlay>}
      
      <Header>
        DataLake Catalog Manager
      </Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}

      <SearchContainer>
        <SearchInput
          type="text"
          placeholder="Search by schema name, table name, or cluster name..."
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
          onKeyPress={(e) => {
            if (e.key === 'Enter') {
              setSearch(searchInput);
              setPage(1);
            }
          }}
        />
        <SearchButton onClick={() => {
          setSearch(searchInput);
          setPage(1);
        }}>
          Search
        </SearchButton>
        {(search || searchInput) && (
          <ClearSearchButton onClick={() => {
            setSearch('');
            setSearchInput('');
            setPage(1);
          }}>
            Clear
          </ClearSearchButton>
        )}
      </SearchContainer>

      <FiltersContainer>
        <Select 
          value={filter.engine}
          onChange={(e) => setFilter({...filter, engine: e.target.value})}
        >
          <option value="">All Engines</option>
          <option value="MSSQL">MSSQL</option>
          <option value="MariaDB">MariaDB</option>
        </Select>

        <Select
          value={filter.status}
          onChange={(e) => setFilter({...filter, status: e.target.value})}
        >
          <option value="">All Status</option>
          <option value="ERROR">ERROR</option>
          <option value="LISTENING_CHANGES">LISTENING_CHANGES</option>
          <option value="NO_DATA">NO_DATA</option>
          <option value="SKIP">SKIP</option>
        </Select>

        <Select
          value={filter.active}
          onChange={(e) => setFilter({...filter, active: e.target.value})}
        >
          <option value="">All States</option>
          <option value="true">Active</option>
          <option value="false">Inactive</option>
        </Select>

        <Select
          value={filter.strategy}
          onChange={(e) => setFilter({...filter, strategy: e.target.value})}
        >
          <option value="">All Strategies</option>
          <option value="PK">Primary Key</option>
          <option value="OFFSET">Offset</option>
        </Select>

        <ResetButton onClick={() => {
          setFilter({ engine: '', status: '', active: '', strategy: '' });
          setSearch('');
          setSearchInput('');
          setPage(1);
        }}>
          Reset All
        </ResetButton>

        <SchemaActionSelect
          defaultValue=""
          data-schema-action
          onChange={(e) => handleSchemaAction(e.target.value)}
        >
          <option value="">Deactivate Schema</option>
          {availableSchemas.map(schema => (
            <option key={schema} value={schema}>Deactivate {schema}</option>
          ))}
        </SchemaActionSelect>
      </FiltersContainer>

      <PaginationInfo>
        Showing {data.length} of {pagination.total} entries (Page {pagination.currentPage} of {pagination.totalPages})
      </PaginationInfo>

      <TableContainer>
        <Table>
          <thead>
            <tr>
              <Th>Schema.Table</Th>
              <Th>Engine</Th>
              <Th>Status</Th>
              <Th>Active</Th>
              <Th>PK Strategy</Th>
              <Th>Sync Column</Th>
              <Th>Cluster</Th>
              <Th>Actions</Th>
            </tr>
          </thead>
          <tbody>
          {data.map((entry, index) => (
            <TableRow key={index}>
              <Td>{entry.schema_name}.{entry.table_name}</Td>
              <Td>{entry.db_engine}</Td>
              <Td>
                <StatusBadge $status={entry.status}>
                  {entry.status}
                </StatusBadge>
              </Td>
              <Td>
                <ActiveBadge $active={entry.active}>
                  {entry.active ? 'Active' : 'Inactive'}
                </ActiveBadge>
              </Td>
              <Td>{entry.pk_strategy || 'OFFSET'}</Td>
              <Td>{entry.last_sync_column}</Td>
              <Td>{entry.cluster_name}</Td>
              <Td>
                <ActionButton onClick={() => setSelectedEntry(entry)}>
                  Edit
                </ActionButton>
                <ActionButton 
                  onClick={() => handleSkipTable(entry)}
                  style={{ 
                    backgroundColor: '#fff3e0', 
                    color: '#ef6c00', 
                    borderColor: '#ff9800' 
                  }}
                >
                  Skip
                </ActionButton>
              </Td>
            </TableRow>
          ))}
        </tbody>
      </Table>
      </TableContainer>

      <Pagination>
        <PageButton
          disabled={page === 1}
          onClick={() => setPage(page - 1)}
        >
          Previous
        </PageButton>
        
        {Array.from({ length: pagination.totalPages }, (_, i) => i + 1)
          .filter(p => Math.abs(p - page) <= 2 || p === 1 || p === pagination.totalPages)
          .map((p, i, arr) => (
            <React.Fragment key={p}>
              {i > 0 && arr[i - 1] !== p - 1 && <span>...</span>}
              <PageButton
                $active={p === page}
                onClick={() => setPage(p)}
              >
                {p}
              </PageButton>
            </React.Fragment>
          ))
        }
        
        <PageButton
          disabled={page === pagination.totalPages}
          onClick={() => setPage(page + 1)}
        >
          Next
        </PageButton>
      </Pagination>

      {selectedEntry && (
        <EditModal
          entry={selectedEntry}
          onClose={() => setSelectedEntry(null)}
          onSave={handleEdit}
        />
      )}
    </CatalogContainer>
  );
};

export default Catalog;