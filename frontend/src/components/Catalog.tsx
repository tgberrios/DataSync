import { useState, useEffect, useCallback, useRef } from "react";
import EditModal from "./EditModal";
import {
  Container,
  Header,
  FiltersContainer,
  Select,
  TableContainer,
  Table,
  Th,
  Td,
  TableRow,
  StatusBadge,
  Pagination,
  PageButton,
  ErrorMessage,
  LoadingOverlay,
  SearchContainer,
  Input,
  Button,
} from "../components/shared/BaseComponents";
import { usePagination } from "../hooks/usePagination";
import { useTableFilters } from "../hooks/useTableFilters";
import { catalogApi } from "../services/api";
import type { CatalogEntry } from "../services/api";
import { extractApiError } from "../utils/errorHandler";
import { sanitizeSearch } from "../utils/validation";
import styled from "styled-components";
import { theme } from "../theme/theme";

const ActiveBadge = styled.span<{ $active: boolean }>`
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

const ActionButton = styled(Button)`
  padding: 6px 12px;
  margin-right: 5px;
  font-size: 0.9em;
`;

const PaginationInfo = styled.div`
  text-align: center;
  margin-bottom: ${theme.spacing.sm};
  color: ${theme.colors.text.secondary};
  font-size: 0.9em;
  animation: fadeIn 0.25s ease-in;
`;

const SearchInput = styled(Input)`
  flex: 1;
  font-size: 14px;
`;

const SearchButton = styled(Button)`
  padding: 10px 20px;
  font-weight: bold;
`;

const ClearSearchButton = styled(Button)`
  padding: 10px 15px;
`;

const SchemaActionSelect = styled(Select)`
  option {
    background: ${theme.colors.background.main};
    color: ${theme.colors.text.primary};
  }

  option[value=""] {
    color: ${theme.colors.text.secondary};
    font-style: italic;
  }
`;

/**
 * Componente principal del Catálogo que permite gestionar y visualizar
 * todas las tablas sincronizadas en el sistema.
 *
 * @returns {JSX.Element} Componente Catalog renderizado
 */
const Catalog = () => {
  const { page, limit, setPage } = usePagination(1, 10);
  const { filters, setFilter, clearFilters } = useTableFilters({
    engine: "",
    status: "",
    active: "",
    strategy: "",
  });

  const [sortField] = useState("active");
  const [sortDirection] = useState("desc");

  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");
  const [availableSchemas, setAvailableSchemas] = useState<string[]>([]);
  const [data, setData] = useState<CatalogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedEntry, setSelectedEntry] = useState<CatalogEntry | null>(null);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 10,
  });
  const isMountedRef = useRef(true);

  /**
   * Carga los schemas únicos disponibles desde la API
   *
   * @returns {Promise<void>}
   */
  const fetchSchemas = useCallback(async () => {
    try {
      const schemas = await catalogApi.getSchemas();
      if (isMountedRef.current) {
        setAvailableSchemas(schemas);
      }
    } catch (err) {
      console.error("Error loading schemas:", err);
    }
  }, []);

  /**
   * Carga los datos del catálogo desde la API con los filtros y paginación actuales
   *
   * @returns {Promise<void>}
   */
  const fetchData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(search, 100);
      const response = await catalogApi.getCatalogEntries({
        page,
        limit,
        engine: filters.engine as string,
        status: filters.status as string,
        active: filters.active as string,
        search: sanitizedSearch,
        sort_field: sortField,
        sort_direction: sortDirection,
      });
      if (isMountedRef.current) {
        setData(response.data);
        setPagination(response.pagination);
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    } finally {
      if (isMountedRef.current) {
        setLoading(false);
      }
    }
  }, [
    page, 
    limit, 
    filters.engine, 
    filters.status, 
    filters.active, 
    search, 
    sortField, 
    sortDirection
  ]);


  /**
   * Maneja la edición de una entrada del catálogo
   *
   * @param {CatalogEntry} updatedEntry - Entrada actualizada
   * @returns {Promise<void>}
   */
  const handleEdit = useCallback(
    async (updatedEntry: CatalogEntry) => {
      try {
        setLoading(true);
        await catalogApi.updateEntry(updatedEntry);
        await fetchData();
        setSelectedEntry(null);
      } catch (err) {
        setError(extractApiError(err));
      } finally {
        setLoading(false);
      }
    },
    [fetchData]
  );

  /**
   * Maneja el marcado de una tabla como SKIP
   *
   * @param {CatalogEntry} entry - Entrada del catálogo a marcar como SKIP
   * @returns {Promise<void>}
   */
  const handleSkipTable = useCallback(
    async (entry: CatalogEntry) => {
      if (
        !confirm(
          `Are you sure you want to mark table "${entry.schema_name}.${entry.table_name}" as SKIP?\n\nThis will:\n- Set status to 'SKIP'\n- Set active to false (table will not be processed)\n- Reset offset to 0\n\nThis action CANNOT be undone.`
        )
      ) {
        return;
      }

      try {
        setLoading(true);
        await catalogApi.skipTable(
          entry.schema_name,
          entry.table_name,
          entry.db_engine
        );
        await fetchData();
        alert(
          `Table "${entry.schema_name}.${entry.table_name}" marked as SKIP successfully.`
        );
      } catch (err) {
        setError(extractApiError(err));
      } finally {
        setLoading(false);
      }
    },
    [fetchData]
  );

  /**
   * Maneja la desactivación de todas las tablas de un schema
   *
   * @param {string} schemaName - Nombre del schema a desactivar
   * @returns {Promise<void>}
   */
  const handleSchemaAction = useCallback(
    async (schemaName: string) => {
      if (!schemaName || schemaName === "") return;

      if (
        !confirm(
          `Are you sure you want to deactivate ALL tables in schema "${schemaName}"?\n\nThis will change status to 'SKIPPED' and reset offsets to 0.\n\nThis action CANNOT be undone.`
        )
      ) {
        const select = document.querySelector(
          'select[data-schema-action]'
        ) as HTMLSelectElement;
        if (select) select.value = "";
        return;
      }

      try {
        setLoading(true);
        const result = await catalogApi.deactivateSchema(schemaName);
        await fetchData();
        alert(
          `Schema "${schemaName}" deactivated successfully.\n${result.affectedRows} tables affected.`
        );
      } catch (err) {
        setError(extractApiError(err));
      } finally {
        setLoading(false);
        const select = document.querySelector(
          'select[data-schema-action]'
        ) as HTMLSelectElement;
        if (select) select.value = "";
      }
    },
    [fetchData]
  );

  /**
   * Maneja la búsqueda con debounce
   */
  useEffect(() => {
    const timer = setTimeout(() => {
      setSearch(searchInput);
      setPage(1);
    }, 500);

    return () => clearTimeout(timer);
  }, [searchInput, setPage]);

  useEffect(() => {
    isMountedRef.current = true;
    fetchSchemas();
    return () => {
      isMountedRef.current = false;
    };
  }, [fetchSchemas]);

  useEffect(() => {
    fetchData();
    const interval = setInterval(() => {
      fetchData();
    }, 30000);
    return () => clearInterval(interval);
  }, [fetchData]);


  return (
    <Container>
      {loading && <LoadingOverlay>Loading...</LoadingOverlay>}

      <Header>DataLake Catalog Manager</Header>

      {error && <ErrorMessage>{error}</ErrorMessage>}

      <SearchContainer>
        <SearchInput
          type="text"
          placeholder="Search by schema name, table name, or cluster name..."
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
          onKeyPress={(e) => {
            if (e.key === "Enter") {
              setSearch(searchInput);
              setPage(1);
            }
          }}
        />
        <SearchButton
          $variant="primary"
          onClick={() => {
            setSearch(searchInput);
            setPage(1);
          }}
        >
          Search
        </SearchButton>
        {(search || searchInput) && (
          <ClearSearchButton
            $variant="secondary"
            onClick={() => {
              setSearch("");
              setSearchInput("");
              setPage(1);
            }}
          >
            Clear
          </ClearSearchButton>
        )}
      </SearchContainer>

      <FiltersContainer>
        <Select
          value={filters.engine as string}
          onChange={(e) => setFilter("engine", e.target.value)}
        >
          <option value="">All Engines</option>
          <option value="MSSQL">MSSQL</option>
          <option value="MariaDB">MariaDB</option>
          <option value="MongoDB">MongoDB</option>
          <option value="Oracle">Oracle</option>
          <option value="PostgreSQL">PostgreSQL</option>
        </Select>

        <Select
          value={filters.status as string}
          onChange={(e) => setFilter("status", e.target.value)}
        >
          <option value="">All Status</option>
          <option value="ERROR">ERROR</option>
          <option value="LISTENING_CHANGES">LISTENING_CHANGES</option>
          <option value="NO_DATA">NO_DATA</option>
          <option value="SKIP">SKIP</option>
          <option value="IN_PROGRESS">IN_PROGRESS</option>
        </Select>

        <Select
          value={filters.active as string}
          onChange={(e) => setFilter("active", e.target.value)}
        >
          <option value="">All States</option>
          <option value="true">Active</option>
          <option value="false">Inactive</option>
        </Select>

        <Select
          value={filters.strategy as string}
          onChange={(e) => setFilter("strategy", e.target.value)}
        >
          <option value="">All Strategies</option>
          <option value="PK">Primary Key</option>
          <option value="OFFSET">Offset</option>
        </Select>

        <Button
          $variant="secondary"
          onClick={() => {
            clearFilters();
            setSearch("");
            setSearchInput("");
            setPage(1);
          }}
        >
          Reset All
        </Button>

        <SchemaActionSelect
          defaultValue=""
          data-schema-action
          onChange={(e) => handleSchemaAction(e.target.value)}
        >
          <option value="">Deactivate Schema</option>
          {availableSchemas.map((schema) => (
            <option key={schema} value={schema}>
              Deactivate {schema}
            </option>
          ))}
        </SchemaActionSelect>
      </FiltersContainer>

      <PaginationInfo>
        Showing {data.length} of {pagination.total} entries (Page{" "}
        {pagination.currentPage} of {pagination.totalPages})
      </PaginationInfo>

      <TableContainer>
        <Table $minWidth="1200px">
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
              <TableRow key={`${entry.schema_name}-${entry.table_name}-${entry.db_engine}-${index}`}>
                <Td>
                  {entry.schema_name}.{entry.table_name}
                </Td>
                <Td>{entry.db_engine}</Td>
                <Td>
                  <StatusBadge $status={entry.status}>
                    {entry.status}
                  </StatusBadge>
                </Td>
                <Td>
                  <ActiveBadge $active={entry.active}>
                    {entry.active ? "Active" : "Inactive"}
                  </ActiveBadge>
                </Td>
                <Td>{entry.pk_strategy || "OFFSET"}</Td>
                <Td>{entry.last_sync_column || "-"}</Td>
                <Td>{entry.cluster_name || "-"}</Td>
                <Td>
                  <ActionButton
                    $variant="secondary"
                    onClick={() => setSelectedEntry(entry)}
                  >
                    Edit
                  </ActionButton>
                  <ActionButton
                    $variant="secondary"
                    onClick={() => handleSkipTable(entry)}
                    style={{
                      backgroundColor: theme.colors.status.warning.bg,
                      color: theme.colors.status.warning.text,
                      borderColor: theme.colors.status.warning.text,
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
          $active={false}
          disabled={page === 1}
          onClick={() => setPage(page - 1)}
        >
          Previous
        </PageButton>

        {Array.from({ length: pagination.totalPages }, (_, i) => i + 1)
          .filter(
            (p) =>
              Math.abs(p - page) <= 2 ||
              p === 1 ||
              p === pagination.totalPages
          )
          .map((p, i, arr) => (
            <span key={p}>
              {i > 0 && arr[i - 1] !== p - 1 && <span>...</span>}
              <PageButton
                $active={p === page}
                onClick={() => setPage(p)}
              >
                {p}
              </PageButton>
            </span>
          ))}

        <PageButton
          $active={false}
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
    </Container>
  );
};

export default Catalog;
