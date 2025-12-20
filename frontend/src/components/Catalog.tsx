import { useState, useEffect, useCallback, useRef } from "react";
import EditModal from "./EditModal";
import AddTableModal from "./AddTableModal";
import CatalogTreeView from "./CatalogTreeView";
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
import styled, { keyframes } from "styled-components";
import { theme } from "../theme/theme";

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

const AnimatedTableRow = styled(TableRow)<{ $delay?: number }>`
  animation: ${fadeIn} 0.3s ease-out;
  animation-delay: ${props => props.$delay || 0}s;
  animation-fill-mode: both;
  border-bottom: none !important;
  
  &:hover {
    ${Td} {
      border-bottom: none !important;
    }
  }
`;

const CleanTd = styled(Td)`
  border-bottom: none !important;
`;

const TooltipTd = styled(CleanTd)`
  overflow: visible !important;
  position: relative;
  z-index: 1;
`;

const TableWrapper = styled.div`
  position: relative;
  overflow-x: auto;
  overflow-y: visible;
`;

const StyledTable = styled(Table)`
  overflow: visible !important;
`;

const ActiveBadge = styled.span<{ $active: boolean }>`
  padding: 6px 12px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.85em;
  font-weight: 600;
  display: inline-flex;
  align-items: center;
  gap: 4px;
  transition: all ${theme.transitions.normal};
  background: ${(props) => (props.$active ? theme.colors.status.success.bg : theme.colors.status.error.bg)};
  color: ${(props) => (props.$active ? theme.colors.status.success.text : theme.colors.status.error.text)};
  border: 1px solid ${(props) => (props.$active ? theme.colors.status.success.text + "30" : theme.colors.status.error.text + "30")};
  box-shadow: ${theme.shadows.sm};

  &:hover {
    transform: translateY(-2px) scale(1.05);
    box-shadow: ${theme.shadows.md};
  }
  
  &::before {
    content: "${props => props.$active ? '✓' : '✗'}";
    font-weight: bold;
  }
`;

const ActionButton = styled(Button)`
  padding: 8px 16px;
  margin-right: 8px;
  font-size: 0.9em;
  font-weight: 500;
  border-radius: ${theme.borderRadius.md};
  transition: all ${theme.transitions.normal};
  display: inline-flex;
  align-items: center;
  gap: 6px;
  
  &:hover {
    transform: translateY(-2px);
    box-shadow: ${theme.shadows.md};
  }
  
  &:active {
    transform: translateY(0);
  }
`;

const PaginationInfo = styled.div`
  text-align: center;
  margin-bottom: ${theme.spacing.sm};
  color: ${theme.colors.text.secondary};
  font-size: 0.9em;
  animation: ${fadeIn} 0.3s ease-out;
  font-weight: 500;
`;

const TableActions = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: ${theme.spacing.md};
  gap: ${theme.spacing.sm};
  padding: ${theme.spacing.md};
  background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
  border-radius: ${theme.borderRadius.md};
  border: 1px solid ${theme.colors.border.light};
  animation: ${fadeIn} 0.3s ease-out;
  box-shadow: ${theme.shadows.sm};
`;

const ExportButton = styled(Button)`
  padding: 8px 16px;
  font-size: 0.9em;
  display: flex;
  align-items: center;
  gap: 6px;
  font-weight: 500;
  border-radius: ${theme.borderRadius.md};
  transition: all ${theme.transitions.normal};
  
  &:hover {
    transform: translateY(-2px);
    box-shadow: ${theme.shadows.md};
  }
`;

const SortableTh = styled(Th)<{ $sortable?: boolean; $active?: boolean; $direction?: "asc" | "desc" }>`
  cursor: ${props => props.$sortable ? "pointer" : "default"};
  user-select: none;
  position: relative;
  transition: all ${theme.transitions.normal};
  font-weight: 600;
  
  ${props => props.$sortable && `
    &:hover {
      background: linear-gradient(135deg, ${theme.colors.primary.light}15 0%, ${theme.colors.primary.main}10 100%);
      color: ${theme.colors.primary.main};
      transform: translateY(-1px);
    }
  `}
  
  ${props => props.$active && `
    background: linear-gradient(135deg, ${theme.colors.primary.main} 0%, ${theme.colors.primary.light} 100%);
    color: ${theme.colors.text.white};
    box-shadow: ${theme.shadows.sm};
    
    &::after {
      content: "${props.$direction === "asc" ? "▲" : "▼"}";
      position: absolute;
      right: 12px;
      font-size: 0.75em;
      opacity: 0.9;
    }
  `}
`;

const SearchInput = styled(Input)`
  flex: 1;
  font-size: 14px;
  transition: all ${theme.transitions.normal};
  border: 2px solid ${theme.colors.border.light};
  
  &:focus {
    border-color: ${theme.colors.primary.main};
    box-shadow: 0 0 0 3px ${theme.colors.primary.main}15;
    outline: none;
  }
`;

const SearchButton = styled(Button)`
  padding: 10px 20px;
  font-weight: 600;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    transform: translateY(-2px);
    box-shadow: ${theme.shadows.md};
  }
`;

const ClearSearchButton = styled(Button)`
  padding: 10px 15px;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    transform: translateY(-2px);
    box-shadow: ${theme.shadows.md};
  }
`;

const FilterSelect = styled(Select)`
  padding: 10px 16px;
  font-size: 0.95em;
  font-weight: 500;
  border: 2px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.md};
  background: linear-gradient(135deg, ${theme.colors.background.main} 0%, ${theme.colors.background.secondary} 100%);
  color: ${theme.colors.text.primary};
  transition: all ${theme.transitions.normal};
  cursor: pointer;
  box-shadow: ${theme.shadows.sm};
  
  &:hover {
    border-color: ${theme.colors.primary.main};
    box-shadow: ${theme.shadows.md};
    transform: translateY(-2px);
  }
  
  &:focus {
    outline: none;
    border-color: ${theme.colors.primary.main};
    box-shadow: 0 0 0 3px ${theme.colors.primary.main}15;
  }
  
  option {
    background: ${theme.colors.background.main};
    color: ${theme.colors.text.primary};
    padding: 8px;
  }

  option[value=""] {
    color: ${theme.colors.text.secondary};
    font-style: italic;
    font-weight: 600;
  }
`;

const ResetButton = styled(Button)`
  padding: 10px 20px;
  font-weight: 600;
  border-radius: ${theme.borderRadius.md};
  transition: all ${theme.transitions.normal};
  background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
  border: 2px solid ${theme.colors.border.medium};
  box-shadow: ${theme.shadows.sm};
  
  &:hover {
    background: linear-gradient(135deg, ${theme.colors.primary.light}10 0%, ${theme.colors.primary.main}08 100%);
    border-color: ${theme.colors.primary.main};
    transform: translateY(-2px);
    box-shadow: ${theme.shadows.md};
  }
`;

const Tooltip = styled.div`
  position: relative;
  display: inline-block;
  overflow: visible;
  
  &:hover .tooltip-content {
    visibility: visible;
    opacity: 1;
  }
`;

const TooltipContent = styled.div`
  visibility: hidden;
  opacity: 0;
  position: absolute;
  z-index: 10000;
  bottom: calc(100% + 10px);
  left: 50%;
  transform: translateX(-50%);
  background-color: ${theme.colors.text.primary};
  color: ${theme.colors.text.white};
  text-align: left;
  border-radius: ${theme.borderRadius.sm};
  padding: 10px 14px;
  font-size: 0.85em;
  white-space: normal;
  min-width: 200px;
  max-width: 350px;
  box-shadow: ${theme.shadows.lg};
  transition: opacity ${theme.transitions.normal};
  line-height: 1.4;
  pointer-events: none;
  
  &:after {
    content: "";
    position: absolute;
    top: 100%;
    left: 50%;
    margin-left: -5px;
    border-width: 5px;
    border-style: solid;
    border-color: ${theme.colors.text.primary} transparent transparent transparent;
  }
`;

const FilterWithTooltip = styled.div`
  position: relative;
  display: inline-block;
`;

const StyledFiltersContainer = styled(FiltersContainer)`
  background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
  border: 1px solid ${theme.colors.border.light};
  box-shadow: ${theme.shadows.md};
  padding: ${theme.spacing.lg};
  animation: ${fadeIn} 0.3s ease-out;
`;

const SchemaActionSelect = styled(FilterSelect)`
  background: linear-gradient(135deg, ${theme.colors.primary.light}08 0%, ${theme.colors.primary.main}05 100%);
  border-color: ${theme.colors.primary.main}40;
  color: ${theme.colors.primary.main};
  font-weight: 600;
  
  &:hover {
    border-color: ${theme.colors.primary.main};
    background: linear-gradient(135deg, ${theme.colors.primary.main}15 0%, ${theme.colors.primary.light}10 100%);
    transform: translateY(-2px);
    box-shadow: ${theme.shadows.md};
  }
`;

const BulkActionsContainer = styled.div`
  display: flex;
  align-items: center;
  gap: ${theme.spacing.md};
  padding: ${theme.spacing.md};
  background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
  border-radius: ${theme.borderRadius.md};
  border: 2px solid ${theme.colors.border.medium};
  margin-bottom: ${theme.spacing.md};
  animation: ${fadeIn} 0.3s ease-out;
`;

const BulkActionButton = styled(Button)`
  padding: 10px 20px;
  font-weight: 600;
  border-radius: ${theme.borderRadius.md};
  transition: all ${theme.transitions.normal};
  box-shadow: ${theme.shadows.sm};
  
  &:hover:not(:disabled) {
    transform: translateY(-2px);
    box-shadow: ${theme.shadows.md};
  }
  
  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
`;

const CheckboxInput = styled.input`
  width: 18px;
  height: 18px;
  cursor: pointer;
  accent-color: ${theme.colors.primary.main};
`;

const SelectAllCheckbox = styled.div`
  display: flex;
  align-items: center;
  gap: ${theme.spacing.sm};
  font-weight: 600;
  color: ${theme.colors.text.primary};
`;

/**
 * Componente principal del Catálogo que permite gestionar y visualizar
 * todas las tablas sincronizadas en el sistema.
 *
 * @returns {JSX.Element} Componente Catalog renderizado
 */
const Catalog = () => {
  const { page, limit, setPage: setPageOriginal } = usePagination(1, 10);
  
  // Wrapper para setPage que agrega logs y previene resets no deseados
  const setPage = useCallback((newPage: number) => {
    const stack = new Error().stack;
    console.log("🔴 setPage called with:", newPage, "Current page:", page);
    console.log("🔴 Call stack:", stack?.split('\n').slice(1, 4).join('\n'));
    setPageOriginal(newPage);
  }, [page, setPageOriginal]);
  const { filters, setFilter, clearFilters } = useTableFilters({
    engine: "",
    status: "",
    active: "",
    strategy: "",
  });

  const [sortField, setSortField] = useState("active");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");

  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");
  const [availableSchemas, setAvailableSchemas] = useState<string[]>([]);
  const [availableEngines, setAvailableEngines] = useState<string[]>([]);
  const [availableStatuses, setAvailableStatuses] = useState<string[]>([]);
  const [availableStrategies, setAvailableStrategies] = useState<string[]>([]);
  const [data, setData] = useState<CatalogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedEntry, setSelectedEntry] = useState<CatalogEntry | null>(null);
  const [showAddModal, setShowAddModal] = useState(false);
  const [selectedEntries, setSelectedEntries] = useState<Set<string>>(new Set());
  const [viewMode, setViewMode] = useState<"table" | "tree">("tree");
  const [allEntries, setAllEntries] = useState<CatalogEntry[]>([]);
  const [loadingTree, setLoadingTree] = useState(false);
  const [pagination, setPagination] = useState({
    total: 0,
    totalPages: 0,
    currentPage: 1,
    limit: 10,
  });
  const isMountedRef = useRef(true);
  const currentPageRef = useRef(page);
  const fetchDataRef = useRef<() => Promise<void>>();
  const lastFetchedPageRef = useRef<number | null>(null);

  /**
   * Carga los valores únicos disponibles desde la API
   *
   * @returns {Promise<void>}
   */
  const fetchFilterOptions = useCallback(async () => {
    try {
      const [schemas, engines, statuses, strategies] = await Promise.all([
        catalogApi.getSchemas(),
        catalogApi.getEngines(),
        catalogApi.getStatuses(),
        catalogApi.getStrategies(),
      ]);
      if (isMountedRef.current) {
        setAvailableSchemas(schemas);
        setAvailableEngines(engines);
        setAvailableStatuses(statuses);
        setAvailableStrategies(strategies);
      }
    } catch (err) {
      console.error("Error loading filter options:", err);
    }
  }, []);

  /**
   * Carga los datos del catálogo desde la API con los filtros y paginación actuales
   * Esta función siempre usa currentPageRef.current para obtener la página actual
   *
   * @returns {Promise<void>}
   */
  const fetchDataForCurrentPage = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoading(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(search, 100);
      // Siempre usar currentPageRef.current para obtener la página actual
      const currentPage = currentPageRef.current;
      const params = {
        page: currentPage,
        limit,
        engine: filters.engine as string,
        status: filters.status as string,
        active: filters.active as string,
        search: sanitizedSearch,
        sort_field: sortField,
        sort_direction: sortDirection,
      };
      
      console.log("=== CATALOG FRONTEND DEBUG ===");
      console.log("🔵 Request params:", params);
      console.log("🔵 currentPageRef.current:", currentPageRef.current);
      console.log("🔵 page state:", page);
      
      const response = await catalogApi.getCatalogEntries(params);
      
      console.log("🟢 API Response:", response);
      console.log("🟢 Response data length:", response?.data?.length || 0);
      console.log("🟢 Response pagination:", response?.pagination);
      console.log("🟢 First item sample:", response?.data?.[0] ? JSON.stringify(response.data[0], null, 2) : "No data");
      console.log("=== END FRONTEND DEBUG ===");
      
      if (isMountedRef.current) {
        setData(response.data || []);
        // Preservar siempre la página actual del cliente, no la del servidor
        const serverPagination = response.pagination || {
          total: 0,
          totalPages: 0,
          currentPage: 1,
          limit: 10
        };
        // Usar la página actual del ref (que se actualiza cuando el usuario cambia de página)
        const preservedPage = currentPageRef.current;
        const finalPage = preservedPage <= serverPagination.totalPages ? preservedPage : (serverPagination.totalPages > 0 ? serverPagination.totalPages : 1);
        
        console.log("🟡 Setting pagination - preservedPage:", preservedPage, "finalPage:", finalPage, "serverPage:", serverPagination.currentPage);
        
        // Actualizar paginación con la página preservada
        // NO actualizar page del hook aquí - dejamos que el usuario controle page directamente
        setPagination({
          ...serverPagination,
          currentPage: finalPage
        });
      }
    } catch (err) {
      console.error("Catalog fetch error:", err);
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    } finally {
      if (isMountedRef.current) {
        setLoading(false);
      }
    }
  }, [
    limit, 
    filters.engine, 
    filters.status, 
    filters.active, 
    search, 
    sortField, 
    sortDirection
  ]);

  const fetchAllEntries = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      setLoadingTree(true);
      setError(null);
      const sanitizedSearch = sanitizeSearch(search, 100);
      const params = {
        page: 1,
        limit: 10000,
        engine: filters.engine as string,
        status: filters.status as string,
        active: filters.active as string,
        search: sanitizedSearch,
        sort_field: sortField,
        sort_direction: sortDirection,
      };
      
      const response = await catalogApi.getCatalogEntries(params);
      if (isMountedRef.current) {
        setAllEntries(response.data || []);
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    } finally {
      if (isMountedRef.current) {
        setLoadingTree(false);
      }
    }
  }, [filters.engine, filters.status, filters.active, search, sortField, sortDirection]);

  // Alias para compatibilidad con código existente
  const fetchData = fetchDataForCurrentPage;


  /**
   * Maneja la creación de una nueva entrada del catálogo
   *
   * @param {Omit<CatalogEntry, 'last_sync_time' | 'updated_at'>} newEntry - Nueva entrada
   * @returns {Promise<void>}
   */
  const handleAdd = useCallback(
    async (newEntry: Omit<CatalogEntry, 'last_sync_time' | 'updated_at'>) => {
      try {
        setLoading(true);
        setError(null);
        await catalogApi.createEntry(newEntry);
        await fetchData();
        setShowAddModal(false);
        alert(`Table "${newEntry.schema_name}.${newEntry.table_name}" added successfully.`);
      } catch (err) {
        setError(extractApiError(err));
      } finally {
        setLoading(false);
      }
    },
    [fetchData]
  );

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

  const handleSort = useCallback((field: string) => {
    if (sortField === field) {
      setSortDirection(prev => prev === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDirection("asc");
    }
    setPage(1);
  }, [sortField, setPage]);

  const getEntryKey = useCallback((entry: CatalogEntry) => {
    return `${entry.schema_name}-${entry.table_name}-${entry.db_engine}`;
  }, []);

  const handleSelectAll = useCallback((checked: boolean) => {
    if (checked) {
      const allKeys = new Set(data.map(entry => getEntryKey(entry)));
      setSelectedEntries(allKeys);
    } else {
      setSelectedEntries(new Set());
    }
  }, [data, getEntryKey]);

  const handleSelectEntry = useCallback((entry: CatalogEntry, checked: boolean) => {
    setSelectedEntries(prev => {
      const newSet = new Set(prev);
      const key = getEntryKey(entry);
      if (checked) {
        newSet.add(key);
      } else {
        newSet.delete(key);
      }
      return newSet;
    });
  }, [getEntryKey]);

  const handleBulkActivate = useCallback(async () => {
    if (selectedEntries.size === 0) return;
    
    if (!confirm(`Are you sure you want to activate ${selectedEntries.size} table(s)?`)) {
      return;
    }

    try {
      setLoading(true);
      const promises = data
        .filter(entry => selectedEntries.has(getEntryKey(entry)))
        .map(entry => 
          catalogApi.updateEntryStatus(
            entry.schema_name,
            entry.table_name,
            entry.db_engine,
            true
          )
        );
      
      await Promise.all(promises);
      setSelectedEntries(new Set());
      await fetchData();
    } catch (err) {
      setError(extractApiError(err));
    } finally {
      setLoading(false);
    }
  }, [selectedEntries, data, getEntryKey, fetchData]);

  const handleBulkDeactivate = useCallback(async () => {
    if (selectedEntries.size === 0) return;
    
    if (!confirm(`Are you sure you want to deactivate ${selectedEntries.size} table(s)?`)) {
      return;
    }

    try {
      setLoading(true);
      const promises = data
        .filter(entry => selectedEntries.has(getEntryKey(entry)))
        .map(entry => 
          catalogApi.updateEntryStatus(
            entry.schema_name,
            entry.table_name,
            entry.db_engine,
            false
          )
        );
      
      await Promise.all(promises);
      setSelectedEntries(new Set());
      await fetchData();
    } catch (err) {
      setError(extractApiError(err));
    } finally {
      setLoading(false);
    }
  }, [selectedEntries, data, getEntryKey, fetchData]);

  const handleExportCSV = useCallback(() => {
    const headers = ["Schema", "Table", "Engine", "Status", "Active", "Strategy", "Cluster"];
    const rows = data.map(entry => [
      entry.schema_name,
      entry.table_name,
      entry.db_engine,
      entry.status,
      entry.active ? "Yes" : "No",
      entry.pk_strategy || "OFFSET",
      entry.cluster_name || ""
    ]);
    
    const csvContent = [
      headers.join(","),
      ...rows.map(row => row.map(cell => `"${String(cell).replace(/"/g, '""')}"`).join(","))
    ].join("\n");
    
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    link.setAttribute("href", url);
    link.setAttribute("download", `catalog_export_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = "hidden";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }, [data]);

  /**
   * Maneja la búsqueda con debounce
   */
  useEffect(() => {
    const timer = setTimeout(() => {
      setSearch(searchInput);
      setPage(1);
    }, 500);

    return () => clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchInput]); // Removido setPage de dependencias para evitar loops

  useEffect(() => {
    isMountedRef.current = true;
    fetchFilterOptions();
    return () => {
      isMountedRef.current = false;
    };
  }, [fetchFilterOptions]);

  // Limpiar selección solo cuando cambian los filtros o búsqueda, no cuando cambia la página
  useEffect(() => {
    setSelectedEntries(new Set());
  }, [filters.engine, filters.status, filters.active, filters.strategy, search]);

  // Guardar la función fetchDataForCurrentPage en un ref para que el intervalo siempre use la versión más reciente
  useEffect(() => {
    fetchDataRef.current = fetchDataForCurrentPage;
  }, [fetchDataForCurrentPage]);

  // Actualizar ref cuando cambia la página
  useEffect(() => {
    console.log("🔵 PAGE CHANGED TO:", page, "Previous ref value:", currentPageRef.current);
    currentPageRef.current = page;
    console.log("🔵 Updated ref to:", currentPageRef.current);
  }, [page]);

  // Ejecutar fetchData cuando cambia la página (separado para evitar loops)
  useEffect(() => {
    // Solo ejecutar si la página realmente cambió y no la hemos cargado ya
    // O si es la primera carga (lastFetchedPageRef.current es null)
    if ((lastFetchedPageRef.current === null || lastFetchedPageRef.current !== page) && fetchDataRef.current) {
      console.log("🟢 FETCHING DATA FOR PAGE:", page, "Last fetched was:", lastFetchedPageRef.current);
      lastFetchedPageRef.current = page;
      fetchDataRef.current();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [page]); // Solo dependemos de page

  // Cargar todas las entradas cuando se cambia a tree view
  useEffect(() => {
    if (viewMode === "tree") {
      fetchAllEntries();
    }
  }, [viewMode, fetchAllEntries]);

  // Configurar el intervalo de actualización (solo una vez al montar)
  useEffect(() => {
    const interval = setInterval(() => {
      if (fetchDataRef.current && isMountedRef.current) {
        fetchDataRef.current();
        if (viewMode === "tree") {
          fetchAllEntries();
        }
      }
    }, 30000);
    
    return () => clearInterval(interval);
  }, [viewMode, fetchAllEntries]); // Sin dependencias - solo se ejecuta una vez al montar


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

      <StyledFiltersContainer>
        <FilterWithTooltip>
          <Tooltip>
            <FilterSelect
              value={filters.engine as string}
              onChange={(e: React.ChangeEvent<HTMLSelectElement>) => {
                setFilter("engine", e.target.value);
                setPage(1);
              }}
            >
              <option value="">All Engines ({availableEngines.length})</option>
              {availableEngines.map((engine) => (
                <option key={engine} value={engine}>
                  {engine}
                </option>
              ))}
            </FilterSelect>
            <TooltipContent className="tooltip-content">
              Filter tables by database engine type. Shows only engines that exist in the catalog.
            </TooltipContent>
          </Tooltip>
        </FilterWithTooltip>

        <FilterWithTooltip>
          <Tooltip>
            <FilterSelect
              value={filters.status as string}
              onChange={(e: React.ChangeEvent<HTMLSelectElement>) => {
                setFilter("status", e.target.value);
                setPage(1);
              }}
            >
              <option value="">All Status ({availableStatuses.length})</option>
              {availableStatuses.map((status) => (
                <option key={status} value={status}>
                  {status}
                </option>
              ))}
            </FilterSelect>
            <TooltipContent className="tooltip-content">
              Filter tables by synchronization status. Shows only statuses that exist in the catalog.
            </TooltipContent>
          </Tooltip>
        </FilterWithTooltip>

        <FilterWithTooltip>
          <Tooltip>
            <FilterSelect
              value={filters.active as string}
              onChange={(e: React.ChangeEvent<HTMLSelectElement>) => {
                setFilter("active", e.target.value);
                setPage(1);
              }}
            >
              <option value="">All States</option>
              <option value="true">Active</option>
              <option value="false">Inactive</option>
            </FilterSelect>
            <TooltipContent className="tooltip-content">
              Filter tables by active state. Active tables are processed during synchronization.
            </TooltipContent>
          </Tooltip>
        </FilterWithTooltip>

        <FilterWithTooltip>
          <Tooltip>
            <FilterSelect
              value={filters.strategy as string}
              onChange={(e: React.ChangeEvent<HTMLSelectElement>) => {
                setFilter("strategy", e.target.value);
                setPage(1);
              }}
            >
              <option value="">All Strategies ({availableStrategies.length})</option>
              {availableStrategies.map((strategy) => (
                <option key={strategy} value={strategy}>
                  {strategy === "PK" ? "Primary Key" : strategy === "OFFSET" ? "Offset" : strategy === "CDC" ? "CDC" : strategy}
                </option>
              ))}
            </FilterSelect>
            <TooltipContent className="tooltip-content">
              Filter tables by primary key strategy. Shows only strategies that exist in the catalog.
            </TooltipContent>
          </Tooltip>
        </FilterWithTooltip>

        <Tooltip>
          <ResetButton
            $variant="secondary"
            onClick={() => {
              clearFilters();
              setSearch("");
              setSearchInput("");
              setPage(1);
            }}
          >
            Reset All
          </ResetButton>
          <TooltipContent className="tooltip-content">
            Clear all filters and search terms to show all catalog entries.
          </TooltipContent>
        </Tooltip>

        <FilterWithTooltip>
          <Tooltip>
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
            <TooltipContent className="tooltip-content">
              Deactivate all tables in a schema. This will set status to SKIP and reset offsets. This action cannot be undone.
            </TooltipContent>
          </Tooltip>
        </FilterWithTooltip>
      </StyledFiltersContainer>

      {selectedEntries.size > 0 && (
        <BulkActionsContainer>
          <SelectAllCheckbox>
            <span>{selectedEntries.size} selected</span>
          </SelectAllCheckbox>
          <BulkActionButton
            $variant="primary"
            onClick={handleBulkActivate}
          >
            Activate Selected
          </BulkActionButton>
          <BulkActionButton
            $variant="secondary"
            onClick={handleBulkDeactivate}
            style={{
              background: `linear-gradient(135deg, ${theme.colors.status.error.bg} 0%, ${theme.colors.status.error.text}15 100%)`,
              color: theme.colors.status.error.text,
              borderColor: theme.colors.status.error.text,
            }}
          >
            Deactivate Selected
          </BulkActionButton>
          <BulkActionButton
            $variant="secondary"
            onClick={() => setSelectedEntries(new Set())}
          >
            Clear Selection
          </BulkActionButton>
        </BulkActionsContainer>
      )}

      <TableActions>
        <PaginationInfo>
          {viewMode === "table" 
            ? `Showing ${data.length} of ${pagination.total} entries (Page ${pagination.currentPage} of ${pagination.totalPages})`
            : `Total: ${allEntries.length} entries`
          }
        </PaginationInfo>
        <div style={{ display: 'flex', gap: theme.spacing.sm }}>
          <Button
            $variant={viewMode === "table" ? "primary" : "secondary"}
            onClick={() => setViewMode("table")}
            style={{ padding: "6px 12px", fontSize: "0.85em" }}
          >
            Table View
          </Button>
          <Button
            $variant={viewMode === "tree" ? "primary" : "secondary"}
            onClick={() => {
              setViewMode("tree");
              fetchAllEntries();
            }}
            style={{ padding: "6px 12px", fontSize: "0.85em" }}
          >
            Tree View
          </Button>
          <Button
            $variant="primary"
            onClick={() => setShowAddModal(true)}
            style={{ display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            Add Table
          </Button>
          <ExportButton $variant="secondary" onClick={handleExportCSV}>
            Export CSV
          </ExportButton>
        </div>
      </TableActions>

      {viewMode === "tree" ? (
        loadingTree ? (
          <LoadingOverlay>Loading tree view...</LoadingOverlay>
        ) : (
          <CatalogTreeView 
            entries={allEntries}
            onEntryClick={(entry) => setSelectedEntry(entry)}
          />
        )
      ) : (
      <TableContainer>
        <TableWrapper>
          <StyledTable $minWidth="1200px">
          <thead>
            <tr>
              <Th style={{ width: '50px' }}>
                <CheckboxInput
                  type="checkbox"
                  checked={selectedEntries.size > 0 && selectedEntries.size === data.length}
                  onChange={(e) => handleSelectAll(e.target.checked)}
                />
              </Th>
              <SortableTh 
                $sortable 
                $active={sortField === "schema_name"} 
                $direction={sortDirection}
                onClick={() => handleSort("schema_name")}
              >
                Schema Table
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "db_engine"} 
                $direction={sortDirection}
                onClick={() => handleSort("db_engine")}
              >
                Engine
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "status"} 
                $direction={sortDirection}
                onClick={() => handleSort("status")}
              >
                <Tooltip>
                  Status
                  <TooltipContent className="tooltip-content">
                    <strong>FULL_LOAD:</strong> Performing initial complete data synchronization.<br/><br/>
                    <strong>LISTENING_CHANGES:</strong> Actively monitoring and syncing real-time changes.<br/><br/>
                    <strong>ERROR:</strong> Synchronization failed. Check logs for details.<br/><br/>
                    <strong>SKIP:</strong> Table is skipped and not being synchronized.<br/><br/>
                    <strong>NO_DATA:</strong> No data available or table is empty.
                  </TooltipContent>
                </Tooltip>
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "active"} 
                $direction={sortDirection}
                onClick={() => handleSort("active")}
              >
                <Tooltip>
                  Active
                  <TooltipContent className="tooltip-content">
                    <strong>Active:</strong> Table is enabled and being synchronized.<br/><br/>
                    <strong>Inactive:</strong> Table is disabled and synchronization is paused.
                  </TooltipContent>
                </Tooltip>
              </SortableTh>
              <SortableTh 
                $sortable 
                $active={sortField === "pk_strategy"} 
                $direction={sortDirection}
                onClick={() => handleSort("pk_strategy")}
              >
                <Tooltip>
                  Strategy
                  <TooltipContent className="tooltip-content">
                    <strong>CDC (Change Data Capture):</strong> Monitors database changes in real-time using transaction logs. Best for continuous synchronization.<br/><br/>
                    <strong>PK (Primary Key):</strong> Uses primary key values to track progress. Suitable for tables with sequential primary keys.<br/><br/>
                    <strong>OFFSET:</strong> Uses row offset/position for tracking. Works when no primary key is available.
                  </TooltipContent>
                </Tooltip>
              </SortableTh>
              <Th>Cluster</Th>
              <Th>Actions</Th>
            </tr>
          </thead>
          <tbody>
            {data.map((entry, index) => (
              <AnimatedTableRow 
                key={`${entry.schema_name}-${entry.table_name}-${entry.db_engine}-${index}`}
                $delay={index * 0.05}
              >
                <CleanTd>
                  <CheckboxInput
                    type="checkbox"
                    checked={selectedEntries.has(getEntryKey(entry))}
                    onChange={(e) => handleSelectEntry(entry, e.target.checked)}
                  />
                </CleanTd>
                <CleanTd>
                  <strong style={{ 
                    color: theme.colors.primary.main,
                    fontSize: '1.05em',
                    fontWeight: 600
                  }}>
                    {entry.schema_name}
                  </strong>
                  <span style={{ 
                    color: theme.colors.text.secondary,
                    marginLeft: '8px',
                    fontSize: '0.95em'
                  }}>
                    {entry.table_name}
                  </span>
                </CleanTd>
                <CleanTd>
                  <span style={{ 
                    padding: "6px 12px", 
                    borderRadius: theme.borderRadius.md,
                    background: `linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%)`,
                    fontSize: "0.85em",
                    fontWeight: 500,
                    border: `1px solid ${theme.colors.border.light}`,
                    display: 'inline-block',
                    transition: 'all 0.2s ease'
                  }}>
                    {entry.db_engine}
                  </span>
                </CleanTd>
                <TooltipTd>
                  <Tooltip>
                    <StatusBadge $status={entry.status}>
                      {entry.status}
                    </StatusBadge>
                    {index >= 2 && (
                      <TooltipContent className="tooltip-content">
                        {entry.status === "FULL_LOAD" && (
                          <>Performing initial complete data synchronization.</>
                        )}
                        {entry.status === "LISTENING_CHANGES" && (
                          <>Actively monitoring and syncing real-time changes.</>
                        )}
                        {entry.status === "ERROR" && (
                          <>Synchronization failed. Check logs for details.</>
                        )}
                        {entry.status === "SKIP" && (
                          <>Table is skipped and not being synchronized.</>
                        )}
                        {entry.status === "NO_DATA" && (
                          <>No data available or table is empty.</>
                        )}
                      </TooltipContent>
                    )}
                  </Tooltip>
                </TooltipTd>
                <TooltipTd>
                  <Tooltip>
                    <ActiveBadge $active={entry.active}>
                      {entry.active ? "Active" : "Inactive"}
                    </ActiveBadge>
                    {index >= 2 && (
                      <TooltipContent className="tooltip-content">
                        {entry.active ? (
                          <>Table is enabled and being synchronized.</>
                        ) : (
                          <>Table is disabled and synchronization is paused.</>
                        )}
                      </TooltipContent>
                    )}
                  </Tooltip>
                </TooltipTd>
                <TooltipTd>
                  <Tooltip>
                    <span style={{ 
                      padding: "6px 12px", 
                      borderRadius: theme.borderRadius.md,
                      background: `linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%)`,
                      fontSize: "0.85em",
                      fontFamily: "monospace",
                      fontWeight: 600,
                      border: `1px solid ${theme.colors.border.light}`,
                      display: 'inline-block',
                      cursor: index >= 2 ? 'help' : 'default'
                    }}>
                      {entry.pk_strategy || "OFFSET"}
                    </span>
                    {index >= 2 && (
                      <TooltipContent className="tooltip-content">
                        {entry.pk_strategy === "CDC" && (
                          <>
                            <strong>CDC (Change Data Capture):</strong> Monitors database changes in real-time using transaction logs. Best for continuous synchronization.
                          </>
                        )}
                        {entry.pk_strategy === "PK" && (
                          <>
                            <strong>PK (Primary Key):</strong> Uses primary key values to track progress. Suitable for tables with sequential primary keys.
                          </>
                        )}
                        {(entry.pk_strategy === "OFFSET" || !entry.pk_strategy) && (
                          <>
                            <strong>OFFSET:</strong> Uses row offset/position for tracking. Works when no primary key is available.
                          </>
                        )}
                      </TooltipContent>
                    )}
                  </Tooltip>
                </TooltipTd>
                <CleanTd style={{ color: theme.colors.text.secondary }}>
                  {entry.cluster_name || "-"}
                </CleanTd>
                <CleanTd>
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
                      background: `linear-gradient(135deg, ${theme.colors.status.warning.bg} 0%, ${theme.colors.status.warning.text}15 100%)`,
                      color: theme.colors.status.warning.text,
                      borderColor: theme.colors.status.warning.text,
                      fontWeight: 600,
                    }}
                  >
                    ⚠ Skip
                  </ActionButton>
                </CleanTd>
              </AnimatedTableRow>
            ))}
          </tbody>
        </StyledTable>
        </TableWrapper>
      </TableContainer>
      )}

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

      {showAddModal && (
        <AddTableModal
          onClose={() => setShowAddModal(false)}
          onSave={handleAdd}
        />
      )}
    </Container>
  );
};

export default Catalog;
