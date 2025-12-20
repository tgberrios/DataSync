import axios from "axios";

const api = axios.create({
  baseURL: "/api",
  headers: {
    "Content-Type": "application/json",
  },
  timeout: 60000,
});

api.interceptors.request.use((config) => {
  const token = localStorage.getItem("authToken");
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      const isLoginEndpoint = error.config?.url?.includes("/auth/login");
      if (!isLoginEndpoint) {
        const errorMessage =
          error.response?.data?.error || error.response?.data?.message || "";

        if (
          errorMessage.includes("Authentication required") ||
          errorMessage.includes("Invalid token") ||
          errorMessage.includes("Token expired")
        ) {
          localStorage.removeItem("authToken");
          localStorage.removeItem("authUser");

          if (window.location.pathname !== "/login") {
            setTimeout(() => {
              window.location.href = "/login";
            }, 100);
          }
        }
      }
    }
    return Promise.reject(error);
  }
);

export const authApi = {
  login: async (username: string, password: string) => {
    const response = await api.post("/auth/login", { username, password });
    if (response.data.token) {
      localStorage.setItem("authToken", response.data.token);
      localStorage.setItem("authUser", JSON.stringify(response.data.user));
    }
    return response.data;
  },

  logout: async () => {
    try {
      await api.post("/auth/logout");
    } catch (err) {
      console.error("Logout error:", err);
    } finally {
      localStorage.removeItem("authToken");
      localStorage.removeItem("authUser");
    }
  },

  getCurrentUser: async () => {
    const response = await api.get("/auth/me");
    return response.data.user;
  },

  changePassword: async (oldPassword: string, newPassword: string) => {
    const response = await api.post("/auth/change-password", {
      oldPassword,
      newPassword,
    });
    return response.data;
  },

  getUsers: async (params?: {
    page?: number;
    limit?: number;
    role?: string;
    active?: string;
    search?: string;
  }) => {
    const response = await api.get("/auth/users", { params });
    return response.data;
  },

  createUser: async (
    username: string,
    email: string,
    password: string,
    role?: string
  ) => {
    const response = await api.post("/auth/users", {
      username,
      email,
      password,
      role: role || "user",
    });
    return response.data;
  },

  updateUser: async (
    id: number,
    data: {
      username?: string;
      email?: string;
      role?: string;
      active?: boolean;
    }
  ) => {
    const response = await api.patch(`/auth/users/${id}`, data);
    return response.data;
  },

  deleteUser: async (id: number) => {
    const response = await api.delete(`/auth/users/${id}`);
    return response.data;
  },

  resetUserPassword: async (id: number, newPassword: string) => {
    const response = await api.post(`/auth/users/${id}/reset-password`, {
      newPassword,
    });
    return response.data;
  },
};

export const getCurrentUser = () => {
  const userStr = localStorage.getItem("authUser");
  return userStr ? JSON.parse(userStr) : null;
};

export const isAuthenticated = () => {
  return !!localStorage.getItem("authToken");
};

export interface CatalogEntry {
  schema_name: string;
  table_name: string;
  db_engine: string;
  connection_string: string;
  active: boolean;
  status: string;
  cluster_name: string;
  updated_at: string;
  pk_strategy?: string;
}

export interface BatchConfig {
  key: string;
  value: string;
  description: string;
  updated_at: string;
}

export interface DashboardStats {
  syncStatus: {
    progress: number;
    listeningChanges: number;
    pending: number;
    fullLoadActive: number;
    fullLoadInactive: number;
    noData: number;
    skip: number;
    errors: number;
    inProgress: number;
    currentProcess: string;
    totalData?: number;
  };
  systemResources: {
    cpuUsage: string;
    memoryUsed: string;
    memoryTotal: string;
    memoryPercentage: string;
    rss: string;
    virtual: string;
  };
  dbHealth: {
    activeConnections: string;
    connectionPercentage: string;
    responseTime: string;
    bufferHitRate: string;
    cacheHitRate: string;
    status: string;
    uptimeSeconds: number;
    activeQueries: number;
    waitingQueries: number;
    avgQueryDuration: number;
    databaseSizeBytes: number;
    queryEfficiencyScore: number;
    longRunningQueries: number;
    blockingQueries: number;
    totalQueries24h: number;
  };
  batchConfig: BatchConfig;
  // Connection pooling removed - using direct connections now
  engineMetrics?: {
    [engine: string]: {
      recordsPerSecond: number;
      bytesTransferred: number;
      cpuUsage: number;
      memoryUsed: number;
      iops: number;
      activeTransfers: number;
    };
  };
  activeTransfersProgress?: {
    schemaName: string;
    tableName: string;
    dbEngine: string;
    tableSize: number;
    progressPercentage: number;
    status: string;
    lastSyncTime: string;
  }[];
  metricsCards?: {
    topTablesThroughput: {
      tableName: string;
      dbEngine: string;
      throughputRps: number;
      recordsTransferred: number;
    }[];
    currentIops: number;
    dataVolumeByTable: {
      tableName: string;
      dbEngine: string;
      totalBytes: number;
      transferCount: number;
    }[];
    currentThroughput: {
      avgRps: number;
      totalRecords: number;
      transferCount: number;
    };
  };
}

export interface CurrentlyProcessing {
  schema_name: string;
  table_name: string;
  db_engine: string;
  new_pk: string | null;
  status: string;
  processed_at: string;
  total_records: number;
}

export const dashboardApi = {
  getDashboardStats: async () => {
    try {
      const response = await api.get<DashboardStats>("/dashboard/stats");
      return response.data;
    } catch (error) {
      console.error("Error fetching dashboard stats:", error);
      if (axios.isAxiosError(error) && error.response) {
        console.error("Server error details:", error.response.data);
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
  getCurrentlyProcessing: async (page: number = 1) => {
    try {
      const response = await api.get<{
        data: CurrentlyProcessing[];
        pagination: {
          page: number;
          limit: number;
          total: number;
          totalPages: number;
          hasNext: boolean;
          hasPrev: boolean;
        };
      }>(`/dashboard/currently-processing?page=${page}`);
      return response.data;
    } catch (error) {
      console.error("Error fetching currently processing tables:", error);
      if (axios.isAxiosError(error) && error.response) {
        console.error("Server error details:", error.response.data);
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
  getSystemLogs: async (limit: number = 60) => {
    try {
      const response = await api.get<{
        logs: Array<{
          timestamp: string;
          cpuUsage: number;
          memoryPercentage: number;
          network: number;
          throughput: number;
        }>;
      }>(`/dashboard/system-logs?limit=${limit}`);
      return response.data.logs;
    } catch (error) {
      console.error("Error fetching system logs:", error);
      if (axios.isAxiosError(error) && error.response) {
        console.error("Server error details:", error.response.data);
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export interface ConfigEntry {
  key: string;
  value: string;
  description: string | null;
  updated_at: string;
}

export const configApi = {
  getConfigs: async () => {
    try {
      const response = await api.get<ConfigEntry[]>("/config");
      return response.data;
    } catch (error) {
      console.error("Error fetching configurations:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  createConfig: async (config: ConfigEntry) => {
    try {
      const response = await api.post<ConfigEntry>("/config", config);
      return response.data;
    } catch (error) {
      console.error("Error creating configuration:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  updateConfig: async (config: ConfigEntry) => {
    try {
      const response = await api.put<ConfigEntry>(
        `/config/${config.key}`,
        config
      );
      return response.data;
    } catch (error) {
      console.error("Error updating configuration:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  deleteConfig: async (key: string) => {
    try {
      const response = await api.delete(`/config/${key}`);
      return response.data;
    } catch (error) {
      console.error("Error deleting configuration:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getBatchConfig: async () => {
    try {
      const response = await api.get<BatchConfig>("/config/batch");
      return response.data;
    } catch (error) {
      console.error("Error fetching batch configuration:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const governanceApi = {
  getGovernanceData: async (params: {
    page?: number;
    limit?: number;
    engine?: string;
    category?: string;
    health?: string;
    domain?: string;
    sensitivity?: string;
  }) => {
    try {
      const response = await api.get("/governance/data", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching governance data:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
  getGovernanceMetrics: async () => {
    try {
      const response = await api.get("/governance/metrics");
      return response.data;
    } catch (error) {
      console.error("Error fetching governance metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const qualityApi = {
  getQualityMetrics: async (params: {
    page?: number;
    limit?: number;
    search?: string;
    status?: string;
    minScore?: number;
    maxScore?: number;
  }) => {
    try {
      const response = await api.get("/quality/metrics", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching quality metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const monitorApi = {
  getActiveQueries: async () => {
    try {
      const response = await api.get("/monitor/queries");
      return response.data;
    } catch (error) {
      console.error("Error fetching active queries:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getProcessingLogs: async (
    page: number = 1,
    limit: number = 20,
    strategy?: string
  ) => {
    try {
      const params = new URLSearchParams({
        page: String(page),
        limit: String(limit),
      });
      if (strategy) params.set("strategy", strategy);
      const response = await api.get(
        `/monitor/processing-logs?${params.toString()}`
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching processing logs:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getProcessingStats: async () => {
    try {
      const response = await api.get("/monitor/processing-logs/stats");
      return response.data;
    } catch (error) {
      console.error("Error fetching processing stats:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  cleanupOldProcessingLogs: async () => {
    try {
      const response = await api.post("/monitor/processing-logs/cleanup");
      return response.data;
    } catch (error) {
      console.error("Error cleaning up old processing logs:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getTransferMetrics: async (params: {
    page?: number;
    limit?: number;
    schema_name?: string;
    table_name?: string;
    db_engine?: string;
    status?: string;
    transfer_type?: string;
    days?: number;
  }) => {
    try {
      const response = await api.get("/monitor/transfer-metrics", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching transfer metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getTransferMetricsStats: async (params?: { days?: number }) => {
    try {
      const response = await api.get("/monitor/transfer-metrics/stats", {
        params,
      });
      return response.data;
    } catch (error) {
      console.error("Error fetching transfer metrics stats:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const catalogApi = {
  // Obtener entradas del catálogo con paginación, filtros y búsqueda
  getCatalogEntries: async (
    params: {
      page?: number;
      limit?: number;
      engine?: string;
      status?: string;
      active?: string;
      search?: string;
      sort_field?: string;
      sort_direction?: string;
    } = {}
  ) => {
    try {
      const response = await api.get("/catalog", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching catalog:", error);
      throw error;
    }
  },

  // Actualizar el estado activo de una entrada
  updateEntryStatus: async (
    schema_name: string,
    table_name: string,
    db_engine: string,
    active: boolean
  ) => {
    try {
      const s = schema_name.toLowerCase();
      const t = table_name.toLowerCase();
      const response = await api.patch<CatalogEntry>("/catalog/status", {
        schema_name: s,
        table_name: t,
        db_engine,
        active,
      });
      return response.data;
    } catch (error) {
      console.error("Error updating status:", error);
      throw error;
    }
  },

  // Forzar una sincronización completa
  triggerFullSync: async (
    schema_name: string,
    table_name: string,
    db_engine: string
  ) => {
    try {
      const s = schema_name.toLowerCase();
      const t = table_name.toLowerCase();
      const response = await api.post<CatalogEntry>("/catalog/sync", {
        schema_name: s,
        table_name: t,
        db_engine,
      });
      return response.data;
    } catch (error) {
      console.error("Error triggering sync:", error);
      throw error;
    }
  },

  // Crear una nueva entrada del catálogo
  createEntry: async (
    entry: Omit<CatalogEntry, "last_sync_time" | "updated_at">
  ) => {
    try {
      const response = await api.post<CatalogEntry>("/catalog", entry);
      return response.data;
    } catch (error) {
      console.error("Error creating entry:", error);
      throw error;
    }
  },

  // Actualizar una entrada del catálogo
  updateEntry: async (entry: CatalogEntry) => {
    try {
      const response = await api.put<CatalogEntry>("/catalog", entry);
      return response.data;
    } catch (error) {
      console.error("Error updating entry:", error);
      throw error;
    }
  },

  // Obtener todos los schemas únicos
  getSchemas: async () => {
    try {
      const response = await api.get<string[]>("/catalog/schemas");
      return response.data;
    } catch (error) {
      console.error("Error fetching schemas:", error);
      throw error;
    }
  },

  // Obtener todos los engines únicos
  getEngines: async () => {
    try {
      const response = await api.get<string[]>("/catalog/engines");
      return response.data;
    } catch (error) {
      console.error("Error fetching engines:", error);
      throw error;
    }
  },

  // Obtener todos los status únicos
  getStatuses: async () => {
    try {
      const response = await api.get<string[]>("/catalog/statuses");
      return response.data;
    } catch (error) {
      console.error("Error fetching statuses:", error);
      throw error;
    }
  },

  // Obtener todas las strategies únicas
  getStrategies: async () => {
    try {
      const response = await api.get<string[]>("/catalog/strategies");
      return response.data;
    } catch (error) {
      console.error("Error fetching strategies:", error);
      throw error;
    }
  },

  // Marcar tabla como SKIP
  skipTable: async (
    schema_name: string,
    table_name: string,
    db_engine: string
  ) => {
    try {
      const s = schema_name.toLowerCase();
      const t = table_name.toLowerCase();
      const response = await api.patch<{
        message: string;
        affectedRows: number;
        entry: CatalogEntry;
      }>("/catalog/skip-table", {
        schema_name: s,
        table_name: t,
        db_engine,
      });
      return response.data;
    } catch (error) {
      console.error("Error skipping table:", error);
      throw error;
    }
  },

  // Desactivar schema completo
  deactivateSchema: async (schema_name: string) => {
    try {
      const s = schema_name.toLowerCase();
      const response = await api.patch<{
        message: string;
        affectedRows: number;
        rows: CatalogEntry[];
      }>("/catalog/deactivate-schema", { schema_name: s });
      return response.data;
    } catch (error) {
      console.error("Error deactivating schema:", error);
      throw error;
    }
  },
};

// Interfaces para logs
export interface LogEntry {
  id?: number;
  timestamp: string;
  level: string;
  category?: string;
  function: string;
  message: string;
  raw: string;
  parsed?: boolean;
}

export interface LogsResponse {
  logs: LogEntry[];
  totalLines: number;
  filePath: string;
  lastModified: string;
  filters?: {
    level: string;
    category: string;
    function?: string;
    search: string;
    startDate: string;
    endDate: string;
  };
}

export interface LogInfo {
  exists: boolean;
  filePath?: string;
  size?: number;
  totalLines?: number;
  lastModified?: string;
  created?: string;
  message?: string;
}

export interface LogStats {
  total: number;
  byLevel: Record<string, number>;
  byCategory: Record<string, number>;
  byFunction: Record<string, number>;
  recent: Array<{
    timestamp: string;
    level: string;
    category: string;
    function: string;
  }>;
}

export interface LogStatsResponse {
  stats: LogStats;
  generatedAt: string;
}

export const logsApi = {
  getLevels: async () => {
    const response = await api.get<string[]>("/logs/levels");
    return response.data;
  },
  getCategories: async () => {
    const response = await api.get<string[]>("/logs/categories");
    return response.data;
  },
  getFunctions: async () => {
    const response = await api.get<string[]>("/logs/functions");
    return response.data;
  },
  getLogs: async (
    params: {
      lines?: number;
      level?: string;
      category?: string;
      function?: string;
      search?: string;
      startDate?: string;
      endDate?: string;
    } = {}
  ) => {
    try {
      const response = await api.get<LogsResponse>("/logs", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching logs:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getLogInfo: async () => {
    try {
      const response = await api.get<LogInfo>("/logs/info");
      return response.data;
    } catch (error) {
      console.error("Error fetching log info:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  clearLogs: async () => {
    try {
      const response = await api.delete("/logs");
      return response.data;
    } catch (error) {
      console.error("Error clearing logs:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getLogStats: async () => {
    try {
      const response = await api.get<LogStatsResponse>("/logs/stats");
      return response.data;
    } catch (error) {
      console.error("Error fetching log stats:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  // Error logs API
  getErrorLogs: async (
    params: {
      lines?: number;
      category?: string;
      search?: string;
      startDate?: string;
      endDate?: string;
    } = {}
  ) => {
    try {
      const response = await api.get<LogsResponse>("/logs/errors", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching error logs:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getErrorLogInfo: async () => {
    try {
      const response = await api.get<LogInfo>("/logs/errors/info");
      return response.data;
    } catch (error) {
      console.error("Error fetching error log info:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const securityApi = {
  getSecurityData: async () => {
    try {
      const response = await api.get("/security/data");
      return response.data;
    } catch (error) {
      console.error("Error fetching security data:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const queryPerformanceApi = {
  getQueries: async (params: {
    page?: number;
    limit?: number;
    performance_tier?: string;
    operation_type?: string;
    source_type?: string;
    search?: string;
  }) => {
    try {
      const response = await api.get("/query-performance/queries", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching query performance data:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMetrics: async () => {
    try {
      const response = await api.get("/query-performance/metrics");
      return response.data;
    } catch (error) {
      console.error("Error fetching query performance metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const maintenanceApi = {
  getMaintenanceItems: async (params: {
    page?: number;
    limit?: number;
    maintenance_type?: string;
    status?: string;
    db_engine?: string;
  }) => {
    try {
      const response = await api.get("/maintenance/items", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching maintenance data:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMetrics: async () => {
    try {
      const response = await api.get("/maintenance/metrics");
      return response.data;
    } catch (error) {
      console.error("Error fetching maintenance metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const columnCatalogApi = {
  getColumns: async (params: {
    page?: number;
    limit?: number;
    schema_name?: string;
    table_name?: string;
    db_engine?: string;
    data_type?: string;
    sensitivity_level?: string;
    contains_pii?: string;
    contains_phi?: string;
    search?: string;
  }) => {
    try {
      const response = await api.get("/column-catalog/columns", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching column catalog data:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMetrics: async () => {
    try {
      const response = await api.get("/column-catalog/metrics");
      return response.data;
    } catch (error) {
      console.error("Error fetching column catalog metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getSchemas: async () => {
    try {
      const response = await api.get("/column-catalog/schemas");
      return response.data;
    } catch (error) {
      console.error("Error fetching schemas:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getTables: async (schemaName: string) => {
    try {
      const response = await api.get(
        `/column-catalog/tables/${encodeURIComponent(schemaName)}`
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching tables:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const catalogLocksApi = {
  getLocks: async () => {
    try {
      const response = await api.get("/catalog-locks/locks");
      return response.data;
    } catch (error) {
      console.error("Error fetching catalog locks:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMetrics: async () => {
    try {
      const response = await api.get("/catalog-locks/metrics");
      return response.data;
    } catch (error) {
      console.error("Error fetching catalog locks metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  unlock: async (lockName: string) => {
    try {
      const response = await api.delete(
        `/catalog-locks/locks/${encodeURIComponent(lockName)}`
      );
      return response.data;
    } catch (error) {
      console.error("Error unlocking lock:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  cleanExpired: async () => {
    try {
      const response = await api.post("/catalog-locks/clean-expired");
      return response.data;
    } catch (error) {
      console.error("Error cleaning expired locks:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const dataLineageApi = {
  getMariaDBLineage: async (params: {
    page?: number;
    limit?: number;
    server_name?: string;
    database_name?: string;
    object_type?: string;
    relationship_type?: string;
    search?: string;
  }) => {
    try {
      const response = await api.get("/data-lineage/mariadb", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching MariaDB lineage data:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMariaDBMetrics: async () => {
    try {
      console.log("API: Calling /data-lineage/mariadb/metrics");
      const response = await api.get("/data-lineage/mariadb/metrics");
      console.log("API: MariaDB metrics response:", response);
      console.log("API: MariaDB metrics response.data:", response.data);
      return response.data;
    } catch (error) {
      console.error("Error fetching MariaDB lineage metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        console.error("API: MariaDB metrics error response:", error.response);
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMariaDBServers: async () => {
    try {
      const response = await api.get("/data-lineage/mariadb/servers");
      return response.data;
    } catch (error) {
      console.error("Error fetching MariaDB servers:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMariaDBDatabases: async (serverName: string) => {
    try {
      const response = await api.get(
        `/data-lineage/mariadb/databases/${encodeURIComponent(serverName)}`
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching MariaDB databases:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const dataLineageMSSQLApi = {
  getMSSQLLineage: async (params: {
    page?: number;
    limit?: number;
    server_name?: string;
    instance_name?: string;
    database_name?: string;
    object_type?: string;
    relationship_type?: string;
    search?: string;
  }) => {
    try {
      const response = await api.get("/data-lineage/mssql", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching MSSQL lineage data:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMSSQLMetrics: async () => {
    try {
      const response = await api.get("/data-lineage/mssql/metrics");
      return response.data;
    } catch (error) {
      console.error("Error fetching MSSQL lineage metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMSSQLServers: async () => {
    try {
      const response = await api.get("/data-lineage/mssql/servers");
      return response.data;
    } catch (error) {
      console.error("Error fetching MSSQL servers:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMSSQLInstances: async (serverName: string) => {
    try {
      const response = await api.get(
        `/data-lineage/mssql/instances/${encodeURIComponent(serverName)}`
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching MSSQL instances:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMSSQLDatabases: async (serverName: string, instanceName: string) => {
    try {
      const response = await api.get(
        `/data-lineage/mssql/databases/${encodeURIComponent(
          serverName
        )}/${encodeURIComponent(instanceName)}`
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching MSSQL databases:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const dataLineageMongoDBApi = {
  getMongoDBLineage: async (params: {
    page?: number;
    limit?: number;
    server_name?: string;
    database_name?: string;
    relationship_type?: string;
    search?: string;
  }) => {
    try {
      const response = await api.get("/data-lineage/mongodb", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching MongoDB lineage data:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMongoDBMetrics: async () => {
    try {
      const response = await api.get("/data-lineage/mongodb/metrics");
      return response.data;
    } catch (error) {
      console.error("Error fetching MongoDB lineage metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMongoDBServers: async () => {
    try {
      const response = await api.get("/data-lineage/mongodb/servers");
      return response.data;
    } catch (error) {
      console.error("Error fetching MongoDB servers:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMongoDBDatabases: async (serverName: string) => {
    try {
      const response = await api.get(
        `/data-lineage/mongodb/databases/${encodeURIComponent(serverName)}`
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching MongoDB databases:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const governanceCatalogApi = {
  getMariaDBItems: async (params: {
    page?: number;
    limit?: number;
    server_name?: string;
    database_name?: string;
    health_status?: string;
    access_frequency?: string;
    search?: string;
  }) => {
    try {
      const response = await api.get("/governance-catalog/mariadb", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching MariaDB governance catalog:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMariaDBMetrics: async () => {
    try {
      console.log("API: Calling /governance-catalog/mariadb/metrics");
      const response = await api.get("/governance-catalog/mariadb/metrics");
      console.log("API: MariaDB governance metrics response:", response);
      console.log(
        "API: MariaDB governance metrics response.data:",
        response.data
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching MariaDB governance metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        console.error("API: MariaDB metrics error response:", error.response);
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMariaDBServers: async () => {
    try {
      const response = await api.get("/governance-catalog/mariadb/servers");
      return response.data;
    } catch (error) {
      console.error("Error fetching MariaDB servers:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMariaDBDatabases: async (serverName: string) => {
    try {
      const response = await api.get(
        `/governance-catalog/mariadb/databases/${encodeURIComponent(
          serverName
        )}`
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching MariaDB databases:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMSSQLItems: async (params: {
    page?: number;
    limit?: number;
    server_name?: string;
    database_name?: string;
    object_type?: string;
    health_status?: string;
    access_frequency?: string;
    search?: string;
  }) => {
    try {
      const response = await api.get("/governance-catalog/mssql", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching MSSQL governance catalog:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMSSQLMetrics: async () => {
    try {
      console.log("API: Calling /governance-catalog/mssql/metrics");
      const response = await api.get("/governance-catalog/mssql/metrics");
      console.log("API: MSSQL governance metrics response:", response);
      console.log(
        "API: MSSQL governance metrics response.data:",
        response.data
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching MSSQL governance metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        console.error("API: MSSQL metrics error response:", error.response);
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMSSQLServers: async () => {
    try {
      const response = await api.get("/governance-catalog/mssql/servers");
      return response.data;
    } catch (error) {
      console.error("Error fetching MSSQL servers:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMSSQLDatabases: async (serverName: string) => {
    try {
      const response = await api.get(
        `/governance-catalog/mssql/databases/${encodeURIComponent(serverName)}`
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching MSSQL databases:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const governanceCatalogMongoDBApi = {
  getMongoDBItems: async (params: {
    page?: number;
    limit?: number;
    server_name?: string;
    database_name?: string;
    health_status?: string;
    access_frequency?: string;
    search?: string;
  }) => {
    try {
      const response = await api.get("/governance-catalog/mongodb", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching MongoDB governance catalog:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMongoDBMetrics: async () => {
    try {
      console.log("API: Calling /governance-catalog/mongodb/metrics");
      const response = await api.get("/governance-catalog/mongodb/metrics");
      console.log("API: MongoDB governance metrics response:", response);
      console.log(
        "API: MongoDB governance metrics response.data:",
        response.data
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching MongoDB governance metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        console.error("API: MongoDB metrics error response:", error.response);
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMongoDBServers: async () => {
    try {
      const response = await api.get("/governance-catalog/mongodb/servers");
      return response.data;
    } catch (error) {
      console.error("Error fetching MongoDB servers:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMongoDBDatabases: async (serverName: string) => {
    try {
      const response = await api.get(
        `/governance-catalog/mongodb/databases/${encodeURIComponent(
          serverName
        )}`
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching MongoDB databases:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const dataLineageOracleApi = {
  getOracleLineage: async (params: {
    page?: number;
    limit?: number;
    server_name?: string;
    schema_name?: string;
    relationship_type?: string;
    search?: string;
  }) => {
    try {
      const response = await api.get("/data-lineage/oracle", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching Oracle lineage data:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getOracleMetrics: async () => {
    try {
      const response = await api.get("/data-lineage/oracle/metrics");
      return response.data;
    } catch (error) {
      console.error("Error fetching Oracle lineage metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getOracleServers: async () => {
    try {
      const response = await api.get("/data-lineage/oracle/servers");
      return response.data;
    } catch (error) {
      console.error("Error fetching Oracle servers:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getOracleSchemas: async (serverName: string) => {
    try {
      const response = await api.get(
        `/data-lineage/oracle/schemas/${encodeURIComponent(serverName)}`
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching Oracle schemas:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const governanceCatalogOracleApi = {
  getOracleItems: async (params: {
    page?: number;
    limit?: number;
    server_name?: string;
    schema_name?: string;
    health_status?: string;
    access_frequency?: string;
    search?: string;
  }) => {
    try {
      const response = await api.get("/governance-catalog/oracle", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching Oracle governance catalog:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getOracleMetrics: async () => {
    try {
      console.log("API: Calling /governance-catalog/oracle/metrics");
      const response = await api.get("/governance-catalog/oracle/metrics");
      console.log("API: Oracle governance metrics response:", response);
      console.log(
        "API: Oracle governance metrics response.data:",
        response.data
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching Oracle governance metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        console.error("API: Oracle metrics error response:", error.response);
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getOracleServers: async () => {
    try {
      const response = await api.get("/governance-catalog/oracle/servers");
      return response.data;
    } catch (error) {
      console.error("Error fetching Oracle servers:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getOracleSchemas: async (serverName: string) => {
    try {
      const response = await api.get(
        `/governance-catalog/oracle/schemas/${encodeURIComponent(serverName)}`
      );
      return response.data;
    } catch (error) {
      console.error("Error fetching Oracle schemas:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export const apiCatalogApi = {
  getAPIs: async (params: {
    page?: number;
    limit?: number;
    api_type?: string;
    target_db_engine?: string;
    status?: string;
    active?: string;
    search?: string;
  }) => {
    try {
      const response = await api.get("/api-catalog", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching API catalog:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  createAPI: async (entry: {
    api_name: string;
    api_type: string;
    base_url: string;
    endpoint: string;
    http_method: string;
    auth_type: string;
    auth_config: Record<string, unknown>;
    target_db_engine: string;
    target_connection_string: string;
    target_schema: string;
    target_table: string;
    request_body?: string | null;
    request_headers: Record<string, unknown>;
    query_params: Record<string, unknown>;
    sync_interval: number;
    status: string;
    active: boolean;
  }) => {
    try {
      const response = await api.post("/api-catalog", entry);
      return response.data;
    } catch (error) {
      console.error("Error creating API:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  updateActive: async (apiName: string, active: boolean) => {
    try {
      const response = await api.patch("/api-catalog/active", {
        api_name: apiName,
        active: active,
      });
      return response.data;
    } catch (error) {
      console.error("Error updating API active status:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getMetrics: async () => {
    try {
      const response = await api.get("/api-catalog/metrics");
      return response.data;
    } catch (error) {
      console.error("Error fetching API catalog metrics:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};

export interface CustomJobEntry {
  id: number;
  job_name: string;
  description: string | null;
  source_db_engine: string;
  source_connection_string: string;
  query_sql: string;
  target_db_engine: string;
  target_connection_string: string;
  target_schema: string;
  target_table: string;
  schedule_cron: string | null;
  active: boolean;
  enabled: boolean;
  transform_config: Record<string, unknown>;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export const customJobsApi = {
  getJobs: async (params: {
    page?: number;
    limit?: number;
    source_db_engine?: string;
    target_db_engine?: string;
    active?: string;
    enabled?: string;
    search?: string;
  }) => {
    try {
      const response = await api.get("/custom-jobs", { params });
      return response.data;
    } catch (error) {
      console.error("Error fetching custom jobs:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  updateActive: async (jobName: string, active: boolean) => {
    try {
      const response = await api.patch(`/custom-jobs/${jobName}/active`, {
        active,
      });
      return response.data;
    } catch (error) {
      console.error(`Error updating job active status for ${jobName}:`, error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  executeJob: async (jobName: string) => {
    try {
      const response = await api.post(`/custom-jobs/${jobName}/execute`);
      return response.data;
    } catch (error) {
      console.error(`Error executing job ${jobName}:`, error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getJobResults: async (jobName: string) => {
    try {
      const response = await api.get(`/custom-jobs/${jobName}/results`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching job results for ${jobName}:`, error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getJobHistory: async (jobName: string) => {
    try {
      const response = await api.get(`/custom-jobs/${jobName}/history`);
      return response.data;
    } catch (error) {
      console.error(`Error fetching job history for ${jobName}:`, error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  deleteJob: async (jobName: string) => {
    try {
      const response = await api.delete(`/custom-jobs/${jobName}`);
      return response.data;
    } catch (error) {
      console.error(`Error deleting job ${jobName}:`, error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },

  getScripts: async () => {
    try {
      const response = await api.get("/custom-jobs/scripts");
      return response.data;
    } catch (error) {
      console.error("Error fetching Python scripts:", error);
      if (axios.isAxiosError(error) && error.response) {
        throw new Error(
          error.response.data.details ||
            error.response.data.error ||
            error.message
        );
      }
      throw error;
    }
  },
};
