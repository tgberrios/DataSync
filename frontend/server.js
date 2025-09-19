import express from "express";
import pkg from "pg";
const { Pool } = pkg;
import cors from "cors";
import { spawn } from "child_process";
import os from "os";
import path from "path";
import fs from "fs";

const app = express();
app.use(cors());
app.use(express.json());

const pool = new Pool({
  host: "10.12.240.40",
  port: 5432,
  database: "DataLake",
  user: "Datalake_User",
  password: "keepprofessional",
});

// Test connection
pool.connect((err, client, done) => {
  if (err) {
    console.error("Error connecting to the database:", err);
  } else {
    console.log("Successfully connected to PostgreSQL");
    done();
  }
});

// Obtener catálogo con paginación, filtros y búsqueda
app.get("/api/catalog", async (req, res) => {
  try {
    const {
      page = 1,
      limit = 10,
      engine = "",
      status = "",
      active = "",
      search = "",
    } = req.query;
    const offset = (page - 1) * limit;

    let whereConditions = [];
    let queryParams = [];
    let paramCount = 0;

    if (engine) {
      paramCount++;
      whereConditions.push(`db_engine = $${paramCount}`);
      queryParams.push(engine);
    }

    if (status) {
      paramCount++;
      whereConditions.push(`status = $${paramCount}`);
      queryParams.push(status);
    }

    if (active !== "") {
      paramCount++;
      whereConditions.push(`active = $${paramCount}`);
      queryParams.push(active === "true");
    }

    if (search) {
      paramCount++;
      whereConditions.push(
        `(schema_name ILIKE $${paramCount} OR table_name ILIKE $${paramCount} OR cluster_name ILIKE $${paramCount})`
      );
      queryParams.push(`%${search}%`);
    }

    const whereClause =
      whereConditions.length > 0
        ? `WHERE ${whereConditions.join(" AND ")}`
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.catalog ${whereClause}`;
    const countResult = await pool.query(countQuery, queryParams);
    const total = parseInt(countResult.rows[0].count);

    paramCount++;
    const dataQuery = `SELECT * FROM metadata.catalog ${whereClause} ORDER BY schema_name, table_name LIMIT $${paramCount} OFFSET $${
      paramCount + 1
    }`;
    queryParams.push(limit, offset);

    const result = await pool.query(dataQuery, queryParams);

    const totalPages = Math.ceil(total / limit);

    res.json({
      data: result.rows,
      pagination: {
        total,
        totalPages,
        currentPage: parseInt(page),
        limit: parseInt(limit),
      },
    });
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Actualizar estado
app.patch("/api/catalog/status", async (req, res) => {
  const { schema_name, table_name, db_engine, active } = req.body;
  try {
    const result = await pool.query(
      `UPDATE metadata.catalog 
       SET active = $1
       WHERE schema_name = $2 AND table_name = $3 AND db_engine = $4
       RETURNING *`,
      [active, schema_name, table_name, db_engine]
    );
    res.json(result.rows[0]);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Forzar sincronización
app.post("/api/catalog/sync", async (req, res) => {
  const { schema_name, table_name, db_engine } = req.body;
  try {
    const result = await pool.query(
      `UPDATE metadata.catalog 
       SET status = 'full_load'
       WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3
       RETURNING *`,
      [schema_name, table_name, db_engine]
    );
    res.json(result.rows[0]);
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: err.message });
  }
});

const PORT = 3000;
// Obtener estadísticas del dashboard
app.get("/api/dashboard/stats", async (req, res) => {
  try {
    console.log("Fetching dashboard stats...");

    // 1. SYNCHRONIZATION STATUS
    const syncStatus = await pool.query(`
      SELECT 
        COUNT(*) FILTER (WHERE status = 'PERFECT_MATCH') as perfect_match,
        COUNT(*) FILTER (WHERE status = 'LISTENING_CHANGES') as listening_changes,
        COUNT(*) FILTER (WHERE active = true) as full_load_active,
        COUNT(*) FILTER (WHERE active = false) as full_load_inactive,
        COUNT(*) FILTER (WHERE status = 'NO_DATA') as no_data,
        COUNT(*) FILTER (WHERE status = 'ERROR') as errors,
        '' as current_process
      FROM metadata.catalog
    `);

    // Get currently processing tables from active queries
    const activeQueries = await pool.query(`
      SELECT 
        query,
        state,
        query_start,
        EXTRACT(EPOCH FROM (now() - query_start))::integer as duration_seconds
      FROM pg_stat_activity 
      WHERE state = 'active' 
        AND query LIKE '%COPY%FROM STDIN%'
        AND query NOT LIKE '%pg_stat_activity%'
      ORDER BY query_start DESC
    `);

    // Extract table names from COPY queries and get their counts
    const processingTablesWithCounts = [];
    
    for (const row of activeQueries.rows) {
      const copyMatch = row.query.match(/COPY\s+"?([^"]+)"?\."?([^"]+)"?\s+FROM\s+STDIN/i);
      if (copyMatch) {
        const schema = copyMatch[1];
        const table = copyMatch[2];
        
        try {
          // Get count for this table
          const countResult = await pool.query(`SELECT COUNT(*) as count FROM "${schema}"."${table}"`);
          const count = parseInt(countResult.rows[0].count);
          processingTablesWithCounts.push(`${schema}.${table} (${count.toLocaleString()} records)`);
        } catch (error) {
          // If count fails, just show table name
          console.log(`Could not get count for ${schema}.${table}:`, error.message);
          processingTablesWithCounts.push(`${schema}.${table}`);
        }
      }
    }

    const currentProcessText = processingTablesWithCounts.length > 0 
      ? processingTablesWithCounts.join(', ')
      : 'No active transfers';

    // 2. TRANSFER PERFORMANCE BY ENGINE
    const transferPerformance = await pool.query(`
      SELECT 
        db_engine,
        COUNT(*) FILTER (WHERE status = 'PROCESSING' AND completed_at IS NULL) as active_transfers,
        ROUND(AVG(transfer_rate_per_second)::numeric, 2) as avg_transfer_rate,
        ROUND(AVG(memory_used_mb)::numeric, 2) as avg_memory_used,
        ROUND(AVG(cpu_usage_percent)::numeric, 2) as avg_cpu_usage,
        ROUND(AVG(io_operations_per_second)::numeric, 2) as avg_iops,
        ROUND(AVG(avg_latency_ms)::numeric, 2) as avg_latency,
        SUM(bytes_transferred) as total_bytes
      FROM metadata.transfer_metrics
      WHERE created_at > NOW() - INTERVAL '5 minutes'
      GROUP BY db_engine
    `);

    // 3. SYSTEM RESOURCES (from OS)
    console.log("Getting system resources...");

    // CPU
    const cpus = os.cpus();
    const cpuCount = cpus.length;
    const loadAvg = os.loadavg()[0];
    const cpuUsagePercent = ((loadAvg * 100) / cpuCount).toFixed(1);

    console.log("CPU Info:", {
      count: cpuCount,
      loadAvg,
      usagePercent: cpuUsagePercent,
    });

    // Memory
    const totalMemory = os.totalmem();
    const freeMemory = os.freemem();
    const usedMemory = totalMemory - freeMemory;
    const memoryUsedGB = (usedMemory / (1024 * 1024 * 1024)).toFixed(2);
    const memoryTotalGB = (totalMemory / (1024 * 1024 * 1024)).toFixed(2);
    const memoryPercentage = ((usedMemory / totalMemory) * 100).toFixed(1);

    console.log("Memory Info:", {
      total: memoryTotalGB,
      used: memoryUsedGB,
      percentage: memoryPercentage,
    });

    // Process Memory
    const processMemory = process.memoryUsage();
    const rssGB = (processMemory.rss / (1024 * 1024 * 1024)).toFixed(2);
    const virtualGB = (processMemory.heapTotal / (1024 * 1024 * 1024)).toFixed(
      2
    );

    console.log("Process Memory:", {
      rss: rssGB,
      virtual: virtualGB,
    });

    const systemResources = {
      rows: [
        {
          cpu_usage: cpuUsagePercent,
          cpu_cores: cpuCount,
          memory_used: memoryUsedGB,
          memory_total: memoryTotalGB,
          memory_percentage: memoryPercentage,
          memory_rss: rssGB,
          memory_virtual: virtualGB,
        },
      ],
    };

    // 4. DATABASE HEALTH
    const dbHealth = await pool.query(`
      SELECT 
        (SELECT count(*) FROM pg_stat_activity) as active_connections,
        (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections,
        EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time())) as uptime_seconds,
        (
          SELECT json_build_object(
            'buffer_hit_ratio', ROUND(COALESCE((sum(heap_blks_hit) * 100.0 / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0)), 100)::numeric, 1),
            'cache_hit_ratio', ROUND(COALESCE((sum(idx_blks_hit) * 100.0 / NULLIF(sum(idx_blks_hit) + sum(idx_blks_read), 0)), 100)::numeric, 1)
          )
          FROM pg_statio_user_tables
        ) as cache_stats
    `);

    // 5. CONNECTION POOLING
    const connectionPool = await pool.query(`
      SELECT 
        COUNT(DISTINCT db_engine) as total_pools,
        COUNT(*) FILTER (WHERE status = 'PROCESSING') as active_connections,
        COUNT(*) FILTER (WHERE status != 'PROCESSING' AND active = true) as idle_connections,
        COUNT(*) FILTER (WHERE status = 'ERROR') as failed_connections,
        MAX(last_sync_time) as last_update
      FROM metadata.catalog
    `);

    // 6. RECENT ACTIVITY
    const recentActivity = await pool.query(`
      SELECT 
        COUNT(*) as transfers_last_hour,
        COUNT(*) FILTER (WHERE status = 'FAILED') as errors_last_hour,
        MIN(created_at) as first_transfer,
        MAX(created_at) as last_transfer,
        SUM(records_transferred) as total_records,
        SUM(bytes_transferred) as total_bytes
      FROM metadata.transfer_metrics
      WHERE created_at > NOW() - INTERVAL '1 hour'
    `);

    // Construir el objeto de respuesta
    const stats = {
      syncStatus: {
        progress: 0,
        perfectMatch: parseInt(syncStatus.rows[0]?.perfect_match || 0),
        listeningChanges: parseInt(syncStatus.rows[0]?.listening_changes || 0),
        fullLoadActive: parseInt(syncStatus.rows[0]?.full_load_active || 0),
        fullLoadInactive: parseInt(syncStatus.rows[0]?.full_load_inactive || 0),
        noData: parseInt(syncStatus.rows[0]?.no_data || 0),
        errors: parseInt(syncStatus.rows[0]?.errors || 0),
        currentProcess: currentProcessText,
      },
      systemResources: {
        cpuUsage:
          systemResources.rows[0].cpu_usage +
          "% (" +
          systemResources.rows[0].cpu_cores +
          " cores)",
        memoryUsed:
          systemResources.rows[0].memory_used +
          "/" +
          systemResources.rows[0].memory_total +
          " GB (" +
          systemResources.rows[0].memory_percentage +
          "%)",
        rss: systemResources.rows[0].memory_rss + " GB",
        virtual: systemResources.rows[0].memory_virtual + " GB",
      },
      dbHealth: {
        activeConnections: dbHealth.rows[0]
          ? dbHealth.rows[0].active_connections +
            "/" +
            dbHealth.rows[0].max_connections
          : "0/0",
        responseTime: "< 1ms",
        bufferHitRate: (
          dbHealth.rows[0]?.cache_stats?.buffer_hit_ratio || 0
        ).toFixed(1),
        cacheHitRate: (
          dbHealth.rows[0]?.cache_stats?.cache_hit_ratio || 0
        ).toFixed(1),
        status: dbHealth.rows[0] ? "Healthy" : "Unknown",
      },
      connectionPool: {
        totalPools: parseInt(connectionPool.rows[0]?.total_pools || 0),
        activeConnections: parseInt(
          connectionPool.rows[0]?.active_connections || 0
        ),
        idleConnections: parseInt(
          connectionPool.rows[0]?.idle_connections || 0
        ),
        failedConnections: parseInt(
          connectionPool.rows[0]?.failed_connections || 0
        ),
        lastCleanup: connectionPool.rows[0]?.last_update
          ? formatUptime(
              (Date.now() -
                new Date(connectionPool.rows[0].last_update).getTime()) /
                1000
            )
          : "0m",
      },
    };

    // Calcular progreso total
    const total =
      stats.syncStatus.perfectMatch +
      stats.syncStatus.listeningChanges +
      stats.syncStatus.fullLoadActive +
      stats.syncStatus.fullLoadInactive +
      stats.syncStatus.noData;

    stats.syncStatus.progress =
      total > 0
        ? Math.round(
            ((stats.syncStatus.perfectMatch +
              stats.syncStatus.listeningChanges) /
              total) *
              100
          )
        : 0;

    // Agregar métricas por motor
    stats.engineMetrics = {};
    transferPerformance.rows.forEach((metric) => {
      stats.engineMetrics[metric.db_engine] = {
        recordsPerSecond: parseFloat(metric.avg_transfer_rate),
        bytesTransferred: parseFloat(metric.total_bytes),
        avgLatencyMs: parseFloat(metric.avg_latency),
        cpuUsage: parseFloat(metric.avg_cpu_usage),
        memoryUsed: parseFloat(metric.avg_memory_used),
        iops: parseFloat(metric.avg_iops),
        activeTransfers: parseInt(metric.active_transfers),
      };
    });

    // Agregar actividad reciente
    stats.recentActivity = {
      transfersLastHour: parseInt(
        recentActivity.rows[0]?.transfers_last_hour || 0
      ),
      errorsLastHour: parseInt(recentActivity.rows[0]?.errors_last_hour || 0),
      totalRecords: parseInt(recentActivity.rows[0]?.total_records || 0),
      totalBytes: parseInt(recentActivity.rows[0]?.total_bytes || 0),
      firstTransfer: recentActivity.rows[0]?.first_transfer || null,
      lastTransfer: recentActivity.rows[0]?.last_transfer || null,
      uptime: formatUptime(dbHealth.rows[0]?.uptime_seconds || 0),
    };

    console.log("Sending dashboard stats");
    res.json(stats);
  } catch (err) {
    console.error("Error getting dashboard stats:", err);
    res.status(500).json({
      error: "Error al obtener estadísticas",
      details: err.message,
    });
  }
});

// Función para formatear el tiempo de uptime
function formatUptime(seconds) {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);

  if (days > 0) {
    return `${days}d ${hours.toString().padStart(2, "0")}h ${minutes
      .toString()
      .padStart(2, "0")}m`;
  } else if (hours > 0) {
    return `${hours.toString().padStart(2, "0")}h ${minutes
      .toString()
      .padStart(2, "0")}m`;
  } else {
    return `${minutes.toString().padStart(2, "0")}m`;
  }
}
// Obtener queries activas
app.get("/api/monitor/queries", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        pid,                    -- ID del proceso
        usename,               -- Usuario que ejecuta la query
        datname,               -- Base de datos
        client_addr,           -- Dirección IP del cliente
        application_name,      -- Nombre de la aplicación
        backend_start,         -- Cuándo inició el proceso
        xact_start,           -- Cuándo inició la transacción
        query_start,          -- Cuándo inició la query
        state_change,         -- Último cambio de estado
        wait_event_type,      -- Tipo de evento que espera
        wait_event,           -- Evento específico que espera
        state,                -- Estado actual (active, idle, etc)
        query,                -- Texto de la query
        EXTRACT(EPOCH FROM (now() - query_start))::integer as duration_seconds  -- Duración en segundos
      FROM pg_stat_activity
      WHERE state IN ('active', 'idle in transaction', 'idle in transaction (aborted)')
      ORDER BY usename DESC
    `);

    const queries = result.rows.map((row) => ({
      ...row,
      duration: formatUptime(row.duration_seconds || 0),
      query: row.query?.trim(),
      state: row.state?.toUpperCase(),
    }));

    res.json(queries);
  } catch (err) {
    console.error("Error getting active queries:", err);
    res.status(500).json({
      error: "Error al obtener queries activas",
      details: err.message,
    });
  }
});

// Obtener métricas de calidad
app.get("/api/quality/metrics", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const offset = (page - 1) * limit;
    const engine = req.query.engine || "";
    const status = req.query.status || "";

    // Construir WHERE clause dinámicamente
    const whereConditions = [];
    const params = [];
    let paramCount = 1;

    if (engine) {
      whereConditions.push(`source_db_engine = $${paramCount}`);
      params.push(engine);
      paramCount++;
    }

    if (status) {
      whereConditions.push(`validation_status = $${paramCount}`);
      params.push(status);
      paramCount++;
    }

    // Agregar paginación
    params.push(limit, offset);

    const whereClause =
      whereConditions.length > 0
        ? "WHERE " + whereConditions.join(" AND ")
        : "";

    // Query principal
    const result = await pool.query(
      `
      WITH latest_checks AS (
        SELECT DISTINCT ON (schema_name, table_name, source_db_engine)
          id,
          schema_name,
          table_name,
          source_db_engine,
          check_timestamp,
          total_rows,
          null_count,
          duplicate_count,
          data_checksum,
          invalid_type_count,
          type_mismatch_details,
          out_of_range_count,
          referential_integrity_errors,
          constraint_violation_count,
          integrity_check_details,
          validation_status,
          error_details,
          quality_score,
          check_duration_ms,
          updated_at
        FROM metadata.data_quality
        ORDER BY schema_name, table_name, source_db_engine, updated_at DESC
      )
      SELECT *
      FROM latest_checks
      ${whereClause}
      ORDER BY check_timestamp DESC
      LIMIT $${paramCount} OFFSET $${paramCount + 1}
    `,
      params
    );

    // Query para el total
    const totalResult = await pool.query(
      `
      WITH latest_checks AS (
        SELECT DISTINCT ON (schema_name, table_name, source_db_engine)
          id
        FROM metadata.data_quality
        ORDER BY schema_name, table_name, source_db_engine, updated_at DESC
      )
      SELECT COUNT(*) as total
      FROM latest_checks
      ${whereClause}
    `,
      params.slice(0, -2)
    );

    const total = parseInt(totalResult.rows[0].total);
    const totalPages = Math.ceil(total / limit);

    res.json({
      data: result.rows,
      pagination: {
        total,
        totalPages,
        currentPage: page,
        limit,
      },
    });
  } catch (err) {
    console.error("Error getting quality metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas de calidad",
      details: err.message,
    });
  }
});

// Obtener datos de governance
app.get("/api/governance/data", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const offset = (page - 1) * limit;
    const engine = req.query.engine || "";
    const category = req.query.category || "";
    const health = req.query.health || "";
    const domain = req.query.domain || "";
    const sensitivity = req.query.sensitivity || "";

    // Construir WHERE clause dinámicamente
    const whereConditions = [];
    const params = [];
    let paramCount = 1;

    if (engine) {
      whereConditions.push(`inferred_source_engine = $${paramCount}`);
      params.push(engine);
      paramCount++;
    }

    if (category) {
      whereConditions.push(`data_category = $${paramCount}`);
      params.push(category);
      paramCount++;
    }

    if (health) {
      whereConditions.push(`health_status = $${paramCount}`);
      params.push(health);
      paramCount++;
    }

    if (domain) {
      whereConditions.push(`business_domain = $${paramCount}`);
      params.push(domain);
      paramCount++;
    }

    if (sensitivity) {
      whereConditions.push(`sensitivity_level = $${paramCount}`);
      params.push(sensitivity);
      paramCount++;
    }

    // Agregar paginación
    params.push(limit, offset);

    const whereClause =
      whereConditions.length > 0
        ? "WHERE " + whereConditions.join(" AND ")
        : "";

    // Query principal
    const result = await pool.query(
      `
      SELECT *
      FROM metadata.data_governance_catalog
      ${whereClause}
      ORDER BY last_analyzed DESC NULLS LAST
      LIMIT $${paramCount} OFFSET $${paramCount + 1}
    `,
      params
    );

    // Query para el total
    const totalResult = await pool.query(
      `
      SELECT COUNT(*) as total
      FROM metadata.data_governance_catalog
      ${whereClause}
    `,
      params.slice(0, -2)
    );

    const total = parseInt(totalResult.rows[0].total);
    const totalPages = Math.ceil(total / limit);

    res.json({
      data: result.rows,
      pagination: {
        total,
        totalPages,
        currentPage: page,
        limit,
      },
    });
  } catch (err) {
    console.error("Error getting governance data:", err);
    res.status(500).json({
      error: "Error al obtener datos de governance",
      details: err.message,
    });
  }
});

// Endpoints para la configuración del sistema
app.get("/api/config", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT key, value, description, updated_at FROM metadata.config ORDER BY key"
    );
    res.json(result.rows);
  } catch (err) {
    console.error("Error getting configurations:", err);
    res.status(500).json({
      error: "Error al obtener configuraciones",
      details: err.message,
    });
  }
});

app.post("/api/config", async (req, res) => {
  const { key, value, description } = req.body;
  try {
    const result = await pool.query(
      "INSERT INTO metadata.config (key, value, description) VALUES ($1, $2, $3) RETURNING *",
      [key, value, description]
    );
    res.json(result.rows[0]);
  } catch (err) {
    console.error("Error creating configuration:", err);
    res.status(500).json({
      error: "Error al crear configuración",
      details: err.message,
    });
  }
});

app.put("/api/config/:key", async (req, res) => {
  const { key } = req.params;
  const { value, description } = req.body;
  try {
    const result = await pool.query(
      "UPDATE metadata.config SET value = $1, description = $2, updated_at = NOW() WHERE key = $3 RETURNING *",
      [value, description, key]
    );
    if (result.rows.length === 0) {
      res.status(404).json({ error: "Configuración no encontrada" });
      return;
    }
    res.json(result.rows[0]);
  } catch (err) {
    console.error("Error updating configuration:", err);
    res.status(500).json({
      error: "Error al actualizar configuración",
      details: err.message,
    });
  }
});

app.delete("/api/config/:key", async (req, res) => {
  const { key } = req.params;
  try {
    const result = await pool.query(
      "DELETE FROM metadata.config WHERE key = $1 RETURNING *",
      [key]
    );
    if (result.rows.length === 0) {
      res.status(404).json({ error: "Configuración no encontrada" });
      return;
    }
    res.json({ message: "Configuración eliminada correctamente" });
  } catch (err) {
    console.error("Error deleting configuration:", err);
    res.status(500).json({
      error: "Error al eliminar configuración",
      details: err.message,
    });
  }
});

// Endpoint para leer logs
app.get("/api/logs", async (req, res) => {
  try {
    const { lines = 100, level = 'ALL' } = req.query;
    const logFilePath = path.join(process.cwd(), '..', 'DataSync.log');
    
    // Verificar si el archivo existe
    if (!fs.existsSync(logFilePath)) {
      return res.json({
        logs: [],
        totalLines: 0,
        message: 'No log file found'
      });
    }

    // Leer el archivo de logs
    const logContent = fs.readFileSync(logFilePath, 'utf8');
    const allLines = logContent.split('\n').filter(line => line.trim() !== '');
    
    // Filtrar por nivel si se especifica
    let filteredLines = allLines;
    if (level !== 'ALL') {
      filteredLines = allLines.filter(line => 
        line.includes(`[${level.toUpperCase()}]`)
      );
    }
    
    // Obtener las últimas N líneas
    const lastLines = filteredLines.slice(-parseInt(lines));
    
    // Parsear cada línea de log
    const parsedLogs = lastLines.map((line, index) => {
      const logMatch = line.match(/^\[([^\]]+)\] \[([^\]]+)\](?: \[([^\]]+)\])? (.+)$/);
      if (logMatch) {
        return {
          id: index,
          timestamp: logMatch[1],
          level: logMatch[2],
          function: logMatch[3] || '',
          message: logMatch[4],
          raw: line
        };
      }
      return {
        id: index,
        timestamp: '',
        level: 'UNKNOWN',
        function: '',
        message: line,
        raw: line
      };
    });

    res.json({
      logs: parsedLogs,
      totalLines: filteredLines.length,
      filePath: logFilePath,
      lastModified: fs.statSync(logFilePath).mtime
    });
    
  } catch (err) {
    console.error("Error reading logs:", err);
    res.status(500).json({
      error: "Error al leer logs",
      details: err.message,
    });
  }
});

// Endpoint para obtener información del archivo de logs
app.get("/api/logs/info", async (req, res) => {
  try {
    const logFilePath = path.join(process.cwd(), '..', 'DataSync.log');
    
    if (!fs.existsSync(logFilePath)) {
      return res.json({
        exists: false,
        message: 'No log file found'
      });
    }

    const stats = fs.statSync(logFilePath);
    const logContent = fs.readFileSync(logFilePath, 'utf8');
    const totalLines = logContent.split('\n').filter(line => line.trim() !== '').length;
    
    res.json({
      exists: true,
      filePath: logFilePath,
      size: stats.size,
      totalLines: totalLines,
      lastModified: stats.mtime,
      created: stats.birthtime
    });
    
  } catch (err) {
    console.error("Error getting log info:", err);
    res.status(500).json({
      error: "Error al obtener información de logs",
      details: err.message,
    });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
