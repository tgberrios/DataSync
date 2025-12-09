import express from "express";
import pkg from "pg";
const { Pool } = pkg;
import cors from "cors";
import { spawn } from "child_process";
import os from "os";
import path from "path";
import fs from "fs";

// Load configuration from shared config file
function loadConfig() {
  try {
    const configPath = path.join(process.cwd(), "..", "config.json");
    const configData = fs.readFileSync(configPath, "utf8");
    const config = JSON.parse(configData);

    console.log("Configuration loaded from:", configPath);
    return config;
  } catch (error) {
    console.error("Error loading config file:", error.message);
    console.log("Using default configuration");

    // Default configuration fallback
    return {
      database: {
        postgres: {
          host: "10.12.240.40",
          port: 5432,
          database: "DataLake",
          user: "Datalake_User",
          password: "keepprofessional",
        },
      },
    };
  }
}

const config = loadConfig();

const app = express();
app.use(cors());
app.use(express.json());

// Middleware: normalize schema/table identifiers to lowercase in body and query
app.use((req, _res, next) => {
  try {
    if (req.body) {
      if (typeof req.body.schema_name === "string") {
        req.body.schema_name = req.body.schema_name.toLowerCase();
      }
      if (typeof req.body.table_name === "string") {
        req.body.table_name = req.body.table_name.toLowerCase();
      }
    }
    if (req.query) {
      if (typeof req.query.schema_name === "string") {
        req.query.schema_name = req.query.schema_name.toLowerCase();
      }
      if (typeof req.query.table_name === "string") {
        req.query.table_name = req.query.table_name.toLowerCase();
      }
    }
  } catch {}
  next();
});

const pool = new Pool({
  host: config.database.postgres.host,
  port: config.database.postgres.port,
  database: config.database.postgres.database,
  user: config.database.postgres.user,
  password: config.database.postgres.password,
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
    if (req.query.strategy && req.query.strategy !== "") {
      paramCount++;
      whereConditions.push(`pk_strategy = $${paramCount}`);
      queryParams.push(req.query.strategy);
    }

    const whereClause =
      whereConditions.length > 0
        ? `WHERE ${whereConditions.join(" AND ")}`
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.catalog ${whereClause}`;
    const countResult = await pool.query(countQuery, queryParams);
    const total = parseInt(countResult.rows[0].count);

    paramCount++;
    const dataQuery = `SELECT * FROM metadata.catalog ${whereClause}
      ORDER BY 
        CASE status
          WHEN 'LISTENING_CHANGES' THEN 1
          WHEN 'FULL_LOAD' THEN 2
          WHEN 'ERROR' THEN 3
          WHEN 'NO_DATA' THEN 4
          WHEN 'SKIP' THEN 5
          ELSE 6
        END,
        active DESC,
        schema_name,
        table_name
      LIMIT $${paramCount} OFFSET $${paramCount + 1}`;
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

// Obtener todos los schemas únicos
app.get("/api/catalog/schemas", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT DISTINCT schema_name FROM metadata.catalog ORDER BY schema_name`
    );
    res.json(result.rows.map((row) => row.schema_name));
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Marcar tabla como SKIP
app.patch("/api/catalog/skip-table", async (req, res) => {
  const { schema_name, table_name, db_engine } = req.body;
  try {
    const result = await pool.query(
      `UPDATE metadata.catalog 
       SET status = 'SKIP', active = false
       WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3
       RETURNING *`,
      [schema_name, table_name, db_engine]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Table not found" });
    }

    res.json({
      message: `Table ${schema_name}.${table_name} marked as SKIP`,
      affectedRows: result.rows.length,
      entry: result.rows[0],
    });
  } catch (err) {
    console.error("Database error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Desactivar schema completo
app.patch("/api/catalog/deactivate-schema", async (req, res) => {
  const { schema_name } = req.body;
  try {
    const result = await pool.query(
      `UPDATE metadata.catalog 
       SET status = 'SKIPPED'
       WHERE schema_name = $1
       RETURNING *`,
      [schema_name]
    );
    res.json({
      message: `Schema ${schema_name} deactivated successfully`,
      affectedRows: result.rows.length,
      rows: result.rows,
    });
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

    // 1. SYNCHRONIZATION STATUS - solo contar registros activos
    const syncStatus = await pool.query(`
      SELECT 
        COUNT(*) FILTER (WHERE active = true AND status = 'LISTENING_CHANGES') as listening_changes,
        COUNT(*) FILTER (WHERE active = true) as full_load_active,
        COUNT(*) FILTER (WHERE active = false) as full_load_inactive,
        COUNT(*) FILTER (WHERE active = false AND status = 'NO_DATA') as no_data,
        COUNT(*) FILTER (WHERE status = 'SKIP') as skip,
        COUNT(*) FILTER (WHERE active = true AND status = 'ERROR') as errors,
        '' as current_process
      FROM metadata.catalog
    `);

    // 2. DATA PROGRESS METRICS - total_data
    const dataProgress = await pool.query(`
      SELECT 
        COALESCE(SUM(table_size), 0) as total_data
      FROM metadata.catalog
      WHERE active = true AND status IN ('LISTENING_CHANGES', 'FULL_LOAD')
    `);

    // Get currently processing table
    const currentProcessingTable = await pool.query(`
      SELECT t.*
      FROM metadata.catalog t
      WHERE status = 'FULL_LOAD'
      LIMIT 1
    `);

    const currentProcessText =
      currentProcessingTable.rows.length > 0
        ? `${String(
            currentProcessingTable.rows[0].schema_name
          ).toLowerCase()}.${String(
            currentProcessingTable.rows[0].table_name
          ).toLowerCase()} [${
            currentProcessingTable.rows[0].db_engine
          }] - Status: ${currentProcessingTable.rows[0].status}`
        : "No active transfers";

    // 2. TRANSFER PERFORMANCE BY ENGINE
    const transferPerformance = await pool.query(`
      SELECT 
        db_engine,
        COUNT(*) FILTER (WHERE status = 'PROCESSING' AND completed_at IS NULL) as active_transfers,
        ROUND(AVG(memory_used_mb)::numeric, 2) as avg_memory_used,
        ROUND(AVG(io_operations_per_second)::numeric, 2) as avg_iops,
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

    // Connection pooling removed - using direct connections now

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

    // Total tables count
    const totalTablesResult = await pool.query(
      `SELECT COUNT(*) as total FROM metadata.catalog`
    );

    // Construir el objeto de respuesta
    const listeningChanges = parseInt(
      syncStatus.rows[0]?.listening_changes || 0
    );
    const fullLoadActive = parseInt(syncStatus.rows[0]?.full_load_active || 0);
    const pending = Math.max(0, fullLoadActive - listeningChanges);

    const stats = {
      syncStatus: {
        progress: 0,
        listeningChanges: listeningChanges,
        pending: pending,
        fullLoadActive: fullLoadActive,
        fullLoadInactive: parseInt(syncStatus.rows[0]?.full_load_inactive || 0),
        noData: parseInt(syncStatus.rows[0]?.no_data || 0),
        skip: parseInt(syncStatus.rows[0]?.skip || 0),
        errors: parseInt(syncStatus.rows[0]?.errors || 0),
        currentProcess: currentProcessText,
        totalData: parseInt(dataProgress.rows[0]?.total_data || 0),
        totalTables: parseInt(totalTablesResult.rows[0]?.total || 0),
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
      // Connection pooling removed - using direct connections now
    };

    // Calcular progreso total - solo contar registros activos
    const totalActive =
      stats.syncStatus.listeningChanges +
      stats.syncStatus.fullLoadActive +
      stats.syncStatus.errors;
    const total =
      totalActive + stats.syncStatus.fullLoadInactive + stats.syncStatus.noData;

    // El progreso se calcula como: Listening Changes / Total Active * 100
    // Representa qué porcentaje de tablas activas han llegado a LISTENING_CHANGES
    stats.syncStatus.progress =
      totalActive > 0
        ? Math.round((stats.syncStatus.listeningChanges / totalActive) * 100)
        : 0;

    console.log("Progress calculation:", {
      listeningChanges: stats.syncStatus.listeningChanges,
      pending: stats.syncStatus.pending,
      noData: stats.syncStatus.noData,
      fullLoadActive: stats.syncStatus.fullLoadActive,
      fullLoadInactive: stats.syncStatus.fullLoadInactive,
      errors: stats.syncStatus.errors,
      totalActive: totalActive,
      total: total,
      progress: stats.syncStatus.progress + "%",
    });

    // Debug: Verificar datos de la consulta original
    console.log("Raw sync status query result:", syncStatus.rows[0]);

    // Agregar métricas por motor
    stats.engineMetrics = {};
    transferPerformance.rows.forEach((metric) => {
      stats.engineMetrics[metric.db_engine] = {
        recordsPerSecond: 0,
        bytesTransferred: parseFloat(metric.total_bytes),
        cpuUsage: 0,
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

    // MÉTRICAS PARA CARDS INFORMATIVAS

    // 1. Top 10 Tablas por Throughput (Records/Segundo)
    const topTablesThroughput = await pool.query(`
      SELECT 
        tm.schema_name,
        tm.table_name,
        tm.db_engine,
        ROUND(tm.records_transferred::numeric / NULLIF(EXTRACT(EPOCH FROM (tm.completed_at - tm.created_at)), 0), 2) as throughput_rps,
        tm.records_transferred
      FROM metadata.transfer_metrics tm
      WHERE tm.created_at > NOW() - INTERVAL '24 hours'
        AND tm.completed_at IS NOT NULL
        AND tm.records_transferred > 0
      ORDER BY throughput_rps DESC
      LIMIT 10
    `);

    // 2. IO Operations promedio actual
    const currentIops = await pool.query(`
      SELECT ROUND(AVG(io_operations_per_second)::numeric, 2) as avg_iops
      FROM metadata.transfer_metrics
      WHERE created_at > NOW() - INTERVAL '1 hour'
        AND io_operations_per_second > 0
    `);

    // 3. Volumen de datos por tabla (últimos 7 días)
    const dataVolumeByTable = await pool.query(`
      SELECT 
        tm.schema_name,
        tm.table_name,
        tm.db_engine,
        SUM(tm.bytes_transferred) as total_bytes,
        COUNT(tm.id) as transfer_count
      FROM metadata.transfer_metrics tm
      WHERE tm.created_at > NOW() - INTERVAL '7 days'
        AND tm.bytes_transferred > 0
      GROUP BY tm.schema_name, tm.table_name, tm.db_engine
      ORDER BY total_bytes DESC
      LIMIT 10
    `);

    // 4. Throughput actual (última hora)
    const currentThroughput = await pool.query(`
      SELECT 
        ROUND(AVG(records_transferred::numeric / NULLIF(EXTRACT(EPOCH FROM (completed_at - created_at)), 0))::numeric, 2) as avg_throughput_rps,
        SUM(records_transferred) as total_records,
        COUNT(*) as transfer_count
      FROM metadata.transfer_metrics
      WHERE created_at > NOW() - INTERVAL '1 hour'
        AND completed_at IS NOT NULL
        AND records_transferred > 0
    `);

    // Agregar las métricas al response
    stats.metricsCards = {
      topTablesThroughput: topTablesThroughput.rows.map((row) => ({
        tableName: `${row.schema_name}.${row.table_name}`,
        dbEngine: row.db_engine,
        throughputRps: parseFloat(row.throughput_rps || 0),
        recordsTransferred: parseInt(row.records_transferred || 0),
      })),
      currentIops: parseFloat(currentIops.rows[0]?.avg_iops || 0),
      dataVolumeByTable: dataVolumeByTable.rows.map((row) => ({
        tableName: `${row.schema_name}.${row.table_name}`,
        dbEngine: row.db_engine,
        totalBytes: parseInt(row.total_bytes || 0),
        transferCount: parseInt(row.transfer_count || 0),
      })),
      currentThroughput: {
        avgRps: parseFloat(currentThroughput.rows[0]?.avg_throughput_rps || 0),
        totalRecords: parseInt(currentThroughput.rows[0]?.total_records || 0),
        transferCount: parseInt(currentThroughput.rows[0]?.transfer_count || 0),
      },
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
          id,
          schema_name,
          table_name,
          source_db_engine,
          check_timestamp,
          total_rows,
          null_count,
          duplicate_count,
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

    // Sorting
    const allowedSortFields = new Set([
      "table_name",
      "schema_name",
      "inferred_source_engine",
      "data_category",
      "business_domain",
      "health_status",
      "sensitivity_level",
      "data_quality_score",
      "table_size_mb",
      "total_rows",
      "access_frequency",
      "last_analyzed",
    ]);
    const sortField = String(req.query.sort_field || "");
    const sortDir =
      String(req.query.sort_direction || "desc").toLowerCase() === "asc"
        ? "ASC"
        : "DESC";
    let orderClause =
      "ORDER BY CASE health_status WHEN 'HEALTHY' THEN 1 WHEN 'WARNING' THEN 2 WHEN 'CRITICAL' THEN 3 ELSE 4 END";
    if (allowedSortFields.has(sortField)) {
      orderClause = `ORDER BY ${sortField} ${sortDir}`;
    }

    // Query principal
    const result = await pool.query(
      `
      SELECT *
      FROM metadata.data_governance_catalog
      ${whereClause}
      ${orderClause}
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

// Obtener configuración de batch size específicamente
app.get("/api/config/batch", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT t.* FROM metadata.config t WHERE key = 'chunk_size'"
    );
    if (result.rows.length === 0) {
      res.json({
        key: "chunk_size",
        value: "25000",
        description: "Tamaño de lote para procesamiento de datos",
        updated_at: new Date().toISOString(),
      });
    } else {
      res.json(result.rows[0]);
    }
  } catch (err) {
    console.error("Error getting batch configuration:", err);
    res.status(500).json({
      error: "Error al obtener configuración de batch",
      details: err.message,
    });
  }
});

// Endpoint para leer logs desde DB (metadata.logs)
app.get("/api/logs", async (req, res) => {
  try {
    const {
      lines = 100,
      level = "ALL",
      category = "ALL",
      function: func = "ALL",
      search = "",
      startDate = "",
      endDate = "",
    } = req.query;

    const params = [];
    let where = [];

    if (level && level !== "ALL") {
      params.push(level);
      where.push(`level = $${params.length}`);
    }
    if (category && category !== "ALL") {
      params.push(category);
      where.push(`category = $${params.length}`);
    }
    if (func && func !== "ALL") {
      params.push(func);
      where.push(`function = $${params.length}`);
    }
    if (search && String(search).trim() !== "") {
      params.push(`%${search}%`);
      params.push(`%${search}%`);
      where.push(
        `(message ILIKE $${params.length - 1} OR function ILIKE $${
          params.length
        })`
      );
    }
    if (startDate) {
      params.push(startDate);
      where.push(`ts >= $${params.length}`);
    }
    if (endDate) {
      params.push(endDate);
      where.push(`ts <= $${params.length}`);
    }

    const whereClause = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const limit = Math.max(1, parseInt(lines));

    const query = `
      SELECT ts, level, category, function, message
      FROM metadata.logs
      ${whereClause}
      ORDER BY ts DESC
      LIMIT $${params.length + 1}
    `;
    const result = await pool.query(query, [...params, limit]);

    const logs = result.rows.map((r) => {
      const tsIso = r.ts ? new Date(r.ts).toISOString() : null;
      const lvl = (r.level || "").toUpperCase();
      const cat = (r.category || "").toUpperCase();
      return {
        timestamp: tsIso,
        level: lvl,
        category: cat,
        function: r.function || "",
        message: r.message || "",
        raw: `[${tsIso ?? ""}] [${lvl}] [${cat}] [${r.function || ""}] ${
          r.message || ""
        }`,
        parsed: true,
      };
    });

    res.json({
      logs,
      totalLines: logs.length,
      filePath: "metadata.logs",
      lastModified: new Date().toISOString(),
      filters: { level, category, function: func, search, startDate, endDate },
    });
  } catch (err) {
    console.error("Error reading logs from DB:", err);
    res.status(500).json({ error: "Error al leer logs", details: err.message });
  }
});

// Endpoint para obtener logs de errores desde DB (niveles WARNING/ERROR/CRITICAL)
app.get("/api/logs/errors", async (req, res) => {
  try {
    const {
      lines = 100,
      category = "ALL",
      search = "",
      startDate = "",
      endDate = "",
    } = req.query;
    const params = [];
    let where = ["level IN ('WARNING','ERROR','CRITICAL')"];
    if (category && category !== "ALL") {
      params.push(category);
      where.push(`category = $${params.length}`);
    }
    if (search && String(search).trim() !== "") {
      params.push(`%${search}%`);
      params.push(`%${search}%`);
      where.push(
        `(message ILIKE $${params.length - 1} OR function ILIKE $${
          params.length
        })`
      );
    }
    if (startDate) {
      params.push(startDate);
      where.push(`ts >= $${params.length}`);
    }
    if (endDate) {
      params.push(endDate);
      where.push(`ts <= $${params.length}`);
    }
    const whereClause = `WHERE ${where.join(" AND ")}`;
    const limit = Math.max(1, parseInt(lines));
    const q = `SELECT ts, level, category, function, message FROM metadata.logs ${whereClause} ORDER BY ts DESC LIMIT $${
      params.length + 1
    }`;
    const result = await pool.query(q, [...params, limit]);
    const logs = result.rows.map((r) => ({
      timestamp: r.ts,
      level: r.level,
      category: r.category,
      function: r.function,
      message: r.message,
      raw: `[${r.ts}] [${r.level}] [${r.category}] [${r.function}] ${r.message}`,
      parsed: true,
    }));
    res.json({
      logs,
      totalLines: logs.length,
      filePath: "metadata.logs",
      lastModified: new Date().toISOString(),
      filters: { category, search, startDate, endDate },
    });
  } catch (err) {
    console.error("Error reading error logs from DB:", err);
    res
      .status(500)
      .json({ error: "Error al leer logs de errores", details: err.message });
  }
});

// Información de logs desde DB
app.get("/api/logs/info", async (req, res) => {
  try {
    const countRes = await pool.query(
      "SELECT COUNT(*) AS total FROM metadata.logs"
    );
    const lastRes = await pool.query(
      "SELECT MAX(ts) AS last_modified FROM metadata.logs"
    );
    res.json({
      exists: true,
      filePath: "metadata.logs",
      size: null,
      totalLines: parseInt(countRes.rows[0]?.total || 0),
      lastModified: lastRes.rows[0]?.last_modified || null,
      created: null,
    });
  } catch (err) {
    console.error("Error getting DB log info:", err);
    res.status(500).json({
      error: "Error al obtener información de logs",
      details: err.message,
    });
  }
});

app.get("/api/logs/errors/info", async (req, res) => {
  try {
    const countRes = await pool.query(
      "SELECT COUNT(*) AS total FROM metadata.logs WHERE level IN ('WARNING','ERROR','CRITICAL')"
    );
    const lastRes = await pool.query(
      "SELECT MAX(ts) AS last_modified FROM metadata.logs WHERE level IN ('WARNING','ERROR','CRITICAL')"
    );
    res.json({
      exists: true,
      filePath: "metadata.logs",
      size: null,
      totalLines: parseInt(countRes.rows[0]?.total || 0),
      lastModified: lastRes.rows[0]?.last_modified || null,
      created: null,
    });
  } catch (err) {
    console.error("Error getting DB error log info:", err);
    res.status(500).json({
      error: "Error al obtener información de logs de errores",
      details: err.message,
    });
  }
});

// Endpoints de filtros para logs desde DB
app.get("/api/logs/levels", async (_req, res) => {
  try {
    const result = await pool.query(
      "SELECT DISTINCT level FROM metadata.logs ORDER BY level"
    );
    res.json(result.rows.map((r) => r.level));
  } catch (err) {
    res
      .status(500)
      .json({ error: "Error al obtener niveles", details: err.message });
  }
});

app.get("/api/logs/categories", async (_req, res) => {
  try {
    const result = await pool.query(
      "SELECT DISTINCT category FROM metadata.logs ORDER BY category"
    );
    res.json(result.rows.map((r) => r.category));
  } catch (err) {
    res
      .status(500)
      .json({ error: "Error al obtener categorías", details: err.message });
  }
});

app.get("/api/logs/functions", async (_req, res) => {
  try {
    const result = await pool.query(
      "SELECT DISTINCT function FROM metadata.logs ORDER BY function"
    );
    res.json(result.rows.map((r) => r.function));
  } catch (err) {
    res
      .status(500)
      .json({ error: "Error al obtener funciones", details: err.message });
  }
});

// Endpoint para obtener estadísticas de logs
app.get("/api/logs/stats", async (req, res) => {
  try {
    const logFilePath = path.join(process.cwd(), "..", "DataSync.log");

    if (!fs.existsSync(logFilePath)) {
      return res.json({
        stats: {},
        message: "Log file not found",
      });
    }

    const logContent = fs.readFileSync(logFilePath, "utf8");
    const allLines = logContent
      .split("\n")
      .filter((line) => line.trim() !== "");

    // Parsear logs para estadísticas
    const parsedLogs = allLines.map((line) => {
      const newMatch = line.match(
        /^\[([^\]]+)\] \[([^\]]+)\] \[([^\]]+)\] \[([^\]]+)\] (.+)$/
      );
      if (newMatch) {
        return {
          level: newMatch[2],
          category: newMatch[3],
          function: newMatch[4],
          timestamp: newMatch[1],
        };
      }
      const oldMatch = line.match(
        /^\[([^\]]+)\] \[([^\]]+)\](?: \[([^\]]+)\])? (.+)$/
      );
      if (oldMatch) {
        return {
          level: oldMatch[2],
          category: "SYSTEM",
          function: oldMatch[3] || "",
          timestamp: oldMatch[1],
        };
      }
      return {
        level: "UNKNOWN",
        category: "UNKNOWN",
        function: "",
        timestamp: "",
      };
    });

    // Calcular estadísticas
    const stats = {
      total: parsedLogs.length,
      byLevel: {},
      byCategory: {},
      byFunction: {},
      recent: parsedLogs.slice(-10).map((log) => ({
        timestamp: log.timestamp,
        level: log.level,
        category: log.category,
        function: log.function,
      })),
    };

    // Contar por nivel
    parsedLogs.forEach((log) => {
      stats.byLevel[log.level] = (stats.byLevel[log.level] || 0) + 1;
    });

    // Contar por categoría
    parsedLogs.forEach((log) => {
      stats.byCategory[log.category] =
        (stats.byCategory[log.category] || 0) + 1;
    });

    // Contar por función (top 10)
    parsedLogs.forEach((log) => {
      if (log.function) {
        stats.byFunction[log.function] =
          (stats.byFunction[log.function] || 0) + 1;
      }
    });

    // Convertir a array y ordenar
    stats.byFunction = Object.entries(stats.byFunction)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 10)
      .reduce((obj, [key, value]) => {
        obj[key] = value;
        return obj;
      }, {});

    res.json({
      stats,
      generatedAt: new Date().toISOString(),
    });
  } catch (err) {
    console.error("Error getting log stats:", err);
    res.status(500).json({
      error: "Error al obtener estadísticas de logs",
      details: err.message,
    });
  }
});

// Endpoint para limpiar logs
app.delete("/api/logs", async (req, res) => {
  try {
    await pool.query("TRUNCATE TABLE metadata.logs");
    res.json({
      success: true,
      message: "Logs table truncated",
      clearedAt: new Date().toISOString(),
    });
  } catch (err) {
    console.error("Error truncating logs:", err);
    res
      .status(500)
      .json({ error: "Error al truncar logs", details: err.message });
  }
});

// Endpoint para obtener tabla actualmente procesándose
app.get("/api/dashboard/currently-processing", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        schema_name,
        table_name,
        db_engine,
        new_offset,
        new_pk,
        status,
        processed_at
      FROM metadata.processing_log
      WHERE processed_at > NOW() - INTERVAL '5 minutes'
      ORDER BY processed_at DESC
      LIMIT 1
    `);

    if (result.rows.length === 0) {
      return res.json(null);
    }

    const processingTable = result.rows[0];

    // Hacer COUNT de la tabla que se está procesando (resolver nombres reales insensibles a mayúsculas)
    let countResult;
    try {
      const providedSchema = String(processingTable.schema_name);
      const providedTable = String(processingTable.table_name);

      const resolved = await pool.query(
        `
        SELECT n.nspname AS schema_name, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE lower(n.nspname) = lower($1)
          AND lower(c.relname) = lower($2)
          AND c.relkind = 'r'
        LIMIT 1
        `,
        [providedSchema, providedTable]
      );

      if (resolved.rows.length === 0) {
        throw new Error(`relation not found`);
      }

      const schema = String(resolved.rows[0].schema_name).replace(/"/g, '""');
      const table = String(resolved.rows[0].table_name).replace(/"/g, '""');
      const countSql = `SELECT COUNT(*) as total_records FROM "${schema}"."${table}"`;
      countResult = await pool.query(countSql);
    } catch (countError) {
      console.warn(
        `Could not get count for ${processingTable.schema_name}.${processingTable.table_name}:`,
        countError.message
      );
      countResult = { rows: [{ total_records: 0 }] };
    }

    const response = {
      ...processingTable,
      schema_name: String(processingTable.schema_name).toLowerCase(),
      table_name: String(processingTable.table_name).toLowerCase(),
      db_engine: String(processingTable.db_engine),
      total_records: parseInt(countResult.rows[0]?.total_records || 0),
    };

    res.json(response);
  } catch (err) {
    console.error("Error getting currently processing table:", err);
    res.status(500).json({ error: err.message });
  }
});

// Endpoint para obtener datos de seguridad
app.get("/api/security/data", async (req, res) => {
  try {
    console.log("Fetching security data...");

    // 1. USER MANAGEMENT
    const users = await pool.query(`
      SELECT 
        COUNT(*) as total_users,
        COUNT(*) FILTER (WHERE rolcanlogin = true) as users_with_login,
        COUNT(*) FILTER (WHERE rolsuper = true) as superusers
      FROM pg_roles
    `);

    const activeUsersCount = await pool.query(`
      SELECT COUNT(DISTINCT usename) as active_users
      FROM pg_stat_activity
      WHERE usename IS NOT NULL
    `);

    // 2. CONNECTION STATUS
    const connections = await pool.query(`
      SELECT 
        COUNT(*) as current_connections,
        (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections,
        COUNT(*) FILTER (WHERE state = 'idle') as idle_connections,
        COUNT(*) FILTER (WHERE state = 'active') as active_connections
      FROM pg_stat_activity
    `);

    // 3. ACTIVE USERS
    const activeUsers = await pool.query(`
      SELECT 
        usename as username,
        CASE 
          WHEN r.rolsuper THEN 'SUPERUSER'
          WHEN r.rolcreatedb THEN 'CREATEDB'
          WHEN r.rolcreaterole THEN 'CREATEROLE'
          WHEN r.rolcanlogin THEN 'LOGIN'
          ELSE 'OTHER'
        END as role_type,
        CASE 
          WHEN sa.state = 'active' THEN 'ACTIVE'
          WHEN sa.state = 'idle' THEN 'IDLE'
          ELSE 'INACTIVE'
        END as status,
        COALESCE(sa.query_start, sa.backend_start) as last_activity,
        sa.client_addr,
        sa.application_name
      FROM pg_stat_activity sa
      JOIN pg_roles r ON sa.usename = r.rolname
      WHERE sa.usename IS NOT NULL
      ORDER BY last_activity DESC
      LIMIT 20
    `);

    // 4. PERMISSIONS OVERVIEW
    const permissionsOverview = await pool.query(`
      SELECT 
        COUNT(*) as total_grants,
        COUNT(DISTINCT table_schema) as schemas_with_access,
        COUNT(DISTINCT table_name) as tables_with_access
      FROM information_schema.table_privileges
      WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    `);

    const securityData = {
      summary: {
        users: {
          total: parseInt(users.rows[0]?.total_users || 0),
          active: parseInt(activeUsersCount.rows[0]?.active_users || 0),
          superusers: parseInt(users.rows[0]?.superusers || 0),
          withLogin: parseInt(users.rows[0]?.users_with_login || 0),
        },
        connections: {
          current: parseInt(connections.rows[0]?.current_connections || 0),
          max: parseInt(connections.rows[0]?.max_connections || 0),
          idle: parseInt(connections.rows[0]?.idle_connections || 0),
          active: parseInt(connections.rows[0]?.active_connections || 0),
        },
        permissions: {
          totalGrants: parseInt(permissionsOverview.rows[0]?.total_grants || 0),
          schemasWithAccess: parseInt(
            permissionsOverview.rows[0]?.schemas_with_access || 0
          ),
          tablesWithAccess: parseInt(
            permissionsOverview.rows[0]?.tables_with_access || 0
          ),
        },
      },
      activeUsers: activeUsers.rows,
    };

    console.log("Sending security data");
    res.json(securityData);
  } catch (err) {
    console.error("Error getting security data:", err);
    res.status(500).json({
      error: "Error al obtener datos de seguridad",
      details: err.message,
    });
  }
});

// Endpoints para Live Changes (Processing Logs)
app.get("/api/monitor/processing-logs", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const strategy = req.query.strategy || "";

    const whereConditions = [];
    const params = [];
    let paramCount = 1;

    if (strategy) {
      whereConditions.push(`c.pk_strategy = $${paramCount}`);
      params.push(strategy);
      paramCount++;
    }

    const whereClause =
      whereConditions.length > 0
        ? `WHERE ${whereConditions.join(" AND ")}`
        : "";

    // Get total count
    const countResult = await pool.query(
      `
      SELECT COUNT(*) as total
      FROM metadata.processing_log pl
      JOIN metadata.catalog c
        ON c.schema_name = pl.schema_name
       AND c.table_name  = pl.table_name
       AND c.db_engine   = pl.db_engine
      ${whereClause}
    `,
      params
    );

    const total = parseInt(countResult.rows[0].total);
    const totalPages = Math.ceil(total / limit);

    // Get paginated data
    params.push(limit, offset);
    const result = await pool.query(
      `
      SELECT 
        pl.id,
        pl.schema_name,
        pl.table_name,
        pl.db_engine,
        c.pk_strategy,
        pl.old_offset,
        pl.new_offset,
        pl.old_pk,
        pl.new_pk,
        pl.status,
        pl.processed_at
      FROM metadata.processing_log pl
      JOIN metadata.catalog c
        ON c.schema_name = pl.schema_name
       AND c.table_name  = pl.table_name
       AND c.db_engine   = pl.db_engine
      ${whereClause}
      ORDER BY pl.processed_at DESC
      LIMIT $${paramCount} OFFSET $${paramCount + 1}
    `,
      params
    );

    res.json({
      data: result.rows,
      pagination: {
        page,
        limit,
        total,
        totalPages,
        hasNext: page < totalPages,
        hasPrev: page > 1,
      },
    });
  } catch (err) {
    console.error("Error getting processing logs:", err);
    res.status(500).json({
      error: "Error al obtener logs de procesamiento",
      details: err.message,
    });
  }
});

app.get("/api/monitor/processing-logs/stats", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE processed_at > NOW() - INTERVAL '24 hours') as last24h,
        COUNT(*) FILTER (WHERE status = 'LISTENING_CHANGES') as listeningChanges,
        COUNT(*) FILTER (WHERE status = 'FULL_LOAD') as fullLoad,
        COUNT(*) FILTER (WHERE status = 'ERROR') as errors
      FROM metadata.processing_log
    `);

    res.json(result.rows[0]);
  } catch (err) {
    console.error("Error getting processing stats:", err);
    res.status(500).json({
      error: "Error al obtener estadísticas de procesamiento",
      details: err.message,
    });
  }
});

app.post("/api/monitor/processing-logs/cleanup", async (req, res) => {
  try {
    const result = await pool.query(`
      DELETE FROM metadata.processing_log 
      WHERE processed_at < NOW() - INTERVAL '24 hours'
    `);

    res.json({
      success: true,
      message: `Cleaned up ${result.rowCount} old processing log entries`,
      deletedCount: result.rowCount,
      cleanedAt: new Date().toISOString(),
    });
  } catch (err) {
    console.error("Error cleaning up processing logs:", err);
    res.status(500).json({
      error: "Error al limpiar logs antiguos",
      details: err.message,
    });
  }
});

app.get("/api/query-performance/queries", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const performance_tier = req.query.performance_tier || "";
    const operation_type = req.query.operation_type || "";
    const source_type = req.query.source_type || "";
    const search = req.query.search || "";

    const whereConditions = [];
    const params = [];
    let paramCount = 1;

    if (performance_tier) {
      whereConditions.push(`performance_tier = $${paramCount}`);
      params.push(performance_tier);
      paramCount++;
    }

    if (operation_type) {
      whereConditions.push(`operation_type = $${paramCount}`);
      params.push(operation_type);
      paramCount++;
    }

    if (source_type) {
      whereConditions.push(`source_type = $${paramCount}`);
      params.push(source_type);
      paramCount++;
    }

    if (search) {
      whereConditions.push(`query_text ILIKE $${paramCount}`);
      params.push(`%${search}%`);
      paramCount++;
    }

    const whereClause =
      whereConditions.length > 0
        ? "WHERE " + whereConditions.join(" AND ")
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.query_performance ${whereClause}`;
    const countResult = await pool.query(countQuery, params);
    const total = parseInt(countResult.rows[0].count);

    params.push(limit, offset);
    const dataQuery = `
      SELECT *
      FROM metadata.query_performance
      ${whereClause}
      ORDER BY captured_at DESC, query_efficiency_score DESC NULLS LAST
      LIMIT $${paramCount} OFFSET $${paramCount + 1}
    `;

    const result = await pool.query(dataQuery, params);

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
    console.error("Error getting query performance data:", err);
    res.status(500).json({
      error: "Error al obtener datos de rendimiento de queries",
      details: err.message,
    });
  }
});

app.get("/api/query-performance/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total_queries,
        COUNT(*) FILTER (WHERE performance_tier = 'EXCELLENT') as excellent_count,
        COUNT(*) FILTER (WHERE performance_tier = 'GOOD') as good_count,
        COUNT(*) FILTER (WHERE performance_tier = 'FAIR') as fair_count,
        COUNT(*) FILTER (WHERE performance_tier = 'POOR') as poor_count,
        COUNT(*) FILTER (WHERE is_long_running = true) as long_running_count,
        COUNT(*) FILTER (WHERE is_blocking = true) as blocking_count,
        ROUND(AVG(query_efficiency_score)::numeric, 2) as avg_efficiency
      FROM metadata.query_performance
      WHERE captured_at > NOW() - INTERVAL '24 hours'
    `);

    res.json(result.rows[0] || {});
  } catch (err) {
    console.error("Error getting query performance metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas de rendimiento de queries",
      details: err.message,
    });
  }
});

app.get("/api/maintenance/items", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const maintenance_type = req.query.maintenance_type || "";
    const status = req.query.status || "";
    const db_engine = req.query.db_engine || "";

    const whereConditions = [];
    const params = [];
    let paramCount = 1;

    if (maintenance_type) {
      whereConditions.push(`maintenance_type = $${paramCount}`);
      params.push(maintenance_type);
      paramCount++;
    }

    if (status) {
      whereConditions.push(`status = $${paramCount}`);
      params.push(status);
      paramCount++;
    }

    if (db_engine) {
      whereConditions.push(`db_engine = $${paramCount}`);
      params.push(db_engine);
      paramCount++;
    }

    const whereClause =
      whereConditions.length > 0
        ? "WHERE " + whereConditions.join(" AND ")
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.maintenance_control ${whereClause}`;
    const countResult = await pool.query(countQuery, params);
    const total = parseInt(countResult.rows[0].count);

    params.push(limit, offset);
    const dataQuery = `
      SELECT *
      FROM metadata.maintenance_control
      ${whereClause}
      ORDER BY 
        CASE status
          WHEN 'PENDING' THEN 1
          WHEN 'RUNNING' THEN 2
          WHEN 'COMPLETED' THEN 3
          WHEN 'FAILED' THEN 4
          ELSE 5
        END,
        priority DESC NULLS LAST,
        impact_score DESC NULLS LAST,
        next_maintenance_date ASC NULLS LAST
      LIMIT $${paramCount} OFFSET $${paramCount + 1}
    `;

    const result = await pool.query(dataQuery, params);

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
    console.error("Error getting maintenance items:", err);
    res.status(500).json({
      error: "Error al obtener items de mantenimiento",
      details: err.message,
    });
  }
});

app.get("/api/maintenance/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) FILTER (WHERE status = 'PENDING') as total_pending,
        COUNT(*) FILTER (WHERE status = 'COMPLETED') as total_completed,
        COUNT(*) FILTER (WHERE status = 'FAILED') as total_failed,
        COUNT(*) FILTER (WHERE status = 'RUNNING') as total_running,
        SUM(space_reclaimed_mb) as total_space_reclaimed_mb,
        ROUND(AVG(impact_score)::numeric, 2) as avg_impact_score,
        COUNT(*) FILTER (WHERE status = 'COMPLETED' AND (space_reclaimed_mb > 0 OR performance_improvement_pct > 0)) as objects_improved
      FROM metadata.maintenance_control
    `);

    res.json(result.rows[0] || {});
  } catch (err) {
    console.error("Error getting maintenance metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas de mantenimiento",
      details: err.message,
    });
  }
});

app.get("/api/column-catalog/columns", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const schema_name = req.query.schema_name || "";
    const table_name = req.query.table_name || "";
    const db_engine = req.query.db_engine || "";
    const data_type = req.query.data_type || "";
    const sensitivity_level = req.query.sensitivity_level || "";
    const contains_pii = req.query.contains_pii || "";
    const contains_phi = req.query.contains_phi || "";
    const search = req.query.search || "";

    const whereConditions = [];
    const params = [];
    let paramCount = 1;

    if (schema_name) {
      whereConditions.push(`schema_name = $${paramCount}`);
      params.push(schema_name);
      paramCount++;
    }

    if (table_name) {
      whereConditions.push(`table_name = $${paramCount}`);
      params.push(table_name);
      paramCount++;
    }

    if (db_engine) {
      whereConditions.push(`db_engine = $${paramCount}`);
      params.push(db_engine);
      paramCount++;
    }

    if (data_type) {
      whereConditions.push(`data_type ILIKE $${paramCount}`);
      params.push(`%${data_type}%`);
      paramCount++;
    }

    if (sensitivity_level) {
      whereConditions.push(`sensitivity_level = $${paramCount}`);
      params.push(sensitivity_level);
      paramCount++;
    }

    if (contains_pii !== "") {
      whereConditions.push(`contains_pii = $${paramCount}`);
      params.push(contains_pii === "true");
      paramCount++;
    }

    if (contains_phi !== "") {
      whereConditions.push(`contains_phi = $${paramCount}`);
      params.push(contains_phi === "true");
      paramCount++;
    }

    if (search) {
      whereConditions.push(`column_name ILIKE $${paramCount}`);
      params.push(`%${search}%`);
      paramCount++;
    }

    const whereClause =
      whereConditions.length > 0
        ? "WHERE " + whereConditions.join(" AND ")
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.column_catalog ${whereClause}`;
    const countResult = await pool.query(countQuery, params);
    const total = parseInt(countResult.rows[0].count);

    params.push(limit, offset);
    const dataQuery = `
      SELECT *
      FROM metadata.column_catalog
      ${whereClause}
      ORDER BY schema_name, table_name, ordinal_position
      LIMIT $${paramCount} OFFSET $${paramCount + 1}
    `;

    const result = await pool.query(dataQuery, params);

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
    console.error("Error getting column catalog data:", err);
    res.status(500).json({
      error: "Error al obtener datos del catálogo de columnas",
      details: err.message,
    });
  }
});

app.get("/api/column-catalog/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total_columns,
        COUNT(*) FILTER (WHERE contains_pii = true) as pii_columns,
        COUNT(*) FILTER (WHERE contains_phi = true) as phi_columns,
        COUNT(*) FILTER (WHERE sensitivity_level = 'HIGH') as high_sensitivity,
        COUNT(*) FILTER (WHERE is_primary_key = true) as primary_keys,
        COUNT(*) FILTER (WHERE is_indexed = true) as indexed_columns
      FROM metadata.column_catalog
    `);

    res.json(result.rows[0] || {});
  } catch (err) {
    console.error("Error getting column catalog metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas del catálogo de columnas",
      details: err.message,
    });
  }
});

app.get("/api/column-catalog/schemas", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT schema_name
      FROM metadata.column_catalog
      ORDER BY schema_name
    `);

    res.json(result.rows.map((row) => row.schema_name));
  } catch (err) {
    console.error("Error getting schemas:", err);
    res.status(500).json({
      error: "Error al obtener schemas",
      details: err.message,
    });
  }
});

app.get("/api/column-catalog/tables/:schemaName", async (req, res) => {
  try {
    const schemaName = req.params.schemaName;
    const result = await pool.query(
      `
        SELECT DISTINCT table_name
        FROM metadata.column_catalog
        WHERE schema_name = $1
        ORDER BY table_name
      `,
      [schemaName]
    );

    res.json(result.rows.map((row) => row.table_name));
  } catch (err) {
    console.error("Error getting tables:", err);
    res.status(500).json({
      error: "Error al obtener tablas",
      details: err.message,
    });
  }
});

app.get("/api/catalog-locks/locks", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        lock_name,
        acquired_at,
        acquired_by,
        expires_at,
        session_id
      FROM metadata.catalog_locks
      ORDER BY acquired_at DESC
    `);

    res.json(result.rows);
  } catch (err) {
    console.error("Error getting catalog locks:", err);
    res.status(500).json({
      error: "Error al obtener locks del catálogo",
      details: err.message,
    });
  }
});

app.get("/api/catalog-locks/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total_locks,
        COUNT(*) FILTER (WHERE expires_at > NOW()) as active_locks,
        COUNT(*) FILTER (WHERE expires_at <= NOW()) as expired_locks,
        COUNT(DISTINCT acquired_by) as unique_hosts
      FROM metadata.catalog_locks
    `);

    res.json(result.rows[0] || {});
  } catch (err) {
    console.error("Error getting catalog locks metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas de locks",
      details: err.message,
    });
  }
});

app.delete("/api/catalog-locks/locks/:lockName", async (req, res) => {
  try {
    const lockName = req.params.lockName;
    const result = await pool.query(
      `DELETE FROM metadata.catalog_locks WHERE lock_name = $1 RETURNING lock_name`,
      [lockName]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({
        error: "Lock not found",
        details: `Lock "${lockName}" does not exist`,
      });
    }

    res.json({
      success: true,
      message: `Lock "${lockName}" has been released`,
      lock_name: result.rows[0].lock_name,
    });
  } catch (err) {
    console.error("Error unlocking lock:", err);
    res.status(500).json({
      error: "Error al liberar lock",
      details: err.message,
    });
  }
});

app.post("/api/catalog-locks/clean-expired", async (req, res) => {
  try {
    const result = await pool.query(
      `DELETE FROM metadata.catalog_locks WHERE expires_at <= NOW() RETURNING lock_name`
    );

    res.json({
      success: true,
      message: `Cleaned ${result.rowCount} expired lock(s)`,
      cleaned_count: result.rowCount,
    });
  } catch (err) {
    console.error("Error cleaning expired locks:", err);
    res.status(500).json({
      error: "Error al limpiar locks expirados",
      details: err.message,
    });
  }
});

app.get("/api/data-lineage/mariadb", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const server_name = req.query.server_name || "";
    const database_name = req.query.database_name || "";
    const object_type = req.query.object_type || "";
    const relationship_type = req.query.relationship_type || "";
    const search = req.query.search || "";

    const whereConditions = [];
    const params = [];
    let paramCount = 1;

    if (server_name) {
      whereConditions.push(`server_name = $${paramCount}`);
      params.push(server_name);
      paramCount++;
    }

    if (database_name) {
      whereConditions.push(`database_name = $${paramCount}`);
      params.push(database_name);
      paramCount++;
    }

    if (object_type) {
      whereConditions.push(`object_type = $${paramCount}`);
      params.push(object_type);
      paramCount++;
    }

    if (relationship_type) {
      whereConditions.push(`relationship_type = $${paramCount}`);
      params.push(relationship_type);
      paramCount++;
    }

    if (search) {
      whereConditions.push(
        `(object_name ILIKE $${paramCount} OR target_object_name ILIKE $${paramCount})`
      );
      params.push(`%${search}%`);
      paramCount++;
    }

    const whereClause =
      whereConditions.length > 0
        ? "WHERE " + whereConditions.join(" AND ")
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.mdb_lineage ${whereClause}`;
    const countResult = await pool.query(countQuery, params);
    const total = parseInt(countResult.rows[0].count);

    params.push(limit, offset);
    const dataQuery = `
      SELECT *
      FROM metadata.mdb_lineage
      ${whereClause}
      ORDER BY dependency_level, confidence_score DESC, last_seen_at DESC
      LIMIT $${paramCount} OFFSET $${paramCount + 1}
    `;

    const result = await pool.query(dataQuery, params);

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
    console.error("Error getting MariaDB lineage data:", err);
    res.status(500).json({
      error: "Error al obtener datos de lineage de MariaDB",
      details: err.message,
    });
  }
});

app.get("/api/data-lineage/mariadb/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total_relationships,
        COUNT(DISTINCT object_name) as unique_objects,
        COUNT(DISTINCT server_name) as unique_servers,
        COUNT(*) FILTER (WHERE confidence_score >= 0.8) as high_confidence,
        ROUND(AVG(confidence_score)::numeric, 4) as avg_confidence
      FROM metadata.mdb_lineage
    `);

    res.json(result.rows[0] || {});
  } catch (err) {
    console.error("Error getting MariaDB lineage metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas de lineage de MariaDB",
      details: err.message,
    });
  }
});

app.get("/api/data-lineage/mariadb/servers", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT server_name
      FROM metadata.mdb_lineage
      WHERE server_name IS NOT NULL
      ORDER BY server_name
    `);

    res.json(result.rows.map((row) => row.server_name));
  } catch (err) {
    console.error("Error getting MariaDB servers:", err);
    res.status(500).json({
      error: "Error al obtener servidores",
      details: err.message,
    });
  }
});

app.get("/api/data-lineage/mariadb/databases/:serverName", async (req, res) => {
  try {
    const serverName = req.params.serverName;
    const result = await pool.query(
      `
        SELECT DISTINCT database_name
        FROM metadata.mdb_lineage
        WHERE server_name = $1 AND database_name IS NOT NULL
        ORDER BY database_name
      `,
      [serverName]
    );

    res.json(result.rows.map((row) => row.database_name));
  } catch (err) {
    console.error("Error getting MariaDB databases:", err);
    res.status(500).json({
      error: "Error al obtener bases de datos",
      details: err.message,
    });
  }
});

app.get("/api/data-lineage/mssql", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const server_name = req.query.server_name || "";
    const instance_name = req.query.instance_name || "";
    const database_name = req.query.database_name || "";
    const object_type = req.query.object_type || "";
    const relationship_type = req.query.relationship_type || "";
    const search = req.query.search || "";

    const whereConditions = [];
    const params = [];
    let paramCount = 1;

    if (server_name) {
      whereConditions.push(`server_name = $${paramCount}`);
      params.push(server_name);
      paramCount++;
    }

    if (instance_name) {
      whereConditions.push(`instance_name = $${paramCount}`);
      params.push(instance_name);
      paramCount++;
    }

    if (database_name) {
      whereConditions.push(`database_name = $${paramCount}`);
      params.push(database_name);
      paramCount++;
    }

    if (object_type) {
      whereConditions.push(`object_type = $${paramCount}`);
      params.push(object_type);
      paramCount++;
    }

    if (relationship_type) {
      whereConditions.push(`relationship_type = $${paramCount}`);
      params.push(relationship_type);
      paramCount++;
    }

    if (search) {
      whereConditions.push(
        `(object_name ILIKE $${paramCount} OR target_object_name ILIKE $${paramCount})`
      );
      params.push(`%${search}%`);
      paramCount++;
    }

    const whereClause =
      whereConditions.length > 0
        ? "WHERE " + whereConditions.join(" AND ")
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.mssql_lineage ${whereClause}`;
    const countResult = await pool.query(countQuery, params);
    const total = parseInt(countResult.rows[0].count);

    params.push(limit, offset);
    const dataQuery = `
      SELECT *
      FROM metadata.mssql_lineage
      ${whereClause}
      ORDER BY dependency_level, confidence_score DESC, last_seen_at DESC
      LIMIT $${paramCount} OFFSET $${paramCount + 1}
    `;

    const result = await pool.query(dataQuery, params);

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
    console.error("Error getting MSSQL lineage data:", err);
    res.status(500).json({
      error: "Error al obtener datos de lineage de MSSQL",
      details: err.message,
    });
  }
});

app.get("/api/data-lineage/mssql/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total_relationships,
        COUNT(DISTINCT object_name) as unique_objects,
        COUNT(DISTINCT server_name) as unique_servers,
        COUNT(*) FILTER (WHERE confidence_score >= 0.8) as high_confidence,
        ROUND(AVG(confidence_score)::numeric, 4) as avg_confidence,
        SUM(COALESCE(execution_count, 0)) as total_executions
      FROM metadata.mssql_lineage
    `);

    res.json(result.rows[0] || {});
  } catch (err) {
    console.error("Error getting MSSQL lineage metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas de lineage de MSSQL",
      details: err.message,
    });
  }
});

app.get("/api/data-lineage/mssql/servers", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT server_name
      FROM metadata.mssql_lineage
      WHERE server_name IS NOT NULL
      ORDER BY server_name
    `);

    res.json(result.rows.map((row) => row.server_name));
  } catch (err) {
    console.error("Error getting MSSQL servers:", err);
    res.status(500).json({
      error: "Error al obtener servidores",
      details: err.message,
    });
  }
});

app.get("/api/data-lineage/mssql/instances/:serverName", async (req, res) => {
  try {
    const serverName = req.params.serverName;
    const result = await pool.query(
      `
        SELECT DISTINCT instance_name
        FROM metadata.mssql_lineage
        WHERE server_name = $1 AND instance_name IS NOT NULL
        ORDER BY instance_name
      `,
      [serverName]
    );

    res.json(result.rows.map((row) => row.instance_name));
  } catch (err) {
    console.error("Error getting MSSQL instances:", err);
    res.status(500).json({
      error: "Error al obtener instancias",
      details: err.message,
    });
  }
});

app.get(
  "/api/data-lineage/mssql/databases/:serverName/:instanceName",
  async (req, res) => {
    try {
      const serverName = req.params.serverName;
      const instanceName = req.params.instanceName;
      const result = await pool.query(
        `
        SELECT DISTINCT database_name
        FROM metadata.mssql_lineage
        WHERE server_name = $1 AND instance_name = $2 AND database_name IS NOT NULL
        ORDER BY database_name
      `,
        [serverName, instanceName]
      );

      res.json(result.rows.map((row) => row.database_name));
    } catch (err) {
      console.error("Error getting MSSQL databases:", err);
      res.status(500).json({
        error: "Error al obtener bases de datos",
        details: err.message,
      });
    }
  }
);

app.get("/api/governance-catalog/mariadb", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const server_name = req.query.server_name || "";
    const database_name = req.query.database_name || "";
    const health_status = req.query.health_status || "";
    const access_frequency = req.query.access_frequency || "";
    const search = req.query.search || "";

    const whereConditions = [];
    const params = [];
    let paramCount = 1;

    if (server_name) {
      whereConditions.push(`server_name = $${paramCount}`);
      params.push(server_name);
      paramCount++;
    }

    if (database_name) {
      whereConditions.push(`database_name = $${paramCount}`);
      params.push(database_name);
      paramCount++;
    }

    if (health_status) {
      whereConditions.push(`health_status = $${paramCount}`);
      params.push(health_status);
      paramCount++;
    }

    if (access_frequency) {
      whereConditions.push(`access_frequency = $${paramCount}`);
      params.push(access_frequency);
      paramCount++;
    }

    if (search) {
      whereConditions.push(`table_name ILIKE $${paramCount}`);
      params.push(`%${search}%`);
      paramCount++;
    }

    const whereClause =
      whereConditions.length > 0
        ? "WHERE " + whereConditions.join(" AND ")
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.data_governance_catalog_mariadb ${whereClause}`;
    const countResult = await pool.query(countQuery, params);
    const total = parseInt(countResult.rows[0].count);

    params.push(limit, offset);
    const dataQuery = `
      SELECT *
      FROM metadata.data_governance_catalog_mariadb
      ${whereClause}
      ORDER BY server_name, database_name, schema_name, table_name
      LIMIT $${paramCount} OFFSET $${paramCount + 1}
    `;

    const result = await pool.query(dataQuery, params);

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
    console.error("Error getting MariaDB governance catalog:", err);
    res.status(500).json({
      error: "Error al obtener catálogo de governance de MariaDB",
      details: err.message,
    });
  }
});

app.get("/api/governance-catalog/mariadb/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total_tables,
        SUM(COALESCE(total_size_mb, 0)) as total_size_mb,
        COUNT(*) FILTER (WHERE health_status IN ('EXCELLENT', 'HEALTHY')) as healthy_count,
        COUNT(*) FILTER (WHERE health_status = 'WARNING') as warning_count,
        COUNT(*) FILTER (WHERE health_status = 'CRITICAL') as critical_count,
        COUNT(DISTINCT server_name) as unique_servers
      FROM metadata.data_governance_catalog_mariadb
    `);

    res.json(result.rows[0] || {});
  } catch (err) {
    console.error("Error getting MariaDB governance metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas de governance de MariaDB",
      details: err.message,
    });
  }
});

app.get("/api/governance-catalog/mariadb/servers", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT server_name
      FROM metadata.data_governance_catalog_mariadb
      WHERE server_name IS NOT NULL
      ORDER BY server_name
    `);

    res.json(result.rows.map((row) => row.server_name));
  } catch (err) {
    console.error("Error getting MariaDB servers:", err);
    res.status(500).json({
      error: "Error al obtener servidores",
      details: err.message,
    });
  }
});

app.get(
  "/api/governance-catalog/mariadb/databases/:serverName",
  async (req, res) => {
    try {
      const serverName = req.params.serverName;
      const result = await pool.query(
        `
        SELECT DISTINCT database_name
        FROM metadata.data_governance_catalog_mariadb
        WHERE server_name = $1 AND database_name IS NOT NULL
        ORDER BY database_name
      `,
        [serverName]
      );

      res.json(result.rows.map((row) => row.database_name));
    } catch (err) {
      console.error("Error getting MariaDB databases:", err);
      res.status(500).json({
        error: "Error al obtener bases de datos",
        details: err.message,
      });
    }
  }
);

app.get("/api/governance-catalog/mssql", async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const offset = (page - 1) * limit;
    const server_name = req.query.server_name || "";
    const database_name = req.query.database_name || "";
    const object_type = req.query.object_type || "";
    const health_status = req.query.health_status || "";
    const access_frequency = req.query.access_frequency || "";
    const search = req.query.search || "";

    const whereConditions = [];
    const params = [];
    let paramCount = 1;

    if (server_name) {
      whereConditions.push(`server_name = $${paramCount}`);
      params.push(server_name);
      paramCount++;
    }

    if (database_name) {
      whereConditions.push(`database_name = $${paramCount}`);
      params.push(database_name);
      paramCount++;
    }

    if (object_type) {
      whereConditions.push(`object_type = $${paramCount}`);
      params.push(object_type);
      paramCount++;
    }

    if (health_status) {
      whereConditions.push(`health_status = $${paramCount}`);
      params.push(health_status);
      paramCount++;
    }

    if (access_frequency) {
      whereConditions.push(`access_frequency = $${paramCount}`);
      params.push(access_frequency);
      paramCount++;
    }

    if (search) {
      whereConditions.push(
        `(object_name ILIKE $${paramCount} OR table_name ILIKE $${paramCount})`
      );
      params.push(`%${search}%`);
      paramCount++;
    }

    const whereClause =
      whereConditions.length > 0
        ? "WHERE " + whereConditions.join(" AND ")
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.data_governance_catalog_mssql ${whereClause}`;
    const countResult = await pool.query(countQuery, params);
    const total = parseInt(countResult.rows[0].count);

    params.push(limit, offset);
    const dataQuery = `
      SELECT *
      FROM metadata.data_governance_catalog_mssql
      ${whereClause}
      ORDER BY server_name, database_name, schema_name, object_name
      LIMIT $${paramCount} OFFSET $${paramCount + 1}
    `;

    const result = await pool.query(dataQuery, params);

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
    console.error("Error getting MSSQL governance catalog:", err);
    res.status(500).json({
      error: "Error al obtener catálogo de governance de MSSQL",
      details: err.message,
    });
  }
});

app.get("/api/governance-catalog/mssql/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total_objects,
        SUM(COALESCE(table_size_mb, 0)) as total_size_mb,
        COUNT(*) FILTER (WHERE health_status IN ('EXCELLENT', 'HEALTHY')) as healthy_count,
        COUNT(*) FILTER (WHERE health_status = 'WARNING') as warning_count,
        COUNT(*) FILTER (WHERE health_status = 'CRITICAL') as critical_count,
        COUNT(DISTINCT server_name) as unique_servers
      FROM metadata.data_governance_catalog_mssql
    `);

    res.json(result.rows[0] || {});
  } catch (err) {
    console.error("Error getting MSSQL governance metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas de governance de MSSQL",
      details: err.message,
    });
  }
});

app.get("/api/governance-catalog/mssql/servers", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT server_name
      FROM metadata.data_governance_catalog_mssql
      WHERE server_name IS NOT NULL
      ORDER BY server_name
    `);

    res.json(result.rows.map((row) => row.server_name));
  } catch (err) {
    console.error("Error getting MSSQL servers:", err);
    res.status(500).json({
      error: "Error al obtener servidores",
      details: err.message,
    });
  }
});

app.get(
  "/api/governance-catalog/mssql/databases/:serverName",
  async (req, res) => {
    try {
      const serverName = req.params.serverName;
      const result = await pool.query(
        `
        SELECT DISTINCT database_name
        FROM metadata.data_governance_catalog_mssql
        WHERE server_name = $1 AND database_name IS NOT NULL
        ORDER BY database_name
      `,
        [serverName]
      );

      res.json(result.rows.map((row) => row.database_name));
    } catch (err) {
      console.error("Error getting MSSQL databases:", err);
      res.status(500).json({
        error: "Error al obtener bases de datos",
        details: err.message,
      });
    }
  }
);

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
