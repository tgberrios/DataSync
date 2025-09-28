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

// Desactivar schema completo
app.patch("/api/catalog/deactivate-schema", async (req, res) => {
  const { schema_name } = req.body;
  try {
    const result = await pool.query(
      `UPDATE metadata.catalog 
       SET status = 'SKIPPED', last_offset = 0
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
        COUNT(*) FILTER (WHERE active = true AND status = 'PERFECT_MATCH') as perfect_match,
        COUNT(*) FILTER (WHERE active = true AND status = 'LISTENING_CHANGES') as listening_changes,
        COUNT(*) FILTER (WHERE active = true) as full_load_active,
        COUNT(*) FILTER (WHERE active = false) as full_load_inactive,
        COUNT(*) FILTER (WHERE active = false AND status = 'NO_DATA') as no_data,
        COUNT(*) FILTER (WHERE active = true AND status = 'ERROR') as errors,
        '' as current_process
      FROM metadata.catalog
    `);

    // Get currently processing table with progress calculation
    const currentProcessingTable = await pool.query(`
      SELECT t.*,
             CASE 
               WHEN t.table_size > 0 THEN ROUND((t.last_offset::numeric / t.table_size::numeric) * 100, 1)
               ELSE 0 
             END as progress_percentage
      FROM metadata.catalog t
      WHERE status = 'FULL_LOAD'
      ORDER BY last_offset desc
      LIMIT 1
    `);

    const currentProcessText =
      currentProcessingTable.rows.length > 0
        ? `${currentProcessingTable.rows[0].schema_name}.${
            currentProcessingTable.rows[0].table_name
          } [${currentProcessingTable.rows[0].db_engine}] (${
            currentProcessingTable.rows[0].last_offset || 0
          }/${currentProcessingTable.rows[0].table_size || 0} - ${
            currentProcessingTable.rows[0].progress_percentage || 0
          }%) - Status: ${currentProcessingTable.rows[0].status}`
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

    // 7. ACTIVE TRANSFERS PROGRESS
    const activeTransfersProgress = await pool.query(`
      SELECT 
        schema_name,
        table_name,
        db_engine,
        last_offset,
        table_size,
        status,
        CASE 
          WHEN table_size > 0 THEN ROUND((last_offset::numeric / table_size::numeric) * 100, 1)
          ELSE 0 
        END as progress_percentage,
        last_sync_time,
        CASE 
          WHEN last_offset > 0 THEN 1
          ELSE 0 
        END as is_processing
      FROM metadata.catalog
      WHERE status = 'FULL_LOAD' AND active = true
      ORDER BY is_processing DESC, table_size ASC
      LIMIT 10
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
      // Connection pooling removed - using direct connections now
    };

    // Calcular progreso total - solo contar registros activos
    const total =
      stats.syncStatus.perfectMatch +
      stats.syncStatus.listeningChanges +
      stats.syncStatus.fullLoadActive +
      stats.syncStatus.fullLoadInactive +
      stats.syncStatus.noData +
      stats.syncStatus.errors;

    // El progreso se calcula como: (Perfect Match + Listening Changes + No Data) / Total * 100
    // NO_DATA también es un estado exitoso (no hay datos que sincronizar)
    stats.syncStatus.progress =
      total > 0
        ? Math.round(
            ((stats.syncStatus.perfectMatch +
              stats.syncStatus.listeningChanges +
              stats.syncStatus.noData) /
              total) *
              100
          )
        : 0;

    console.log("Progress calculation:", {
      perfectMatch: stats.syncStatus.perfectMatch,
      listeningChanges: stats.syncStatus.listeningChanges,
      noData: stats.syncStatus.noData,
      fullLoadActive: stats.syncStatus.fullLoadActive,
      fullLoadInactive: stats.syncStatus.fullLoadInactive,
      errors: stats.syncStatus.errors,
      total: total,
      successfulStates:
        stats.syncStatus.perfectMatch +
        stats.syncStatus.listeningChanges +
        stats.syncStatus.noData,
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

    // Agregar progreso de transferencias activas
    stats.activeTransfersProgress = activeTransfersProgress.rows.map((row) => ({
      schemaName: row.schema_name,
      tableName: row.table_name,
      dbEngine: row.db_engine,
      lastOffset: parseInt(row.last_offset || 0),
      tableSize: parseInt(row.table_size || 0),
      progressPercentage: parseFloat(row.progress_percentage || 0),
      status: row.status,
      lastSyncTime: row.last_sync_time,
    }));

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

// Endpoint para leer logs
app.get("/api/logs", async (req, res) => {
  try {
    const {
      lines = 100,
      level = "ALL",
      category = "ALL",
      search = "",
      startDate = "",
      endDate = "",
    } = req.query;
    const logFilePath = path.join(process.cwd(), "..", "DataSync.log");

    // Verificar si el archivo existe
    if (!fs.existsSync(logFilePath)) {
      return res.json({
        logs: [],
        totalLines: 0,
        message: "No log file found",
      });
    }

    // Leer el archivo de logs
    const logContent = fs.readFileSync(logFilePath, "utf8");
    const allLines = logContent
      .split("\n")
      .filter((line) => line.trim() !== "");

    // Parsear logs y aplicar filtros
    let filteredLines = allLines.map((line) => {
      // Nuevo formato con categoría: [timestamp] [level] [category] [function] message
      const newMatch = line.match(
        /^\[([^\]]+)\] \[([^\]]+)\] \[([^\]]+)\] \[([^\]]+)\] (.+)$/
      );
      if (newMatch) {
        return {
          timestamp: newMatch[1],
          level: newMatch[2],
          category: newMatch[3],
          function: newMatch[4],
          message: newMatch[5],
          raw: line,
          parsed: true,
        };
      }
      // Formato antiguo: [timestamp] [level] [function] message
      const oldMatch = line.match(
        /^\[([^\]]+)\] \[([^\]]+)\](?: \[([^\]]+)\])? (.+)$/
      );
      if (oldMatch) {
        return {
          timestamp: oldMatch[1],
          level: oldMatch[2],
          category: "SYSTEM",
          function: oldMatch[3] || "",
          message: oldMatch[4],
          raw: line,
          parsed: true,
        };
      }
      return {
        timestamp: "",
        level: "UNKNOWN",
        category: "UNKNOWN",
        function: "",
        message: line,
        raw: line,
        parsed: false,
      };
    });

    // Aplicar filtros
    if (level !== "ALL") {
      filteredLines = filteredLines.filter((log) => log.level === level);
    }

    if (category !== "ALL") {
      filteredLines = filteredLines.filter((log) => log.category === category);
    }

    if (search) {
      filteredLines = filteredLines.filter(
        (log) =>
          log.message.toLowerCase().includes(search.toLowerCase()) ||
          (log.function &&
            log.function.toLowerCase().includes(search.toLowerCase()))
      );
    }

    if (startDate) {
      const start = new Date(startDate);
      filteredLines = filteredLines.filter((log) => {
        if (!log.timestamp) return false;
        const logDate = new Date(log.timestamp);
        return logDate >= start;
      });
    }

    if (endDate) {
      const end = new Date(endDate);
      filteredLines = filteredLines.filter((log) => {
        if (!log.timestamp) return false;
        const logDate = new Date(log.timestamp);
        return logDate <= end;
      });
    }

    // Obtener las últimas N líneas
    const lastLines = filteredLines.slice(-parseInt(lines));

    res.json({
      logs: lastLines,
      totalLines: filteredLines.length,
      filePath: logFilePath,
      lastModified: fs.statSync(logFilePath).mtime,
      filters: {
        level,
        category,
        search,
        startDate,
        endDate,
      },
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
    const logFilePath = path.join(process.cwd(), "..", "DataSync.log");

    if (!fs.existsSync(logFilePath)) {
      return res.json({
        exists: false,
        message: "No log file found",
      });
    }

    const stats = fs.statSync(logFilePath);
    const logContent = fs.readFileSync(logFilePath, "utf8");
    const totalLines = logContent
      .split("\n")
      .filter((line) => line.trim() !== "").length;

    res.json({
      exists: true,
      filePath: logFilePath,
      size: stats.size,
      totalLines: totalLines,
      lastModified: stats.mtime,
      created: stats.birthtime,
    });
  } catch (err) {
    console.error("Error getting log info:", err);
    res.status(500).json({
      error: "Error al obtener información de logs",
      details: err.message,
    });
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
    const logDir = path.join(process.cwd(), "..");
    const logFilePath = path.join(logDir, "DataSync.log");

    let totalClearedSize = 0;
    let clearedFiles = [];

    // Clear the main log file
    if (fs.existsSync(logFilePath)) {
      const stats = fs.statSync(logFilePath);
      totalClearedSize += stats.size;
      clearedFiles.push("DataSync.log");
      fs.writeFileSync(logFilePath, "");
    }

    // Find and delete rotated log files (DataSync.log.1, DataSync.log.2, etc.)
    const files = fs.readdirSync(logDir);
    const rotatedLogFiles = files.filter((file) =>
      file.match(/^DataSync\.log\.\d+$/)
    );

    for (const rotatedFile of rotatedLogFiles) {
      const rotatedFilePath = path.join(logDir, rotatedFile);
      try {
        const stats = fs.statSync(rotatedFilePath);
        totalClearedSize += stats.size;
        clearedFiles.push(rotatedFile);
        fs.unlinkSync(rotatedFilePath);
      } catch (err) {
        console.warn(`Warning: Could not delete ${rotatedFile}:`, err.message);
      }
    }

    res.json({
      success: true,
      message: "Logs cleared successfully",
      clearedFiles: clearedFiles,
      totalClearedSize: totalClearedSize,
      clearedAt: new Date().toISOString(),
    });
  } catch (err) {
    console.error("Error clearing logs:", err);
    res.status(500).json({
      error: "Error clearing logs",
      details: err.message,
    });
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

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
