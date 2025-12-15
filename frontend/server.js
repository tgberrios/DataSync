import express from "express";
import pkg from "pg";
const { Pool } = pkg;
import cors from "cors";
import helmet from "helmet";
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

    // Default configuration fallback - use environment variables if available
    return {
      database: {
        postgres: {
          host: process.env.POSTGRES_HOST || "localhost",
          port: parseInt(process.env.POSTGRES_PORT || "5432", 10),
          database: process.env.POSTGRES_DATABASE || "DataLake",
          user: process.env.POSTGRES_USER || "postgres",
          password: process.env.POSTGRES_PASSWORD || "",
        },
      },
    };
  }
}

const config = loadConfig();

const app = express();

const isProduction = process.env.NODE_ENV === "production";

if (isProduction) {
  app.use((req, res, next) => {
    if (req.header("x-forwarded-proto") !== "https") {
      res.redirect(`https://${req.header("host")}${req.url}`);
    } else {
      next();
    }
  });
}

app.use(
  helmet({
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        styleSrc: ["'self'", "'unsafe-inline'"],
        scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'"],
        imgSrc: ["'self'", "data:", "https:"],
        connectSrc: ["'self'"],
        fontSrc: ["'self'", "data:"],
        objectSrc: ["'none'"],
        mediaSrc: ["'self'"],
        frameSrc: ["'none'"],
      },
    },
    hsts: {
      maxAge: 31536000,
      includeSubDomains: true,
      preload: true,
    },
    xFrameOptions: { action: "deny" },
    xContentTypeOptions: true,
    referrerPolicy: { policy: "strict-origin-when-cross-origin" },
  })
);

const corsOptions = {
  origin: isProduction
    ? process.env.ALLOWED_ORIGINS?.split(",") || false
    : true,
  credentials: true,
  optionsSuccessStatus: 200,
  methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
};

app.use(cors(corsOptions));
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

import {
  sanitizeSearch,
  validatePage,
  validateLimit,
  validateBoolean,
  validateIdentifier,
  validateEnum,
} from "./server-utils/validation.js";
import { sanitizeError } from "./server-utils/errorHandler.js";
import {
  initializeUsersTable,
  authenticateUser,
  requireAuth,
  requireRole,
  createUser,
  changePassword,
} from "./server-utils/auth.js";
import bcrypt from "bcryptjs";
import { generalLimiter, authLimiter } from "./server-utils/rateLimiter.js";

initializeUsersTable().catch((err) => {
  console.error("Failed to initialize users table:", err);
});

app.use(generalLimiter);

const publicPaths = ["/api/auth/login", "/api/health"];
app.use((req, res, next) => {
  if (!req.path.startsWith("/api")) {
    return next();
  }
  const requestPath = req.path || (req.url ? req.url.split("?")[0] : "");
  if (publicPaths.includes(requestPath)) {
    return next();
  }
  requireAuth(req, res, next);
});

app.post("/api/auth/login", authLimiter, async (req, res) => {
  try {
    const { username, password } = req.body;

    if (!username || !password) {
      return res.status(400).json({
        error: "Username and password are required",
      });
    }

    const result = await authenticateUser(username, password);

    if (!result.success) {
      return res.status(401).json({ error: result.error });
    }

    res.json({
      user: result.user,
      token: result.token,
    });
  } catch (err) {
    console.error("Login error:", err);
    const safeError = sanitizeError(
      err,
      "Login failed",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.post("/api/auth/logout", requireAuth, async (req, res) => {
  res.json({ message: "Logged out successfully" });
});

app.get("/api/auth/me", requireAuth, async (req, res) => {
  res.json({ user: req.user });
});

app.post("/api/auth/change-password", requireAuth, async (req, res) => {
  try {
    const { oldPassword, newPassword } = req.body;

    if (!oldPassword || !newPassword) {
      return res.status(400).json({
        error: "Old password and new password are required",
      });
    }

    if (newPassword.length < 8) {
      return res.status(400).json({
        error: "New password must be at least 8 characters long",
      });
    }

    const result = await changePassword(
      req.user.userId,
      oldPassword,
      newPassword
    );

    if (!result.success) {
      return res.status(400).json({ error: result.error });
    }

    res.json({ message: "Password changed successfully" });
  } catch (err) {
    console.error("Change password error:", err);
    const safeError = sanitizeError(
      err,
      "Failed to change password",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.get(
  "/api/auth/users",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
    try {
      const page = validatePage(req.query.page);
      const limit = validateLimit(req.query.limit, 1, 100);
      const roleFilter = req.query.role || "";
      const activeFilter = req.query.active;
      const search = sanitizeSearch(req.query.search, 100) || "";
      const offset = (page - 1) * limit;

      let whereClause = "WHERE 1=1";
      const countParams = [];
      let paramCount = 0;

      if (roleFilter) {
        paramCount++;
        whereClause += ` AND role = $${paramCount}`;
        countParams.push(roleFilter);
      }

      if (activeFilter !== undefined) {
        paramCount++;
        whereClause += ` AND active = $${paramCount}`;
        countParams.push(activeFilter === "true");
      }

      if (search) {
        paramCount++;
        whereClause += ` AND (username ILIKE $${paramCount} OR email ILIKE $${paramCount})`;
        countParams.push(`%${search}%`);
      }

      const countQuery = `SELECT COUNT(*) as total FROM metadata.users ${whereClause}`;
      const countResult = await pool.query(countQuery, countParams);
      const total = parseInt(countResult.rows[0]?.total || 0);

      const dataParams = [...countParams];
      let query = `SELECT id, username, email, role, active, created_at, updated_at, last_login FROM metadata.users ${whereClause} ORDER BY created_at DESC`;

      paramCount++;
      query += ` LIMIT $${paramCount}`;
      dataParams.push(limit);
      paramCount++;
      query += ` OFFSET $${paramCount}`;
      dataParams.push(offset);

      const result = await pool.query(query, dataParams);

      res.json({
        data: result.rows,
        pagination: {
          page,
          limit,
          total,
          totalPages: Math.ceil(total / limit),
        },
      });
    } catch (err) {
      console.error("List users error:", err);
      const safeError = sanitizeError(
        err,
        "Failed to list users",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

app.post(
  "/api/auth/users",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
    try {
      const { username, email, password, role } = req.body;

      if (!username || !email || !password) {
        return res.status(400).json({
          error: "Username, email, and password are required",
        });
      }

      if (password.length < 8) {
        return res.status(400).json({
          error: "Password must be at least 8 characters long",
        });
      }

      const result = await createUser(
        username,
        email,
        password,
        role || "user"
      );

      if (!result.success) {
        return res.status(400).json({ error: result.error });
      }

      res.status(201).json({ user: result.user });
    } catch (err) {
      console.error("Create user error:", err);
      const safeError = sanitizeError(
        err,
        "Failed to create user",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

app.patch(
  "/api/auth/users/:id",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
    try {
      const userId = parseInt(req.params.id);
      const { username, email, role, active } = req.body;

      if (isNaN(userId)) {
        return res.status(400).json({ error: "Invalid user ID" });
      }

      const updates = [];
      const params = [];
      let paramCount = 0;

      if (username !== undefined) {
        paramCount++;
        updates.push(`username = $${paramCount}`);
        params.push(username);
      }

      if (email !== undefined) {
        paramCount++;
        updates.push(`email = $${paramCount}`);
        params.push(email);
      }

      if (role !== undefined) {
        if (!["admin", "user", "viewer"].includes(role)) {
          return res.status(400).json({ error: "Invalid role" });
        }
        paramCount++;
        updates.push(`role = $${paramCount}`);
        params.push(role);
      }

      if (active !== undefined) {
        paramCount++;
        updates.push(`active = $${paramCount}`);
        params.push(validateBoolean(active, true));
      }

      if (updates.length === 0) {
        return res.status(400).json({ error: "No fields to update" });
      }

      paramCount++;
      updates.push(`updated_at = NOW()`);
      paramCount++;
      params.push(userId);

      const query = `UPDATE metadata.users SET ${updates.join(
        ", "
      )} WHERE id = $${paramCount} RETURNING id, username, email, role, active, created_at, updated_at, last_login`;

      const result = await pool.query(query, params);

      if (result.rows.length === 0) {
        return res.status(404).json({ error: "User not found" });
      }

      res.json({ user: result.rows[0] });
    } catch (err) {
      console.error("Update user error:", err);
      const safeError = sanitizeError(
        err,
        "Failed to update user",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

app.delete(
  "/api/auth/users/:id",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
    try {
      const userId = parseInt(req.params.id);

      if (isNaN(userId)) {
        return res.status(400).json({ error: "Invalid user ID" });
      }

      const userCheck = await pool.query(
        "SELECT id, username FROM metadata.users WHERE id = $1",
        [userId]
      );

      if (userCheck.rows.length === 0) {
        return res.status(404).json({ error: "User not found" });
      }

      if (userCheck.rows[0].username === req.user.username) {
        return res
          .status(400)
          .json({ error: "Cannot delete your own account" });
      }

      await pool.query("DELETE FROM metadata.users WHERE id = $1", [userId]);

      res.json({ message: "User deleted successfully" });
    } catch (err) {
      console.error("Delete user error:", err);
      const safeError = sanitizeError(
        err,
        "Failed to delete user",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

app.post(
  "/api/auth/users/:id/reset-password",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
    try {
      const userId = parseInt(req.params.id);
      const { newPassword } = req.body;

      if (isNaN(userId)) {
        return res.status(400).json({ error: "Invalid user ID" });
      }

      if (!newPassword || newPassword.length < 8) {
        return res.status(400).json({
          error: "Password must be at least 8 characters long",
        });
      }

      const userCheck = await pool.query(
        "SELECT id FROM metadata.users WHERE id = $1",
        [userId]
      );

      if (userCheck.rows.length === 0) {
        return res.status(404).json({ error: "User not found" });
      }

      const passwordHash = await bcrypt.hash(newPassword, 10);

      await pool.query(
        "UPDATE metadata.users SET password_hash = $1, updated_at = NOW() WHERE id = $2",
        [passwordHash, userId]
      );

      res.json({ message: "Password reset successfully" });
    } catch (err) {
      console.error("Reset password error:", err);
      const safeError = sanitizeError(
        err,
        "Failed to reset password",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

app.get("/api/health", (req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

// Obtener catálogo con paginación, filtros y búsqueda
app.get("/api/catalog", requireAuth, async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const engine = validateEnum(
      req.query.engine,
      ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle", ""],
      ""
    );
    const status = validateEnum(
      req.query.status,
      [
        "LISTENING_CHANGES",
        "FULL_LOAD",
        "ERROR",
        "NO_DATA",
        "SKIP",
        "IN_PROGRESS",
        "",
      ],
      ""
    );
    const active =
      req.query.active !== undefined ? validateBoolean(req.query.active) : "";
    const search = sanitizeSearch(req.query.search, 100);
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
    const safeError = sanitizeError(
      err,
      "Error fetching catalog data",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

// Actualizar estado
app.patch(
  "/api/catalog/status",
  requireAuth,
  requireRole("admin", "user"),
  async (req, res) => {
    const schema_name = validateIdentifier(req.body.schema_name);
    const table_name = validateIdentifier(req.body.table_name);
    const db_engine = validateEnum(
      req.body.db_engine,
      ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle"],
      null
    );
    const active = validateBoolean(req.body.active);

    if (!schema_name || !table_name || !db_engine) {
      return res
        .status(400)
        .json({ error: "Invalid schema_name, table_name, or db_engine" });
    }

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
      const safeError = sanitizeError(
        err,
        "Error fetching catalog data",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

// Forzar sincronización
app.post(
  "/api/catalog/sync",
  requireAuth,
  requireRole("admin", "user"),
  async (req, res) => {
    const schema_name = validateIdentifier(req.body.schema_name);
    const table_name = validateIdentifier(req.body.table_name);
    const db_engine = validateEnum(
      req.body.db_engine,
      ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle"],
      null
    );

    if (!schema_name || !table_name || !db_engine) {
      return res
        .status(400)
        .json({ error: "Invalid schema_name, table_name, or db_engine" });
    }

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
      const safeError = sanitizeError(
        err,
        "Error fetching catalog data",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

// Crear nueva entrada en el catálogo
app.post(
  "/api/catalog",
  requireAuth,
  requireRole("admin", "user"),
  async (req, res) => {
    const schema_name = validateIdentifier(req.body.schema_name);
    const table_name = validateIdentifier(req.body.table_name);
    const db_engine = validateEnum(
      req.body.db_engine,
      ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle"],
      null
    );
    const connection_string = req.body.connection_string;
    const status = validateEnum(
      req.body.status,
      [
        "PENDING",
        "IN_PROGRESS",
        "LISTENING_CHANGES",
        "NO_DATA",
        "ERROR",
        "SKIP",
      ],
      "PENDING"
    );
    const active = validateBoolean(req.body.active, true);
    const cluster_name = req.body.cluster_name || "";
    const pk_strategy = validateEnum(
      req.body.pk_strategy,
      ["PK", "OFFSET"],
      "OFFSET"
    );
    const last_sync_column = req.body.last_sync_column || null;

    if (!schema_name || !table_name || !db_engine || !connection_string) {
      return res.status(400).json({
        error:
          "schema_name, table_name, db_engine, and connection_string are required",
      });
    }

    try {
      const checkResult = await pool.query(
        `SELECT schema_name, table_name, db_engine FROM metadata.catalog 
       WHERE schema_name = $1 AND table_name = $2 AND db_engine = $3`,
        [schema_name, table_name, db_engine]
      );

      if (checkResult.rows.length > 0) {
        return res.status(409).json({
          error:
            "Entry already exists with this schema_name, table_name, and db_engine",
        });
      }

      const result = await pool.query(
        `INSERT INTO metadata.catalog 
       (schema_name, table_name, db_engine, connection_string, status, active, 
        cluster_name, pk_strategy, last_sync_column, last_sync_time)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL)
       RETURNING *`,
        [
          schema_name,
          table_name,
          db_engine,
          connection_string,
          status,
          active,
          cluster_name,
          pk_strategy,
          last_sync_column,
        ]
      );

      res.json(result.rows[0]);
    } catch (err) {
      console.error("Database error:", err);
      const safeError = sanitizeError(
        err,
        "Error creating catalog entry",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

// Obtener todos los schemas únicos
app.get("/api/catalog/schemas", async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT DISTINCT schema_name FROM metadata.catalog ORDER BY schema_name`
    );
    res.json(result.rows.map((row) => row.schema_name));
  } catch (err) {
    console.error("Database error:", err);
    const safeError = sanitizeError(
      err,
      "Error fetching catalog data",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

// Marcar tabla como SKIP
app.patch(
  "/api/catalog/skip-table",
  requireAuth,
  requireRole("admin", "user"),
  async (req, res) => {
    const schema_name = validateIdentifier(req.body.schema_name);
    const table_name = validateIdentifier(req.body.table_name);
    const db_engine = validateEnum(
      req.body.db_engine,
      ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle"],
      null
    );

    if (!schema_name || !table_name || !db_engine) {
      return res.status(400).json({
        error: "Invalid schema_name, table_name, or db_engine",
      });
    }

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
      const safeError = sanitizeError(
        err,
        "Error fetching catalog data",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

// Desactivar schema completo
app.patch(
  "/api/catalog/deactivate-schema",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
    const schema_name = validateIdentifier(req.body.schema_name);

    if (!schema_name) {
      return res.status(400).json({ error: "Invalid schema_name" });
    }

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
      const safeError = sanitizeError(
        err,
        "Error fetching catalog data",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

const PORT = process.env.PORT || 3000;
// Obtener estadísticas del dashboard
app.get("/api/dashboard/stats", async (req, res) => {
  try {
    console.log("Fetching dashboard stats...");

    const syncStatus = await pool.query(`
      SELECT 
        COUNT(*) FILTER (WHERE active = true AND status = 'LISTENING_CHANGES') as listening_changes,
        COUNT(*) FILTER (WHERE active = true) as full_load_active,
        COUNT(*) FILTER (WHERE active = false) as full_load_inactive,
        COUNT(*) FILTER (WHERE active = false AND status = 'NO_DATA') as no_data,
        COUNT(*) FILTER (WHERE status = 'SKIP') as skip,
        COUNT(*) FILTER (WHERE active = true AND status = 'ERROR') as errors,
        COUNT(*) FILTER (WHERE status = 'IN_PROGRESS') as in_progress,
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
    const cpuUsagePercent =
      cpuCount > 0 ? ((loadAvg * 100) / cpuCount).toFixed(1) : "0.0";

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
        inProgress: parseInt(syncStatus.rows[0]?.in_progress || 0),
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
    const safeError = sanitizeError(
      err,
      "Error al obtener estadísticas",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener queries activas",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

// Obtener métricas de calidad
app.get("/api/quality/metrics", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const engine = validateEnum(
      req.query.engine,
      ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle", ""],
      ""
    );
    const status = validateEnum(
      req.query.status,
      ["PASSED", "FAILED", "WARNING", ""],
      ""
    );

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
    const safeError = sanitizeError(
      err,
      "Error al obtener métricas de calidad",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

// Obtener datos de governance
app.get("/api/governance/data", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const engine = validateEnum(
      req.query.engine,
      ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle", ""],
      ""
    );
    const category = sanitizeSearch(req.query.category, 50);
    const health = validateEnum(
      req.query.health,
      ["EXCELLENT", "HEALTHY", "WARNING", "CRITICAL", ""],
      ""
    );
    const domain = sanitizeSearch(req.query.domain, 50);
    const sensitivity = validateEnum(
      req.query.sensitivity,
      ["PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED", ""],
      ""
    );

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
    const safeError = sanitizeError(
      err,
      "Error al obtener datos de governance",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener configuraciones",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.post("/api/config", requireAuth, requireRole("admin"), async (req, res) => {
  const { key, value, description } = req.body;
  try {
    const result = await pool.query(
      "INSERT INTO metadata.config (key, value, description) VALUES ($1, $2, $3) RETURNING *",
      [key, value, description]
    );
    res.json(result.rows[0]);
  } catch (err) {
    console.error("Error creating configuration:", err);
    const safeError = sanitizeError(
      err,
      "Error al crear configuración",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.put(
  "/api/config/:key",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
    const key = sanitizeSearch(req.params.key, 100);
    const value = sanitizeSearch(req.body.value, 500);
    const description = sanitizeSearch(req.body.description, 500);

    if (!key || key.trim() === "") {
      return res.status(400).json({ error: "Invalid key" });
    }

    try {
      const result = await pool.query(
        "UPDATE metadata.config SET value = $1, description = $2, updated_at = NOW() WHERE key = $3 RETURNING *",
        [value || "", description || "", key]
      );
      if (result.rows.length === 0) {
        res.status(404).json({ error: "Configuración no encontrada" });
        return;
      }
      res.json(result.rows[0]);
    } catch (err) {
      console.error("Error updating configuration:", err);
      const safeError = sanitizeError(
        err,
        "Error al actualizar configuración",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

app.delete(
  "/api/config/:key",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
    const key = sanitizeSearch(req.params.key, 100);

    if (!key || key.trim() === "") {
      return res.status(400).json({ error: "Invalid key" });
    }

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
      const safeError = sanitizeError(
        err,
        "Error al eliminar configuración",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

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
    const safeError = sanitizeError(
      err,
      "Error al obtener configuración de batch",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

// Endpoint para leer logs desde DB (metadata.logs)
app.get("/api/logs", async (req, res) => {
  try {
    const lines = validateLimit(req.query.lines, 1, 1000);
    const level = validateEnum(
      req.query.level,
      ["ALL", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
      "ALL"
    );
    const category = validateEnum(
      req.query.category,
      [
        "ALL",
        "SYSTEM",
        "DATABASE",
        "TRANSFER",
        "CUSTOM_JOB",
        "API",
        "QUALITY",
        "GOVERNANCE",
      ],
      "ALL"
    );
    const func = sanitizeSearch(req.query.function, 100) || "ALL";
    const search = sanitizeSearch(req.query.search, 200);
    const startDate = sanitizeSearch(req.query.startDate, 50);
    const endDate = sanitizeSearch(req.query.endDate, 50);

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
    if (search && search.trim() !== "") {
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
    const limit = lines;

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
    const safeError = sanitizeError(
      err,
      "Error al leer logs",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

// Endpoint para obtener logs de errores desde DB (niveles WARNING/ERROR/CRITICAL)
app.get("/api/logs/errors", async (req, res) => {
  try {
    const lines = validateLimit(req.query.lines, 1, 1000);
    const category = validateEnum(
      req.query.category,
      [
        "ALL",
        "SYSTEM",
        "DATABASE",
        "TRANSFER",
        "CUSTOM_JOB",
        "API",
        "QUALITY",
        "GOVERNANCE",
      ],
      "ALL"
    );
    const search = sanitizeSearch(req.query.search, 200);
    const startDate = sanitizeSearch(req.query.startDate, 50);
    const endDate = sanitizeSearch(req.query.endDate, 50);
    const params = [];
    let where = ["level IN ('WARNING','ERROR','CRITICAL')"];
    if (category && category !== "ALL") {
      params.push(category);
      where.push(`category = $${params.length}`);
    }
    if (search && search.trim() !== "") {
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
    const limit = lines;
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
    res.status(500).json({
      error: sanitizeError(
        err,
        "Error al leer logs de errores",
        process.env.NODE_ENV === "production"
      ),
    });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener información de logs",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener información de logs de errores",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    res.status(500).json({
      error: sanitizeError(
        err,
        "Error al obtener niveles",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.get("/api/logs/categories", async (_req, res) => {
  try {
    const result = await pool.query(
      "SELECT DISTINCT category FROM metadata.logs ORDER BY category"
    );
    res.json(result.rows.map((r) => r.category));
  } catch (err) {
    res.status(500).json({
      error: sanitizeError(
        err,
        "Error al obtener categorías",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.get("/api/logs/functions", async (_req, res) => {
  try {
    const result = await pool.query(
      "SELECT DISTINCT function FROM metadata.logs ORDER BY function"
    );
    res.json(result.rows.map((r) => r.function));
  } catch (err) {
    res.status(500).json({
      error: sanitizeError(
        err,
        "Error al obtener funciones",
        process.env.NODE_ENV === "production"
      ),
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
    const safeError = sanitizeError(
      err,
      "Error al obtener estadísticas de logs",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

// Endpoint para limpiar logs
app.delete("/api/logs", requireAuth, requireRole("admin"), async (req, res) => {
  try {
    await pool.query("TRUNCATE TABLE metadata.logs");
    res.json({
      success: true,
      message: "Logs table truncated",
      clearedAt: new Date().toISOString(),
    });
  } catch (err) {
    console.error("Error truncating logs:", err);
    const safeError = sanitizeError(
      err,
      "Error al truncar logs",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

// Endpoint para obtener tabla actualmente procesándose
app.get("/api/dashboard/currently-processing", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);

    const maxWorkersResult = await pool.query(
      "SELECT value FROM metadata.config WHERE key = 'max_workers'"
    );
    const maxWorkers = maxWorkersResult.rows[0]
      ? parseInt(maxWorkersResult.rows[0].value) || 8
      : 8;
    const limit = validateLimit(req.query.limit, 1, Math.max(maxWorkers, 100));
    const pageSize = limit;
    const offset = (page - 1) * pageSize;

    const currentlyProcessingResult = await pool.query(`
      SELECT DISTINCT ON (pl.schema_name, pl.table_name, pl.db_engine)
        pl.schema_name,
        pl.table_name,
        pl.db_engine,
        pl.status,
        pl.processed_at,
        pl.new_pk,
        pl.record_count
      FROM metadata.processing_log pl
      WHERE pl.status = 'IN_PROGRESS'
      ORDER BY pl.schema_name, pl.table_name, pl.db_engine, pl.processed_at DESC
    `);

    const recentHistoryResult = await pool.query(`
      SELECT DISTINCT ON (pl.schema_name, pl.table_name, pl.db_engine)
        pl.schema_name,
        pl.table_name,
        pl.db_engine,
        pl.status,
        pl.processed_at,
        pl.new_pk,
        pl.record_count
      FROM metadata.processing_log pl
      WHERE pl.status != 'IN_PROGRESS'
        AND pl.processed_at > NOW() - INTERVAL '15 minutes'
      ORDER BY pl.schema_name, pl.table_name, pl.db_engine, pl.processed_at DESC
      LIMIT 10
    `);

    const allResults = [
      ...currentlyProcessingResult.rows,
      ...recentHistoryResult.rows,
    ];

    const results = [];

    for (const row of allResults) {
      let countResult;
      try {
        const providedSchema = String(row.schema_name);
        const providedTable = String(row.table_name);

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

        if (resolved.rows.length > 0) {
          const schema = String(resolved.rows[0].schema_name).replace(
            /"/g,
            '""'
          );
          const table = String(resolved.rows[0].table_name).replace(/"/g, '""');
          const countSql = `SELECT COUNT(*) as total_records FROM "${schema}"."${table}"`;
          countResult = await pool.query(countSql);
        } else {
          countResult = { rows: [{ total_records: row.record_count || 0 }] };
        }
      } catch (countError) {
        countResult = { rows: [{ total_records: row.record_count || 0 }] };
      }

      results.push({
        schema_name: String(row.schema_name).toLowerCase(),
        table_name: String(row.table_name).toLowerCase(),
        db_engine: String(row.db_engine),
        new_pk: row.new_pk || null,
        status: String(row.status),
        processed_at: row.processed_at,
        total_records: parseInt(
          countResult.rows[0]?.total_records || row.record_count || 0
        ),
      });
    }

    const inProgressResults = results.filter((r) => r.status === "IN_PROGRESS");
    const historyResults = results.filter((r) => r.status !== "IN_PROGRESS");

    const sortedResults = [
      ...inProgressResults.sort(
        (a, b) =>
          new Date(b.processed_at).getTime() -
          new Date(a.processed_at).getTime()
      ),
      ...historyResults.sort(
        (a, b) =>
          new Date(b.processed_at).getTime() -
          new Date(a.processed_at).getTime()
      ),
    ];

    const total = sortedResults.length;
    const totalPages = Math.ceil(total / pageSize);
    const paginatedResults = sortedResults.slice(offset, offset + pageSize);

    res.json({
      data: paginatedResults,
      pagination: {
        page,
        limit: pageSize,
        total,
        totalPages,
        hasNext: page < totalPages,
        hasPrev: page > 1,
      },
    });
  } catch (err) {
    console.error("Error getting currently processing tables:", err);
    const safeError = sanitizeError(
      err,
      "Error al obtener tablas en procesamiento",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener datos de seguridad",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.get("/api/monitor/processing-logs", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const strategy = validateEnum(
      req.query.strategy,
      ["AUTO_INCREMENT", "TIMESTAMP", "UUID", ""],
      ""
    );

    const params = [];
    let paramCount = 1;
    const whereConditions = [];

    if (strategy) {
      whereConditions.push(`c.pk_strategy = $${paramCount}`);
      params.push(strategy);
      paramCount++;
    }

    const whereClause =
      whereConditions.length > 0
        ? `WHERE ${whereConditions.join(" AND ")}`
        : "";

    const latestStatusSubquery = `
      SELECT DISTINCT ON (pl2.schema_name, pl2.table_name, pl2.db_engine)
        pl2.schema_name,
        pl2.table_name,
        pl2.db_engine,
        pl2.status,
        pl2.processed_at
      FROM metadata.processing_log pl2
      ORDER BY pl2.schema_name, pl2.table_name, pl2.db_engine, pl2.processed_at DESC
    `;

    const countResult = await pool.query(
      `
      SELECT COUNT(DISTINCT ls.schema_name || '.' || ls.table_name || '.' || ls.db_engine) as total
      FROM (${latestStatusSubquery}) ls
      LEFT JOIN metadata.catalog c 
        ON c.schema_name = ls.schema_name 
        AND c.table_name = ls.table_name 
        AND c.db_engine = ls.db_engine
      ${whereClause}
    `,
      params
    );

    const total = parseInt(countResult.rows[0].total);
    const totalPages = Math.ceil(total / limit);

    params.push(limit, offset);
    const result = await pool.query(
      `
      SELECT 
        latest.id,
        latest.schema_name,
        latest.table_name,
        latest.db_engine,
        latest.pk_strategy,
        latest.status,
        latest.processed_at,
        latest.new_pk,
        latest.record_count
      FROM (
        SELECT DISTINCT ON (pl.schema_name, pl.table_name, pl.db_engine)
          pl.id,
          pl.schema_name,
          pl.table_name,
          pl.db_engine,
          COALESCE(c.pk_strategy, 'N/A') as pk_strategy,
          pl.status,
          pl.processed_at,
          pl.new_pk,
          pl.record_count
        FROM metadata.processing_log pl
        LEFT JOIN metadata.catalog c 
          ON c.schema_name = pl.schema_name 
          AND c.table_name = pl.table_name 
          AND c.db_engine = pl.db_engine
        ${whereClause}
        ORDER BY pl.schema_name, pl.table_name, pl.db_engine, pl.processed_at DESC
      ) latest
      ORDER BY latest.processed_at DESC
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
    const safeError = sanitizeError(
      err,
      "Error al obtener logs de procesamiento",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.get("/api/monitor/processing-logs/stats", async (req, res) => {
  try {
    const latestStatusQuery = `
      SELECT DISTINCT ON (pl.schema_name, pl.table_name, pl.db_engine)
        pl.schema_name,
        pl.table_name,
        pl.db_engine,
        pl.status,
        pl.processed_at
      FROM metadata.processing_log pl
      ORDER BY pl.schema_name, pl.table_name, pl.db_engine, pl.processed_at DESC
    `;

    const result = await pool.query(`
      WITH latest_status AS (${latestStatusQuery})
      SELECT 
        COUNT(DISTINCT ls.schema_name || '.' || ls.table_name || '.' || ls.db_engine) FILTER (
          WHERE ls.status = 'IN_PROGRESS' 
          AND ls.processed_at > NOW() - INTERVAL '5 minutes'
        ) as total,
        (SELECT COUNT(*) FROM metadata.processing_log WHERE processed_at > NOW() - INTERVAL '24 hours') as last24h,
        COUNT(DISTINCT ls.schema_name || '.' || ls.table_name || '.' || ls.db_engine) FILTER (
          WHERE ls.status = 'LISTENING_CHANGES'
        ) as listeningChanges,
        COUNT(DISTINCT ls.schema_name || '.' || ls.table_name || '.' || ls.db_engine) FILTER (
          WHERE ls.status = 'FULL_LOAD'
        ) as fullLoad,
        COUNT(DISTINCT ls.schema_name || '.' || ls.table_name || '.' || ls.db_engine) FILTER (
          WHERE ls.status = 'ERROR'
        ) as errors
      FROM latest_status ls
    `);

    res.json({
      total: parseInt(result.rows[0]?.total || 0),
      last24h: parseInt(result.rows[0]?.last24h || 0),
      listeningChanges: parseInt(result.rows[0]?.listeningChanges || 0),
      fullLoad: parseInt(result.rows[0]?.fullLoad || 0),
      errors: parseInt(result.rows[0]?.errors || 0),
    });
  } catch (err) {
    console.error("Error getting processing stats:", err);
    const safeError = sanitizeError(
      err,
      "Error al obtener estadísticas de procesamiento",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.post(
  "/api/monitor/processing-logs/cleanup",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
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
      const safeError = sanitizeError(
        err,
        "Error al limpiar logs antiguos",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

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
    const safeError = sanitizeError(
      err,
      "Error al obtener datos de rendimiento de queries",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener métricas de rendimiento de queries",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener items de mantenimiento",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener métricas de mantenimiento",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.get("/api/column-catalog/columns", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const schema_name = validateIdentifier(req.query.schema_name) || "";
    const table_name = validateIdentifier(req.query.table_name) || "";
    const db_engine = validateEnum(
      req.query.db_engine,
      ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle", ""],
      ""
    );
    const data_type = sanitizeSearch(req.query.data_type, 50);
    const sensitivity_level = validateEnum(
      req.query.sensitivity_level,
      ["LOW", "MEDIUM", "HIGH", "CRITICAL", ""],
      ""
    );
    const contains_pii =
      req.query.contains_pii !== undefined
        ? validateBoolean(req.query.contains_pii)
        : "";
    const contains_phi =
      req.query.contains_phi !== undefined
        ? validateBoolean(req.query.contains_phi)
        : "";
    const search = sanitizeSearch(req.query.search, 100);

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
    const safeError = sanitizeError(
      err,
      "Error al obtener datos del catálogo de columnas",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener métricas del catálogo de columnas",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.get("/api/column-catalog/tables/:schemaName", async (req, res) => {
  try {
    const schemaName = validateIdentifier(req.params.schemaName);
    if (!schemaName) {
      return res.status(400).json({ error: "Invalid schemaName" });
    }
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
    const safeError = sanitizeError(
      err,
      "Error al obtener tablas",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener locks del catálogo",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener métricas de locks",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.delete(
  "/api/catalog-locks/locks/:lockName",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
    const lockName = validateIdentifier(req.params.lockName);

    if (!lockName) {
      return res.status(400).json({ error: "Invalid lockName" });
    }

    try {
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
      const safeError = sanitizeError(
        err,
        "Error al liberar lock",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

app.post(
  "/api/catalog-locks/clean-expired",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
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
      const safeError = sanitizeError(
        err,
        "Error al limpiar locks expirados",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

app.get("/api/data-lineage/mariadb", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const server_name = sanitizeSearch(req.query.server_name, 100);
    const database_name = sanitizeSearch(req.query.database_name, 100);
    const object_type = validateEnum(
      req.query.object_type,
      ["TABLE", "VIEW", "PROCEDURE", "FUNCTION", ""],
      ""
    );
    const relationship_type = validateEnum(
      req.query.relationship_type,
      ["FOREIGN_KEY", "VIEW_DEPENDENCY", "PROCEDURE_DEPENDENCY", ""],
      ""
    );
    const search = sanitizeSearch(req.query.search, 100);

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
      ORDER BY confidence_score DESC, last_seen_at DESC
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
    const safeError = sanitizeError(
      err,
      "Error al obtener datos de lineage de MariaDB",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener métricas de lineage de MariaDB",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener servidores",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener bases de datos",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.get("/api/data-lineage/mssql", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const server_name = sanitizeSearch(req.query.server_name, 100);
    const instance_name = sanitizeSearch(req.query.instance_name, 100);
    const database_name = sanitizeSearch(req.query.database_name, 100);
    const object_type = validateEnum(
      req.query.object_type,
      ["TABLE", "VIEW", "PROCEDURE", "FUNCTION", ""],
      ""
    );
    const relationship_type = validateEnum(
      req.query.relationship_type,
      ["FOREIGN_KEY", "VIEW_DEPENDENCY", "PROCEDURE_DEPENDENCY", ""],
      ""
    );
    const search = sanitizeSearch(req.query.search, 100);

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
      ORDER BY confidence_score DESC, last_seen_at DESC
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
    const safeError = sanitizeError(
      err,
      "Error al obtener datos de lineage de MSSQL",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener métricas de lineage de MSSQL",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener servidores",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.get("/api/data-lineage/mssql/instances/:serverName", async (req, res) => {
  try {
    const serverName = sanitizeSearch(req.params.serverName, 100);
    if (!serverName) {
      return res.status(400).json({ error: "Invalid serverName" });
    }
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
    const safeError = sanitizeError(
      err,
      "Error al obtener instancias",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
        error: sanitizeError(
          err,
          "Error en el servidor",
          process.env.NODE_ENV === "production"
        ),
      });
    }
  }
);

app.get("/api/governance-catalog/mariadb", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const server_name = sanitizeSearch(req.query.server_name, 100);
    const database_name = sanitizeSearch(req.query.database_name, 100);
    const health_status = validateEnum(
      req.query.health_status,
      ["HEALTHY", "WARNING", "CRITICAL", ""],
      ""
    );
    const access_frequency = validateEnum(
      req.query.access_frequency,
      ["HIGH", "MEDIUM", "LOW", ""],
      ""
    );
    const search = sanitizeSearch(req.query.search, 100);

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
    const safeError = sanitizeError(
      err,
      "Error al obtener catálogo de governance de MariaDB",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener métricas de governance de MariaDB",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener servidores",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
        error: sanitizeError(
          err,
          "Error en el servidor",
          process.env.NODE_ENV === "production"
        ),
      });
    }
  }
);

app.get("/api/governance-catalog/mssql", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const server_name = sanitizeSearch(req.query.server_name, 100);
    const database_name = sanitizeSearch(req.query.database_name, 100);
    const object_type = validateEnum(
      req.query.object_type,
      ["TABLE", "VIEW", "PROCEDURE", "FUNCTION", ""],
      ""
    );
    const health_status = validateEnum(
      req.query.health_status,
      ["HEALTHY", "WARNING", "CRITICAL", ""],
      ""
    );
    const access_frequency = validateEnum(
      req.query.access_frequency,
      ["HIGH", "MEDIUM", "LOW", ""],
      ""
    );
    const search = sanitizeSearch(req.query.search, 100);

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
    const safeError = sanitizeError(
      err,
      "Error al obtener catálogo de governance de MSSQL",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener métricas de governance de MSSQL",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
    const safeError = sanitizeError(
      err,
      "Error al obtener servidores",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
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
        error: sanitizeError(
          err,
          "Error en el servidor",
          process.env.NODE_ENV === "production"
        ),
      });
    }
  }
);

// MongoDB Lineage endpoints
app.get("/api/data-lineage/mongodb", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const server_name = sanitizeSearch(req.query.server_name, 100);
    const database_name = sanitizeSearch(req.query.database_name, 100);
    const relationship_type = validateEnum(
      req.query.relationship_type,
      ["VIEW_DEPENDENCY", "COLLECTION_REFERENCE", ""],
      ""
    );
    const search = sanitizeSearch(req.query.search, 100);

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

    if (relationship_type) {
      whereConditions.push(`relationship_type = $${paramCount}`);
      params.push(relationship_type);
      paramCount++;
    }

    if (search) {
      whereConditions.push(
        `(source_collection ILIKE $${paramCount} OR target_collection ILIKE $${paramCount})`
      );
      params.push(`%${search}%`);
      paramCount++;
    }

    const whereClause =
      whereConditions.length > 0
        ? "WHERE " + whereConditions.join(" AND ")
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.mongo_lineage ${whereClause}`;
    const countResult = await pool.query(countQuery, params);
    const total = parseInt(countResult.rows[0].count);

    params.push(limit, offset);
    const dataQuery = `
      SELECT *
      FROM metadata.mongo_lineage
      ${whereClause}
      ORDER BY dependency_level, confidence_score DESC, snapshot_date DESC
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
    console.error("Error getting MongoDB lineage data:", err);
    res.status(500).json({
      error: "Error al obtener datos de lineage de MongoDB",
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.get("/api/data-lineage/mongodb/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total_relationships,
        COUNT(DISTINCT source_collection) + COUNT(DISTINCT target_collection) as unique_collections,
        COUNT(DISTINCT server_name) as unique_servers,
        COUNT(*) FILTER (WHERE confidence_score >= 0.8) as high_confidence,
        ROUND(AVG(confidence_score)::numeric, 4) as avg_confidence
      FROM metadata.mongo_lineage
    `);

    res.json(result.rows[0] || {});
  } catch (err) {
    console.error("Error getting MongoDB lineage metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas de lineage de MongoDB",
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.get("/api/data-lineage/mongodb/servers", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT server_name
      FROM metadata.mongo_lineage
      WHERE server_name IS NOT NULL
      ORDER BY server_name
    `);

    res.json(result.rows.map((row) => row.server_name));
  } catch (err) {
    console.error("Error getting MongoDB servers:", err);
    const safeError = sanitizeError(
      err,
      "Error al obtener servidores",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.get("/api/data-lineage/mongodb/databases/:serverName", async (req, res) => {
  try {
    const serverName = sanitizeSearch(req.params.serverName, 100);
    if (!serverName) {
      return res.status(400).json({ error: "Invalid serverName" });
    }
    const result = await pool.query(
      `
        SELECT DISTINCT database_name
        FROM metadata.mongo_lineage
        WHERE server_name = $1 AND database_name IS NOT NULL
        ORDER BY database_name
      `,
      [serverName]
    );

    res.json(result.rows.map((row) => row.database_name));
  } catch (err) {
    console.error("Error getting MongoDB databases:", err);
    const safeError = sanitizeError(
      err,
      "Error al obtener bases de datos",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

// MongoDB Governance Catalog endpoints
app.get("/api/governance-catalog/mongodb", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const server_name = sanitizeSearch(req.query.server_name, 100);
    const database_name = sanitizeSearch(req.query.database_name, 100);
    const health_status = validateEnum(
      req.query.health_status,
      ["HEALTHY", "WARNING", "CRITICAL", ""],
      ""
    );
    const access_frequency = validateEnum(
      req.query.access_frequency,
      ["HIGH", "MEDIUM", "LOW", ""],
      ""
    );
    const search = sanitizeSearch(req.query.search, 100);

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
      whereConditions.push(`collection_name ILIKE $${paramCount}`);
      params.push(`%${search}%`);
      paramCount++;
    }

    const whereClause =
      whereConditions.length > 0
        ? "WHERE " + whereConditions.join(" AND ")
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.data_governance_catalog_mongodb ${whereClause}`;
    const countResult = await pool.query(countQuery, params);
    const total = parseInt(countResult.rows[0].count);

    params.push(limit, offset);
    const dataQuery = `
      SELECT *
      FROM metadata.data_governance_catalog_mongodb
      ${whereClause}
      ORDER BY server_name, database_name, collection_name, index_name
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
    console.error("Error getting MongoDB governance catalog:", err);
    res.status(500).json({
      error: "Error al obtener catálogo de governance de MongoDB",
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.get("/api/governance-catalog/mongodb/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(DISTINCT collection_name) as total_collections,
        SUM(COALESCE(total_size_mb, 0)) as total_size_mb,
        SUM(COALESCE(document_count, 0)) as total_documents,
        COUNT(*) FILTER (WHERE health_status IN ('EXCELLENT', 'HEALTHY')) as healthy_count,
        COUNT(*) FILTER (WHERE health_status = 'WARNING') as warning_count,
        COUNT(*) FILTER (WHERE health_status = 'CRITICAL') as critical_count,
        COUNT(DISTINCT server_name) as unique_servers
      FROM metadata.data_governance_catalog_mongodb
    `);

    res.json(result.rows[0] || {});
  } catch (err) {
    console.error("Error getting MongoDB governance metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas de governance de MongoDB",
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.get("/api/governance-catalog/mongodb/servers", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT server_name
      FROM metadata.data_governance_catalog_mongodb
      WHERE server_name IS NOT NULL
      ORDER BY server_name
    `);

    res.json(result.rows.map((row) => row.server_name));
  } catch (err) {
    console.error("Error getting MongoDB servers:", err);
    const safeError = sanitizeError(
      err,
      "Error al obtener servidores",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.get(
  "/api/governance-catalog/mongodb/databases/:serverName",
  async (req, res) => {
    try {
      const serverName = req.params.serverName;
      const result = await pool.query(
        `
        SELECT DISTINCT database_name
        FROM metadata.data_governance_catalog_mongodb
        WHERE server_name = $1 AND database_name IS NOT NULL
        ORDER BY database_name
      `,
        [serverName]
      );

      res.json(result.rows.map((row) => row.database_name));
    } catch (err) {
      console.error("Error getting MongoDB databases:", err);
      res.status(500).json({
        error: "Error al obtener bases de datos",
        error: sanitizeError(
          err,
          "Error en el servidor",
          process.env.NODE_ENV === "production"
        ),
      });
    }
  }
);

// Oracle Lineage endpoints
app.get("/api/data-lineage/oracle", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const server_name = sanitizeSearch(req.query.server_name, 100);
    const schema_name = validateIdentifier(req.query.schema_name) || "";
    const relationship_type = validateEnum(
      req.query.relationship_type,
      ["FOREIGN_KEY", "VIEW_DEPENDENCY", "TRIGGER_DEPENDENCY", ""],
      ""
    );
    const search = sanitizeSearch(req.query.search, 100);

    const whereConditions = [];
    const params = [];
    let paramCount = 1;

    if (server_name) {
      whereConditions.push(`server_name = $${paramCount}`);
      params.push(server_name);
      paramCount++;
    }

    if (schema_name) {
      whereConditions.push(`schema_name = $${paramCount}`);
      params.push(schema_name);
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

    const countQuery = `SELECT COUNT(*) FROM metadata.oracle_lineage ${whereClause}`;
    const countResult = await pool.query(countQuery, params);
    const total = parseInt(countResult.rows[0].count);

    params.push(limit, offset);
    const dataQuery = `
      SELECT *
      FROM metadata.oracle_lineage
      ${whereClause}
      ORDER BY confidence_score DESC, last_seen_at DESC
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
    console.error("Error getting Oracle lineage data:", err);
    res.status(500).json({
      error: "Error al obtener datos de lineage de Oracle",
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.get("/api/data-lineage/oracle/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total_relationships,
        COUNT(DISTINCT object_name) + COUNT(DISTINCT target_object_name) as unique_objects,
        COUNT(DISTINCT server_name) as unique_servers,
        COUNT(*) FILTER (WHERE confidence_score >= 0.8) as high_confidence,
        ROUND(AVG(confidence_score)::numeric, 4) as avg_confidence
      FROM metadata.oracle_lineage
    `);

    res.json(result.rows[0] || {});
  } catch (err) {
    console.error("Error getting Oracle lineage metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas de lineage de Oracle",
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.get("/api/data-lineage/oracle/servers", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT server_name
      FROM metadata.oracle_lineage
      WHERE server_name IS NOT NULL
      ORDER BY server_name
    `);

    res.json(result.rows.map((row) => row.server_name));
  } catch (err) {
    console.error("Error getting Oracle servers:", err);
    const safeError = sanitizeError(
      err,
      "Error al obtener servidores",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.get("/api/data-lineage/oracle/schemas/:serverName", async (req, res) => {
  try {
    const serverName = sanitizeSearch(req.params.serverName, 100);
    if (!serverName) {
      return res.status(400).json({ error: "Invalid serverName" });
    }
    const result = await pool.query(
      `
        SELECT DISTINCT schema_name
        FROM metadata.oracle_lineage
        WHERE server_name = $1 AND schema_name IS NOT NULL
        ORDER BY schema_name
      `,
      [serverName]
    );

    res.json(result.rows.map((row) => row.schema_name));
  } catch (err) {
    console.error("Error getting Oracle schemas:", err);
    const safeError = sanitizeError(
      err,
      "Error al obtener schemas de Oracle",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

// Oracle Governance Catalog endpoints
app.get("/api/governance-catalog/oracle", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const server_name = sanitizeSearch(req.query.server_name, 100);
    const schema_name = validateIdentifier(req.query.schema_name) || "";
    const health_status = validateEnum(
      req.query.health_status,
      ["HEALTHY", "WARNING", "CRITICAL", "EXCELLENT", ""],
      ""
    );
    const access_frequency = validateEnum(
      req.query.access_frequency,
      ["HIGH", "MEDIUM", "LOW", ""],
      ""
    );
    const search = sanitizeSearch(req.query.search, 100);

    const whereConditions = [];
    const params = [];
    let paramCount = 1;

    if (server_name) {
      whereConditions.push(`server_name = $${paramCount}`);
      params.push(server_name);
      paramCount++;
    }

    if (schema_name) {
      whereConditions.push(`schema_name = $${paramCount}`);
      params.push(schema_name);
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
        `(table_name ILIKE $${paramCount} OR schema_name ILIKE $${paramCount})`
      );
      params.push(`%${search}%`);
      paramCount++;
    }

    const whereClause =
      whereConditions.length > 0
        ? "WHERE " + whereConditions.join(" AND ")
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.data_governance_catalog_oracle ${whereClause}`;
    const countResult = await pool.query(countQuery, params);
    const total = parseInt(countResult.rows[0].count);

    params.push(limit, offset);
    const dataQuery = `
      SELECT *
      FROM metadata.data_governance_catalog_oracle
      ${whereClause}
      ORDER BY health_score DESC NULLS LAST, table_name
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
    console.error("Error getting Oracle governance catalog:", err);
    res.status(500).json({
      error: "Error al obtener datos de governance de Oracle",
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.get("/api/governance-catalog/oracle/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(DISTINCT CASE WHEN index_name IS NULL THEN table_name END) as total_tables,
        SUM(COALESCE(total_size_mb, 0)) as total_size_mb,
        SUM(COALESCE(row_count, 0)) as total_rows,
        COUNT(*) FILTER (WHERE health_status IN ('EXCELLENT', 'HEALTHY')) as healthy_count,
        COUNT(*) FILTER (WHERE health_status = 'WARNING') as warning_count,
        COUNT(*) FILTER (WHERE health_status = 'CRITICAL') as critical_count,
        COUNT(DISTINCT server_name) as unique_servers
      FROM metadata.data_governance_catalog_oracle
    `);

    res.json(result.rows[0] || {});
  } catch (err) {
    console.error("Error getting Oracle governance metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas de governance de Oracle",
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.get("/api/governance-catalog/oracle/servers", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT DISTINCT server_name
      FROM metadata.data_governance_catalog_oracle
      WHERE server_name IS NOT NULL
      ORDER BY server_name
    `);

    res.json(result.rows.map((row) => row.server_name));
  } catch (err) {
    console.error("Error getting Oracle servers:", err);
    const safeError = sanitizeError(
      err,
      "Error al obtener servidores",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.get(
  "/api/governance-catalog/oracle/schemas/:serverName",
  async (req, res) => {
    try {
      const serverName = req.params.serverName;
      const result = await pool.query(
        `
        SELECT DISTINCT schema_name
        FROM metadata.data_governance_catalog_oracle
        WHERE server_name = $1 AND schema_name IS NOT NULL
        ORDER BY schema_name
      `,
        [serverName]
      );

      res.json(result.rows.map((row) => row.schema_name));
    } catch (err) {
      console.error("Error getting Oracle schemas:", err);
      res.status(500).json({
        error: "Error al obtener schemas",
        error: sanitizeError(
          err,
          "Error en el servidor",
          process.env.NODE_ENV === "production"
        ),
      });
    }
  }
);

app.get("/api/api-catalog", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const api_type = validateEnum(
      req.query.api_type,
      ["REST", "GraphQL", "SOAP", ""],
      ""
    );
    const target_db_engine = validateEnum(
      req.query.target_db_engine,
      ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle", ""],
      ""
    );
    const status = validateEnum(
      req.query.status,
      ["SUCCESS", "ERROR", "IN_PROGRESS", "PENDING", ""],
      ""
    );
    const active =
      req.query.active !== undefined ? validateBoolean(req.query.active) : "";
    const search = sanitizeSearch(req.query.search, 100);

    let whereConditions = [];
    let queryParams = [];
    let paramCount = 0;

    if (api_type) {
      paramCount++;
      whereConditions.push(`api_type = $${paramCount}`);
      queryParams.push(api_type);
    }

    if (target_db_engine) {
      paramCount++;
      whereConditions.push(`target_db_engine = $${paramCount}`);
      queryParams.push(target_db_engine);
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
        `(api_name ILIKE $${paramCount} OR endpoint ILIKE $${paramCount} OR target_schema ILIKE $${paramCount} OR target_table ILIKE $${paramCount})`
      );
      queryParams.push(`%${search}%`);
    }

    const whereClause =
      whereConditions.length > 0
        ? `WHERE ${whereConditions.join(" AND ")}`
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.api_catalog ${whereClause}`;
    const countResult = await pool.query(countQuery, queryParams);
    const total = parseInt(countResult.rows[0].count);

    paramCount++;
    const dataQuery = `SELECT * FROM metadata.api_catalog ${whereClause}
      ORDER BY 
        CASE status
          WHEN 'SUCCESS' THEN 1
          WHEN 'IN_PROGRESS' THEN 2
          WHEN 'ERROR' THEN 3
          WHEN 'PENDING' THEN 4
          ELSE 5
        END,
        active DESC,
        api_name
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
    console.error("Error getting API catalog:", err);
    res.status(500).json({
      error: "Error al obtener catálogo de APIs",
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.patch("/api/api-catalog/active", async (req, res) => {
  const { api_name, active } = req.body;
  try {
    const result = await pool.query(
      `UPDATE metadata.api_catalog 
       SET active = $1, updated_at = NOW()
       WHERE api_name = $2
       RETURNING *`,
      [active, api_name]
    );
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "API not found" });
    }
    res.json(result.rows[0]);
  } catch (err) {
    console.error("Error updating API active status:", err);
    res.status(500).json({
      error: "Error al actualizar estado de API",
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.post(
  "/api/api-catalog",
  requireAuth,
  requireRole("admin", "user"),
  async (req, res) => {
    const {
      api_name,
      api_type,
      base_url,
      endpoint,
      http_method,
      auth_type,
      auth_config,
      target_db_engine,
      target_connection_string,
      target_schema,
      target_table,
      request_body,
      request_headers,
      query_params,
      sync_interval,
      status,
      active,
    } = req.body;

    if (
      !api_name ||
      !api_type ||
      !base_url ||
      !endpoint ||
      !http_method ||
      !auth_type ||
      !target_db_engine ||
      !target_connection_string ||
      !target_schema ||
      !target_table
    ) {
      return res.status(400).json({
        error:
          "Missing required fields: api_name, api_type, base_url, endpoint, http_method, auth_type, target_db_engine, target_connection_string, target_schema, target_table",
      });
    }

    const validApiType = validateEnum(
      api_type,
      ["REST", "GraphQL", "SOAP"],
      null
    );
    if (!validApiType) {
      return res.status(400).json({ error: "Invalid api_type" });
    }

    const validHttpMethod = validateEnum(
      http_method,
      ["GET", "POST", "PUT", "PATCH", "DELETE"],
      null
    );
    if (!validHttpMethod) {
      return res.status(400).json({ error: "Invalid http_method" });
    }

    const validAuthType = validateEnum(
      auth_type,
      ["NONE", "BASIC", "BEARER", "API_KEY", "OAUTH2"],
      null
    );
    if (!validAuthType) {
      return res.status(400).json({ error: "Invalid auth_type" });
    }

    const validTargetEngine = validateEnum(
      target_db_engine,
      ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle"],
      null
    );
    if (!validTargetEngine) {
      return res.status(400).json({ error: "Invalid target_db_engine" });
    }

    const validStatus = validateEnum(
      status || "PENDING",
      ["SUCCESS", "ERROR", "IN_PROGRESS", "PENDING"],
      "PENDING"
    );

    const interval =
      sync_interval && sync_interval > 0 ? parseInt(sync_interval) : 3600;
    const isActive =
      active !== undefined ? validateBoolean(active, true) : true;

    try {
      const checkResult = await pool.query(
        `SELECT api_name FROM metadata.api_catalog WHERE api_name = $1`,
        [api_name]
      );

      if (checkResult.rows.length > 0) {
        return res.status(409).json({
          error: "API with this name already exists",
        });
      }

      const result = await pool.query(
        `INSERT INTO metadata.api_catalog 
       (api_name, api_type, base_url, endpoint, http_method, auth_type, auth_config,
        target_db_engine, target_connection_string, target_schema, target_table,
        request_body, request_headers, query_params, status, active, sync_interval)
       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12, $13::jsonb, $14::jsonb, $15, $16, $17)
       RETURNING *`,
        [
          api_name,
          api_type,
          base_url,
          endpoint,
          http_method,
          auth_type,
          JSON.stringify(auth_config || {}),
          target_db_engine,
          target_connection_string,
          target_schema.toLowerCase(),
          target_table.toLowerCase(),
          request_body || null,
          JSON.stringify(request_headers || {}),
          JSON.stringify(query_params || {}),
          validStatus,
          isActive,
          interval,
        ]
      );

      res.json(result.rows[0]);
    } catch (err) {
      console.error("Database error:", err);
      const safeError = sanitizeError(
        err,
        "Error creating API entry",
        process.env.NODE_ENV === "production"
      );
      res.status(500).json({ error: safeError });
    }
  }
);

app.get("/api/api-catalog/metrics", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        COUNT(*) as total_apis,
        COUNT(*) FILTER (WHERE active = true) as active_apis,
        COUNT(*) FILTER (WHERE status = 'SUCCESS') as success_apis,
        COUNT(*) FILTER (WHERE status = 'ERROR') as error_apis,
        COUNT(*) FILTER (WHERE status = 'IN_PROGRESS') as in_progress_apis,
        COUNT(*) FILTER (WHERE status = 'PENDING') as pending_apis,
        COUNT(DISTINCT api_type) as api_types_count,
        COUNT(DISTINCT target_db_engine) as target_engines_count
      FROM metadata.api_catalog
    `);
    res.json(result.rows[0]);
  } catch (err) {
    console.error("Error getting API catalog metrics:", err);
    res.status(500).json({
      error: "Error al obtener métricas del catálogo de APIs",
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.get("/api/custom-jobs/scripts", async (req, res) => {
  try {
    const scriptsPath = path.join(process.cwd(), "scripts");
    if (!fs.existsSync(scriptsPath)) {
      return res.json([]);
    }
    const files = fs.readdirSync(scriptsPath);
    const scripts = files
      .filter((file) => file.endsWith(".py"))
      .map((file) => {
        const filePath = path.join(scriptsPath, file);
        const content = fs.readFileSync(filePath, "utf8");
        return {
          name: file,
          content: content,
        };
      });
    res.json(scripts);
  } catch (err) {
    console.error("Error reading Python scripts:", err);
    const safeError = sanitizeError(
      err,
      "Error al leer scripts de Python",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.post(
  "/api/custom-jobs",
  requireAuth,
  requireRole("admin", "user"),
  async (req, res) => {
    try {
      const job_name = validateIdentifier(req.body.job_name);
      const description = sanitizeSearch(req.body.description, 500);
      const source_db_engine = validateEnum(
        req.body.source_db_engine,
        ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle"],
        null
      );
      const source_connection_string = sanitizeSearch(
        req.body.source_connection_string,
        500
      );
      const query_sql = sanitizeSearch(req.body.query_sql, 10000);
      const target_db_engine = validateEnum(
        req.body.target_db_engine,
        ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle"],
        null
      );
      const target_connection_string = sanitizeSearch(
        req.body.target_connection_string,
        500
      );
      const target_schema = validateIdentifier(req.body.target_schema);
      const target_table = validateIdentifier(req.body.target_table);
      const schedule_cron = sanitizeSearch(req.body.schedule_cron, 100);
      const active = validateBoolean(req.body.active, true);
      const enabled = validateBoolean(req.body.enabled, true);
      const transform_config = req.body.transform_config || {};
      const metadata = req.body.metadata || {};

      if (!job_name) {
        return res.status(400).json({ error: "job_name is required" });
      }
      if (!source_db_engine) {
        return res.status(400).json({ error: "Invalid source_db_engine" });
      }
      if (!target_db_engine) {
        return res.status(400).json({ error: "Invalid target_db_engine" });
      }
      if (!target_schema || !target_table) {
        return res
          .status(400)
          .json({ error: "target_schema and target_table are required" });
      }

      const result = await pool.query(
        `INSERT INTO metadata.custom_jobs 
       (job_name, description, source_db_engine, source_connection_string, 
        query_sql, target_db_engine, target_connection_string, target_schema, 
        target_table, schedule_cron, active, enabled, transform_config, metadata)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14::jsonb)
       ON CONFLICT (job_name) 
       DO UPDATE SET 
         description = EXCLUDED.description,
         source_db_engine = EXCLUDED.source_db_engine,
         source_connection_string = EXCLUDED.source_connection_string,
         query_sql = EXCLUDED.query_sql,
         target_db_engine = EXCLUDED.target_db_engine,
         target_connection_string = EXCLUDED.target_connection_string,
         target_schema = EXCLUDED.target_schema,
         target_table = EXCLUDED.target_table,
         schedule_cron = EXCLUDED.schedule_cron,
         active = EXCLUDED.active,
         enabled = EXCLUDED.enabled,
         transform_config = EXCLUDED.transform_config,
         metadata = EXCLUDED.metadata,
         updated_at = NOW()
       RETURNING *`,
        [
          job_name,
          description || null,
          source_db_engine,
          source_connection_string,
          query_sql,
          target_db_engine,
          target_connection_string,
          target_schema,
          target_table,
          schedule_cron || null,
          active !== undefined ? active : true,
          enabled !== undefined ? enabled : true,
          JSON.stringify(transform_config || {}),
          JSON.stringify(metadata || {}),
        ]
      );
      res.json(result.rows[0]);
    } catch (err) {
      console.error("Error creating/updating custom job:", err);
      res.status(500).json({
        error: "Error al crear/actualizar job personalizado",
        error: sanitizeError(
          err,
          "Error en el servidor",
          process.env.NODE_ENV === "production"
        ),
      });
    }
  }
);

app.get("/api/custom-jobs", async (req, res) => {
  try {
    const page = validatePage(req.query.page, 1);
    const limit = validateLimit(req.query.limit, 1, 100);
    const offset = (page - 1) * limit;
    const source_db_engine = validateEnum(
      req.query.source_db_engine,
      ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle", "Python", ""],
      ""
    );
    const target_db_engine = validateEnum(
      req.query.target_db_engine,
      ["PostgreSQL", "MariaDB", "MSSQL", "MongoDB", "Oracle", "Python", ""],
      ""
    );
    const active =
      req.query.active !== undefined ? validateBoolean(req.query.active) : "";
    const enabled =
      req.query.enabled !== undefined ? validateBoolean(req.query.enabled) : "";
    const search = sanitizeSearch(req.query.search, 100);

    let whereConditions = [];
    let queryParams = [];
    let paramCount = 1;

    if (source_db_engine) {
      whereConditions.push(`source_db_engine = $${paramCount}`);
      queryParams.push(source_db_engine);
      paramCount++;
    }
    if (target_db_engine) {
      whereConditions.push(`target_db_engine = $${paramCount}`);
      queryParams.push(target_db_engine);
      paramCount++;
    }
    if (active !== "") {
      whereConditions.push(`active = $${paramCount}`);
      queryParams.push(active === "true");
      paramCount++;
    }
    if (enabled !== "") {
      whereConditions.push(`enabled = $${paramCount}`);
      queryParams.push(enabled === "true");
      paramCount++;
    }
    if (search) {
      whereConditions.push(
        `(job_name ILIKE $${paramCount} OR description ILIKE $${paramCount} OR target_schema ILIKE $${paramCount} OR target_table ILIKE $${paramCount})`
      );
      queryParams.push(`%${search}%`);
      paramCount++;
    }

    const whereClause =
      whereConditions.length > 0
        ? `WHERE ${whereConditions.join(" AND ")}`
        : "";

    const countQuery = `SELECT COUNT(*) FROM metadata.custom_jobs ${whereClause}`;
    const countResult = await pool.query(countQuery, queryParams);
    const total = parseInt(countResult.rows[0].count);

    const dataQuery = `
      SELECT *
      FROM metadata.custom_jobs
      ${whereClause}
      ORDER BY job_name
      LIMIT $${paramCount} OFFSET $${paramCount + 1}
    `;
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
    console.error("Error fetching custom jobs:", err);
    res.status(500).json({
      error: "Error al obtener jobs personalizados",
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.post("/api/custom-jobs/:jobName/execute", async (req, res) => {
  try {
    const jobName = validateIdentifier(req.params.jobName);
    if (!jobName) {
      return res.status(400).json({ error: "Invalid jobName" });
    }

    const jobCheck = await pool.query(
      "SELECT job_name, active, enabled FROM metadata.custom_jobs WHERE job_name = $1",
      [jobName]
    );

    if (jobCheck.rows.length === 0) {
      return res.status(404).json({
        error: "Job not found",
        job_name: jobName,
      });
    }

    const job = jobCheck.rows[0];
    if (!job.active || !job.enabled) {
      return res.status(400).json({
        error: "Job is not active or enabled",
        job_name: jobName,
        active: job.active,
        enabled: job.enabled,
      });
    }

    const executeMetadata = {
      execute_now: true,
      execute_timestamp: new Date().toISOString(),
    };

    await pool.query(
      `UPDATE metadata.custom_jobs 
       SET metadata = COALESCE(metadata, '{}'::jsonb) || $1::jsonb
       WHERE job_name = $2`,
      [JSON.stringify(executeMetadata), jobName]
    );

    const processLogId = await pool.query(
      `INSERT INTO metadata.process_log 
       (process_type, process_name, status, start_time, end_time, total_rows_processed, error_message, metadata)
       VALUES ($1, $2, $3, NOW(), NOW(), 0, '', $4::jsonb)
       RETURNING id`,
      [
        "CUSTOM_JOB",
        jobName,
        "PENDING",
        JSON.stringify({ triggered_by: "api", job_name: jobName }),
      ]
    );

    res.json({
      message: "Job execution queued. DataSync will execute it shortly.",
      job_name: jobName,
      process_log_id: processLogId.rows[0].id,
    });
  } catch (err) {
    console.error("Error executing custom job:", err);
    const safeError = sanitizeError(
      err,
      "Error al ejecutar job personalizado",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.get("/api/custom-jobs/:jobName/results", async (req, res) => {
  try {
    const jobName = validateIdentifier(req.params.jobName);
    if (!jobName) {
      return res.status(400).json({ error: "Invalid jobName" });
    }
    const result = await pool.query(
      `SELECT * FROM metadata.job_results 
       WHERE job_name = $1 
       ORDER BY created_at DESC 
       LIMIT 10`,
      [jobName]
    );
    res.json(result.rows);
  } catch (err) {
    console.error("Error fetching job results:", err);
    res.status(500).json({
      error: "Error al obtener resultados del job",
      error: sanitizeError(
        err,
        "Error en el servidor",
        process.env.NODE_ENV === "production"
      ),
    });
  }
});

app.get("/api/custom-jobs/:jobName/history", async (req, res) => {
  try {
    const jobName = validateIdentifier(req.params.jobName);
    if (!jobName) {
      return res.status(400).json({ error: "Invalid jobName" });
    }
    const result = await pool.query(
      `SELECT * FROM metadata.process_log 
       WHERE process_type = 'CUSTOM_JOB' AND process_name = $1 
       ORDER BY created_at DESC 
       LIMIT 50`,
      [jobName]
    );
    res.json(result.rows);
  } catch (err) {
    console.error("Error fetching job history:", err);
    const safeError = sanitizeError(
      err,
      "Error al obtener historial del job",
      process.env.NODE_ENV === "production"
    );
    res.status(500).json({ error: safeError });
  }
});

app.patch(
  "/api/custom-jobs/:jobName/active",
  requireAuth,
  requireRole("admin", "user"),
  async (req, res) => {
    try {
      const jobName = validateIdentifier(req.params.jobName);
      if (!jobName) {
        return res.status(400).json({ error: "Invalid jobName" });
      }
      const active = validateBoolean(req.body.active);
      const result = await pool.query(
        `UPDATE metadata.custom_jobs 
       SET active = $1, updated_at = NOW()
       WHERE job_name = $2
       RETURNING *`,
        [active, jobName]
      );
      if (result.rows.length === 0) {
        return res.status(404).json({ error: "Job not found" });
      }
      res.json(result.rows[0]);
    } catch (err) {
      console.error("Error updating job active status:", err);
      res.status(500).json({
        error: "Error al actualizar estado del job",
        error: sanitizeError(
          err,
          "Error en el servidor",
          process.env.NODE_ENV === "production"
        ),
      });
    }
  }
);

app.delete(
  "/api/custom-jobs/:jobName",
  requireAuth,
  requireRole("admin"),
  async (req, res) => {
    try {
      const jobName = validateIdentifier(req.params.jobName);
      if (!jobName) {
        return res.status(400).json({ error: "Invalid jobName" });
      }
      const result = await pool.query(
        `DELETE FROM metadata.custom_jobs 
       WHERE job_name = $1
       RETURNING *`,
        [jobName]
      );
      if (result.rows.length === 0) {
        return res.status(404).json({ error: "Job not found" });
      }
      res.json({ message: "Job deleted successfully", job: result.rows[0] });
    } catch (err) {
      console.error("Error deleting custom job:", err);
      res.status(500).json({
        error: "Error al eliminar job personalizado",
        error: sanitizeError(
          err,
          "Error en el servidor",
          process.env.NODE_ENV === "production"
        ),
      });
    }
  }
);

// Export app for testing
export default app;

// Only start server if not in test environment
if (process.env.NODE_ENV !== "test") {
  app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}
