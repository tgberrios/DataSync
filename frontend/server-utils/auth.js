import jwt from "jsonwebtoken";
import bcrypt from "bcryptjs";
import { Pool } from "pg";
import path from "path";
import fs from "fs";

function loadConfig() {
  try {
    const configPath = path.join(process.cwd(), "..", "config.json");
    const configData = fs.readFileSync(configPath, "utf8");
    return JSON.parse(configData);
  } catch (error) {
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
const pool = new Pool({
  host: config.database.postgres.host,
  port: config.database.postgres.port,
  database: config.database.postgres.database,
  user: config.database.postgres.user,
  password: config.database.postgres.password,
});

const JWT_SECRET =
  process.env.JWT_SECRET || "datasync-secret-key-change-in-production";
const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN || "24h";

async function initializeUsersTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS metadata.users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(100) UNIQUE NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        role VARCHAR(50) NOT NULL DEFAULT 'user' CHECK (role IN ('admin', 'user', 'viewer')),
        active BOOLEAN NOT NULL DEFAULT true,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        last_login TIMESTAMP
      )
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_users_username ON metadata.users(username);
      CREATE INDEX IF NOT EXISTS idx_users_email ON metadata.users(email);
      CREATE INDEX IF NOT EXISTS idx_users_active ON metadata.users(active);
    `);

    const existingAdmin = await pool.query(
      "SELECT id FROM metadata.users WHERE username = $1",
      ["admin"]
    );

    if (existingAdmin.rows.length === 0) {
      const defaultPassword = process.env.DEFAULT_ADMIN_PASSWORD || "admin123";
      const passwordHash = await bcrypt.hash(defaultPassword, 10);

      await pool.query(
        `INSERT INTO metadata.users (username, email, password_hash, role, active)
         VALUES ($1, $2, $3, $4, $5)`,
        ["admin", "admin@datasync.local", passwordHash, "admin", true]
      );
      console.log("Default admin user created: admin / " + defaultPassword);
    }
  } catch (error) {
    console.error("Error initializing users table:", error);
    throw error;
  }
}

async function authenticateUser(username, password) {
  try {
    const result = await pool.query(
      "SELECT id, username, email, password_hash, role, active FROM metadata.users WHERE username = $1 OR email = $1",
      [username]
    );

    if (result.rows.length === 0) {
      return { success: false, error: "Invalid credentials" };
    }

    const user = result.rows[0];

    if (!user.active) {
      return { success: false, error: "User account is inactive" };
    }

    const passwordMatch = await bcrypt.compare(password, user.password_hash);

    if (!passwordMatch) {
      return { success: false, error: "Invalid credentials" };
    }

    await pool.query(
      "UPDATE metadata.users SET last_login = NOW() WHERE id = $1",
      [user.id]
    );

    const token = jwt.sign(
      {
        userId: user.id,
        username: user.username,
        role: user.role,
      },
      JWT_SECRET,
      { expiresIn: JWT_EXPIRES_IN }
    );

    return {
      success: true,
      user: {
        id: user.id,
        username: user.username,
        email: user.email,
        role: user.role,
      },
      token,
    };
  } catch (error) {
    console.error("Error authenticating user:", error);
    return { success: false, error: "Authentication failed" };
  }
}

function verifyToken(token) {
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    return { success: true, user: decoded };
  } catch (error) {
    return { success: false, error: "Invalid or expired token" };
  }
}

function requireAuth(req, res, next) {
  const authHeader = req.headers.authorization;

  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Authentication required" });
  }

  const token = authHeader.substring(7);
  const verification = verifyToken(token);

  if (!verification.success) {
    return res.status(401).json({ error: verification.error });
  }

  req.user = verification.user;
  next();
}

function requireRole(...allowedRoles) {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({ error: "Authentication required" });
    }

    if (!allowedRoles.includes(req.user.role)) {
      return res.status(403).json({ error: "Insufficient permissions" });
    }

    next();
  };
}

async function createUser(username, email, password, role = "user") {
  try {
    if (!["admin", "user", "viewer"].includes(role)) {
      return { success: false, error: "Invalid role" };
    }

    const existingUser = await pool.query(
      "SELECT id FROM metadata.users WHERE username = $1 OR email = $2",
      [username, email]
    );

    if (existingUser.rows.length > 0) {
      return { success: false, error: "User already exists" };
    }

    const passwordHash = await bcrypt.hash(password, 10);

    const result = await pool.query(
      `INSERT INTO metadata.users (username, email, password_hash, role, active)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, username, email, role, active, created_at`,
      [username, email, passwordHash, role, true]
    );

    return {
      success: true,
      user: result.rows[0],
    };
  } catch (error) {
    console.error("Error creating user:", error);
    return { success: false, error: "Failed to create user" };
  }
}

async function changePassword(userId, oldPassword, newPassword) {
  try {
    const result = await pool.query(
      "SELECT password_hash FROM metadata.users WHERE id = $1",
      [userId]
    );

    if (result.rows.length === 0) {
      return { success: false, error: "User not found" };
    }

    const passwordMatch = await bcrypt.compare(
      oldPassword,
      result.rows[0].password_hash
    );

    if (!passwordMatch) {
      return { success: false, error: "Current password is incorrect" };
    }

    const newPasswordHash = await bcrypt.hash(newPassword, 10);

    await pool.query(
      "UPDATE metadata.users SET password_hash = $1, updated_at = NOW() WHERE id = $2",
      [newPasswordHash, userId]
    );

    return { success: true };
  } catch (error) {
    console.error("Error changing password:", error);
    return { success: false, error: "Failed to change password" };
  }
}

export {
  initializeUsersTable,
  authenticateUser,
  verifyToken,
  requireAuth,
  requireRole,
  createUser,
  changePassword,
};
