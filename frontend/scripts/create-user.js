import bcrypt from "bcryptjs";
import pkg from "pg";
const { Pool } = pkg;
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

async function createUser() {
  const username = "tomy.berrios";
  const email = "tomy.berrios@datasync.local";
  const password = "Yucaquemada1";
  const role = "admin";

  try {
    // Primero crear la tabla si no existe
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

    console.log("✅ Tabla metadata.users verificada/creada");

    // Verificar si el usuario ya existe
    const existingUser = await pool.query(
      "SELECT id FROM metadata.users WHERE username = $1",
      [username]
    );

    if (existingUser.rows.length > 0) {
      console.log(`Usuario ${username} ya existe. Actualizando contraseña...`);

      const passwordHash = await bcrypt.hash(password, 10);
      await pool.query(
        `UPDATE metadata.users 
         SET password_hash = $1, email = $2, role = $3, updated_at = NOW()
         WHERE username = $4`,
        [passwordHash, email, role, username]
      );
      console.log(`✅ Usuario ${username} actualizado exitosamente`);
    } else {
      console.log(`Creando usuario ${username}...`);

      const passwordHash = await bcrypt.hash(password, 10);
      const result = await pool.query(
        `INSERT INTO metadata.users (username, email, password_hash, role, active)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id, username, email, role`,
        [username, email, passwordHash, role, true]
      );

      console.log(`✅ Usuario creado exitosamente:`);
      console.log(`   Username: ${result.rows[0].username}`);
      console.log(`   Email: ${result.rows[0].email}`);
      console.log(`   Role: ${result.rows[0].role}`);
    }
  } catch (error) {
    console.error("❌ Error:", error.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

createUser();
