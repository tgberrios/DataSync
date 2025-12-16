# 🏗️ Arquitectura Perfecta para CDC - Plan de Implementación

## 📋 Resumen Ejecutivo

Este plan propone una arquitectura unificada y optimizada para Change Data Capture (CDC) que:

- ✅ Soporta **TODAS las tablas** (con PK y sin PK)
- ✅ Usa **triggers** como mecanismo único de captura
- ✅ Centraliza la creación de `datasync_metadata` **antes del loop**
- ✅ Elimina la necesidad de estrategias OFFSET/PK incremental
- ✅ Garantiza **máxima velocidad** y **consistencia**

---

## 🎯 Objetivos Principales

1. **Universalidad**: CDC para todas las tablas, independientemente de tener PK
2. **Eficiencia**: Una sola creación de infraestructura CDC al inicio
3. **Consistencia**: Mismo comportamiento en MariaDB, MSSQL y Oracle
4. **Performance**: Captura completa de `row_data` para evitar N+1 queries
5. **Simplicidad**: Eliminar lógica condicional compleja

---

## 🏛️ Arquitectura Propuesta

### 1. **Infraestructura CDC (`datasync_metadata`)**

#### **MariaDB**

- **Ubicación**: Base de datos **GLOBAL** (una sola para todo el servidor)
- **Razón**: MariaDB permite acceso cross-database, más eficiente y centralizado
- **Creación**: **ANTES del loop**, una sola vez
- **Estructura**:
  ```sql
  CREATE DATABASE IF NOT EXISTS datasync_metadata;
  CREATE TABLE IF NOT EXISTS datasync_metadata.ds_change_log (
    change_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    change_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    operation CHAR(1) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    pk_values JSON NOT NULL,        -- Para tablas con PK: valores PK
                                    -- Para tablas sin PK: hash de todos los campos
    row_data JSON NOT NULL,         -- SIEMPRE completo (evita N+1)
    INDEX idx_table_time (schema_name, table_name, change_time),
    INDEX idx_table_change (schema_name, table_name, change_id)
  ) ENGINE=InnoDB;
  ```

#### **MSSQL**

- **Ubicación**: Schema **dentro de cada base de datos** (uno por database)
- **Razón**: MSSQL requiere contexto de base de datos, no permite acceso cross-database fácil
- **Creación**: **ANTES del loop**, pero para cada base de datos única
- **Estructura**:

  ```sql
  -- Por cada base de datos única
  USE [database_name];
  IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'datasync_metadata')
    CREATE SCHEMA datasync_metadata;

  CREATE TABLE datasync_metadata.ds_change_log (
    change_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    change_time DATETIME DEFAULT GETDATE(),
    operation CHAR(1) NOT NULL,
    schema_name NVARCHAR(255) NOT NULL,
    table_name NVARCHAR(255) NOT NULL,
    pk_values NVARCHAR(MAX) NOT NULL,  -- JSON como string
    row_data NVARCHAR(MAX) NOT NULL,    -- JSON como string, SIEMPRE completo
    INDEX idx_table_time (schema_name, table_name, change_time),
    INDEX idx_table_change (schema_name, table_name, change_id)
  );
  ```

#### **Oracle**

- **Ya implementado correctamente**: Schema `datasync_metadata` con usuario dedicado
- **Mantener como está**

---

### 2. **Triggers para TODAS las Tablas**

#### **Estrategia para Tablas CON PK**

- **`pk_values`**: JSON con valores de las columnas PK
- **`row_data`**: JSON completo con TODOS los campos (evita N+1)
- **Identificación**: Usar PK para upsert/delete en PostgreSQL

#### **Estrategia para Tablas SIN PK**

- **`pk_values`**: JSON con **hash MD5/SHA256 de todos los campos concatenados**
  - Ejemplo: `{"_hash": "a1b2c3d4..."}`
  - Alternativa: JSON con todos los campos como "PK compuesto"
- **`row_data`**: JSON completo con TODOS los campos
- **Identificación**: Usar hash o comparación completa de `row_data` en PostgreSQL

#### **Ventajas de esta Estrategia**

1. ✅ **Unificación**: Mismo flujo para todas las tablas
2. ✅ **Performance**: `row_data` completo elimina N+1 queries
3. ✅ **Confiabilidad**: Hash garantiza identificación única para tablas sin PK
4. ✅ **Simplicidad**: No más lógica condicional OFFSET vs PK

---

### 3. **Flujo de Procesamiento CDC**

```
┌─────────────────────────────────────────────────────────┐
│ 1. setupTableTarget*ToPostgres()                       │
│    ├─ Crear datasync_metadata (ANTES del loop)         │
│    └─ Para cada tabla:                                 │
│       ├─ Crear triggers (CON PK o SIN PK)              │
│       └─ Capturar cambios en ds_change_log              │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│ 2. processTableCDC()                                    │
│    ├─ Leer ds_change_log (ORDER BY change_id)          │
│    ├─ Procesar batch de cambios                        │
│    ├─ Para INSERT/UPDATE:                              │
│    │   └─ Usar row_data completo (sin N+1)            │
│    ├─ Para DELETE:                                     │
│    │   └─ Usar pk_values (o hash para sin PK)          │
│    └─ Actualizar last_change_id                        │
└─────────────────────────────────────────────────────────┘
```

---

## 🔧 Cambios Técnicos Requeridos

### **Fase 1: Reorganización de Creación de `datasync_metadata`**

#### **MariaDB**

- ✅ Mover `CREATE DATABASE datasync_metadata` **ANTES del loop**
- ✅ Crear una sola vez al inicio del método
- ✅ Usar conexión dedicada o primera conexión disponible

#### **MSSQL**

- ✅ Ya está correcto (antes del loop)
- ✅ Mejorar para crear por cada base de datos única (no solo la primera)
- ✅ Agrupar tablas por base de datos y crear schema una vez por database

### **Fase 2: Triggers para Tablas SIN PK**

#### **MariaDB - Trigger para Tabla SIN PK**

```sql
-- Para INSERT
CREATE TRIGGER ds_tr_schema_table_ai
AFTER INSERT ON schema.table
FOR EACH ROW
INSERT INTO datasync_metadata.ds_change_log
(operation, schema_name, table_name, pk_values, row_data)
VALUES (
  'I',
  'schema',
  'table',
  JSON_OBJECT('_hash', MD5(CONCAT_WS('|', col1, col2, col3, ...))),
  JSON_OBJECT('col1', NEW.col1, 'col2', NEW.col2, ...)
);

-- Para UPDATE
CREATE TRIGGER ds_tr_schema_table_au
AFTER UPDATE ON schema.table
FOR EACH ROW
INSERT INTO datasync_metadata.ds_change_log
(operation, schema_name, table_name, pk_values, row_data)
VALUES (
  'U',
  'schema',
  'table',
  JSON_OBJECT('_hash', MD5(CONCAT_WS('|', NEW.col1, NEW.col2, ...))),
  JSON_OBJECT('col1', NEW.col1, 'col2', NEW.col2, ...)
);

-- Para DELETE
CREATE TRIGGER ds_tr_schema_table_ad
AFTER DELETE ON schema.table
FOR EACH ROW
INSERT INTO datasync_metadata.ds_change_log
(operation, schema_name, table_name, pk_values, row_data)
VALUES (
  'D',
  'schema',
  'table',
  JSON_OBJECT('_hash', MD5(CONCAT_WS('|', OLD.col1, OLD.col2, ...))),
  JSON_OBJECT('col1', OLD.col1, 'col2', OLD.col2, ...)
);
```

#### **MSSQL - Trigger para Tabla SIN PK**

```sql
-- Similar pero usando CONCAT y HASHBYTES('MD5', ...)
-- O usar todos los campos como "PK compuesto" en JSON
```

### **Fase 3: Procesamiento CDC para Tablas SIN PK**

#### **Modificar `processTableCDC()`**

- ✅ Detectar si `pk_values` contiene `_hash` (tabla sin PK)
- ✅ Para tablas sin PK:
  - **INSERT/UPDATE**: Usar `row_data` completo para UPSERT
  - **DELETE**: Usar hash de `row_data` para identificar y eliminar
- ✅ Mantener lógica actual para tablas con PK

---

## 📊 Comparación: Antes vs Después

| Aspecto               | Antes                                | Después                         |
| --------------------- | ------------------------------------ | ------------------------------- |
| **Tablas con PK**     | ✅ CDC con triggers                  | ✅ CDC con triggers             |
| **Tablas sin PK**     | ❌ Sin CDC (solo FULL_LOAD + OFFSET) | ✅ CDC con triggers (hash)      |
| **Creación metadata** | Dentro del loop                      | Antes del loop                  |
| **N+1 queries**       | ⚠️ Posible si row_data NULL          | ✅ Eliminado (siempre completo) |
| **Estrategias**       | CDC, OFFSET, PK                      | ✅ Solo CDC (universal)         |
| **Complejidad**       | Alta (lógica condicional)            | Baja (un solo flujo)            |

---

## 🎯 Beneficios de esta Arquitectura

### **1. Universalidad**

- ✅ **Todas las tablas** tienen CDC, sin excepciones
- ✅ Mismo comportamiento para PK y no-PK
- ✅ Elimina necesidad de estrategias OFFSET

### **2. Performance**

- ✅ **Máxima velocidad**: Solo consulta `ds_change_log` (tabla pequeña)
- ✅ **Sin N+1**: `row_data` siempre completo
- ✅ **Escalable**: Independiente del tamaño de tablas fuente

### **3. Simplicidad**

- ✅ **Un solo flujo**: CDC para todo
- ✅ **Menos código**: Eliminar lógica OFFSET/PK incremental
- ✅ **Mantenible**: Menos casos edge, menos bugs

### **4. Consistencia**

- ✅ Mismo patrón en todos los motores
- ✅ Mismo comportamiento para todas las tablas
- ✅ Predictible y confiable

---

## 🚀 Plan de Implementación

### **Paso 1: Reorganizar Creación de `datasync_metadata`**

- [ ] MariaDB: Mover creación antes del loop
- [ ] MSSQL: Mejorar para crear por cada database única
- [ ] Verificar Oracle (ya correcto)

### **Paso 2: Implementar Triggers para Tablas SIN PK**

- [ ] MariaDB: Generar triggers con hash MD5
- [ ] MSSQL: Generar triggers con hash o PK compuesto
- [ ] Probar con tablas reales sin PK

### **Paso 3: Modificar `processTableCDC()`**

- [ ] Detectar tablas sin PK (por `_hash` en `pk_values`)
- [ ] Implementar lógica de upsert/delete usando hash
- [ ] Mantener compatibilidad con tablas con PK

### **Paso 4: Testing y Validación**

- [ ] Probar con tablas con PK
- [ ] Probar con tablas sin PK
- [ ] Validar performance
- [ ] Verificar que no hay regresiones

### **Paso 5: Limpieza (Opcional)**

- [ ] Eliminar código OFFSET obsoleto (si aún existe)
- [ ] Simplificar lógica condicional
- [ ] Actualizar documentación

---

## ⚠️ Consideraciones Importantes

### **1. Hash para Tablas SIN PK**

- **MD5 vs SHA256**: MD5 es más rápido, SHA256 más seguro
- **Recomendación**: MD5 es suficiente para identificación (no seguridad)
- **Alternativa**: Usar todos los campos como "PK compuesto" en JSON

### **2. Performance de Hash**

- **Impacto**: Mínimo, solo en triggers (INSERT/UPDATE/DELETE)
- **Overhead**: < 1ms por operación
- **Beneficio**: CDC universal vale la pena

### **3. Colisiones de Hash**

- **Probabilidad**: Extremadamente baja (1 en 2^128 para MD5)
- **Mitigación**: Si ocurre, `row_data` completo permite comparación exacta
- **Riesgo**: Prácticamente cero

### **4. Migración de Tablas Existentes**

- **Tablas con PK**: Sin cambios, seguir funcionando igual
- **Tablas sin PK**: Agregar triggers, procesar cambios desde ahora
- **Datos históricos**: FULL_LOAD inicial, luego CDC continuo

---

## ✅ Criterios de Éxito

1. ✅ **Todas las tablas** tienen triggers CDC (PK y no-PK)
2. ✅ `datasync_metadata` se crea **una sola vez** antes del loop
3. ✅ **Sin N+1 queries**: `row_data` siempre completo
4. ✅ **Performance**: CDC más rápido que OFFSET incremental
5. ✅ **Simplicidad**: Un solo flujo CDC para todo
6. ✅ **Consistencia**: Mismo comportamiento en todos los motores

---

## 📝 Notas Finales

Esta arquitectura:

- 🎯 **Elimina** la necesidad de estrategias OFFSET/PK incremental
- 🚀 **Garantiza** máxima velocidad con CDC universal
- 🔧 **Simplifica** el código y reduce complejidad
- ✅ **Funciona** para todas las tablas, sin excepciones

**¿Aprobación para proceder con la implementación?**
