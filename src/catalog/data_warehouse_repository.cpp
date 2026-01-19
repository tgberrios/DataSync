#include "catalog/data_warehouse_repository.h"
#include "core/logger.h"
#include <algorithm>
#include <stdexcept>

DataWarehouseRepository::DataWarehouseRepository(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

void DataWarehouseRepository::createTables() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec("CREATE SCHEMA IF NOT EXISTS metadata");

    txn.exec("CREATE TABLE IF NOT EXISTS metadata.data_warehouse_catalog ("
             "id SERIAL PRIMARY KEY,"
             "warehouse_name VARCHAR(255) UNIQUE NOT NULL,"
             "description TEXT,"
             "schema_type VARCHAR(20) NOT NULL CHECK (schema_type IN "
             "('STAR_SCHEMA', 'SNOWFLAKE_SCHEMA')),"
             "target_layer VARCHAR(20) DEFAULT 'BRONZE' CHECK (target_layer IN "
             "('BRONZE', 'SILVER', 'GOLD')),"
             "source_db_engine VARCHAR(50) NOT NULL,"
             "source_connection_string TEXT NOT NULL,"
             "target_db_engine VARCHAR(50) NOT NULL,"
             "target_connection_string TEXT NOT NULL,"
             "target_schema VARCHAR(100) NOT NULL,"
             "dimensions JSONB NOT NULL,"
             "facts JSONB NOT NULL,"
             "schedule_cron VARCHAR(100),"
             "active BOOLEAN DEFAULT true,"
             "enabled BOOLEAN DEFAULT true,"
             "metadata JSONB,"
             "created_at TIMESTAMP DEFAULT NOW(),"
             "updated_at TIMESTAMP DEFAULT NOW(),"
             "last_build_time TIMESTAMP,"
             "last_build_status VARCHAR(50),"
             "notes TEXT,"
             "CONSTRAINT chk_target_engine CHECK (target_db_engine IN "
             "('PostgreSQL', 'Snowflake', 'BigQuery', 'Redshift'))"
             ")");

    txn.exec("CREATE INDEX IF NOT EXISTS idx_warehouse_active ON "
             "metadata.data_warehouse_catalog(active)");
    txn.exec("CREATE INDEX IF NOT EXISTS idx_warehouse_enabled ON "
             "metadata.data_warehouse_catalog(enabled)");
    txn.exec("CREATE INDEX IF NOT EXISTS idx_warehouse_status ON "
             "metadata.data_warehouse_catalog(last_build_status)");

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "createTables",
                  "Error creating data warehouse tables: " +
                      std::string(e.what()));
    throw;
  }
}

std::vector<DataWarehouseModel> DataWarehouseRepository::getAllWarehouses() {
  std::vector<DataWarehouseModel> warehouses;
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);
    auto result = txn.exec("SELECT * FROM metadata.data_warehouse_catalog "
                           "ORDER BY warehouse_name");

    for (const auto &row : result) {
      warehouses.push_back(rowToWarehouse(row));
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getAllWarehouses",
                  "Error getting warehouses: " + std::string(e.what()));
    throw;
  }
  return warehouses;
}

std::vector<DataWarehouseModel> DataWarehouseRepository::getActiveWarehouses() {
  std::vector<DataWarehouseModel> warehouses;
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);
    auto result = txn.exec("SELECT * FROM metadata.data_warehouse_catalog "
                           "WHERE active = true AND enabled = true "
                           "ORDER BY warehouse_name");

    for (const auto &row : result) {
      warehouses.push_back(rowToWarehouse(row));
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getActiveWarehouses",
                  "Error getting active warehouses: " + std::string(e.what()));
    throw;
  }
  return warehouses;
}

DataWarehouseModel
DataWarehouseRepository::getWarehouse(const std::string &warehouseName) {
  DataWarehouseModel warehouse;
  warehouse.warehouse_name = "";
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);
    auto result =
        txn.exec_params("SELECT * FROM metadata.data_warehouse_catalog WHERE "
                        "warehouse_name = $1",
                        warehouseName);

    if (!result.empty()) {
      warehouse = rowToWarehouse(result[0]);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getWarehouse",
                  "Error getting warehouse: " + std::string(e.what()));
    throw;
  }
  return warehouse;
}

void DataWarehouseRepository::insertOrUpdateWarehouse(
    const DataWarehouseModel &warehouse) {
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);

    json dimensionsJson = json::array();
    for (const auto &dim : warehouse.dimensions) {
      json dimJson;
      dimJson["dimension_name"] = dim.dimension_name;
      dimJson["target_schema"] = dim.target_schema;
      dimJson["target_table"] = dim.target_table;
      dimJson["scd_type"] = dimensionTypeToString(dim.scd_type);
      dimJson["source_query"] = dim.source_query;
      dimJson["business_keys"] = dim.business_keys;
      dimJson["valid_from_column"] = dim.valid_from_column;
      dimJson["valid_to_column"] = dim.valid_to_column;
      dimJson["is_current_column"] = dim.is_current_column;
      dimJson["index_columns"] = dim.index_columns;
      dimJson["partition_column"] = dim.partition_column;
      dimensionsJson.push_back(dimJson);
    }

    json factsJson = json::array();
    for (const auto &fact : warehouse.facts) {
      json factJson;
      factJson["fact_name"] = fact.fact_name;
      factJson["target_schema"] = fact.target_schema;
      factJson["target_table"] = fact.target_table;
      factJson["source_query"] = fact.source_query;
      factJson["dimension_keys"] = fact.dimension_keys;
      factJson["measures"] = fact.measures;
      factJson["index_columns"] = fact.index_columns;
      factJson["partition_column"] = fact.partition_column;
      factsJson.push_back(factJson);
    }

    std::string dimensionsStr = dimensionsJson.dump();
    std::string factsStr = factsJson.dump();
    std::string metadataStr = warehouse.metadata.dump();
    std::string scheduleCron =
        warehouse.schedule_cron.empty() ? "" : warehouse.schedule_cron;

    auto result =
        txn.exec_params("SELECT id FROM metadata.data_warehouse_catalog WHERE "
                        "warehouse_name = $1",
                        warehouse.warehouse_name);

    if (result.empty()) {
      if (scheduleCron.empty()) {
        txn.exec_params(
            "INSERT INTO metadata.data_warehouse_catalog "
            "(warehouse_name, description, schema_type, target_layer, "
            "source_db_engine, "
            "source_connection_string, target_db_engine, "
            "target_connection_string, "
            "target_schema, dimensions, facts, schedule_cron, active, enabled, "
            "metadata) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, "
            "$11::jsonb, "
            "NULL, $12, $13, $14::jsonb)",
            warehouse.warehouse_name, warehouse.description,
            schemaTypeToString(warehouse.schema_type),
            dataLayerToString(warehouse.target_layer),
            warehouse.source_db_engine, warehouse.source_connection_string,
            warehouse.target_db_engine, warehouse.target_connection_string,
            warehouse.target_schema, dimensionsStr, factsStr, warehouse.active,
            warehouse.enabled, metadataStr);
      } else {
        txn.exec_params(
            "INSERT INTO metadata.data_warehouse_catalog "
            "(warehouse_name, description, schema_type, target_layer, "
            "source_db_engine, "
            "source_connection_string, target_db_engine, "
            "target_connection_string, "
            "target_schema, dimensions, facts, schedule_cron, active, enabled, "
            "metadata) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, "
            "$11::jsonb, "
            "$12, $13, $14, $15::jsonb)",
            warehouse.warehouse_name, warehouse.description,
            schemaTypeToString(warehouse.schema_type),
            dataLayerToString(warehouse.target_layer),
            warehouse.source_db_engine, warehouse.source_connection_string,
            warehouse.target_db_engine, warehouse.target_connection_string,
            warehouse.target_schema, dimensionsStr, factsStr, scheduleCron,
            warehouse.active, warehouse.enabled, metadataStr);
      }
    } else {
      int id = result[0][0].as<int>();
      if (scheduleCron.empty()) {
        txn.exec_params(
            "UPDATE metadata.data_warehouse_catalog SET "
            "description = $2, schema_type = $3, target_layer = $4, "
            "source_db_engine = $5, "
            "source_connection_string = $6, target_db_engine = $7, "
            "target_connection_string = $8, target_schema = $9, dimensions = "
            "$10::jsonb, "
            "facts = $11::jsonb, schedule_cron = NULL, active = $12, enabled = "
            "$13, "
            "metadata = $14::jsonb, updated_at = NOW() WHERE id = $1",
            id, warehouse.description,
            schemaTypeToString(warehouse.schema_type),
            dataLayerToString(warehouse.target_layer),
            warehouse.source_db_engine, warehouse.source_connection_string,
            warehouse.target_db_engine, warehouse.target_connection_string,
            warehouse.target_schema, dimensionsStr, factsStr, warehouse.active,
            warehouse.enabled, metadataStr);
      } else {
        txn.exec_params(
            "UPDATE metadata.data_warehouse_catalog SET "
            "description = $2, schema_type = $3, target_layer = $4, "
            "source_db_engine = $5, "
            "source_connection_string = $6, target_db_engine = $7, "
            "target_connection_string = $8, target_schema = $9, dimensions = "
            "$10::jsonb, "
            "facts = $11::jsonb, schedule_cron = $12, active = $13, enabled = "
            "$14, "
            "metadata = $15::jsonb, updated_at = NOW() WHERE id = $1",
            id, warehouse.description,
            schemaTypeToString(warehouse.schema_type),
            dataLayerToString(warehouse.target_layer),
            warehouse.source_db_engine, warehouse.source_connection_string,
            warehouse.target_db_engine, warehouse.target_connection_string,
            warehouse.target_schema, dimensionsStr, factsStr, scheduleCron,
            warehouse.active, warehouse.enabled, metadataStr);
      }
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateWarehouse",
                  "Error inserting/updating warehouse: " +
                      std::string(e.what()));
    throw;
  }
}

void DataWarehouseRepository::deleteWarehouse(
    const std::string &warehouseName) {
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params(
        "DELETE FROM metadata.data_warehouse_catalog WHERE warehouse_name = $1",
        warehouseName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "deleteWarehouse",
                  "Error deleting warehouse: " + std::string(e.what()));
    throw;
  }
}

void DataWarehouseRepository::updateWarehouseActive(
    const std::string &warehouseName, bool active) {
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params("UPDATE metadata.data_warehouse_catalog SET active = $1, "
                    "updated_at = NOW() "
                    "WHERE warehouse_name = $2",
                    active, warehouseName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateWarehouseActive",
                  "Error updating warehouse active status: " +
                      std::string(e.what()));
    throw;
  }
}

void DataWarehouseRepository::updateBuildStatus(
    const std::string &warehouseName, const std::string &status,
    const std::string &buildTime, const std::string &notes) {
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);
    if (notes.empty()) {
      txn.exec_params(
          "UPDATE metadata.data_warehouse_catalog SET last_build_status = $1, "
          "last_build_time = $2, updated_at = NOW() WHERE warehouse_name = $3",
          status, buildTime, warehouseName);
    } else {
      txn.exec_params(
          "UPDATE metadata.data_warehouse_catalog SET last_build_status = $1, "
          "last_build_time = $2, notes = $3, updated_at = NOW() WHERE "
          "warehouse_name = $4",
          status, buildTime, notes, warehouseName);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateBuildStatus",
                  "Error updating build status: " + std::string(e.what()));
    throw;
  }
}

pqxx::connection DataWarehouseRepository::getConnection() {
  return pqxx::connection(connectionString_);
}

DataWarehouseModel
DataWarehouseRepository::rowToWarehouse(const pqxx::row &row) {
  DataWarehouseModel warehouse;
  warehouse.id = row["id"].as<int>();
  warehouse.warehouse_name = row["warehouse_name"].as<std::string>();
  warehouse.description =
      row["description"].is_null() ? "" : row["description"].as<std::string>();
  warehouse.schema_type =
      stringToSchemaType(row["schema_type"].as<std::string>());
  if (row["target_layer"].is_null()) {
    warehouse.target_layer = DataLayer::BRONZE;
  } else {
    warehouse.target_layer =
        stringToDataLayer(row["target_layer"].as<std::string>());
  }
  warehouse.source_db_engine = row["source_db_engine"].as<std::string>();
  warehouse.source_connection_string =
      row["source_connection_string"].as<std::string>();
  warehouse.target_db_engine = row["target_db_engine"].as<std::string>();
  warehouse.target_connection_string =
      row["target_connection_string"].as<std::string>();
  warehouse.target_schema = row["target_schema"].as<std::string>();
  warehouse.schedule_cron = row["schedule_cron"].is_null()
                                ? ""
                                : row["schedule_cron"].as<std::string>();
  warehouse.active = row["active"].as<bool>();
  warehouse.enabled = row["enabled"].as<bool>();
  warehouse.created_at =
      row["created_at"].is_null() ? "" : row["created_at"].as<std::string>();
  warehouse.updated_at =
      row["updated_at"].is_null() ? "" : row["updated_at"].as<std::string>();
  warehouse.last_build_time = row["last_build_time"].is_null()
                                  ? ""
                                  : row["last_build_time"].as<std::string>();
  warehouse.last_build_status =
      row["last_build_status"].is_null()
          ? ""
          : row["last_build_status"].as<std::string>();
  warehouse.notes =
      row["notes"].is_null() ? "" : row["notes"].as<std::string>();

  if (!row["metadata"].is_null()) {
    warehouse.metadata = parseJsonField(row["metadata"].as<std::string>());
  }

  if (!row["dimensions"].is_null()) {
    json dimsJson = parseJsonField(row["dimensions"].as<std::string>());
    for (const auto &dimJson : dimsJson) {
      DimensionTable dim;
      dim.dimension_name = dimJson["dimension_name"];
      dim.target_schema = dimJson["target_schema"];
      dim.target_table = dimJson["target_table"];
      dim.scd_type = stringToDimensionType(dimJson["scd_type"]);
      dim.source_query = dimJson["source_query"];
      dim.business_keys =
          dimJson["business_keys"].get<std::vector<std::string>>();
      dim.valid_from_column = dimJson["valid_from_column"];
      dim.valid_to_column = dimJson["valid_to_column"];
      dim.is_current_column = dimJson["is_current_column"];
      if (dimJson.contains("index_columns")) {
        dim.index_columns =
            dimJson["index_columns"].get<std::vector<std::string>>();
      }
      if (dimJson.contains("partition_column")) {
        dim.partition_column = dimJson["partition_column"];
      }
      warehouse.dimensions.push_back(dim);
    }
  }

  if (!row["facts"].is_null()) {
    json factsJson = parseJsonField(row["facts"].as<std::string>());
    for (const auto &factJson : factsJson) {
      FactTable fact;
      fact.fact_name = factJson["fact_name"];
      fact.target_schema = factJson["target_schema"];
      fact.target_table = factJson["target_table"];
      fact.source_query = factJson["source_query"];
      fact.dimension_keys =
          factJson["dimension_keys"].get<std::vector<std::string>>();
      fact.measures = factJson["measures"].get<std::vector<std::string>>();
      if (factJson.contains("index_columns")) {
        fact.index_columns =
            factJson["index_columns"].get<std::vector<std::string>>();
      }
      if (factJson.contains("partition_column")) {
        fact.partition_column = factJson["partition_column"];
      }
      warehouse.facts.push_back(fact);
    }
  }

  return warehouse;
}

json DataWarehouseRepository::parseJsonField(const std::string &jsonStr) {
  try {
    return json::parse(jsonStr);
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::DATABASE, "parseJsonField",
                    "Error parsing JSON: " + std::string(e.what()));
    return json::object();
  }
}

std::string DataWarehouseRepository::schemaTypeToString(SchemaType type) {
  switch (type) {
  case SchemaType::STAR_SCHEMA:
    return "STAR_SCHEMA";
  case SchemaType::SNOWFLAKE_SCHEMA:
    return "SNOWFLAKE_SCHEMA";
  default:
    return "STAR_SCHEMA";
  }
}

SchemaType DataWarehouseRepository::stringToSchemaType(const std::string &str) {
  if (str == "SNOWFLAKE_SCHEMA") {
    return SchemaType::SNOWFLAKE_SCHEMA;
  }
  return SchemaType::STAR_SCHEMA;
}

std::string DataWarehouseRepository::dimensionTypeToString(DimensionType type) {
  switch (type) {
  case DimensionType::TYPE_1:
    return "TYPE_1";
  case DimensionType::TYPE_2:
    return "TYPE_2";
  case DimensionType::TYPE_3:
    return "TYPE_3";
  default:
    return "TYPE_1";
  }
}

DimensionType
DataWarehouseRepository::stringToDimensionType(const std::string &str) {
  if (str == "TYPE_2") {
    return DimensionType::TYPE_2;
  } else if (str == "TYPE_3") {
    return DimensionType::TYPE_3;
  }
  return DimensionType::TYPE_1;
}

std::string DataWarehouseRepository::dataLayerToString(DataLayer layer) {
  switch (layer) {
  case DataLayer::BRONZE:
    return "BRONZE";
  case DataLayer::SILVER:
    return "SILVER";
  case DataLayer::GOLD:
    return "GOLD";
  default:
    return "BRONZE";
  }
}

DataLayer DataWarehouseRepository::stringToDataLayer(const std::string &str) {
  if (str == "SILVER") {
    return DataLayer::SILVER;
  } else if (str == "GOLD") {
    return DataLayer::GOLD;
  }
  return DataLayer::BRONZE;
}
