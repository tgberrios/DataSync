#include "catalog/data_vault_repository.h"
#include "core/logger.h"
#include <algorithm>
#include <stdexcept>

DataVaultRepository::DataVaultRepository(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

void DataVaultRepository::createTables() {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    txn.exec("CREATE SCHEMA IF NOT EXISTS metadata");

    txn.exec("CREATE TABLE IF NOT EXISTS metadata.data_vault_catalog ("
             "id SERIAL PRIMARY KEY,"
             "vault_name VARCHAR(255) UNIQUE NOT NULL,"
             "description TEXT,"
             "source_db_engine VARCHAR(50) NOT NULL,"
             "source_connection_string TEXT NOT NULL,"
             "target_db_engine VARCHAR(50) NOT NULL,"
             "target_connection_string TEXT NOT NULL,"
             "target_schema VARCHAR(100) NOT NULL,"
             "hubs JSONB NOT NULL DEFAULT '[]',"
             "links JSONB NOT NULL DEFAULT '[]',"
             "satellites JSONB NOT NULL DEFAULT '[]',"
             "point_in_time_tables JSONB NOT NULL DEFAULT '[]',"
             "bridge_tables JSONB NOT NULL DEFAULT '[]',"
             "schedule_cron VARCHAR(100),"
             "active BOOLEAN DEFAULT true,"
             "enabled BOOLEAN DEFAULT true,"
             "metadata JSONB,"
             "created_at TIMESTAMP DEFAULT NOW(),"
             "updated_at TIMESTAMP DEFAULT NOW(),"
             "last_build_time TIMESTAMP,"
             "last_build_status VARCHAR(50),"
             "CONSTRAINT chk_target_engine CHECK (target_db_engine IN "
             "('PostgreSQL', 'Snowflake', 'BigQuery', 'Redshift'))"
             ")");

    txn.exec("CREATE INDEX IF NOT EXISTS idx_data_vault_active ON "
             "metadata.data_vault_catalog(active)");
    txn.exec("CREATE INDEX IF NOT EXISTS idx_data_vault_enabled ON "
             "metadata.data_vault_catalog(enabled)");
    txn.exec("CREATE INDEX IF NOT EXISTS idx_data_vault_status ON "
             "metadata.data_vault_catalog(last_build_status)");

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "createTables",
                  "Error creating data vault tables: " +
                      std::string(e.what()));
    throw;
  }
}

std::vector<DataVaultModel> DataVaultRepository::getAllVaults() {
  std::vector<DataVaultModel> vaults;
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);
    auto result = txn.exec("SELECT * FROM metadata.data_vault_catalog "
                           "ORDER BY vault_name");

    for (const auto &row : result) {
      vaults.push_back(rowToVault(row));
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getAllVaults",
                  "Error getting vaults: " + std::string(e.what()));
    throw;
  }
  return vaults;
}

std::vector<DataVaultModel> DataVaultRepository::getActiveVaults() {
  std::vector<DataVaultModel> vaults;
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);
    auto result = txn.exec("SELECT * FROM metadata.data_vault_catalog "
                           "WHERE active = true AND enabled = true "
                           "ORDER BY vault_name");

    for (const auto &row : result) {
      vaults.push_back(rowToVault(row));
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getActiveVaults",
                  "Error getting active vaults: " + std::string(e.what()));
    throw;
  }
  return vaults;
}

DataVaultModel DataVaultRepository::getVault(const std::string &vaultName) {
  DataVaultModel vault;
  vault.vault_name = "";
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);
    auto result =
        txn.exec_params("SELECT * FROM metadata.data_vault_catalog WHERE "
                        "vault_name = $1",
                        vaultName);

    if (!result.empty()) {
      vault = rowToVault(result[0]);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getVault",
                  "Error getting vault: " + std::string(e.what()));
    throw;
  }
  return vault;
}

void DataVaultRepository::insertOrUpdateVault(const DataVaultModel &vault) {
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);

    json hubsJson = json::array();
    for (const auto &hub : vault.hubs) {
      json hubJson;
      hubJson["hub_name"] = hub.hub_name;
      hubJson["target_schema"] = hub.target_schema;
      hubJson["target_table"] = hub.target_table;
      hubJson["source_query"] = hub.source_query;
      hubJson["business_keys"] = hub.business_keys;
      hubJson["hub_key_column"] = hub.hub_key_column;
      hubJson["load_date_column"] = hub.load_date_column;
      hubJson["record_source_column"] = hub.record_source_column;
      hubJson["index_columns"] = hub.index_columns;
      hubsJson.push_back(hubJson);
    }

    json linksJson = json::array();
    for (const auto &link : vault.links) {
      json linkJson;
      linkJson["link_name"] = link.link_name;
      linkJson["target_schema"] = link.target_schema;
      linkJson["target_table"] = link.target_table;
      linkJson["source_query"] = link.source_query;
      linkJson["hub_references"] = link.hub_references;
      linkJson["link_key_column"] = link.link_key_column;
      linkJson["load_date_column"] = link.load_date_column;
      linkJson["record_source_column"] = link.record_source_column;
      linkJson["index_columns"] = link.index_columns;
      linksJson.push_back(linkJson);
    }

    json satellitesJson = json::array();
    for (const auto &sat : vault.satellites) {
      json satJson;
      satJson["satellite_name"] = sat.satellite_name;
      satJson["target_schema"] = sat.target_schema;
      satJson["target_table"] = sat.target_table;
      satJson["parent_hub_name"] = sat.parent_hub_name;
      satJson["parent_link_name"] = sat.parent_link_name;
      satJson["source_query"] = sat.source_query;
      satJson["parent_key_column"] = sat.parent_key_column;
      satJson["load_date_column"] = sat.load_date_column;
      satJson["load_end_date_column"] = sat.load_end_date_column;
      satJson["record_source_column"] = sat.record_source_column;
      satJson["descriptive_attributes"] = sat.descriptive_attributes;
      satJson["index_columns"] = sat.index_columns;
      satJson["is_historized"] = sat.is_historized;
      satellitesJson.push_back(satJson);
    }

    json pitJson = json::array();
    for (const auto &pit : vault.point_in_time_tables) {
      json pitTableJson;
      pitTableJson["pit_name"] = pit.pit_name;
      pitTableJson["target_schema"] = pit.target_schema;
      pitTableJson["target_table"] = pit.target_table;
      pitTableJson["hub_name"] = pit.hub_name;
      pitTableJson["satellite_names"] = pit.satellite_names;
      pitTableJson["snapshot_date_column"] = pit.snapshot_date_column;
      pitTableJson["index_columns"] = pit.index_columns;
      pitJson.push_back(pitTableJson);
    }

    json bridgeJson = json::array();
    for (const auto &bridge : vault.bridge_tables) {
      json bridgeTableJson;
      bridgeTableJson["bridge_name"] = bridge.bridge_name;
      bridgeTableJson["target_schema"] = bridge.target_schema;
      bridgeTableJson["target_table"] = bridge.target_table;
      bridgeTableJson["hub_name"] = bridge.hub_name;
      bridgeTableJson["link_names"] = bridge.link_names;
      bridgeTableJson["snapshot_date_column"] = bridge.snapshot_date_column;
      bridgeTableJson["index_columns"] = bridge.index_columns;
      bridgeJson.push_back(bridgeTableJson);
    }

    std::string hubsStr = hubsJson.dump();
    std::string linksStr = linksJson.dump();
    std::string satellitesStr = satellitesJson.dump();
    std::string pitStr = pitJson.dump();
    std::string bridgeStr = bridgeJson.dump();
    std::string metadataStr = vault.metadata.dump();
    std::string scheduleCron = vault.schedule_cron;

    auto result = txn.exec_params(
        "SELECT id FROM metadata.data_vault_catalog WHERE vault_name = $1",
        vault.vault_name);

    if (result.empty()) {
      if (scheduleCron.empty()) {
        txn.exec_params(
            "INSERT INTO metadata.data_vault_catalog "
            "(vault_name, description, source_db_engine, "
            "source_connection_string, target_db_engine, "
            "target_connection_string, target_schema, hubs, links, satellites, "
            "point_in_time_tables, bridge_tables, schedule_cron, active, "
            "enabled, metadata) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, "
            "$10::jsonb, $11::jsonb, $12::jsonb, NULL, $13, $14, $15::jsonb)",
            vault.vault_name, vault.description, vault.source_db_engine,
            vault.source_connection_string, vault.target_db_engine,
            vault.target_connection_string, vault.target_schema, hubsStr,
            linksStr, satellitesStr, pitStr, bridgeStr, vault.active,
            vault.enabled, metadataStr);
      } else {
        txn.exec_params(
            "INSERT INTO metadata.data_vault_catalog "
            "(vault_name, description, source_db_engine, "
            "source_connection_string, target_db_engine, "
            "target_connection_string, target_schema, hubs, links, satellites, "
            "point_in_time_tables, bridge_tables, schedule_cron, active, "
            "enabled, metadata) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, "
            "$10::jsonb, $11::jsonb, $12::jsonb, $13, $14, $15, $16::jsonb)",
            vault.vault_name, vault.description, vault.source_db_engine,
            vault.source_connection_string, vault.target_db_engine,
            vault.target_connection_string, vault.target_schema, hubsStr,
            linksStr, satellitesStr, pitStr, bridgeStr, scheduleCron,
            vault.active, vault.enabled, metadataStr);
      }
    } else {
      int id = result[0][0].as<int>();
      if (scheduleCron.empty()) {
        txn.exec_params(
            "UPDATE metadata.data_vault_catalog SET "
            "description = $2, source_db_engine = $3, "
            "source_connection_string = $4, target_db_engine = $5, "
            "target_connection_string = $6, target_schema = $7, hubs = "
            "$8::jsonb, "
            "links = $9::jsonb, satellites = $10::jsonb, "
            "point_in_time_tables = $11::jsonb, bridge_tables = $12::jsonb, "
            "schedule_cron = NULL, active = $13, enabled = $14, "
            "metadata = $15::jsonb, updated_at = NOW() WHERE id = $1",
            id, vault.description, vault.source_db_engine,
            vault.source_connection_string, vault.target_db_engine,
            vault.target_connection_string, vault.target_schema, hubsStr,
            linksStr, satellitesStr, pitStr, bridgeStr, vault.active,
            vault.enabled, metadataStr);
      } else {
        txn.exec_params(
            "UPDATE metadata.data_vault_catalog SET "
            "description = $2, source_db_engine = $3, "
            "source_connection_string = $4, target_db_engine = $5, "
            "target_connection_string = $6, target_schema = $7, hubs = "
            "$8::jsonb, "
            "links = $9::jsonb, satellites = $10::jsonb, "
            "point_in_time_tables = $11::jsonb, bridge_tables = $12::jsonb, "
            "schedule_cron = $13, active = $14, enabled = $15, "
            "metadata = $16::jsonb, updated_at = NOW() WHERE id = $1",
            id, vault.description, vault.source_db_engine,
            vault.source_connection_string, vault.target_db_engine,
            vault.target_connection_string, vault.target_schema, hubsStr,
            linksStr, satellitesStr, pitStr, bridgeStr, scheduleCron,
            vault.active, vault.enabled, metadataStr);
      }
    }

    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateVault",
                  "Error inserting/updating vault: " +
                      std::string(e.what()));
    throw;
  }
}

void DataVaultRepository::deleteVault(const std::string &vaultName) {
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params(
        "DELETE FROM metadata.data_vault_catalog WHERE vault_name = $1",
        vaultName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "deleteVault",
                  "Error deleting vault: " + std::string(e.what()));
    throw;
  }
}

void DataVaultRepository::updateVaultActive(const std::string &vaultName,
                                              bool active) {
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params("UPDATE metadata.data_vault_catalog SET active = $1, "
                    "updated_at = NOW() "
                    "WHERE vault_name = $2",
                    active, vaultName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateVaultActive",
                  "Error updating vault active status: " +
                      std::string(e.what()));
    throw;
  }
}

void DataVaultRepository::updateBuildStatus(const std::string &vaultName,
                                             const std::string &status,
                                             const std::string &buildTime,
                                             const std::string &notes) {
  try {
    pqxx::connection conn = getConnection();
    pqxx::work txn(conn);
    txn.exec_params(
        "UPDATE metadata.data_vault_catalog SET last_build_status = $1, "
        "last_build_time = $2, updated_at = NOW() WHERE vault_name = $3",
        status, buildTime, vaultName);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateBuildStatus",
                  "Error updating build status: " + std::string(e.what()));
    throw;
  }
}

pqxx::connection DataVaultRepository::getConnection() {
  return pqxx::connection(connectionString_);
}

DataVaultModel DataVaultRepository::rowToVault(const pqxx::row &row) {
  DataVaultModel vault;
  vault.id = row["id"].as<int>();
  vault.vault_name = row["vault_name"].as<std::string>();
  vault.description =
      row["description"].is_null() ? "" : row["description"].as<std::string>();
  vault.source_db_engine = row["source_db_engine"].as<std::string>();
  vault.source_connection_string =
      row["source_connection_string"].as<std::string>();
  vault.target_db_engine = row["target_db_engine"].as<std::string>();
  vault.target_connection_string =
      row["target_connection_string"].as<std::string>();
  vault.target_schema = row["target_schema"].as<std::string>();
  vault.schedule_cron = row["schedule_cron"].is_null()
                            ? ""
                            : row["schedule_cron"].as<std::string>();
  vault.active = row["active"].as<bool>();
  vault.enabled = row["enabled"].as<bool>();
  vault.created_at =
      row["created_at"].is_null() ? "" : row["created_at"].as<std::string>();
  vault.updated_at =
      row["updated_at"].is_null() ? "" : row["updated_at"].as<std::string>();
  vault.last_build_time = row["last_build_time"].is_null()
                              ? ""
                              : row["last_build_time"].as<std::string>();
  vault.last_build_status =
      row["last_build_status"].is_null()
          ? ""
          : row["last_build_status"].as<std::string>();

  if (!row["metadata"].is_null()) {
    vault.metadata = parseJsonField(row["metadata"].as<std::string>());
  }

  if (!row["hubs"].is_null()) {
    json hubsJson = parseJsonField(row["hubs"].as<std::string>());
    for (const auto &hubJson : hubsJson) {
      HubTable hub;
      hub.hub_name = hubJson["hub_name"];
      hub.target_schema = hubJson["target_schema"];
      hub.target_table = hubJson["target_table"];
      hub.source_query = hubJson["source_query"];
      hub.business_keys =
          hubJson["business_keys"].get<std::vector<std::string>>();
      hub.hub_key_column = hubJson["hub_key_column"];
      hub.load_date_column = hubJson["load_date_column"];
      hub.record_source_column = hubJson["record_source_column"];
      if (hubJson.contains("index_columns")) {
        hub.index_columns =
            hubJson["index_columns"].get<std::vector<std::string>>();
      }
      vault.hubs.push_back(hub);
    }
  }

  if (!row["links"].is_null()) {
    json linksJson = parseJsonField(row["links"].as<std::string>());
    for (const auto &linkJson : linksJson) {
      LinkTable link;
      link.link_name = linkJson["link_name"];
      link.target_schema = linkJson["target_schema"];
      link.target_table = linkJson["target_table"];
      link.source_query = linkJson["source_query"];
      link.hub_references =
          linkJson["hub_references"].get<std::vector<std::string>>();
      link.link_key_column = linkJson["link_key_column"];
      link.load_date_column = linkJson["load_date_column"];
      link.record_source_column = linkJson["record_source_column"];
      if (linkJson.contains("index_columns")) {
        link.index_columns =
            linkJson["index_columns"].get<std::vector<std::string>>();
      }
      vault.links.push_back(link);
    }
  }

  if (!row["satellites"].is_null()) {
    json satellitesJson = parseJsonField(row["satellites"].as<std::string>());
    for (const auto &satJson : satellitesJson) {
      SatelliteTable sat;
      sat.satellite_name = satJson["satellite_name"];
      sat.target_schema = satJson["target_schema"];
      sat.target_table = satJson["target_table"];
      sat.parent_hub_name = satJson["parent_hub_name"];
      sat.parent_link_name = satJson["parent_link_name"];
      sat.source_query = satJson["source_query"];
      sat.parent_key_column = satJson["parent_key_column"];
      sat.load_date_column = satJson["load_date_column"];
      sat.load_end_date_column = satJson["load_end_date_column"];
      sat.record_source_column = satJson["record_source_column"];
      sat.descriptive_attributes =
          satJson["descriptive_attributes"].get<std::vector<std::string>>();
      if (satJson.contains("index_columns")) {
        sat.index_columns =
            satJson["index_columns"].get<std::vector<std::string>>();
      }
      sat.is_historized = satJson["is_historized"];
      vault.satellites.push_back(sat);
    }
  }

  if (!row["point_in_time_tables"].is_null()) {
    json pitJson = parseJsonField(row["point_in_time_tables"].as<std::string>());
    for (const auto &pitTableJson : pitJson) {
      PointInTimeTable pit;
      pit.pit_name = pitTableJson["pit_name"];
      pit.target_schema = pitTableJson["target_schema"];
      pit.target_table = pitTableJson["target_table"];
      pit.hub_name = pitTableJson["hub_name"];
      pit.satellite_names =
          pitTableJson["satellite_names"].get<std::vector<std::string>>();
      pit.snapshot_date_column = pitTableJson["snapshot_date_column"];
      if (pitTableJson.contains("index_columns")) {
        pit.index_columns =
            pitTableJson["index_columns"].get<std::vector<std::string>>();
      }
      vault.point_in_time_tables.push_back(pit);
    }
  }

  if (!row["bridge_tables"].is_null()) {
    json bridgeJson = parseJsonField(row["bridge_tables"].as<std::string>());
    for (const auto &bridgeTableJson : bridgeJson) {
      BridgeTable bridge;
      bridge.bridge_name = bridgeTableJson["bridge_name"];
      bridge.target_schema = bridgeTableJson["target_schema"];
      bridge.target_table = bridgeTableJson["target_table"];
      bridge.hub_name = bridgeTableJson["hub_name"];
      bridge.link_names =
          bridgeTableJson["link_names"].get<std::vector<std::string>>();
      bridge.snapshot_date_column = bridgeTableJson["snapshot_date_column"];
      if (bridgeTableJson.contains("index_columns")) {
        bridge.index_columns =
            bridgeTableJson["index_columns"].get<std::vector<std::string>>();
      }
      vault.bridge_tables.push_back(bridge);
    }
  }

  return vault;
}

json DataVaultRepository::parseJsonField(const std::string &jsonStr) {
  try {
    return json::parse(jsonStr);
  } catch (const std::exception &e) {
    Logger::warning(LogCategory::DATABASE, "parseJsonField",
                    "Error parsing JSON: " + std::string(e.what()));
    return json::object();
  }
}
