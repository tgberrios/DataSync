#ifndef DATA_VAULT_REPOSITORY_H
#define DATA_VAULT_REPOSITORY_H

#include "third_party/json.hpp"
#include <pqxx/pqxx>
#include <string>
#include <vector>

using json = nlohmann::json;

struct HubTable {
  std::string hub_name;
  std::string target_schema;
  std::string target_table;
  std::string source_query;
  std::vector<std::string> business_keys;
  std::string hub_key_column;
  std::string load_date_column;
  std::string record_source_column;
  std::vector<std::string> index_columns;
};

struct LinkTable {
  std::string link_name;
  std::string target_schema;
  std::string target_table;
  std::string source_query;
  std::vector<std::string> hub_references;
  std::string link_key_column;
  std::string load_date_column;
  std::string record_source_column;
  std::vector<std::string> index_columns;
};

struct SatelliteTable {
  std::string satellite_name;
  std::string target_schema;
  std::string target_table;
  std::string parent_hub_name;
  std::string parent_link_name;
  std::string source_query;
  std::string parent_key_column;
  std::string load_date_column;
  std::string load_end_date_column;
  std::string record_source_column;
  std::vector<std::string> descriptive_attributes;
  std::vector<std::string> index_columns;
  bool is_historized;
};

struct PointInTimeTable {
  std::string pit_name;
  std::string target_schema;
  std::string target_table;
  std::string hub_name;
  std::vector<std::string> satellite_names;
  std::string snapshot_date_column;
  std::vector<std::string> index_columns;
};

struct BridgeTable {
  std::string bridge_name;
  std::string target_schema;
  std::string target_table;
  std::string hub_name;
  std::vector<std::string> link_names;
  std::string snapshot_date_column;
  std::vector<std::string> index_columns;
};

struct DataVaultModel {
  int id;
  std::string vault_name;
  std::string description;
  std::string source_db_engine;
  std::string source_connection_string;
  std::string target_db_engine;
  std::string target_connection_string;
  std::string target_schema;
  std::vector<HubTable> hubs;
  std::vector<LinkTable> links;
  std::vector<SatelliteTable> satellites;
  std::vector<PointInTimeTable> point_in_time_tables;
  std::vector<BridgeTable> bridge_tables;
  std::string schedule_cron;
  bool active;
  bool enabled;
  json metadata;
  std::string created_at;
  std::string updated_at;
  std::string last_build_time;
  std::string last_build_status;
};

class DataVaultRepository {
  std::string connectionString_;

public:
  explicit DataVaultRepository(std::string connectionString);

  void createTables();
  std::vector<DataVaultModel> getAllVaults();
  std::vector<DataVaultModel> getActiveVaults();
  DataVaultModel getVault(const std::string &vaultName);
  void insertOrUpdateVault(const DataVaultModel &vault);
  void deleteVault(const std::string &vaultName);
  void updateVaultActive(const std::string &vaultName, bool active);
  void updateBuildStatus(const std::string &vaultName,
                         const std::string &status,
                         const std::string &buildTime,
                         const std::string &notes = "");

private:
  pqxx::connection getConnection();
  DataVaultModel rowToVault(const pqxx::row &row);
  json parseJsonField(const std::string &jsonStr);
};

#endif
