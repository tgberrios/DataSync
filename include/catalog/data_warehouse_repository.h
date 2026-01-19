#ifndef DATA_WAREHOUSE_REPOSITORY_H
#define DATA_WAREHOUSE_REPOSITORY_H

#include "third_party/json.hpp"
#include <pqxx/pqxx>
#include <string>
#include <vector>

using json = nlohmann::json;

enum class SchemaType { STAR_SCHEMA, SNOWFLAKE_SCHEMA };

enum class DimensionType { TYPE_1, TYPE_2, TYPE_3 };

enum class DataLayer { BRONZE, SILVER, GOLD };

struct DimensionTable {
  std::string dimension_name;
  std::string target_schema;
  std::string target_table;
  DimensionType scd_type;
  std::string source_query;
  std::vector<std::string> business_keys;
  std::string valid_from_column;
  std::string valid_to_column;
  std::string is_current_column;
  std::vector<std::string> index_columns;
  std::string partition_column;
};

struct FactTable {
  std::string fact_name;
  std::string target_schema;
  std::string target_table;
  std::string source_query;
  std::vector<std::string> dimension_keys;
  std::vector<std::string> measures;
  std::vector<std::string> index_columns;
  std::string partition_column;
};

struct DataWarehouseModel {
  int id;
  std::string warehouse_name;
  std::string description;
  SchemaType schema_type;
  DataLayer target_layer;
  std::string source_db_engine;
  std::string source_connection_string;
  std::string target_db_engine;
  std::string target_connection_string;
  std::string target_schema;
  std::vector<DimensionTable> dimensions;
  std::vector<FactTable> facts;
  std::string schedule_cron;
  bool active;
  bool enabled;
  json metadata;
  std::string created_at;
  std::string updated_at;
  std::string last_build_time;
  std::string last_build_status;
  std::string notes;
};

class DataWarehouseRepository {
  std::string connectionString_;

public:
  explicit DataWarehouseRepository(std::string connectionString);

  void createTables();
  std::vector<DataWarehouseModel> getAllWarehouses();
  std::vector<DataWarehouseModel> getActiveWarehouses();
  DataWarehouseModel getWarehouse(const std::string &warehouseName);
  void insertOrUpdateWarehouse(const DataWarehouseModel &warehouse);
  void deleteWarehouse(const std::string &warehouseName);
  void updateWarehouseActive(const std::string &warehouseName, bool active);
  void updateBuildStatus(const std::string &warehouseName,
                         const std::string &status,
                         const std::string &buildTime,
                         const std::string &notes = "");

private:
  pqxx::connection getConnection();
  DataWarehouseModel rowToWarehouse(const pqxx::row &row);
  json parseJsonField(const std::string &jsonStr);
  std::string schemaTypeToString(SchemaType type);
  SchemaType stringToSchemaType(const std::string &str);
  std::string dimensionTypeToString(DimensionType type);
  DimensionType stringToDimensionType(const std::string &str);
  std::string dataLayerToString(DataLayer layer);
  DataLayer stringToDataLayer(const std::string &str);
};

#endif
