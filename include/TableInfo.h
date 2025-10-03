#ifndef TABLEINFO_H
#define TABLEINFO_H

#include <string>

struct TableInfo {
  std::string schema_name;
  std::string table_name;
  std::string cluster_name;
  std::string db_engine;
  std::string connection_string;
  std::string last_sync_time;
  std::string last_sync_column;
  std::string status;
  std::string last_offset;
  std::string last_processed_pk;
  std::string pk_strategy;
  std::string pk_columns;
  std::string candidate_columns;
  bool has_pk;
};

#endif // TABLEINFO_H
