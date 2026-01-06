#ifndef DATABASE_ENGINE_H
#define DATABASE_ENGINE_H

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

struct CatalogTableInfo {
  std::string schema;
  std::string table;
  std::string connectionString;
};

class IDatabaseEngine {
public:
  virtual ~IDatabaseEngine() = default;

  virtual std::vector<CatalogTableInfo> discoverTables() = 0;
  virtual std::vector<std::string>
  detectPrimaryKey(const std::string &schema, const std::string &table) = 0;
  virtual std::string detectTimeColumn(const std::string &schema,
                                       const std::string &table) = 0;
  virtual std::pair<int, int>
  getColumnCounts(const std::string &schema, const std::string &table,
                  const std::string &targetConnStr) = 0;
};

inline std::string
determinePKStrategy(const std::vector<std::string> & /* pkColumns */) {
  return "CDC";
}

inline std::string columnsToJSON(const std::vector<std::string> &columns) {
  if (columns.empty())
    return "[]";

  std::string json = "[";
  for (size_t i = 0; i < columns.size(); ++i) {
    if (i > 0)
      json += ",";
    std::string escaped = columns[i];
    size_t pos = 0;
    while ((pos = escaped.find('"', pos)) != std::string::npos) {
      escaped.replace(pos, 1, "\\\"");
      pos += 2;
    }
    pos = 0;
    while ((pos = escaped.find('\\', pos)) != std::string::npos) {
      escaped.replace(pos, 1, "\\\\");
      pos += 2;
    }
    json += "\"" + escaped + "\"";
  }
  json += "]";
  return json;
}

inline std::string escapeSQL(const std::string &value) {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

#endif
