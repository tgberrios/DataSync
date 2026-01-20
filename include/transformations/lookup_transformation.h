#ifndef LOOKUP_TRANSFORMATION_H
#define LOOKUP_TRANSFORMATION_H

#include "transformation_engine.h"
#include <pqxx/pqxx>
#include <string>
#include <vector>
#include <map>

// Lookup transformation: Enrich data with reference table data
class LookupTransformation : public Transformation {
public:
  LookupTransformation();
  ~LookupTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "lookup"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Load lookup table data into memory cache
  std::vector<json> loadLookupTable(
    const std::string& connectionString,
    const std::string& dbEngine,
    const std::string& schema,
    const std::string& table,
    const std::vector<std::string>& lookupColumns,
    const std::vector<std::string>& returnColumns
  );
  
  // Perform in-memory lookup
  std::vector<json> performLookup(
    const std::vector<json>& inputData,
    const std::vector<json>& lookupData,
    const std::vector<std::string>& sourceColumns,
    const std::vector<std::string>& lookupColumns,
    const std::vector<std::string>& returnColumns
  );
  
  // Generate SQL for lookup (alternative approach)
  std::string generateLookupSQL(
    const std::string& sourceQuery,
    const std::string& lookupSchema,
    const std::string& lookupTable,
    const std::vector<std::string>& sourceColumns,
    const std::vector<std::string>& lookupColumns,
    const std::vector<std::string>& returnColumns
  );
  
  // Cache for lookup tables (key: connection_string:db_engine:schema:table)
  std::map<std::string, std::vector<json>> lookupCache_;
};

#endif // LOOKUP_TRANSFORMATION_H
