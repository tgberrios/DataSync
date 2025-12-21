#include "utils/engine_factory.h"
#include "core/logger.h"
#include "engines/mariadb_engine.h"
#include "engines/mongodb_engine.h"
#include "engines/mssql_engine.h"
#include "engines/oracle_engine.h"
#include "engines/postgres_engine.h"

namespace EngineFactory {
std::unique_ptr<IDatabaseEngine>
createEngine(const std::string &dbEngine, const std::string &connectionString) {
  if (dbEngine == "MariaDB")
    return std::make_unique<MariaDBEngine>(connectionString);
  else if (dbEngine == "MSSQL")
    return std::make_unique<MSSQLEngine>(connectionString);
  else if (dbEngine == "PostgreSQL")
    return std::make_unique<PostgreSQLEngine>(connectionString);
  else if (dbEngine == "MongoDB")
    return std::make_unique<MongoDBEngine>(connectionString);
  else if (dbEngine == "Oracle")
    return std::make_unique<OracleEngine>(connectionString);
  else {
    Logger::warning(LogCategory::DATABASE, "EngineFactory",
                    "Unsupported database engine: " + dbEngine);
    return nullptr;
  }
}
} // namespace EngineFactory
