#include "utils/engine_factory.h"
#include "core/logger.h"
#include "engines/mariadb_engine.h"
#include "engines/mongodb_engine.h"
#include "engines/mssql_engine.h"
#include "engines/oracle_engine.h"
#include "engines/postgres_engine.h"
#include "engines/salesforce_engine.h"
#include "engines/sap_engine.h"
#include "engines/teradata_engine.h"
#include "engines/netezza_engine.h"
#include "engines/hive_engine.h"
#include "engines/cassandra_engine.h"
#include "engines/dynamodb_engine.h"
#include "engines/as400_engine.h"

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
  else if (dbEngine == "Salesforce")
    return std::make_unique<SalesforceEngine>(connectionString);
  else if (dbEngine == "SAP")
    return std::make_unique<SAPEngine>(connectionString);
  else if (dbEngine == "Teradata")
    return std::make_unique<TeradataEngine>(connectionString);
  else if (dbEngine == "Netezza")
    return std::make_unique<NetezzaEngine>(connectionString);
  else if (dbEngine == "Hive")
    return std::make_unique<HiveEngine>(connectionString);
  else if (dbEngine == "Cassandra")
    return std::make_unique<CassandraEngine>(connectionString);
  else if (dbEngine == "DynamoDB")
    return std::make_unique<DynamoDBEngine>(connectionString);
  else if (dbEngine == "AS400")
    return std::make_unique<AS400Engine>(connectionString);
  else {
    Logger::warning(LogCategory::DATABASE, "EngineFactory",
                    "Unsupported database engine: " + dbEngine);
    return nullptr;
  }
}
} // namespace EngineFactory
