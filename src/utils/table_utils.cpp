#include "utils/table_utils.h"
#include "core/logger.h"
#include <algorithm>

bool TableUtils::tableExistsInPostgres(pqxx::connection &conn,
                                       const std::string &schema,
                                       const std::string &table) {
  try {
    pqxx::work txn(conn);

    std::string lowerSchema = schema;
    std::transform(lowerSchema.begin(), lowerSchema.end(), lowerSchema.begin(),
                   ::tolower);
    std::string lowerTable = table;
    std::transform(lowerTable.begin(), lowerTable.end(), lowerTable.begin(),
                   ::tolower);

    auto result =
        txn.exec_params("SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = $1 AND table_name = $2",
                        lowerSchema, lowerTable);
    txn.commit();

    return !result.empty() && result[0][0].as<int>() > 0;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "TableUtils",
                  "Error checking table existence: " + std::string(e.what()));
    return false;
  }
}
