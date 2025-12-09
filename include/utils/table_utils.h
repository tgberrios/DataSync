#ifndef TABLE_UTILS_H
#define TABLE_UTILS_H

#include <pqxx/pqxx>
#include <string>

namespace TableUtils {

bool tableExistsInPostgres(pqxx::connection &conn, const std::string &schema,
                           const std::string &table);

} // namespace TableUtils

#endif
