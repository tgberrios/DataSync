#ifndef JSON_UTILS_H
#define JSON_UTILS_H

#include "third_party/json.hpp"
#include <pqxx/pqxx>
#include <string>

using json = nlohmann::json;

namespace JsonUtils {
// Helper function to safely parse JSON fields from PostgreSQL rows.
// Returns an empty JSON object if the field is NULL or parsing fails.
inline json parseJSONField(const pqxx::row &row, int index) {
  if (row[index].is_null()) {
    return json{};
  }
  try {
    return json::parse(row[index].as<std::string>());
  } catch (const std::exception &) {
    return json{};
  }
}
} // namespace JsonUtils

#endif
