#include "utils/PostgreSQLClusterNameProvider.h"
#include "utils/HostnamePatternMatcher.h"
#include "utils/connection_utils.h"

std::string
PostgreSQLClusterNameProvider::resolve(const std::string &connectionString) {
  auto params = ConnectionStringParser::parse(connectionString);
  if (!params)
    return "";
  return HostnamePatternMatcher::deriveClusterName(params->host);
}
