#include "utils/cluster_name_resolver.h"
#include "utils/HostnamePatternMatcher.h"
#include "utils/IClusterNameProvider.h"
#include "utils/MSSQLClusterNameProvider.h"
#include "utils/MariaDBClusterNameProvider.h"
#include "utils/PostgreSQLClusterNameProvider.h"
#include "utils/connection_utils.h"
#include "utils/string_utils.h"

std::string ClusterNameResolver::resolve(const std::string &connectionString,
                                         const std::string &dbEngine) {
  if (connectionString.empty() || dbEngine.empty()) {
    return "";
  }

  std::unique_ptr<IClusterNameProvider> provider = createProvider(dbEngine);
  std::string clusterName;

  if (provider) {
    clusterName = provider->resolve(connectionString);
  }

  if (clusterName.empty()) {
    std::string hostname = extractHostname(connectionString);
    clusterName = HostnamePatternMatcher::deriveClusterName(hostname);
  }

  return clusterName;
}

std::unique_ptr<IClusterNameProvider>
ClusterNameResolver::createProvider(const std::string &dbEngine) {
  std::string normalizedEngine = StringUtils::toLower(dbEngine);

  if (normalizedEngine == "mariadb")
    return std::make_unique<MariaDBClusterNameProvider>();
  else if (normalizedEngine == "mssql")
    return std::make_unique<MSSQLClusterNameProvider>();
  else if (normalizedEngine == "postgresql")
    return std::make_unique<PostgreSQLClusterNameProvider>();

  return nullptr;
}

std::string
ClusterNameResolver::extractHostname(const std::string &connectionString) {
  auto params = ConnectionStringParser::parse(connectionString);
  return params ? params->host : "";
}
