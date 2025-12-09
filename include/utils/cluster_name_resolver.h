#ifndef CLUSTER_NAME_RESOLVER_H
#define CLUSTER_NAME_RESOLVER_H

#include <string>

class ClusterNameResolver {
public:
  static std::string resolve(const std::string &connectionString,
                             const std::string &dbEngine);

private:
  static std::string resolveMariaDB(const std::string &connectionString);
  static std::string resolveMSSQL(const std::string &connectionString);
  static std::string resolvePostgreSQL(const std::string &connectionString);
  static std::string extractHostname(const std::string &connectionString);
  static std::string getClusterNameFromHostname(const std::string &hostname);
};

#endif
