#ifndef CLUSTER_NAME_RESOLVER_H
#define CLUSTER_NAME_RESOLVER_H

#include <memory>
#include <string>

class IClusterNameProvider;

class ClusterNameResolver {
public:
  static std::string resolve(const std::string &connectionString,
                             const std::string &dbEngine);

private:
  static std::unique_ptr<IClusterNameProvider>
  createProvider(const std::string &dbEngine);
  static std::string extractHostname(const std::string &connectionString);
};

#endif
