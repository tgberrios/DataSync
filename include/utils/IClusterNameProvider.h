#ifndef I_CLUSTER_NAME_PROVIDER_H
#define I_CLUSTER_NAME_PROVIDER_H

#include <string>

class IClusterNameProvider {
public:
  virtual ~IClusterNameProvider() = default;
  virtual std::string resolve(const std::string &connectionString) = 0;
};

#endif
