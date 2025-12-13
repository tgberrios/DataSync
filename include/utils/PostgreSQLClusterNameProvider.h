#ifndef POSTGRESQL_CLUSTER_NAME_PROVIDER_H
#define POSTGRESQL_CLUSTER_NAME_PROVIDER_H

#include "utils/IClusterNameProvider.h"

class PostgreSQLClusterNameProvider : public IClusterNameProvider {
public:
  std::string resolve(const std::string &connectionString) override;
};

#endif
