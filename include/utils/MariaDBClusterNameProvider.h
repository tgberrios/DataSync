#ifndef MARIADB_CLUSTER_NAME_PROVIDER_H
#define MARIADB_CLUSTER_NAME_PROVIDER_H

#include "utils/IClusterNameProvider.h"

class MariaDBClusterNameProvider : public IClusterNameProvider {
public:
  std::string resolve(const std::string &connectionString) override;
};

#endif
