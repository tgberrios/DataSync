#ifndef MSSQL_CLUSTER_NAME_PROVIDER_H
#define MSSQL_CLUSTER_NAME_PROVIDER_H

#include "utils/IClusterNameProvider.h"

class MSSQLClusterNameProvider : public IClusterNameProvider {
public:
  std::string resolve(const std::string &connectionString) override;
};

#endif
