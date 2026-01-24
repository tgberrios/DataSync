#ifndef DYNAMODB_ENGINE_H
#define DYNAMODB_ENGINE_H

#include "engines/database_engine.h"
#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <memory>

#ifdef HAVE_AWS_SDK
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#ifdef HAVE_DYNAMODB_SDK
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/ScanRequest.h>
#include <aws/dynamodb/model/ListTablesRequest.h>
#endif
#endif

using json = nlohmann::json;

class DynamoDBEngine : public IDatabaseEngine {
  std::string connectionString_;
  std::string accessKeyId_;
  std::string secretAccessKey_;
  std::string region_;
#ifdef HAVE_AWS_SDK
#ifdef HAVE_DYNAMODB_SDK
  std::shared_ptr<Aws::DynamoDB::DynamoDBClient> dynamoClient_;
#endif
  Aws::SDKOptions options_;
  bool sdkInitialized_;
  void initializeSDK();
#endif

  void parseConnectionString();
  std::vector<json> scanTable(const std::string &tableName);
  std::vector<json> queryTable(const std::string &tableName, const json &keyCondition);

public:
  explicit DynamoDBEngine(std::string connectionString);
  ~DynamoDBEngine();

  std::vector<CatalogTableInfo> discoverTables() override;
  std::vector<std::string> detectPrimaryKey(const std::string &schema,
                                            const std::string &table) override;
  std::string detectTimeColumn(const std::string &schema,
                               const std::string &table) override;
  std::pair<int, int> getColumnCounts(const std::string &schema,
                                      const std::string &table,
                                      const std::string &targetConnStr) override;
};

#endif
