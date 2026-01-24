#include "engines/dynamodb_engine.h"
#include "core/logger.h"
#include "utils/connection_utils.h"

// HAVE_AWS_SDK is defined by CMake

DynamoDBEngine::DynamoDBEngine(std::string connectionString)
    : connectionString_(std::move(connectionString)), accessKeyId_(""), secretAccessKey_(""), region_("us-east-1")
#ifdef HAVE_AWS_SDK
#ifdef HAVE_DYNAMODB_SDK
    , dynamoClient_(nullptr)
    , sdkInitialized_(false)
#else
    , sdkInitialized_(false)
#endif
#endif
{
  parseConnectionString();
#ifdef HAVE_AWS_SDK
  initializeSDK();
#endif
}

DynamoDBEngine::~DynamoDBEngine() {
#ifdef HAVE_AWS_SDK
  if (sdkInitialized_) {
    Aws::ShutdownAPI(options_);
  }
#endif
}

#ifdef HAVE_AWS_SDK
void DynamoDBEngine::initializeSDK() {
  Aws::InitAPI(options_);
  sdkInitialized_ = true;

#ifdef HAVE_DYNAMODB_SDK
  Aws::Client::ClientConfiguration clientConfig;
  clientConfig.region = region_;

  Aws::Auth::AWSCredentials credentials(accessKeyId_, secretAccessKey_);
  dynamoClient_ = std::make_shared<Aws::DynamoDB::DynamoDBClient>(credentials, nullptr, clientConfig);
#endif
}
#endif

void DynamoDBEngine::parseConnectionString() {
  auto params = ConnectionStringParser::parse(connectionString_);
  if (params) {
    accessKeyId_ = params->user;
    secretAccessKey_ = params->password;
    region_ = params->host.empty() ? "us-east-1" : params->host;
  }
}

std::vector<json> DynamoDBEngine::scanTable(const std::string &tableName) {
#ifdef HAVE_AWS_SDK
#ifdef HAVE_DYNAMODB_SDK
  if (!dynamoClient_) {
    Logger::error(LogCategory::DATABASE, "DynamoDBEngine",
                  "DynamoDB client not initialized");
    return {};
  }

  std::vector<json> results;
  Aws::DynamoDB::Model::ScanRequest request;
  request.SetTableName(tableName);

  bool done = false;
  while (!done) {
    auto outcome = dynamoClient_->Scan(request);
    if (!outcome.IsSuccess()) {
      Logger::error(LogCategory::DATABASE, "DynamoDBEngine",
                    "Failed to scan table: " + outcome.GetError().GetMessage());
      break;
    }

    auto result = outcome.GetResult();
    for (const auto &item : result.GetItems()) {
      json record;
      for (const auto &attr : item) {
        std::string key = attr.first;
        auto value = attr.second;
        
        if (value.GetS().length() > 0) {
          record[key] = value.GetS();
        } else if (value.GetN().length() > 0) {
          record[key] = std::stod(value.GetN());
        } else if (value.GetBool()) {
          record[key] = value.GetBool();
        } else if (value.GetSS().size() > 0) {
          // String set
          json strArray = json::array();
          for (const auto &str : value.GetSS()) {
            strArray.push_back(str);
          }
          record[key] = strArray;
        } else if (value.GetNS().size() > 0) {
          // Number set
          json numArray = json::array();
          for (const auto &numStr : value.GetNS()) {
            numArray.push_back(std::stod(numStr));
          }
          record[key] = numArray;
        }
      }
      results.push_back(record);
    }

    done = result.GetLastEvaluatedKey().empty();
    if (!done) {
      request.SetExclusiveStartKey(result.GetLastEvaluatedKey());
    }
  }

  return results;
#else
  Logger::warning(LogCategory::DATABASE, "DynamoDBEngine",
                  "DynamoDB scan requires DynamoDB SDK - returning empty results");
  return {};
#endif
#else
  Logger::warning(LogCategory::DATABASE, "DynamoDBEngine",
                  "DynamoDB scan requires AWS SDK - returning empty results");
  return {};
#endif
}

std::vector<json> DynamoDBEngine::queryTable(const std::string &tableName, const json &keyCondition) {
  Logger::warning(LogCategory::DATABASE, "DynamoDBEngine",
                  "DynamoDB query requires AWS SDK - returning empty results");
  return {};
}

std::vector<CatalogTableInfo> DynamoDBEngine::discoverTables() {
#ifdef HAVE_AWS_SDK
#ifdef HAVE_DYNAMODB_SDK
  if (!dynamoClient_) {
    Logger::error(LogCategory::DATABASE, "DynamoDBEngine",
                  "DynamoDB client not initialized");
    return {};
  }

  std::vector<CatalogTableInfo> tables;
  Aws::DynamoDB::Model::ListTablesRequest request;
  
  bool done = false;
  while (!done) {
    auto outcome = dynamoClient_->ListTables(request);
    if (!outcome.IsSuccess()) {
      Logger::error(LogCategory::DATABASE, "DynamoDBEngine",
                    "Failed to list tables: " + outcome.GetError().GetMessage());
      break;
    }

    auto result = outcome.GetResult();
    for (const auto &tableName : result.GetTableNames()) {
      CatalogTableInfo info;
      info.table = tableName;
      info.schema = "";
      info.connectionString = connectionString_;
      tables.push_back(info);
    }

    done = result.GetLastEvaluatedTableName().empty();
    if (!done) {
      request.SetExclusiveStartTableName(result.GetLastEvaluatedTableName());
    }
  }

  return tables;
#else
  Logger::warning(LogCategory::DATABASE, "DynamoDBEngine",
                  "DynamoDB table discovery requires DynamoDB SDK - returning empty list");
  return {};
#endif
#else
  Logger::warning(LogCategory::DATABASE, "DynamoDBEngine",
                  "DynamoDB table discovery requires AWS SDK - returning empty list");
  return {};
#endif
}

std::vector<std::string> DynamoDBEngine::detectPrimaryKey(const std::string &schema, const std::string &table) {
  std::vector<std::string> pk;
  pk.push_back("id");
  return pk;
}

std::string DynamoDBEngine::detectTimeColumn(const std::string &schema, const std::string &table) {
  return "updated_at";
}

std::pair<int, int> DynamoDBEngine::getColumnCounts(const std::string &schema, const std::string &table, const std::string &targetConnStr) {
  Logger::warning(LogCategory::DATABASE, "DynamoDBEngine",
                  "DynamoDB column count requires AWS SDK - returning 0,0");
  return {0, 0};
}
