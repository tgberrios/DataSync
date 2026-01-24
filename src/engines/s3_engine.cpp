#include "engines/s3_engine.h"
#include "core/logger.h"
#include "utils/connection_utils.h"
#include <sstream>

#ifdef HAVE_AWS_SDK
#define HAVE_AWS_SDK
#endif

S3Engine::S3Engine(const std::string &connectionString, const S3Config &config)
    : connectionString_(connectionString), config_(config)
#ifdef HAVE_AWS_SDK
    , s3Client_(nullptr), sdkInitialized_(false)
#endif
{
  parseConnectionString();
#ifdef HAVE_AWS_SDK
  initializeSDK();
#endif
}

S3Engine::~S3Engine() {
#ifdef HAVE_AWS_SDK
  if (sdkInitialized_) {
    Aws::ShutdownAPI(options_);
  }
#endif
}

#ifdef HAVE_AWS_SDK
void S3Engine::initializeSDK() {
  Aws::InitAPI(options_);
  sdkInitialized_ = true;

  Aws::S3::S3ClientConfiguration clientConfig;
  clientConfig.region = config_.region;
  
  if (!config_.endpoint.empty()) {
    clientConfig.endpointOverride = config_.endpoint;
  }

  Aws::Auth::AWSCredentials credentials(config_.access_key_id, config_.secret_access_key);
  s3Client_ = std::make_shared<Aws::S3::S3Client>(credentials, nullptr, clientConfig);
}
#endif

void S3Engine::parseConnectionString() {
  auto params = ConnectionStringParser::parse(connectionString_);
  if (params) {
    config_.access_key_id = params->user;
    config_.secret_access_key = params->password;
    config_.bucket_name = params->db;
    if (!params->host.empty()) {
      config_.endpoint = params->host;
    }
  }
}

std::string S3Engine::buildS3URL(const std::string &objectKey) {
  std::ostringstream url;
  
  if (!config_.endpoint.empty()) {
    url << (config_.use_ssl ? "https://" : "http://");
    url << config_.endpoint;
  } else {
    url << "https://" << config_.bucket_name << ".s3." << config_.region << ".amazonaws.com";
  }
  
  if (!objectKey.empty() && objectKey[0] != '/') {
    url << "/";
  }
  url << objectKey;
  
  return url.str();
}

std::vector<uint8_t> S3Engine::downloadObject(const std::string &objectKey) {
#ifdef HAVE_AWS_SDK
  if (!s3Client_) {
    Logger::error(LogCategory::DATABASE, "S3Engine",
                  "S3 client not initialized");
    return {};
  }

  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(config_.bucket_name);
  request.SetKey(objectKey);

  auto outcome = s3Client_->GetObject(request);
  if (!outcome.IsSuccess()) {
    Logger::error(LogCategory::DATABASE, "S3Engine",
                  "Failed to download object: " + outcome.GetError().GetMessage());
    return {};
  }

  auto &retrievedFile = outcome.GetResultWithOwnership().GetBody();
  std::vector<uint8_t> data((std::istreambuf_iterator<char>(retrievedFile)),
                            std::istreambuf_iterator<char>());
  return data;
#else
  Logger::warning(LogCategory::DATABASE, "S3Engine",
                  "S3 object download requires AWS SDK - returning empty data");
  return {};
#endif
}

std::vector<std::string> S3Engine::listObjects(const std::string &prefix) {
#ifdef HAVE_AWS_SDK
  if (!s3Client_) {
    Logger::error(LogCategory::DATABASE, "S3Engine",
                  "S3 client not initialized");
    return {};
  }

  std::vector<std::string> objectKeys;
  Aws::S3::Model::ListObjectsV2Request request;
  request.SetBucket(config_.bucket_name);
  if (!prefix.empty()) {
    request.SetPrefix(prefix);
  }

  bool done = false;
  while (!done) {
    auto outcome = s3Client_->ListObjectsV2(request);
    if (!outcome.IsSuccess()) {
      Logger::error(LogCategory::DATABASE, "S3Engine",
                    "Failed to list objects: " + outcome.GetError().GetMessage());
      break;
    }

    auto result = outcome.GetResult();
    for (const auto &object : result.GetContents()) {
      objectKeys.push_back(object.GetKey());
    }

    done = !result.GetIsTruncated();
    if (!done) {
      request.SetContinuationToken(result.GetNextContinuationToken());
    }
  }

  return objectKeys;
#else
  Logger::warning(LogCategory::DATABASE, "S3Engine",
                  "S3 object listing requires AWS SDK - returning empty list");
  return {};
#endif
}

std::vector<uint8_t> S3Engine::getObject(const std::string &objectKey) {
  return downloadObject(objectKey);
}

std::vector<std::string> S3Engine::listBucket(const std::string &prefix) {
  return listObjects(prefix);
}

std::vector<json> S3Engine::parseObjectAsJSON(const std::string &objectKey) {
  std::vector<uint8_t> data = getObject(objectKey);
  if (data.empty()) {
    return {};
  }
  
  try {
    std::string jsonStr(data.begin(), data.end());
    json parsed = json::parse(jsonStr);
    
    if (parsed.is_array()) {
      std::vector<json> result;
      for (const auto &item : parsed) {
        result.push_back(item);
      }
      return result;
    } else {
      return {parsed};
    }
  } catch (const json::parse_error &e) {
    Logger::error(LogCategory::DATABASE, "S3Engine",
                  "Failed to parse JSON from S3 object: " + std::string(e.what()));
    return {};
  }
}

void S3Engine::setConfig(const S3Config &config) {
  config_ = config;
}
