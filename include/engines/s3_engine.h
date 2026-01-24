#ifndef S3_ENGINE_H
#define S3_ENGINE_H

#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <memory>

#ifdef HAVE_AWS_SDK
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#endif

using json = nlohmann::json;

struct S3Config {
  std::string access_key_id;
  std::string secret_access_key;
  std::string region = "us-east-1";
  std::string bucket_name;
  std::string endpoint = "";
  bool use_ssl = true;
};

class S3Engine {
  std::string connectionString_;
  S3Config config_;
#ifdef HAVE_AWS_SDK
  std::shared_ptr<Aws::S3::S3Client> s3Client_;
  Aws::SDKOptions options_;
  bool sdkInitialized_;
#endif

  void parseConnectionString();
  std::string buildS3URL(const std::string &objectKey);
  std::vector<uint8_t> downloadObject(const std::string &objectKey);
  std::vector<std::string> listObjects(const std::string &prefix = "");
#ifdef HAVE_AWS_SDK
  void initializeSDK();
#endif

public:
  explicit S3Engine(const std::string &connectionString, 
                   const S3Config &config = S3Config());
  ~S3Engine();

  S3Engine(const S3Engine &) = delete;
  S3Engine &operator=(const S3Engine &) = delete;

  std::vector<uint8_t> getObject(const std::string &objectKey);
  std::vector<std::string> listBucket(const std::string &prefix = "");
  std::vector<json> parseObjectAsJSON(const std::string &objectKey);
  void setConfig(const S3Config &config);
};

#endif
