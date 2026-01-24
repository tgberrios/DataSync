#ifndef GRAPHQL_ENGINE_H
#define GRAPHQL_ENGINE_H

#include "third_party/json.hpp"
#include <curl/curl.h>
#include <string>
#include <vector>
#include <memory>

using json = nlohmann::json;

struct GraphQLConfig {
  std::string endpoint_url;
  std::string auth_token = "";
  std::string auth_header = "Authorization";
  int timeout_seconds = 30;
  bool use_ssl = true;
};

class GraphQLEngine {
  std::string baseUrl_;
  GraphQLConfig config_;
  CURL *curl_;

  static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp);
  std::string executeQuery(const std::string &query, const json &variables = json::object());
  json parseResponse(const std::string &response);

public:
  explicit GraphQLEngine(const std::string &baseUrl, 
                        const GraphQLConfig &config = GraphQLConfig());
  ~GraphQLEngine();

  GraphQLEngine(const GraphQLEngine &) = delete;
  GraphQLEngine &operator=(const GraphQLEngine &) = delete;

  json query(const std::string &query, const json &variables = json::object());
  std::vector<json> queryList(const std::string &query, const json &variables = json::object());
  void setConfig(const GraphQLConfig &config);
};

#endif
