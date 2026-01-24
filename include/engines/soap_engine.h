#ifndef SOAP_ENGINE_H
#define SOAP_ENGINE_H

#include "third_party/json.hpp"
#include <curl/curl.h>
#include <string>
#include <vector>
#include <memory>

using json = nlohmann::json;

struct SOAPConfig {
  std::string endpoint_url;
  std::string action = "";
  std::string namespace_uri = "";
  std::string username = "";
  std::string password = "";
  int timeout_seconds = 30;
  bool use_ssl = true;
};

class SOAPEngine {
  std::string baseUrl_;
  SOAPConfig config_;
  CURL *curl_;

  static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp);
  std::string buildSOAPEnvelope(const std::string &method, const json &parameters);
  std::string executeSOAPRequest(const std::string &soapBody);
  json parseSOAPResponse(const std::string &soapResponse);

public:
  explicit SOAPEngine(const std::string &baseUrl, 
                    const SOAPConfig &config = SOAPConfig());
  ~SOAPEngine();

  SOAPEngine(const SOAPEngine &) = delete;
  SOAPEngine &operator=(const SOAPEngine &) = delete;

  json callMethod(const std::string &method, const json &parameters = json::object());
  std::vector<json> parseResponse(const std::string &response);
  void setConfig(const SOAPConfig &config);
};

#endif
