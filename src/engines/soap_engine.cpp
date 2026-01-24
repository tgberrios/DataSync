#include "engines/soap_engine.h"
#include "core/logger.h"
#include <sstream>
#include <iomanip>

SOAPEngine::SOAPEngine(const std::string &baseUrl, const SOAPConfig &config)
    : baseUrl_(baseUrl), config_(config), curl_(nullptr) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl_ = curl_easy_init();
  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "SOAPEngine",
                  "Failed to initialize CURL");
  }
}

SOAPEngine::~SOAPEngine() {
  if (curl_) {
    curl_easy_cleanup(curl_);
  }
  curl_global_cleanup();
}

size_t SOAPEngine::WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
  ((std::string *)userp)->append((char *)contents, size * nmemb);
  return size * nmemb;
}

std::string SOAPEngine::buildSOAPEnvelope(const std::string &method, const json &parameters) {
  std::ostringstream envelope;
  std::string namespaceURI = config_.namespace_uri.empty() ? "http://tempuri.org/" : config_.namespace_uri;
  
  envelope << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
  envelope << "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\">\n";
  envelope << "  <soap:Header/>\n";
  envelope << "  <soap:Body>\n";
  envelope << "    <" << method << " xmlns=\"" << namespaceURI << "\">\n";
  
  for (const auto &param : parameters.items()) {
    envelope << "      <" << param.key() << ">";
    if (param.value().is_string()) {
      envelope << param.value().get<std::string>();
    } else if (param.value().is_number()) {
      envelope << param.value().dump();
    } else {
      envelope << param.value().dump();
    }
    envelope << "</" << param.key() << ">\n";
  }
  
  envelope << "    </" << method << ">\n";
  envelope << "  </soap:Body>\n";
  envelope << "</soap:Envelope>";
  
  return envelope.str();
}

std::string SOAPEngine::executeSOAPRequest(const std::string &soapBody) {
  if (!curl_) {
    throw std::runtime_error("CURL not initialized");
  }

  std::string responseBody;
  curl_easy_reset(curl_);

  std::string url = config_.endpoint_url.empty() ? baseUrl_ : config_.endpoint_url;
  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, soapBody.c_str());
  curl_easy_setopt(curl_, CURLOPT_POSTFIELDSIZE, soapBody.length());
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &responseBody);
  curl_easy_setopt(curl_, CURLOPT_TIMEOUT, static_cast<long>(config_.timeout_seconds));
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, config_.use_ssl ? 1L : 0L);
  curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, config_.use_ssl ? 2L : 0L);

  struct curl_slist *headerList = nullptr;
  headerList = curl_slist_append(headerList, "Content-Type: text/xml; charset=utf-8");
  
  if (!config_.action.empty()) {
    std::string soapAction = "SOAPAction: \"" + config_.action + "\"";
    headerList = curl_slist_append(headerList, soapAction.c_str());
  }
  
  curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headerList);

  if (!config_.username.empty() && !config_.password.empty()) {
    std::string userpwd = config_.username + ":" + config_.password;
    curl_easy_setopt(curl_, CURLOPT_USERPWD, userpwd.c_str());
  }

  CURLcode res = curl_easy_perform(curl_);
  
  if (headerList) {
    curl_slist_free_all(headerList);
  }

  if (res != CURLE_OK) {
    throw std::runtime_error("SOAP request failed: " + std::string(curl_easy_strerror(res)));
  }

  long httpCode = 0;
  curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &httpCode);

  if (httpCode != 200) {
    throw std::runtime_error("SOAP HTTP error " + std::to_string(httpCode));
  }

  return responseBody;
}

json SOAPEngine::parseSOAPResponse(const std::string &soapResponse) {
  Logger::warning(LogCategory::DATABASE, "SOAPEngine",
                  "SOAP response parsing requires XML parser - returning empty JSON");
  return json::object();
}

json SOAPEngine::callMethod(const std::string &method, const json &parameters) {
  std::string soapBody = buildSOAPEnvelope(method, parameters);
  std::string response = executeSOAPRequest(soapBody);
  return parseSOAPResponse(response);
}

std::vector<json> SOAPEngine::parseResponse(const std::string &response) {
  json parsed = parseSOAPResponse(response);
  std::vector<json> result;
  result.push_back(parsed);
  return result;
}

void SOAPEngine::setConfig(const SOAPConfig &config) {
  config_ = config;
}
