#ifndef XML_ENGINE_H
#define XML_ENGINE_H

#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <fstream>
#include <memory>

using json = nlohmann::json;

struct XMLConfig {
  std::string root_element = "";
  std::string record_element = "";
  bool flatten_nested = false;
  std::string encoding = "UTF-8";
  bool validate_schema = false;
  std::string schema_path = "";
};

class XMLEngine {
  std::string source_;
  XMLConfig config_;

  std::string readFromFile(const std::string &filePath);
  std::string readFromURL(const std::string &url);
  json parseXMLToJSON(const std::string &xmlContent);
  json flattenJSON(const json &data);
  std::vector<json> extractRecords(const json &parsedXML);

public:
  explicit XMLEngine(const std::string &source, 
                    const XMLConfig &config = XMLConfig());
  ~XMLEngine() = default;

  XMLEngine(const XMLEngine &) = delete;
  XMLEngine &operator=(const XMLEngine &) = delete;

  std::vector<json> parseXML();
  std::vector<std::string> detectColumns(const std::vector<json> &data);
  void setConfig(const XMLConfig &config);
};

#endif
