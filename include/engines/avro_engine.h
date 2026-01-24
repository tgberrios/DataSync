#ifndef AVRO_ENGINE_H
#define AVRO_ENGINE_H

#include "third_party/json.hpp"
#include <string>
#include <vector>
#include <fstream>
#include <memory>

#ifdef HAVE_AVRO_CPP
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/Schema.hh>
#include <avro/ValidSchema.hh>
#endif

using json = nlohmann::json;

struct AvroConfig {
  std::string schema_path = "";
  bool use_embedded_schema = true;
  int max_records = 0;
};

class AvroEngine {
  std::string source_;
  AvroConfig config_;

  std::vector<uint8_t> readFromFile(const std::string &filePath);
  json parseSchema(const std::vector<uint8_t> &fileData);
  std::vector<json> parseAvroRecords(const std::vector<uint8_t> &fileData, 
                                    const json &schema);
#ifdef HAVE_AVRO_CPP
  json avroValueToJson(const avro::GenericDatum &datum);
#endif

public:
  explicit AvroEngine(const std::string &source, 
                     const AvroConfig &config = AvroConfig());
  ~AvroEngine() = default;

  AvroEngine(const AvroEngine &) = delete;
  AvroEngine &operator=(const AvroEngine &) = delete;

  std::vector<json> parseAvro();
  std::vector<std::string> detectColumns(const std::vector<json> &data);
  void setConfig(const AvroConfig &config);
};

#endif
