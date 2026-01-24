#include "engines/avro_engine.h"
#include "core/logger.h"
#include <fstream>
#include <sstream>
#include <memory>

#ifdef HAVE_AVRO_CPP
#include <avro/Stream.hh>
#include <avro/DataFile.hh>
#define HAVE_AVRO_CPP
#endif

AvroEngine::AvroEngine(const std::string &source, const AvroConfig &config)
    : source_(source), config_(config) {}

std::vector<uint8_t> AvroEngine::readFromFile(const std::string &filePath) {
  std::ifstream file(filePath, std::ios::binary);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open Avro file: " + filePath);
  }

  std::vector<uint8_t> buffer((std::istreambuf_iterator<char>(file)),
                               std::istreambuf_iterator<char>());
  file.close();

  return buffer;
}

json AvroEngine::parseSchema(const std::vector<uint8_t> &fileData) {
#ifdef HAVE_AVRO_CPP
  try {
    // Use file path directly if available, otherwise create InputStream from memory
    if (!source_.empty() && source_ != "memory") {
      avro::DataFileReader<avro::GenericDatum> reader(source_.c_str());
      avro::ValidSchema schema = reader.dataSchema();
      
      // Convert Avro schema to JSON
      std::ostringstream schemaStream;
      schema.toJson(schemaStream);
      return json::parse(schemaStream.str());
    } else {
      // For in-memory data, we need to write to temp file or use InputStream
      // For now, return empty schema if source is not a file path
      Logger::warning(LogCategory::DATABASE, "AvroEngine",
                      "Schema parsing from memory requires file path - returning empty schema");
      return json::object();
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "AvroEngine",
                  "Failed to parse schema: " + std::string(e.what()));
    return json::object();
  }
#else
  Logger::warning(LogCategory::DATABASE, "AvroEngine",
                  "Schema parsing requires avro-cpp library - returning empty schema");
  return json::object();
#endif
}

#ifdef HAVE_AVRO_CPP
json AvroEngine::avroValueToJson(const avro::GenericDatum &datum) {
  json result;
  
  if (datum.type() == avro::AVRO_NULL) {
    result = nullptr;
  } else if (datum.type() == avro::AVRO_BOOL) {
    result = datum.value<bool>();
  } else if (datum.type() == avro::AVRO_INT) {
    result = datum.value<int32_t>();
  } else if (datum.type() == avro::AVRO_LONG) {
    result = datum.value<int64_t>();
  } else if (datum.type() == avro::AVRO_FLOAT) {
    result = datum.value<float>();
  } else if (datum.type() == avro::AVRO_DOUBLE) {
    result = datum.value<double>();
  } else if (datum.type() == avro::AVRO_STRING) {
    result = datum.value<std::string>();
  } else if (datum.type() == avro::AVRO_BYTES) {
    auto bytes = datum.value<std::vector<uint8_t>>();
    result = std::string(bytes.begin(), bytes.end());
  } else if (datum.type() == avro::AVRO_RECORD) {
    const avro::GenericRecord &record = datum.value<avro::GenericRecord>();
    for (size_t i = 0; i < record.fieldCount(); ++i) {
      std::string fieldName = record.schema()->nameAt(i);
      result[fieldName] = avroValueToJson(record.fieldAt(i));
    }
  } else if (datum.type() == avro::AVRO_ARRAY) {
    const avro::GenericArray &array = datum.value<avro::GenericArray>();
    for (size_t i = 0; i < array.value().size(); ++i) {
      result.push_back(avroValueToJson(array.value()[i]));
    }
  } else if (datum.type() == avro::AVRO_MAP) {
    const avro::GenericMap &map = datum.value<avro::GenericMap>();
    for (const auto &pair : map.value()) {
      result[pair.first] = avroValueToJson(pair.second);
    }
  }
  
  return result;
}
#endif

std::vector<json> AvroEngine::parseAvroRecords(const std::vector<uint8_t> &fileData, const json &schema) {
  (void)schema; // Unused parameter
#ifdef HAVE_AVRO_CPP
  try {
    std::vector<json> records;
    
    // Use file path directly if available
    if (!source_.empty() && source_ != "memory") {
      avro::DataFileReader<avro::GenericDatum> reader(source_.c_str());
      
      avro::GenericDatum datum(reader.dataSchema());
      int count = 0;
      while (reader.read(datum) && (config_.max_records == 0 || count < config_.max_records)) {
        records.push_back(avroValueToJson(datum));
        count++;
      }
    } else {
      Logger::warning(LogCategory::DATABASE, "AvroEngine",
                      "Avro record parsing from memory requires file path - returning empty records");
    }
    
    return records;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "AvroEngine",
                  "Failed to parse Avro records: " + std::string(e.what()));
    return {};
  }
#else
  Logger::warning(LogCategory::DATABASE, "AvroEngine",
                  "Avro record parsing requires avro-cpp library - returning empty records");
  return {};
#endif
}

std::vector<json> AvroEngine::parseAvro() {
  std::vector<uint8_t> fileData = readFromFile(source_);
  json schema = parseSchema(fileData);
  return parseAvroRecords(fileData, schema);
}

std::vector<std::string> AvroEngine::detectColumns(const std::vector<json> &data) {
  if (data.empty()) {
    return {};
  }

  std::vector<std::string> columns;
  for (const auto &item : data[0].items()) {
    columns.push_back(item.key());
  }
  return columns;
}

void AvroEngine::setConfig(const AvroConfig &config) {
  config_ = config;
}
