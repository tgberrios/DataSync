#include "sync/MessageSerializer.h"
#include "core/logger.h"
#include "engines/avro_engine.h"

MessageSerializer::MessageSerializer() {
  Logger::info(LogCategory::SYSTEM, "MessageSerializer", "Initializing MessageSerializer");
}

MessageSerializer::~MessageSerializer() {
  std::lock_guard<std::mutex> lock(schemasMutex_);
  avroSchemas_.clear();
  protobufSchemas_.clear();
  jsonSchemas_.clear();
}

std::string MessageSerializer::serialize(const json& message, SerializationFormat format,
                                         const std::string& schemaName) {
  switch (format) {
    case SerializationFormat::JSON:
      return message.dump();
    case SerializationFormat::AVRO:
      return serializeAvro(message, schemaName);
    case SerializationFormat::PROTOBUF:
      return serializeProtobuf(message, schemaName);
    case SerializationFormat::JSON_SCHEMA:
      return serializeJSONSchema(message, schemaName);
  }
  return "";
}

json MessageSerializer::deserialize(const std::string& data, SerializationFormat format,
                                   const std::string& schemaName) {
  switch (format) {
    case SerializationFormat::JSON:
      try {
        return json::parse(data);
      } catch (const json::parse_error& e) {
        Logger::error(LogCategory::SYSTEM, "MessageSerializer",
                     "JSON parse error: " + std::string(e.what()));
        return json(nullptr);
      }
    case SerializationFormat::AVRO:
      return deserializeAvro(data, schemaName);
    case SerializationFormat::PROTOBUF:
      return deserializeProtobuf(data, schemaName);
    case SerializationFormat::JSON_SCHEMA:
      return deserializeJSONSchema(data, schemaName);
  }
  return json(nullptr);
}

bool MessageSerializer::registerSchema(const std::string& schemaName,
                                      const std::string& schemaDefinition,
                                      SerializationFormat format) {
  std::lock_guard<std::mutex> lock(schemasMutex_);
  
  switch (format) {
    case SerializationFormat::AVRO:
      avroSchemas_[schemaName] = schemaDefinition;
      break;
    case SerializationFormat::PROTOBUF:
      protobufSchemas_[schemaName] = schemaDefinition;
      break;
    case SerializationFormat::JSON_SCHEMA:
      jsonSchemas_[schemaName] = schemaDefinition;
      break;
    case SerializationFormat::JSON:
      Logger::warning(LogCategory::SYSTEM, "MessageSerializer",
                     "JSON format does not require schema registration");
      return false;
  }

  Logger::info(LogCategory::SYSTEM, "MessageSerializer",
               "Schema registered: " + schemaName);
  return true;
}

bool MessageSerializer::validateSchema(const json& message, const std::string& schemaName) {
  // TODO: Implementar validación según formato
  // Para JSON Schema, usar librería de validación
  // Para Avro/Protobuf, validar estructura básica
  return true;
}

std::string MessageSerializer::getSchema(const std::string& schemaName) const {
  std::lock_guard<std::mutex> lock(schemasMutex_);
  
  auto it = avroSchemas_.find(schemaName);
  if (it != avroSchemas_.end()) {
    return it->second;
  }
  
  it = protobufSchemas_.find(schemaName);
  if (it != protobufSchemas_.end()) {
    return it->second;
  }
  
  it = jsonSchemas_.find(schemaName);
  if (it != jsonSchemas_.end()) {
    return it->second;
  }
  
  return "";
}

std::string MessageSerializer::serializeAvro(const json& message, const std::string& schemaName) {
  // TODO: Implementar serialización Avro usando AvroEngine
  // Por ahora retornar JSON como fallback
  return message.dump();
}

json MessageSerializer::deserializeAvro(const std::string& data, const std::string& schemaName) {
  // TODO: Implementar deserialización Avro
  try {
    return json::parse(data);
  } catch (...) {
    return json(nullptr);
  }
}

std::string MessageSerializer::serializeProtobuf(const json& message, const std::string& schemaName) {
  // TODO: Implementar serialización Protobuf
  // Por ahora retornar JSON como fallback
  return message.dump();
}

json MessageSerializer::deserializeProtobuf(const std::string& data, const std::string& schemaName) {
  // TODO: Implementar deserialización Protobuf
  try {
    return json::parse(data);
  } catch (...) {
    return json(nullptr);
  }
}

std::string MessageSerializer::serializeJSONSchema(const json& message, const std::string& schemaName) {
  // JSON Schema es principalmente para validación, la serialización es JSON normal
  return message.dump();
}

json MessageSerializer::deserializeJSONSchema(const std::string& data, const std::string& schemaName) {
  try {
    return json::parse(data);
  } catch (...) {
    return json(nullptr);
  }
}
