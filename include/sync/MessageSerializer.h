#ifndef MESSAGE_SERIALIZER_H
#define MESSAGE_SERIALIZER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <map>
#include <memory>
#include <mutex>

using json = nlohmann::json;

// MessageSerializer: Serializa/deserializa mensajes en diferentes formatos
class MessageSerializer {
public:
  enum class SerializationFormat {
    AVRO,
    PROTOBUF,
    JSON_SCHEMA,
    JSON
  };

  explicit MessageSerializer();
  ~MessageSerializer();

  // Serializar mensaje
  std::string serialize(const json& message, SerializationFormat format,
                       const std::string& schemaName = "");

  // Deserializar mensaje
  json deserialize(const std::string& data, SerializationFormat format,
                  const std::string& schemaName = "");

  // Registrar schema (para Avro, Protobuf, JSON Schema)
  bool registerSchema(const std::string& schemaName, const std::string& schemaDefinition,
                     SerializationFormat format);

  // Validar mensaje contra schema
  bool validateSchema(const json& message, const std::string& schemaName);

  // Obtener schema registrado
  std::string getSchema(const std::string& schemaName) const;

private:
  mutable std::mutex schemasMutex_;
  std::map<std::string, std::string> avroSchemas_;
  std::map<std::string, std::string> protobufSchemas_;
  std::map<std::string, std::string> jsonSchemas_;

  // Serializar/deserializar según formato
  std::string serializeAvro(const json& message, const std::string& schemaName);
  json deserializeAvro(const std::string& data, const std::string& schemaName);
  std::string serializeProtobuf(const json& message, const std::string& schemaName);
  json deserializeProtobuf(const std::string& data, const std::string& schemaName);
  std::string serializeJSONSchema(const json& message, const std::string& schemaName);
  json deserializeJSONSchema(const std::string& data, const std::string& schemaName);
};

#endif // MESSAGE_SERIALIZER_H
