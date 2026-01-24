#ifndef STREAM_PROCESSING_MANAGER_H
#define STREAM_PROCESSING_MANAGER_H

#include "engines/kafka_engine.h"
#include "engines/rabbitmq_engine.h"
#include "engines/redis_streams_engine.h"
#include "transformations/transformation_engine.h"
#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <memory>
#include <atomic>
#include <map>
#include <vector>
#include <functional>
#include <thread>
#include <mutex>

using json = nlohmann::json;

// StreamProcessingManager: Orquesta el procesamiento de streams desde
// diferentes sistemas de mensajería (Kafka, RabbitMQ, Redis Streams)
class StreamProcessingManager {
public:
  enum class StreamType {
    KAFKA,
    RABBITMQ,
    REDIS_STREAMS
  };

  enum class SerializationFormat {
    AVRO,
    PROTOBUF,
    JSON_SCHEMA,
    JSON
  };

  struct StreamConfig {
    StreamType streamType{StreamType::KAFKA};
    std::string topic;              // Para Kafka
    std::string queue;               // Para RabbitMQ
    std::string stream;              // Para Redis Streams
    std::string consumerGroup;
    std::string consumerName;
    SerializationFormat serializationFormat{SerializationFormat::JSON};
    std::string schemaRegistryUrl;  // Para Avro/Protobuf/JSON Schema
    json engineConfig;               // Configuración específica del engine
  };

  struct StreamMessage {
    std::string id;                  // Message ID
    std::string key;
    std::string value;
    std::map<std::string, std::string> headers;
    int64_t timestamp{0};
    std::string source;              // topic/queue/stream name
    json metadata;                   // Metadata adicional
  };

  struct StreamStats {
    int64_t messagesProcessed{0};
    int64_t messagesFailed{0};
    int64_t bytesProcessed{0};
    double averageLatencyMs{0.0};
    int64_t errors{0};
    std::map<std::string, int64_t> errorsByType;
    std::chrono::system_clock::time_point startTime;
    std::chrono::system_clock::time_point lastMessageTime;
  };

  struct ConsumerInfo {
    std::string consumerId;
    std::string consumerGroup;
    std::string consumerName;
    StreamType streamType;
    std::string source;              // topic/queue/stream
    bool isRunning{false};
    std::thread::id threadId;
    StreamStats stats;
  };

  explicit StreamProcessingManager(const StreamConfig& config);
  ~StreamProcessingManager();

  // Inicializar el manager
  bool initialize();

  // Cerrar el manager
  void shutdown();

  // Iniciar consumidor de stream
  std::string startConsumer(const StreamConfig& config,
                            std::function<bool(const StreamMessage&)> messageHandler);

  // Detener consumidor
  bool stopConsumer(const std::string& consumerId);

  // Procesar stream (polling loop)
  void processStream(const std::string& consumerId,
                    std::function<bool(const StreamMessage&)> messageHandler);

  // Obtener estadísticas de un consumidor
  StreamStats getStreamStats(const std::string& consumerId) const;

  // Obtener información de todos los consumidores
  std::vector<ConsumerInfo> getConsumers() const;

  // Verificar disponibilidad de engines
  bool isKafkaAvailable() const;
  bool isRabbitMQAvailable() const;
  bool isRedisAvailable() const;

private:
  StreamConfig defaultConfig_;
  bool initialized_{false};
  std::atomic<bool> running_{true};
  
  // Engines de mensajería
  std::unique_ptr<KafkaEngine> kafkaEngine_;
  std::unique_ptr<RabbitMQEngine> rabbitMQEngine_;
  std::unique_ptr<RedisStreamsEngine> redisEngine_;

  // Transformation engine para procesar mensajes
  std::unique_ptr<TransformationEngine> transformationEngine_;

  // Consumidores activos
  mutable std::mutex consumersMutex_;
  std::map<std::string, ConsumerInfo> consumers_;
  std::map<std::string, std::thread> consumerThreads_;

  // Crear engine según tipo
  bool createEngine(StreamType type, const json& engineConfig);

  // Convertir mensaje del engine a StreamMessage
  StreamMessage convertKafkaMessage(const KafkaEngine::KafkaMessage& msg);
  StreamMessage convertRabbitMQMessage(const RabbitMQEngine::RabbitMQMessage& msg);
  StreamMessage convertRedisMessage(const RedisStreamsEngine::StreamEntry& entry,
                                     const std::string& streamName);

  // Validar configuración
  bool validateConfig(const StreamConfig& config) const;

  // Generar ID único para consumidor
  std::string generateConsumerId() const;
};

#endif // STREAM_PROCESSING_MANAGER_H
