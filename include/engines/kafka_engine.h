#ifndef KAFKA_ENGINE_H
#define KAFKA_ENGINE_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <memory>
#include <map>
#include <functional>

using json = nlohmann::json;

#ifdef HAVE_KAFKA

// KafkaEngine: Wrapper de librdkafka para integración con Apache Kafka
class KafkaEngine {
public:
  struct KafkaConfig {
    std::string brokers;              // Comma-separated list of brokers
    std::string clientId{"DataSync"};
    std::string groupId;              // Consumer group ID
    std::string securityProtocol{"plaintext"};  // plaintext, ssl, sasl_plaintext, sasl_ssl
    std::string saslMechanism;        // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
    std::string saslUsername;
    std::string saslPassword;
    std::string autoOffsetReset{"latest"};  // earliest, latest, none
    bool enableAutoCommit{true};
    int sessionTimeoutMs{30000};
    int maxPollRecords{500};
    std::map<std::string, std::string> kafkaConf;  // Additional Kafka configs
  };

  struct KafkaMessage {
    std::string topic;
    int partition{-1};
    int64_t offset{-1};
    std::string key;
    std::string value;
    std::map<std::string, std::string> headers;
    int64_t timestamp{0};
  };

  struct KafkaStats {
    int64_t messagesProduced{0};
    int64_t messagesConsumed{0};
    int64_t bytesProduced{0};
    int64_t bytesConsumed{0};
    int64_t errors{0};
    double latencyMs{0.0};
  };

  explicit KafkaEngine(const KafkaConfig& config);
  ~KafkaEngine();

  // Inicializar conexión con Kafka
  bool initialize();

  // Cerrar conexión
  void shutdown();

  // Verificar si Kafka está disponible
  bool isAvailable() const { return available_; }

  // Producer methods
  bool publishMessage(const std::string& topic, const std::string& key, 
                      const std::string& value, 
                      const std::map<std::string, std::string>& headers = {});

  // Consumer methods
  bool subscribe(const std::vector<std::string>& topics);
  std::vector<KafkaMessage> pollMessages(int timeoutMs = 1000, int maxMessages = 100);
  bool commitOffset(const std::string& topic, int partition, int64_t offset);
  bool commitOffsets();  // Commit all offsets

  // Obtener estadísticas
  KafkaStats getStats() const;

private:
  KafkaConfig config_;
  bool initialized_{false};
  bool available_{false};
  void* producer_{nullptr};  // rd_kafka_t* producer
  void* consumer_{nullptr};  // rd_kafka_t* consumer

  // Validar configuración
  bool validateConfig() const;

  // Detectar si Kafka está disponible
  bool detectKafkaAvailability();

  // Callbacks de librdkafka
  static void deliveryCallback(void* payload, size_t len, int error_code, void* opaque);
};

#else

// Stub implementation cuando Kafka no está disponible
class KafkaEngine {
public:
  struct KafkaConfig {
    std::string brokers;
    std::string clientId{"DataSync"};
    std::string groupId;
    std::string securityProtocol{"plaintext"};
    std::string saslMechanism;
    std::string saslUsername;
    std::string saslPassword;
    std::string autoOffsetReset{"latest"};
    bool enableAutoCommit{true};
    int sessionTimeoutMs{30000};
    int maxPollRecords{500};
    std::map<std::string, std::string> kafkaConf;
  };

  struct KafkaMessage {
    std::string topic;
    int partition{-1};
    int64_t offset{-1};
    std::string key;
    std::string value;
    std::map<std::string, std::string> headers;
    int64_t timestamp{0};
  };

  struct KafkaStats {
    int64_t messagesProduced{0};
    int64_t messagesConsumed{0};
    int64_t bytesProduced{0};
    int64_t bytesConsumed{0};
    int64_t errors{0};
    double latencyMs{0.0};
  };

  explicit KafkaEngine(const KafkaConfig& config [[maybe_unused]]) {
    Logger::warning(LogCategory::SYSTEM, "KafkaEngine",
                    "Kafka support not compiled. Install librdkafka and rebuild with HAVE_KAFKA.");
  }

  ~KafkaEngine() = default;

  bool initialize() { return false; }
  void shutdown() {}
  bool isAvailable() const { return false; }
  bool publishMessage(const std::string&, const std::string&, const std::string&, 
                      const std::map<std::string, std::string>& = {}) { return false; }
  bool subscribe(const std::vector<std::string>&) { return false; }
  std::vector<KafkaMessage> pollMessages(int = 1000, int = 100) { return {}; }
  bool commitOffset(const std::string&, int, int64_t) { return false; }
  bool commitOffsets() { return false; }
  KafkaStats getStats() const { return KafkaStats{}; }
};

#endif // HAVE_KAFKA

#endif // KAFKA_ENGINE_H
