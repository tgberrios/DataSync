#ifndef RABBITMQ_ENGINE_H
#define RABBITMQ_ENGINE_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <memory>
#include <map>

using json = nlohmann::json;

#ifdef HAVE_RABBITMQ

// RabbitMQEngine: Wrapper de rabbitmq-c para integración con RabbitMQ
class RabbitMQEngine {
public:
  struct RabbitMQConfig {
    std::string host{"localhost"};
    int port{5672};
    std::string vhost{"/"};
    std::string username{"guest"};
    std::string password{"guest"};
    std::string exchange;
    std::string exchangeType{"direct"};  // direct, topic, fanout, headers
    std::string routingKey;
    std::string queueName;
    bool durable{true};
    bool autoDelete{false};
    bool exclusive{false};
    std::map<std::string, std::string> arguments;  // Queue/exchange arguments
  };

  struct RabbitMQMessage {
    std::string exchange;
    std::string routingKey;
    std::string body;
    std::map<std::string, std::string> headers;
    std::string deliveryTag;
    bool redelivered{false};
  };

  struct RabbitMQStats {
    int64_t messagesPublished{0};
    int64_t messagesConsumed{0};
    int64_t bytesPublished{0};
    int64_t bytesConsumed{0};
    int64_t errors{0};
  };

  explicit RabbitMQEngine(const RabbitMQConfig& config);
  ~RabbitMQEngine();

  // Inicializar conexión con RabbitMQ
  bool initialize();

  // Cerrar conexión
  void shutdown();

  // Verificar si RabbitMQ está disponible
  bool isAvailable() const { return available_; }

  // Exchange and Queue management
  bool declareExchange(const std::string& exchange, const std::string& type, 
                       bool durable = true, bool autoDelete = false);
  bool declareQueue(const std::string& queue, bool durable = true, 
                    bool exclusive = false, bool autoDelete = false,
                    const std::map<std::string, std::string>& arguments = {});
  bool bindQueue(const std::string& queue, const std::string& exchange, 
                 const std::string& routingKey);

  // Producer methods
  bool publishMessage(const std::string& exchange, const std::string& routingKey,
                      const std::string& body,
                      const std::map<std::string, std::string>& headers = {});

  // Consumer methods
  bool consumeMessages(const std::string& queue, 
                       std::function<bool(const RabbitMQMessage&)> callback);
  bool ackMessage(const std::string& deliveryTag);
  bool nackMessage(const std::string& deliveryTag, bool requeue = true);

  // Obtener estadísticas
  RabbitMQStats getStats() const;

private:
  RabbitMQConfig config_;
  bool initialized_{false};
  bool available_{false};
  void* conn_{nullptr};  // amqp_connection_state_t
  void* channel_{nullptr};  // amqp_channel_t

  // Validar configuración
  bool validateConfig() const;

  // Detectar si RabbitMQ está disponible
  bool detectRabbitMQAvailability();
};

#else

// Stub implementation cuando RabbitMQ no está disponible
class RabbitMQEngine {
public:
  struct RabbitMQConfig {
    std::string host{"localhost"};
    int port{5672};
    std::string vhost{"/"};
    std::string username{"guest"};
    std::string password{"guest"};
    std::string exchange;
    std::string exchangeType{"direct"};
    std::string routingKey;
    std::string queueName;
    bool durable{true};
    bool autoDelete{false};
    bool exclusive{false};
    std::map<std::string, std::string> arguments;
  };

  struct RabbitMQMessage {
    std::string exchange;
    std::string routingKey;
    std::string body;
    std::map<std::string, std::string> headers;
    std::string deliveryTag;
    bool redelivered{false};
  };

  struct RabbitMQStats {
    int64_t messagesPublished{0};
    int64_t messagesConsumed{0};
    int64_t bytesPublished{0};
    int64_t bytesConsumed{0};
    int64_t errors{0};
  };

  explicit RabbitMQEngine(const RabbitMQConfig& config [[maybe_unused]]) {
    Logger::warning(LogCategory::SYSTEM, "RabbitMQEngine",
                    "RabbitMQ support not compiled. Install rabbitmq-c and rebuild with HAVE_RABBITMQ.");
  }

  ~RabbitMQEngine() = default;

  bool initialize() { return false; }
  void shutdown() {}
  bool isAvailable() const { return false; }
  bool declareExchange(const std::string&, const std::string&, bool = true, bool = false) { return false; }
  bool declareQueue(const std::string&, bool = true, bool = false, bool = false,
                    const std::map<std::string, std::string>& = {}) { return false; }
  bool bindQueue(const std::string&, const std::string&, const std::string&) { return false; }
  bool publishMessage(const std::string&, const std::string&, const std::string&,
                      const std::map<std::string, std::string>& = {}) { return false; }
  bool consumeMessages(const std::string&, std::function<bool(const RabbitMQMessage&)>) { return false; }
  bool ackMessage(const std::string&) { return false; }
  bool nackMessage(const std::string&, bool = true) { return false; }
  RabbitMQStats getStats() const { return RabbitMQStats{}; }
};

#endif // HAVE_RABBITMQ

#endif // RABBITMQ_ENGINE_H
