#ifndef REDIS_STREAMS_ENGINE_H
#define REDIS_STREAMS_ENGINE_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <memory>
#include <map>

using json = nlohmann::json;

#ifdef HAVE_REDIS

// RedisStreamsEngine: Wrapper de hiredis para integración con Redis Streams
class RedisStreamsEngine {
public:
  struct RedisStreamsConfig {
    std::string host{"localhost"};
    int port{6379};
    std::string password;
    std::string streamName;
    std::string consumerGroup;
    std::string consumerName;
    int maxLen{0};  // 0 = no limit, >0 = approximate max length
    bool approximateMaxLen{true};
    int blockMs{1000};  // Block time for XREAD/XREADGROUP
    int count{100};     // Max entries per read
  };

  struct StreamEntry {
    std::string id;  // Stream entry ID
    std::map<std::string, std::string> fields;
  };

  struct StreamReadResult {
    std::string streamName;
    std::vector<StreamEntry> entries;
  };

  struct PendingEntry {
    std::string id;
    std::string consumerName;
    int64_t idleTimeMs{0};
    int deliveryCount{0};
  };

  struct RedisStreamsStats {
    int64_t messagesAdded{0};
    int64_t messagesRead{0};
    int64_t messagesAcknowledged{0};
    int64_t pendingMessages{0};
    int64_t errors{0};
  };

  explicit RedisStreamsEngine(const RedisStreamsConfig& config);
  ~RedisStreamsEngine();

  // Inicializar conexión con Redis
  bool initialize();

  // Cerrar conexión
  void shutdown();

  // Verificar si Redis está disponible
  bool isAvailable() const { return available_; }

  // Stream operations
  std::string xadd(const std::string& stream, const std::map<std::string, std::string>& fields,
                   const std::string& id = "*");  // "*" = auto-generate ID
  std::vector<StreamReadResult> xread(const std::vector<std::string>& streams,
                                       const std::vector<std::string>& ids,
                                       int blockMs = 1000, int count = 100);
  std::vector<StreamReadResult> xreadgroup(const std::string& group, const std::string& consumer,
                                            const std::vector<std::string>& streams,
                                            const std::vector<std::string>& ids,
                                            int blockMs = 1000, int count = 100);
  bool xack(const std::string& stream, const std::string& group,
            const std::vector<std::string>& ids);
  std::vector<PendingEntry> xpending(const std::string& stream, const std::string& group,
                                      const std::string& start = "-",
                                      const std::string& end = "+",
                                      int count = 100,
                                      const std::string& consumer = "");
  bool xgroupCreate(const std::string& stream, const std::string& group,
                    const std::string& startId = "0", bool mkstream = true);
  bool xgroupDestroy(const std::string& stream, const std::string& group);

  // Obtener estadísticas
  RedisStreamsStats getStats() const;

private:
  RedisStreamsConfig config_;
  bool initialized_{false};
  bool available_{false};
  void* redisContext_{nullptr};  // redisContext*

  // Validar configuración
  bool validateConfig() const;

  // Detectar si Redis está disponible
  bool detectRedisAvailability();

  // Ejecutar comando Redis
  void* executeCommand(const std::string& command);
  void freeReply(void* reply);
};

#else

// Stub implementation cuando Redis no está disponible
class RedisStreamsEngine {
public:
  struct RedisStreamsConfig {
    std::string host{"localhost"};
    int port{6379};
    std::string password;
    std::string streamName;
    std::string consumerGroup;
    std::string consumerName;
    int maxLen{0};
    bool approximateMaxLen{true};
    int blockMs{1000};
    int count{100};
  };

  struct StreamEntry {
    std::string id;
    std::map<std::string, std::string> fields;
  };

  struct StreamReadResult {
    std::string streamName;
    std::vector<StreamEntry> entries;
  };

  struct PendingEntry {
    std::string id;
    std::string consumerName;
    int64_t idleTimeMs{0};
    int deliveryCount{0};
  };

  struct RedisStreamsStats {
    int64_t messagesAdded{0};
    int64_t messagesRead{0};
    int64_t messagesAcknowledged{0};
    int64_t pendingMessages{0};
    int64_t errors{0};
  };

  explicit RedisStreamsEngine(const RedisStreamsConfig& config [[maybe_unused]]) {
    Logger::warning(LogCategory::SYSTEM, "RedisStreamsEngine",
                    "Redis support not compiled. Install hiredis and rebuild with HAVE_REDIS.");
  }

  ~RedisStreamsEngine() = default;

  bool initialize() { return false; }
  void shutdown() {}
  bool isAvailable() const { return false; }
  std::string xadd(const std::string&, const std::map<std::string, std::string>&,
                   const std::string& = "*") { return ""; }
  std::vector<StreamReadResult> xread(const std::vector<std::string>&,
                                       const std::vector<std::string>&,
                                       int = 1000, int = 100) { return {}; }
  std::vector<StreamReadResult> xreadgroup(const std::string&, const std::string&,
                                            const std::vector<std::string>&,
                                            const std::vector<std::string>&,
                                            int = 1000, int = 100) { return {}; }
  bool xack(const std::string&, const std::string&, const std::vector<std::string>&) { return false; }
  std::vector<PendingEntry> xpending(const std::string&, const std::string&,
                                      const std::string& = "-",
                                      const std::string& = "+",
                                      int = 100,
                                      const std::string& = "") { return {}; }
  bool xgroupCreate(const std::string&, const std::string&,
                    const std::string& = "0", bool = true) { return false; }
  bool xgroupDestroy(const std::string&, const std::string&) { return false; }
  RedisStreamsStats getStats() const { return RedisStreamsStats{}; }
};

#endif // HAVE_REDIS

#endif // REDIS_STREAMS_ENGINE_H
