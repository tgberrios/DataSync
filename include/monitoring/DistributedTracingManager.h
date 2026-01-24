#ifndef DISTRIBUTED_TRACING_MANAGER_H
#define DISTRIBUTED_TRACING_MANAGER_H

#include "core/logger.h"
#include "third_party/json.hpp"
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>

using json = nlohmann::json;

// DistributedTracingManager: Gestión de distributed tracing con OpenTelemetry/Jaeger
class DistributedTracingManager {
public:
  struct TraceContext {
    std::string traceId;
    std::string spanId;
    std::string parentSpanId;
    int flags;
  };

  struct Span {
    std::string spanId;
    std::string traceId;
    std::string parentSpanId;
    std::string operationName;
    std::string serviceName;
    std::chrono::system_clock::time_point startTime;
    std::chrono::system_clock::time_point endTime;
    int64_t durationMicroseconds;
    std::map<std::string, std::string> tags;
    std::vector<json> logs;
    std::string status; // "ok", "error"
    std::string errorMessage;
  };

  struct Trace {
    std::string traceId;
    std::string serviceName;
    std::chrono::system_clock::time_point startTime;
    std::chrono::system_clock::time_point endTime;
    int64_t durationMicroseconds;
    int spanCount;
    std::vector<Span> spans;
  };

  explicit DistributedTracingManager(const std::string& connectionString);
  ~DistributedTracingManager() = default;

  // Generar nuevo trace ID (UUID v4)
  std::string generateTraceId();

  // Generar nuevo span ID
  std::string generateSpanId();

  // Crear nuevo span
  std::string startSpan(const std::string& traceId, const std::string& parentSpanId,
                        const std::string& operationName, const std::string& serviceName = "datasync");

  // Finalizar span
  bool endSpan(const std::string& spanId, const std::string& status = "ok",
               const std::string& errorMessage = "");

  // Agregar tag a span
  bool addSpanTag(const std::string& spanId, const std::string& key, const std::string& value);

  // Agregar log a span
  bool addSpanLog(const std::string& spanId, const std::string& message, const json& fields = json::object());

  // Obtener trace completo
  std::unique_ptr<Trace> getTrace(const std::string& traceId);

  // Listar traces
  std::vector<Trace> listTraces(const std::string& serviceName = "", int limit = 100);

  // Context propagation: inject trace context en headers
  std::map<std::string, std::string> injectContext(const TraceContext& context);

  // Context propagation: extract trace context desde headers
  std::unique_ptr<TraceContext> extractContext(const std::map<std::string, std::string>& headers);

  // Export trace a Jaeger (HTTP)
  bool exportToJaeger(const std::string& traceId, const std::string& jaegerEndpoint);

  // Configurar export endpoint
  bool configureExport(const std::string& exportType, const std::string& endpoint);

  // Export traces pendientes
  int exportPendingTraces();

private:
  std::string connectionString_;
  std::string jaegerEndpoint_;
  std::map<std::string, Span> activeSpans_;

  std::string generateUUID();
  bool saveTraceToDatabase(const Trace& trace);
  bool saveSpanToDatabase(const Span& span);
  std::vector<Span> loadSpansFromDatabase(const std::string& traceId);
  std::string buildJaegerPayload(const Trace& trace);
};

#endif // DISTRIBUTED_TRACING_MANAGER_H
