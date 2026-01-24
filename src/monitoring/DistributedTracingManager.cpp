#include "monitoring/DistributedTracingManager.h"
#include "core/logger.h"
#include <pqxx/pqxx>
#include <random>
#include <sstream>
#include <iomanip>
#include <curl/curl.h>
#include <ctime>

DistributedTracingManager::DistributedTracingManager(const std::string& connectionString)
    : connectionString_(connectionString) {
  // Crear tablas si no existen
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Crear tabla distributed_traces
    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.distributed_traces ("
        "trace_id VARCHAR(36) PRIMARY KEY,"
        "service_name VARCHAR(100) NOT NULL,"
        "start_time TIMESTAMP NOT NULL,"
        "end_time TIMESTAMP,"
        "duration_microseconds BIGINT,"
        "span_count INTEGER DEFAULT 0,"
        "created_at TIMESTAMP DEFAULT NOW()"
        ")");

    // Crear tabla trace_spans
    txn.exec(
        "CREATE TABLE IF NOT EXISTS metadata.trace_spans ("
        "span_id VARCHAR(36) PRIMARY KEY,"
        "trace_id VARCHAR(36) NOT NULL,"
        "parent_span_id VARCHAR(36),"
        "operation_name VARCHAR(255) NOT NULL,"
        "service_name VARCHAR(100) NOT NULL,"
        "start_time TIMESTAMP NOT NULL,"
        "end_time TIMESTAMP,"
        "duration_microseconds BIGINT,"
        "tags JSONB DEFAULT '{}'::jsonb,"
        "logs JSONB DEFAULT '[]'::jsonb,"
        "status VARCHAR(20) DEFAULT 'ok',"
        "error_message TEXT,"
        "created_at TIMESTAMP DEFAULT NOW(),"
        "FOREIGN KEY (trace_id) REFERENCES metadata.distributed_traces(trace_id) ON DELETE CASCADE"
        ")");

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "DistributedTracingManager",
                  "Error creating tables: " + std::string(e.what()));
  }
}

std::string DistributedTracingManager::generateUUID() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 15);

  std::ostringstream oss;
  oss << std::hex;
  for (int i = 0; i < 32; ++i) {
    if (i == 8 || i == 12 || i == 16 || i == 20) {
      oss << "-";
    }
    oss << dis(gen);
  }
  return oss.str();
}

std::string DistributedTracingManager::generateTraceId() {
  return generateUUID();
}

std::string DistributedTracingManager::generateSpanId() {
  return generateUUID();
}

std::string DistributedTracingManager::startSpan(const std::string& traceId,
                                                 const std::string& parentSpanId,
                                                 const std::string& operationName,
                                                 const std::string& serviceName) {
  std::string spanId = generateSpanId();
  auto now = std::chrono::system_clock::now();

  Span span;
  span.spanId = spanId;
  span.traceId = traceId;
  span.parentSpanId = parentSpanId;
  span.operationName = operationName;
  span.serviceName = serviceName;
  span.startTime = now;
  span.status = "ok";

  activeSpans_[spanId] = span;

  return spanId;
}

bool DistributedTracingManager::endSpan(const std::string& spanId,
                                         const std::string& status,
                                         const std::string& errorMessage) {
  if (activeSpans_.find(spanId) == activeSpans_.end()) {
    return false;
  }

  Span& span = activeSpans_[spanId];
  span.endTime = std::chrono::system_clock::now();
  span.durationMicroseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                                   span.endTime - span.startTime)
                                   .count();
  span.status = status;
  span.errorMessage = errorMessage;

  // Guardar en base de datos
  bool saved = saveSpanToDatabase(span);
  if (saved) {
    activeSpans_.erase(spanId);
  }

  return saved;
}

bool DistributedTracingManager::addSpanTag(const std::string& spanId,
                                            const std::string& key,
                                            const std::string& value) {
  if (activeSpans_.find(spanId) == activeSpans_.end()) {
    return false;
  }

  activeSpans_[spanId].tags[key] = value;
  return true;
}

bool DistributedTracingManager::addSpanLog(const std::string& spanId,
                                            const std::string& message,
                                            const json& fields) {
  if (activeSpans_.find(spanId) == activeSpans_.end()) {
    return false;
  }

  json logEntry;
  logEntry["timestamp"] = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();
  logEntry["message"] = message;
  logEntry["fields"] = fields;

  activeSpans_[spanId].logs.push_back(logEntry);
  return true;
}

bool DistributedTracingManager::saveSpanToDatabase(const Span& span) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    json tagsJson = json::object();
    for (const auto& [key, value] : span.tags) {
      tagsJson[key] = value;
    }

    json logsJson = json::array();
    for (const auto& log : span.logs) {
      logsJson.push_back(log);
    }

    auto startTimeT = std::chrono::system_clock::to_time_t(span.startTime);
    std::tm startTm = *std::localtime(&startTimeT);
    std::ostringstream startTimeStr;
    startTimeStr << std::put_time(&startTm, "%Y-%m-%d %H:%M:%S");

    std::string endTimeStr;
    if (span.endTime.time_since_epoch().count() > 0) {
      auto endTimeT = std::chrono::system_clock::to_time_t(span.endTime);
      std::tm endTm = *std::localtime(&endTimeT);
      std::ostringstream endTimeOss;
      endTimeOss << std::put_time(&endTm, "%Y-%m-%d %H:%M:%S");
      endTimeStr = endTimeOss.str();
    }

    txn.exec_params(
        "INSERT INTO metadata.trace_spans "
        "(span_id, trace_id, parent_span_id, operation_name, service_name, start_time, end_time, "
        "duration_microseconds, tags, logs, status, error_message) "
        "VALUES ($1, $2, $3, $4, $5, $6::timestamp, $7::timestamp, $8, $9, $10, $11, $12) "
        "ON CONFLICT (span_id) DO UPDATE SET "
        "end_time = EXCLUDED.end_time, duration_microseconds = EXCLUDED.duration_microseconds, "
        "tags = EXCLUDED.tags, logs = EXCLUDED.logs, status = EXCLUDED.status, "
        "error_message = EXCLUDED.error_message",
        span.spanId, span.traceId,
        span.parentSpanId.empty() ? nullptr : span.parentSpanId, span.operationName,
        span.serviceName, startTimeStr.str(), endTimeStr.empty() ? nullptr : endTimeStr,
        span.durationMicroseconds, tagsJson.dump(), logsJson.dump(), span.status,
        span.errorMessage.empty() ? nullptr : span.errorMessage);

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "DistributedTracingManager",
                  "Error saving span: " + std::string(e.what()));
    return false;
  }
}

std::unique_ptr<DistributedTracingManager::Trace> DistributedTracingManager::getTrace(
    const std::string& traceId) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params("SELECT * FROM metadata.distributed_traces WHERE trace_id = $1",
                                   traceId);

    if (result.empty()) {
      return nullptr;
    }

    auto row = result[0];
    auto trace = std::make_unique<Trace>();
    trace->traceId = traceId;
    trace->serviceName = row["service_name"].as<std::string>();
    trace->startTime = std::chrono::system_clock::from_time_t(
        std::chrono::seconds(row["start_time"].as<int64_t>()).count());
    if (!row["end_time"].is_null()) {
      trace->endTime = std::chrono::system_clock::from_time_t(
          std::chrono::seconds(row["end_time"].as<int64_t>()).count());
    }
    trace->durationMicroseconds = row["duration_microseconds"].as<int64_t>();
    trace->spanCount = row["span_count"].as<int>();

    trace->spans = loadSpansFromDatabase(traceId);

    return trace;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "DistributedTracingManager",
                  "Error loading trace: " + std::string(e.what()));
    return nullptr;
  }
}

std::vector<DistributedTracingManager::Span> DistributedTracingManager::loadSpansFromDatabase(
    const std::string& traceId) {
  std::vector<Span> spans;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec_params(
        "SELECT * FROM metadata.trace_spans WHERE trace_id = $1 ORDER BY start_time", traceId);

    for (const auto& row : result) {
      Span span;
      span.spanId = row["span_id"].as<std::string>();
      span.traceId = row["trace_id"].as<std::string>();
      if (!row["parent_span_id"].is_null()) {
        span.parentSpanId = row["parent_span_id"].as<std::string>();
      }
      span.operationName = row["operation_name"].as<std::string>();
      span.serviceName = row["service_name"].as<std::string>();
      if (!row["start_time"].is_null()) {
        auto startTimeStr = row["start_time"].as<std::string>();
        std::tm tm = {};
        std::istringstream ss(startTimeStr);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        span.startTime = std::chrono::system_clock::from_time_t(std::mktime(&tm));
      }
      if (!row["end_time"].is_null()) {
        auto endTimeStr = row["end_time"].as<std::string>();
        std::tm tm = {};
        std::istringstream ss(endTimeStr);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        span.endTime = std::chrono::system_clock::from_time_t(std::mktime(&tm));
      }
      span.durationMicroseconds = row["duration_microseconds"].as<int64_t>();
      span.status = row["status"].as<std::string>();
      if (!row["error_message"].is_null()) {
        span.errorMessage = row["error_message"].as<std::string>();
      }

      // Parse tags
      if (!row["tags"].is_null()) {
        json tagsJson = json::parse(row["tags"].as<std::string>());
        for (auto it = tagsJson.begin(); it != tagsJson.end(); ++it) {
          span.tags[it.key()] = it.value().get<std::string>();
        }
      }

      // Parse logs
      if (!row["logs"].is_null()) {
        span.logs = json::parse(row["logs"].as<std::string>());
      }

      spans.push_back(span);
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "DistributedTracingManager",
                  "Error loading spans: " + std::string(e.what()));
  }

  return spans;
}

std::vector<DistributedTracingManager::Trace> DistributedTracingManager::listTraces(
    const std::string& serviceName, int limit) {
  std::vector<Trace> traces;

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string query = "SELECT * FROM metadata.distributed_traces";
    if (!serviceName.empty()) {
      query += " WHERE service_name = $1";
      query += " ORDER BY start_time DESC LIMIT $" + std::to_string(serviceName.empty() ? 1 : 2);
    } else {
      query += " ORDER BY start_time DESC LIMIT $1";
    }

    pqxx::result result;
    if (!serviceName.empty()) {
      result = txn.exec_params(query, serviceName, limit);
    } else {
      result = txn.exec_params(query, limit);
    }

    for (const auto& row : result) {
      std::string traceId = row["trace_id"].as<std::string>();
      auto trace = getTrace(traceId);
      if (trace) {
        traces.push_back(*trace);
      }
    }
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "DistributedTracingManager",
                  "Error listing traces: " + std::string(e.what()));
  }

  return traces;
}

std::map<std::string, std::string> DistributedTracingManager::injectContext(
    const TraceContext& context) {
  std::map<std::string, std::string> headers;
  headers["X-Trace-Id"] = context.traceId;
  headers["X-Span-Id"] = context.spanId;
  if (!context.parentSpanId.empty()) {
    headers["X-Parent-Span-Id"] = context.parentSpanId;
  }
  headers["X-Trace-Flags"] = std::to_string(context.flags);
  return headers;
}

std::unique_ptr<DistributedTracingManager::TraceContext> DistributedTracingManager::extractContext(
    const std::map<std::string, std::string>& headers) {
  auto context = std::make_unique<TraceContext>();

  if (headers.find("X-Trace-Id") != headers.end()) {
    context->traceId = headers.at("X-Trace-Id");
  }
  if (headers.find("X-Span-Id") != headers.end()) {
    context->spanId = headers.at("X-Span-Id");
  }
  if (headers.find("X-Parent-Span-Id") != headers.end()) {
    context->parentSpanId = headers.at("X-Parent-Span-Id");
  }
  if (headers.find("X-Trace-Flags") != headers.end()) {
    context->flags = std::stoi(headers.at("X-Trace-Flags"));
  }

  return context;
}

bool DistributedTracingManager::configureExport(const std::string& exportType,
                                                 const std::string& endpoint) {
  if (exportType == "jaeger") {
    jaegerEndpoint_ = endpoint;
    return true;
  }
  return false;
}

std::string DistributedTracingManager::buildJaegerPayload(const Trace& trace) {
  json payload;
  payload["id"] = 1;
  payload["traceId"] = trace.traceId;

  json spansJson = json::array();
  for (const auto& span : trace.spans) {
    json spanJson;
    spanJson["traceId"] = span.traceId;
    spanJson["id"] = span.spanId;
    if (!span.parentSpanId.empty()) {
      spanJson["parentId"] = span.parentSpanId;
    }
    spanJson["name"] = span.operationName;
    spanJson["kind"] = "SERVER";
    spanJson["timestamp"] = std::chrono::duration_cast<std::chrono::microseconds>(
                                span.startTime.time_since_epoch())
                                .count();
    spanJson["duration"] = span.durationMicroseconds;

    json tagsJson = json::array();
    tagsJson.push_back({{"key", "service.name"}, {"value", span.serviceName}});
    for (const auto& [key, value] : span.tags) {
      tagsJson.push_back({{"key", key}, {"value", value}});
    }
    spanJson["tags"] = tagsJson;

    if (!span.logs.empty()) {
      json logsJson = json::array();
      for (const auto& log : span.logs) {
        logsJson.push_back(log);
      }
      spanJson["logs"] = logsJson;
    }

    spansJson.push_back(spanJson);
  }
  payload["spans"] = spansJson;

  return payload.dump();
}

bool DistributedTracingManager::exportToJaeger(const std::string& traceId,
                                                const std::string& jaegerEndpoint) {
  auto trace = getTrace(traceId);
  if (!trace) {
    return false;
  }

  std::string payload = buildJaegerPayload(*trace);

  CURL* curl = curl_easy_init();
  if (!curl) {
    return false;
  }

  std::string url = jaegerEndpoint + "/api/traces";
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());

  struct curl_slist* headers = nullptr;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  CURLcode res = curl_easy_perform(curl);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  return res == CURLE_OK;
}

int DistributedTracingManager::exportPendingTraces() {
  // Export traces que no han sido exportados aún
  // Por simplicidad, exportamos los últimos 100 traces
  auto traces = listTraces("", 100);
  int exported = 0;

  for (const auto& trace : traces) {
    if (exportToJaeger(trace.traceId, jaegerEndpoint_)) {
      exported++;
    }
  }

  return exported;
}

bool DistributedTracingManager::saveTraceToDatabase(const Trace& trace) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string startTimeStr = std::to_string(
        std::chrono::duration_cast<std::chrono::seconds>(trace.startTime.time_since_epoch())
            .count());
    std::string endTimeStr = trace.endTime.time_since_epoch().count() > 0
                                 ? std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                                                       trace.endTime.time_since_epoch())
                                                       .count())
                                 : "NULL";

    txn.exec_params(
        "INSERT INTO metadata.distributed_traces "
        "(trace_id, service_name, start_time, end_time, duration_microseconds, span_count) "
        "VALUES ($1, $2, TO_TIMESTAMP($3), TO_TIMESTAMP($4), $5, $6) "
        "ON CONFLICT (trace_id) DO UPDATE SET "
        "end_time = EXCLUDED.end_time, duration_microseconds = EXCLUDED.duration_microseconds, "
        "span_count = EXCLUDED.span_count",
        trace.traceId, trace.serviceName, startTimeStr,
        endTimeStr.empty() || endTimeStr == "NULL" ? nullptr : endTimeStr,
        trace.durationMicroseconds, trace.spanCount);

    txn.commit();
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::MONITORING, "DistributedTracingManager",
                  "Error saving trace: " + std::string(e.what()));
    return false;
  }
}
