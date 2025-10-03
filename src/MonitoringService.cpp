#include "MonitoringService.h"
#include "DataQuality.h"
#include "MetricsCollector.h"
#include <pqxx/pqxx>
#include <thread>

MonitoringService::MonitoringService() {
  dataQuality = new DataQuality();
  metricsCollector = new MetricsCollector();
  Logger::getInstance().info(LogCategory::MONITORING,
                             "MonitoringService initialized");
}

MonitoringService::~MonitoringService() {
  delete dataQuality;
  delete metricsCollector;
}

void MonitoringService::startMonitoring() {
  monitoring = true;
  Logger::getInstance().info(LogCategory::MONITORING,
                             "Monitoring service started");
}

void MonitoringService::stopMonitoring() {
  monitoring = false;
  Logger::getInstance().info(LogCategory::MONITORING,
                             "Monitoring service stopped");
}

bool MonitoringService::isMonitoring() const { return monitoring.load(); }

void MonitoringService::runQualityChecks() {
  if (!monitoring)
    return;

  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Starting data quality validation cycle");

    auto conn = createConnection();
    if (!conn) {
      Logger::getInstance().error(
          LogCategory::MONITORING,
          "Cannot establish database connection for quality checks");
      return;
    }

    validateTablesByEngine("MariaDB");
    validateTablesByEngine("MSSQL");
    validateTablesByEngine("PostgreSQL");

    delete static_cast<pqxx::connection *>(conn);

    Logger::getInstance().info(LogCategory::MONITORING,
                               "Data quality validation cycle completed");
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error in quality checks: " +
                                    std::string(e.what()));
  }
}

void MonitoringService::validateTablesByEngine(const std::string &engine) {
  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Starting " + engine + " table validation");

    auto conn = createConnection();
    if (!conn) {
      Logger::getInstance().error(LogCategory::MONITORING,
                                  "Cannot establish connection for " + engine +
                                      " validation");
      return;
    }

    auto *pgConn = static_cast<pqxx::connection *>(conn);
    pqxx::work txn(*pgConn);
    auto tables =
        txn.exec("SELECT schema_name, table_name FROM metadata.catalog WHERE "
                 "db_engine = " +
                 txn.quote(engine) + " AND status = 'LISTENING_CHANGES'");
    txn.commit();

    size_t tableCount = 0;
    for (const auto &row : tables) {
      try {
        std::string schema = row[0].as<std::string>();
        std::string table = row[1].as<std::string>();

        Logger::getInstance().info(LogCategory::MONITORING,
                                   "Validating " + engine +
                                       " table: " + schema + "." + table);

        dataQuality->validateTable(*pgConn, schema, table, engine);
        tableCount++;
      } catch (const std::exception &e) {
        Logger::getInstance().error(LogCategory::MONITORING,
                                    "Error validating " + engine + " table " +
                                        row[0].as<std::string>() + "." +
                                        row[1].as<std::string>() + ": " +
                                        std::string(e.what()));
      }
    }

    delete pgConn;

    logQualityResults(engine, tableCount);
  } catch (const std::exception &e) {
    Logger::getInstance().error(
        LogCategory::MONITORING,
        "Critical error in " + engine +
            " table validation: " + std::string(e.what()));
  }
}

void MonitoringService::collectSystemMetrics() {
  if (!monitoring)
    return;

  try {
    Logger::getInstance().info(LogCategory::MONITORING,
                               "Collecting system metrics");
    metricsCollector->collectAllMetrics();
    logMetricsResults();
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error collecting system metrics: " +
                                    std::string(e.what()));
  }
}

void *MonitoringService::createConnection() {
  try {
    std::string connStr = DatabaseConfig::getPostgresConnectionString();
    if (connStr.empty()) {
      Logger::getInstance().error(LogCategory::MONITORING,
                                  "Empty PostgreSQL connection string");
      return nullptr;
    }

    auto conn = new pqxx::connection(connStr);
    conn->set_session_var("statement_timeout", "30000");
    conn->set_session_var("lock_timeout", "10000");

    validateConnection(conn);
    return conn;
  } catch (const std::exception &e) {
    Logger::getInstance().error(LogCategory::MONITORING,
                                "Error creating database connection: " +
                                    std::string(e.what()));
    return nullptr;
  }
}

void MonitoringService::validateConnection(void *conn) {
  auto *pgConn = static_cast<pqxx::connection *>(conn);
  if (!pgConn->is_open()) {
    throw std::runtime_error("Database connection is not open");
  }
}

void MonitoringService::logQualityResults(const std::string &engine,
                                          size_t tableCount) {
  Logger::getInstance().info(LogCategory::MONITORING,
                             engine + " table validation completed - " +
                                 std::to_string(tableCount) +
                                 " tables processed");
}

void MonitoringService::logMetricsResults() {
  Logger::getInstance().info(LogCategory::MONITORING,
                             "System metrics collection completed");
}
