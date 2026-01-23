#include "core/Config.h"
#include "sync/StreamingData.h"
#include "backup/backup_manager.h"
#include "backup/backup_scheduler.h"
#include "sync/DBTExecutor.h"
#include "third_party/json.hpp"
#include <atomic>
#include <csignal>
#include <iostream>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <thread>

namespace {
constexpr int EXIT_SUCCESS_CODE = 0;
constexpr int EXIT_INIT_ERROR = 2;
constexpr int EXIT_EXECUTION_ERROR = 3;
constexpr int EXIT_CRITICAL_ERROR = 4;
constexpr int EXIT_UNKNOWN_ERROR = 5;
constexpr int EXIT_CONFIG_ERROR = 6;
constexpr int EXIT_SIGNAL_ERROR = 7;

std::atomic<bool> g_shutdownRequested{false};
std::atomic<StreamingData *> g_streamingData{nullptr};

void cleanupLogger() {
  try {
    Logger::shutdown();
  } catch (...) {
  }
}

void signalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    static std::atomic<bool> shutdownInProgress{false};
    if (shutdownInProgress.exchange(true)) {
      std::cerr << "\nForce shutdown requested, exiting immediately...\n";
      cleanupLogger();
      std::exit(EXIT_SUCCESS_CODE);
      return;
    }
    
    std::cerr << "\n\nShutdown signal received (Ctrl+C). Initiating graceful shutdown...\n";
    g_shutdownRequested.store(true);
    
    StreamingData *sd = g_streamingData.load();
    if (sd != nullptr) {
      Logger::info(LogCategory::SYSTEM, "signalHandler",
                   "Shutdown signal received, initiating graceful shutdown...");
      try {
        sd->shutdown();
      } catch (...) {
        std::cerr << "Error during shutdown, forcing exit...\n";
        cleanupLogger();
        std::exit(EXIT_SUCCESS_CODE);
      }
    }
  }
}
} // namespace

int handleBackupCommand(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "Usage: DataSync backup <create|schedule> <config_json>" << std::endl;
    return 1;
  }
  
  std::string command = argv[2];
  
  if (command == "create") {
    if (argc < 4) {
      std::cerr << "Usage: DataSync backup create <config_json>" << std::endl;
      return 1;
    }
    
    try {
      std::ifstream config_file(argv[3]);
      if (!config_file.is_open()) {
        std::cerr << "Error: Cannot open config file: " << argv[3] << std::endl;
        return 1;
      }
      
      nlohmann::json config_json;
      config_file >> config_json;
      
      BackupConfig config;
      config.backup_name = config_json["backup_name"];
      config.db_engine = config_json["db_engine"];
      config.connection_string = config_json["connection_string"];
      config.database_name = config_json["database_name"];
      config.backup_type = BackupManager::parseBackupType(config_json["backup_type"]);
      config.file_path = config_json["file_path"];
      
      auto start_time = std::chrono::steady_clock::now();
      BackupResult result = BackupManager::createBackup(config);
      auto end_time = std::chrono::steady_clock::now();
      int duration = std::chrono::duration_cast<std::chrono::seconds>(
        end_time - start_time).count();
      
      nlohmann::json output;
      output["success"] = result.success;
      output["file_path"] = result.file_path;
      output["file_size"] = result.file_size;
      output["duration_seconds"] = duration;
      if (!result.success) {
        output["error_message"] = result.error_message;
      }
      
      std::cout << output.dump(2) << std::endl;
      
      return result.success ? 0 : 1;
    } catch (const std::exception& e) {
      std::cerr << "Error: " << e.what() << std::endl;
      return 1;
    }
  } else if (command == "schedule") {
    DatabaseConfig::loadFromFile("config.json");
    if (!DatabaseConfig::isInitialized()) {
      std::cerr << "Error: Database configuration failed to initialize." << std::endl;
      return 1;
    }
    
    Logger::initialize();
    BackupScheduler::start();
    
    std::signal(SIGINT, [](int) { BackupScheduler::stop(); std::exit(0); });
    std::signal(SIGTERM, [](int) { BackupScheduler::stop(); std::exit(0); });
    
    while (BackupScheduler::isRunning()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    Logger::shutdown();
    return 0;
  } else {
    std::cerr << "Unknown backup command: " << command << std::endl;
    return 1;
  }
}

int handleDBTCommand(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "Usage: DataSync --execute-dbt-model <modelName>" << std::endl;
    std::cerr << "       DataSync --run-dbt-tests <modelName>" << std::endl;
    std::cerr << "       DataSync --compile-dbt-model <modelName>" << std::endl;
    return 1;
  }
  
  std::string command = argv[1];
  std::string modelName = argv[2];
  
  try {
    DatabaseConfig::loadFromFile("config.json");
    if (!DatabaseConfig::isInitialized()) {
      std::cerr << "Error: Database configuration failed to initialize." << std::endl;
      return 1;
    }
    
    Logger::initialize();
    
    std::string metadataConnStr = DatabaseConfig::getPostgresConnectionString();
    DBTExecutor executor(metadataConnStr);
    
    if (command == "--execute-dbt-model") {
      executor.executeModel(modelName);
      nlohmann::json output;
      output["success"] = true;
      output["message"] = "Model executed successfully";
      std::cout << output.dump(2) << std::endl;
      Logger::shutdown();
      return 0;
      
    } else if (command == "--run-dbt-tests") {
      executor.runAllTests(modelName);
      nlohmann::json output;
      output["success"] = true;
      output["message"] = "Tests executed successfully";
      std::cout << output.dump(2) << std::endl;
      Logger::shutdown();
      return 0;
      
    } else if (command == "--compile-dbt-model") {
      std::string compiledSQL = executor.compileModel(modelName);
      nlohmann::json output;
      output["success"] = true;
      output["compiled_sql"] = compiledSQL;
      std::cout << output.dump(2) << std::endl;
      Logger::shutdown();
      return 0;
      
    } else {
      std::cerr << "Unknown DBT command: " << command << std::endl;
      Logger::shutdown();
      return 1;
    }
    
  } catch (const std::exception& e) {
    nlohmann::json output;
    output["success"] = false;
    output["error"] = e.what();
    std::cerr << output.dump(2) << std::endl;
    Logger::shutdown();
    return 1;
  }
}

int main(int argc, char* argv[]) {
  if (argc > 1 && std::string(argv[1]) == "backup") {
    return handleBackupCommand(argc, argv);
  }
  
  if (argc > 1 && (std::string(argv[1]) == "--execute-dbt-model" || 
                   std::string(argv[1]) == "--run-dbt-tests" || 
                   std::string(argv[1]) == "--compile-dbt-model")) {
    return handleDBTCommand(argc, argv);
  }
  try {
    DatabaseConfig::loadFromFile("config.json");

    if (!DatabaseConfig::isInitialized()) {
      std::cerr << "Error: Database configuration failed to initialize. "
                   "Please check config.json or environment variables."
                << std::endl;
      return EXIT_CONFIG_ERROR;
    }

    std::string postgresPassword = DatabaseConfig::getPostgresPassword();
    if (postgresPassword.empty()) {
      std::cerr << "Warning: PostgreSQL password is empty. "
                   "Database connections may fail."
                << std::endl;
    }

    Logger::initialize();

    if (std::signal(SIGINT, signalHandler) == SIG_ERR) {
      std::cerr << "Error: Failed to register SIGINT handler" << std::endl;
      cleanupLogger();
      return EXIT_SIGNAL_ERROR;
    }

    if (std::signal(SIGTERM, signalHandler) == SIG_ERR) {
      std::cerr << "Error: Failed to register SIGTERM handler" << std::endl;
      cleanupLogger();
      return EXIT_SIGNAL_ERROR;
    }

    Logger::info(LogCategory::SYSTEM, "main", "DataSync started");
    Logger::info(LogCategory::SYSTEM, "main",
                 "Configuration loaded from config.json (DB: " +
                     DatabaseConfig::getPostgresDB() + "@" +
                     DatabaseConfig::getPostgresHost() + ")");

    std::cout << "\n";
    std::cout << "██████╗  █████╗ ████████╗ █████╗ ███████╗██╗   ██╗███╗   ██╗ "
                 "██████╗\n";
    std::cout << "██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔════╝╚██╗ ██╔╝████╗  "
                 "██║██╔════╝\n";
    std::cout << "██║  ██║███████║   ██║   ███████║███████╗ ╚████╔╝ ██╔██╗ "
                 "██║██║     \n";
    std::cout << "██║  ██║██╔══██║   ██║   ██╔══██║╚════██║  ╚██╔╝  "
                 "██║╚██╗██║██║     \n";
    std::cout << "██████╔╝██║  ██║   ██║   ██║  ██║███████║   ██║   ██║ "
                 "╚████║╚██████╗\n";
    std::cout << "╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═══╝ "
                 "╚═════╝\n";
    std::cout << "\n";
    std::cout
        << "          Enterprise Data Synchronization & Replication System\n";
    std::cout << "                            Version 2.0.0\n";
    std::cout << "\n";

    StreamingData sd;
    g_streamingData.store(&sd);

    bool initialized = false;
    try {
      sd.initialize();
      initialized = true;
      Logger::info(LogCategory::SYSTEM, "main",
                   "StreamingData initialized successfully");
    } catch (const std::exception &e) {
      Logger::error(LogCategory::SYSTEM, "main",
                    "Exception during StreamingData initialization: " +
                        std::string(e.what()));
      std::cerr << "Initialization error: " << e.what() << std::endl;
      g_streamingData.store(nullptr);
      cleanupLogger();
      return EXIT_INIT_ERROR;
    }

    if (!initialized) {
      g_streamingData.store(nullptr);
      cleanupLogger();
      return EXIT_INIT_ERROR;
    }

    try {
      sd.run([&]() { return g_shutdownRequested.load(); });
      
      if (g_shutdownRequested.load()) {
        Logger::info(LogCategory::SYSTEM, "main",
                     "DataSync shutdown requested, cleaning up...");
        sd.shutdown();
        Logger::info(LogCategory::SYSTEM, "main",
                     "DataSync shutdown completed successfully");
      } else {
        Logger::info(LogCategory::SYSTEM, "main",
                     "DataSync completed successfully");
      }
    } catch (const std::exception &e) {
      Logger::error(LogCategory::SYSTEM, "main",
                    "Exception during DataSync execution: " +
                        std::string(e.what()));
      std::cerr << "Execution error: " << e.what() << std::endl;
      if (g_shutdownRequested.load()) {
        try {
          sd.shutdown();
        } catch (...) {
        }
      }
      g_streamingData.store(nullptr);
      cleanupLogger();
      return EXIT_EXECUTION_ERROR;
    }

    g_streamingData.store(nullptr);
    cleanupLogger();
    return EXIT_SUCCESS_CODE;

  } catch (const std::exception &e) {
    std::cerr << "Critical error in main: " << e.what() << std::endl;
    g_streamingData.store(nullptr);
    cleanupLogger();
    return EXIT_CRITICAL_ERROR;
  } catch (...) {
    std::cerr << "Unknown critical error in main" << std::endl;
    g_streamingData.store(nullptr);
    cleanupLogger();
    return EXIT_UNKNOWN_ERROR;
  }
}
