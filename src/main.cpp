#include "core/Config.h"
#include "sync/StreamingData.h"
#include <atomic>
#include <csignal>
#include <iostream>
#include <cstdlib>

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

int main() {
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
