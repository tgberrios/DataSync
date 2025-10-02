#include "DDLExporterInterface.h"
#include "logger.h"
#include <algorithm>

DDLExporterInterface::DDLExporterInterface(DatabaseConnectionManager &connManager, DDLFileManager &fileManager)
    : connectionManager(connManager), fileManager(fileManager) {
}

std::string DDLExporterInterface::escapeSQL(const std::string &value) {
  std::string escaped = value;
  size_t pos = 0;
  while ((pos = escaped.find("'", pos)) != std::string::npos) {
    escaped.replace(pos, 1, "''");
    pos += 2;
  }
  return escaped;
}

void DDLExporterInterface::logError(const std::string &operation, const std::string &error) {
  Logger::error(LogCategory::DDL_EXPORT, "DDLExporterInterface", 
                operation + " error: " + error);
}

void DDLExporterInterface::logInfo(const std::string &operation, const std::string &message) {
  Logger::info(LogCategory::DDL_EXPORT, "DDLExporterInterface", 
               operation + ": " + message);
}
