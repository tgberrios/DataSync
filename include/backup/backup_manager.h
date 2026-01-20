#ifndef BACKUP_MANAGER_H
#define BACKUP_MANAGER_H

#include "core/logger.h"
#include <string>
#include <map>

enum class BackupType {
  STRUCTURE,
  DATA,
  FULL,
  CONFIG
};

struct ConnectionInfo {
  std::string host;
  int port;
  std::string database;
  std::string user;
  std::string password;
};

struct BackupConfig {
  std::string backup_name;
  std::string db_engine;
  std::string connection_string;
  std::string database_name;
  BackupType backup_type;
  std::string file_path;
};

struct BackupResult {
  bool success;
  std::string file_path;
  int64_t file_size;
  std::string error_message;
  int duration_seconds;
};

class BackupManager {
public:
  static ConnectionInfo parseConnectionString(const std::string& conn_str, 
                                              const std::string& db_engine);
  
  static BackupResult createBackup(const BackupConfig& config);
  
  static bool restoreBackup(const std::string& backup_file,
                            const std::string& connection_string,
                            const std::string& db_engine,
                            const std::string& database_name);
  
  static std::string getFileExtension(const std::string& db_engine);
  
  static BackupType parseBackupType(const std::string& backup_type_str);

private:
  static BackupResult createPostgreSQLBackup(const ConnectionInfo& conn_info,
                                              const std::string& database_name,
                                              BackupType backup_type,
                                              const std::string& output_path);
  
  static BackupResult createMariaDBBackup(const ConnectionInfo& conn_info,
                                           const std::string& database_name,
                                           BackupType backup_type,
                                           const std::string& output_path);
  
  static BackupResult createMongoDBBackup(const ConnectionInfo& conn_info,
                                           const std::string& database_name,
                                           BackupType backup_type,
                                           const std::string& output_path);
  
  static BackupResult createOracleBackup(const ConnectionInfo& conn_info,
                                         const std::string& database_name,
                                         BackupType backup_type,
                                         const std::string& output_path);
  
  static int64_t getFileSize(const std::string& file_path);
};

#endif
