#include "backup/backup_manager.h"
#include "utils/string_utils.h"
#include <cstdio>
#include <fstream>
#include <sstream>
#include <chrono>
#include <thread>
#include <cstring>
#include <sys/stat.h>
#include <unistd.h>
#include <filesystem>
#include <cstdlib>

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/wait.h>
#include <fcntl.h>
#endif

ConnectionInfo BackupManager::parseConnectionString(const std::string& conn_str, 
                                                     const std::string& db_engine) {
  ConnectionInfo info;
  info.port = 0;
  
  if (db_engine == "PostgreSQL") {
    size_t pos = conn_str.find("://");
    if (pos != std::string::npos) {
      std::string url_part = conn_str.substr(pos + 3);
      size_t at_pos = url_part.find("@");
      if (at_pos != std::string::npos) {
        std::string auth = url_part.substr(0, at_pos);
        std::string host_part = url_part.substr(at_pos + 1);
        
        size_t colon_pos = auth.find(":");
        if (colon_pos != std::string::npos) {
          info.user = auth.substr(0, colon_pos);
          info.password = auth.substr(colon_pos + 1);
        }
        
        size_t slash_pos = host_part.find("/");
        if (slash_pos != std::string::npos) {
          std::string host_port = host_part.substr(0, slash_pos);
          info.database = host_part.substr(slash_pos + 1);
          
          size_t port_pos = host_port.find(":");
          if (port_pos != std::string::npos) {
            info.host = host_port.substr(0, port_pos);
            info.port = std::stoi(host_port.substr(port_pos + 1));
          } else {
            info.host = host_port;
            info.port = 5432;
          }
        }
      }
    }
  } else if (db_engine == "MariaDB") {
    std::istringstream iss(conn_str);
    std::string token;
    while (std::getline(iss, token, ';')) {
      size_t eq_pos = token.find("=");
      if (eq_pos != std::string::npos) {
        std::string key = StringUtils::toLower(StringUtils::trim(token.substr(0, eq_pos)));
        std::string value = StringUtils::trim(token.substr(eq_pos + 1));
        
        if (key == "host" || key == "server") {
          info.host = value;
        } else if (key == "port") {
          info.port = std::stoi(value);
        } else if (key == "database" || key == "db") {
          info.database = value;
        } else if (key == "user" || key == "uid") {
          info.user = value;
        } else if (key == "password" || key == "pwd") {
          info.password = value;
        }
      }
    }
    if (info.port == 0) info.port = 3306;
  } else if (db_engine == "MongoDB") {
    size_t pos = conn_str.find("://");
    if (pos != std::string::npos) {
      std::string url_part = conn_str.substr(pos + 3);
      size_t at_pos = url_part.find("@");
      if (at_pos != std::string::npos) {
        std::string auth = url_part.substr(0, at_pos);
        std::string host_part = url_part.substr(at_pos + 1);
        
        size_t colon_pos = auth.find(":");
        if (colon_pos != std::string::npos) {
          info.user = auth.substr(0, colon_pos);
          info.password = auth.substr(colon_pos + 1);
        }
        
        size_t slash_pos = host_part.find("/");
        if (slash_pos != std::string::npos) {
          info.database = host_part.substr(slash_pos + 1);
          size_t q_pos = info.database.find("?");
          if (q_pos != std::string::npos) {
            info.database = info.database.substr(0, q_pos);
          }
          host_part = host_part.substr(0, slash_pos);
        }
        
        size_t host_colon_pos = host_part.find(":");
        if (host_colon_pos != std::string::npos) {
          info.host = host_part.substr(0, host_colon_pos);
          info.port = std::stoi(host_part.substr(host_colon_pos + 1));
        } else {
          info.host = host_part;
          info.port = 27017;
        }
      }
    }
  } else if (db_engine == "Oracle") {
    std::istringstream iss(conn_str);
    std::string token;
    while (std::getline(iss, token, ';')) {
      size_t eq_pos = token.find("=");
      if (eq_pos != std::string::npos) {
        std::string key = StringUtils::toLower(StringUtils::trim(token.substr(0, eq_pos)));
        std::string value = StringUtils::trim(token.substr(eq_pos + 1));
        
        if (key == "host") {
          info.host = value;
        } else if (key == "port") {
          info.port = std::stoi(value);
        } else if (key == "servicename" || key == "sid") {
          info.database = value;
        } else if (key == "user") {
          info.user = value;
        } else if (key == "password") {
          info.password = value;
        }
      }
    }
    if (info.port == 0) info.port = 1521;
  }
  
  return info;
}

BackupType BackupManager::parseBackupType(const std::string& backup_type_str) {
  std::string lower = StringUtils::toLower(backup_type_str);
  if (lower == "structure") return BackupType::STRUCTURE;
  if (lower == "data") return BackupType::DATA;
  if (lower == "config") return BackupType::CONFIG;
  return BackupType::FULL;
}

std::string BackupManager::getFileExtension(const std::string& db_engine) {
  if (db_engine == "PostgreSQL") return "dump";
  if (db_engine == "MariaDB") return "sql";
  if (db_engine == "MongoDB") return "gz";
  if (db_engine == "Oracle") return "dmp";
  return "bak";
}

int64_t BackupManager::getFileSize(const std::string& file_path) {
  try {
    return std::filesystem::file_size(file_path);
  } catch (...) {
    return 0;
  }
}

BackupResult BackupManager::createPostgreSQLBackup(const ConnectionInfo& conn_info,
                                                    const std::string& database_name,
                                                    BackupType backup_type,
                                                    const std::string& output_path) {
  BackupResult result;
  result.success = false;
  result.file_path = output_path;
  result.file_size = 0;
  result.duration_seconds = 0;
  
  auto start_time = std::chrono::steady_clock::now();
  
  std::vector<std::string> args;
  args.push_back("pg_dump");
  
  if (backup_type == BackupType::STRUCTURE) {
    args.push_back("--schema-only");
  } else if (backup_type == BackupType::DATA) {
    args.push_back("--data-only");
  }
  
  args.push_back("-h");
  args.push_back(conn_info.host);
  args.push_back("-p");
  args.push_back(std::to_string(conn_info.port));
  args.push_back("-U");
  args.push_back(conn_info.user);
  args.push_back("-d");
  args.push_back(database_name);
  args.push_back("-f");
  args.push_back(output_path);
  args.push_back("-F");
  args.push_back("c");
  
  std::vector<const char*> argv;
  for (const auto& arg : args) {
    argv.push_back(arg.c_str());
  }
  argv.push_back(nullptr);
  
  pid_t pid = fork();
  if (pid == 0) {
    setenv("PGPASSWORD", conn_info.password.c_str(), 1);
    execvp("pg_dump", const_cast<char* const*>(argv.data()));
    exit(1);
  } else if (pid > 0) {
    int status;
    waitpid(pid, &status, 0);
    
    auto end_time = std::chrono::steady_clock::now();
    result.duration_seconds = std::chrono::duration_cast<std::chrono::seconds>(
      end_time - start_time).count();
    
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
      if (std::filesystem::exists(output_path)) {
        result.success = true;
        result.file_size = getFileSize(output_path);
      } else {
        result.error_message = "Backup file was not created";
      }
    } else {
      result.error_message = "pg_dump failed with exit code " + std::to_string(WEXITSTATUS(status));
    }
  } else {
    result.error_message = "Failed to fork process";
  }
  
  return result;
}

BackupResult BackupManager::createMariaDBBackup(const ConnectionInfo& conn_info,
                                                 const std::string& database_name,
                                                 BackupType backup_type,
                                                 const std::string& output_path) {
  BackupResult result;
  result.success = false;
  result.file_path = output_path;
  result.file_size = 0;
  result.duration_seconds = 0;
  
  auto start_time = std::chrono::steady_clock::now();
  
  std::vector<std::string> args;
  args.push_back("mysqldump");
  
  if (backup_type == BackupType::STRUCTURE) {
    args.push_back("--no-data");
  } else if (backup_type == BackupType::DATA) {
    args.push_back("--no-create-info");
  }
  
  args.push_back("-h");
  args.push_back(conn_info.host);
  args.push_back("-P");
  args.push_back(std::to_string(conn_info.port));
  args.push_back("-u");
  args.push_back(conn_info.user);
  args.push_back("-p" + conn_info.password);
  args.push_back(database_name);
  
  std::vector<const char*> argv;
  for (const auto& arg : args) {
    argv.push_back(arg.c_str());
  }
  argv.push_back(nullptr);
  
  pid_t pid = fork();
  if (pid == 0) {
    int fd = open(output_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
      dup2(fd, STDOUT_FILENO);
      close(fd);
    }
    execvp("mysqldump", const_cast<char* const*>(argv.data()));
    exit(1);
  } else if (pid > 0) {
    int status;
    waitpid(pid, &status, 0);
    
    auto end_time = std::chrono::steady_clock::now();
    result.duration_seconds = std::chrono::duration_cast<std::chrono::seconds>(
      end_time - start_time).count();
    
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
      if (std::filesystem::exists(output_path)) {
        result.success = true;
        result.file_size = getFileSize(output_path);
      } else {
        result.error_message = "Backup file was not created";
      }
    } else {
      result.error_message = "mysqldump failed with exit code " + std::to_string(WEXITSTATUS(status));
    }
  } else {
    result.error_message = "Failed to fork process";
  }
  
  return result;
}

BackupResult BackupManager::createMongoDBBackup(const ConnectionInfo& conn_info,
                                                const std::string& database_name,
                                                BackupType backup_type,
                                                const std::string& output_path) {
  BackupResult result;
  result.success = false;
  result.file_path = output_path;
  result.file_size = 0;
  result.duration_seconds = 0;
  
  auto start_time = std::chrono::steady_clock::now();
  
  std::string output_dir = std::filesystem::path(output_path).parent_path().string();
  
  std::vector<std::string> args;
  args.push_back("mongodump");
  
  if (backup_type == BackupType::STRUCTURE) {
    args.push_back("--gzip");
  }
  
  args.push_back("--host");
  args.push_back(conn_info.host + ":" + std::to_string(conn_info.port));
  
  if (!conn_info.user.empty()) {
    args.push_back("--username");
    args.push_back(conn_info.user);
  }
  
  if (!conn_info.password.empty()) {
    args.push_back("--password");
    args.push_back(conn_info.password);
  }
  
  args.push_back("--db");
  args.push_back(database_name);
  args.push_back("--out");
  args.push_back(output_dir);
  
  std::vector<const char*> argv;
  for (const auto& arg : args) {
    argv.push_back(arg.c_str());
  }
  argv.push_back(nullptr);
  
  pid_t pid = fork();
  if (pid == 0) {
    execvp("mongodump", const_cast<char* const*>(argv.data()));
    exit(1);
  } else if (pid > 0) {
    int status;
    waitpid(pid, &status, 0);
    
    auto end_time = std::chrono::steady_clock::now();
    result.duration_seconds = std::chrono::duration_cast<std::chrono::seconds>(
      end_time - start_time).count();
    
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
      std::string final_path = output_dir + "/" + database_name;
      if (std::filesystem::exists(final_path)) {
        result.success = true;
        result.file_path = final_path;
        result.file_size = getFileSize(final_path);
      } else {
        result.error_message = "MongoDB backup directory was not created";
      }
    } else {
      result.error_message = "mongodump failed with exit code " + std::to_string(WEXITSTATUS(status));
    }
  } else {
    result.error_message = "Failed to fork process";
  }
  
  return result;
}

BackupResult BackupManager::createOracleBackup(const ConnectionInfo& conn_info,
                                                const std::string& database_name,
                                                BackupType backup_type,
                                                const std::string& output_path) {
  BackupResult result;
  result.success = false;
  result.file_path = output_path;
  result.file_size = 0;
  result.duration_seconds = 0;
  
  auto start_time = std::chrono::steady_clock::now();
  
  std::string filename = std::filesystem::path(output_path).filename().string();
  std::string logfile = filename;
  size_t dot_pos = logfile.find_last_of(".");
  if (dot_pos != std::string::npos) {
    logfile.replace(dot_pos, logfile.length() - dot_pos, ".log");
  } else {
    logfile += ".log";
  }
  
  std::string expdp_command = "expdp " + conn_info.user + "/" + conn_info.password + "@" +
                              conn_info.host + ":" + std::to_string(conn_info.port) + "/" +
                              database_name + " ";
  
  if (backup_type == BackupType::STRUCTURE) {
    expdp_command += "SCHEMAS=" + conn_info.user + " CONTENT=METADATA_ONLY ";
  } else if (backup_type == BackupType::DATA) {
    expdp_command += "SCHEMAS=" + conn_info.user + " CONTENT=DATA_ONLY ";
  } else {
    expdp_command += "SCHEMAS=" + conn_info.user + " ";
  }
  
  expdp_command += "DIRECTORY=DATA_PUMP_DIR ";
  expdp_command += "DUMPFILE=" + filename + " ";
  expdp_command += "LOGFILE=" + logfile;
  
  pid_t pid = fork();
  if (pid == 0) {
    execl("/bin/bash", "bash", "-c", expdp_command.c_str(), nullptr);
    exit(1);
  } else if (pid > 0) {
    int status;
    waitpid(pid, &status, 0);
    
    auto end_time = std::chrono::steady_clock::now();
    result.duration_seconds = std::chrono::duration_cast<std::chrono::seconds>(
      end_time - start_time).count();
    
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
      if (std::filesystem::exists(output_path)) {
        result.success = true;
        result.file_size = getFileSize(output_path);
      } else {
        result.error_message = "Oracle backup file was not created";
      }
    } else {
      result.error_message = "expdp failed with exit code " + std::to_string(WEXITSTATUS(status));
    }
  } else {
    result.error_message = "Failed to fork process";
  }
  
  return result;
}

BackupResult BackupManager::createBackup(const BackupConfig& config) {
  ConnectionInfo conn_info = parseConnectionString(config.connection_string, config.db_engine);
  
  if (conn_info.host.empty()) {
    BackupResult result;
    result.success = false;
    result.error_message = "Failed to parse connection string";
    return result;
  }
  
  std::string output_dir = std::filesystem::path(config.file_path).parent_path().string();
  if (!std::filesystem::exists(output_dir)) {
    std::filesystem::create_directories(output_dir);
  }
  
  if (config.db_engine == "PostgreSQL") {
    return createPostgreSQLBackup(conn_info, config.database_name, 
                                  config.backup_type, config.file_path);
  } else if (config.db_engine == "MariaDB") {
    return createMariaDBBackup(conn_info, config.database_name, 
                               config.backup_type, config.file_path);
  } else if (config.db_engine == "MongoDB") {
    return createMongoDBBackup(conn_info, config.database_name, 
                               config.backup_type, config.file_path);
  } else if (config.db_engine == "Oracle") {
    return createOracleBackup(conn_info, config.database_name, 
                              config.backup_type, config.file_path);
  } else {
    BackupResult result;
    result.success = false;
    result.error_message = "Unsupported database engine: " + config.db_engine;
    return result;
  }
}

bool BackupManager::restoreBackup(const std::string& backup_file,
                                  const std::string& connection_string,
                                  const std::string& db_engine,
                                  const std::string& database_name) {
  Logger::warning(LogCategory::SYSTEM, "BackupManager", 
                  "Restore functionality not yet implemented");
  return false;
}
