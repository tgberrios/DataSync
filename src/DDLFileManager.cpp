#include "DDLFileManager.h"
#include "logger.h"
#include <algorithm>
#include <ctime>
#include <fstream>

DDLFileManager::DDLFileManager(const std::string &basePath) : exportPath(basePath) {
}

void DDLFileManager::createFolderStructure() {
  try {
    std::filesystem::create_directories(exportPath);
    Logger::info(LogCategory::DDL_EXPORT, "DDLFileManager", 
                 "Created base export directory: " + exportPath);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLFileManager", 
                  "Error creating folder structure: " + std::string(e.what()));
  }
}

void DDLFileManager::createClusterFolder(const std::string &cluster) {
  try {
    std::string clusterPath = exportPath + "/" + sanitizeFileName(cluster);
    std::filesystem::create_directories(clusterPath);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLFileManager", 
                  "Error creating cluster folder: " + std::string(e.what()));
  }
}

void DDLFileManager::createEngineFolder(const std::string &cluster, const std::string &engine) {
  try {
    std::string enginePath = exportPath + "/" + sanitizeFileName(cluster) + "/" + sanitizeFileName(engine);
    std::filesystem::create_directories(enginePath);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLFileManager", 
                  "Error creating engine folder: " + std::string(e.what()));
  }
}

void DDLFileManager::createDatabaseFolder(const std::string &cluster, const std::string &engine, const std::string &database) {
  try {
    std::string dbPath = exportPath + "/" + sanitizeFileName(cluster) + "/" + 
                        sanitizeFileName(engine) + "/" + sanitizeFileName(database);
    std::filesystem::create_directories(dbPath);
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLFileManager", 
                  "Error creating database folder: " + std::string(e.what()));
  }
}

void DDLFileManager::createSchemaFolder(const std::string &cluster, const std::string &engine, 
                                       const std::string &database, const std::string &schema) {
  try {
    std::string schemaPath = exportPath + "/" + sanitizeFileName(cluster) + "/" + 
                            sanitizeFileName(engine) + "/" + sanitizeFileName(database) + "/" + 
                            sanitizeFileName(schema);
    std::filesystem::create_directories(schemaPath + "/tables");
    std::filesystem::create_directories(schemaPath + "/indexes");
    std::filesystem::create_directories(schemaPath + "/constraints");
    std::filesystem::create_directories(schemaPath + "/functions");
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLFileManager", 
                  "Error creating schema folder: " + std::string(e.what()));
  }
}

void DDLFileManager::saveTableDDL(const std::string &cluster, const std::string &engine, 
                                 const std::string &database, const std::string &schema, 
                                 const std::string &table_name, const std::string &ddl) {
  try {
    std::string filePath = getFilePath("tables", cluster, engine, database, schema, table_name);
    
    if (!validatePath(filePath)) {
      return;
    }
    
    ensureDirectoryExists(filePath);
    
    std::ofstream file(filePath);
    if (file.is_open()) {
      file << "-- Table DDL for " << schema << "." << table_name << std::endl;
      file << "-- Engine: " << engine << std::endl;
      file << "-- Database: " << database << std::endl;
      file << "-- Generated: " << std::time(nullptr) << std::endl;
      file << std::endl;
      file << ddl << std::endl;
      
      if (file.good()) {
        file.close();
      } else {
        Logger::error(LogCategory::DDL_EXPORT, "DDLFileManager", 
                      "Error writing to file: " + filePath);
        file.close();
        std::filesystem::remove(filePath);
      }
    } else {
      Logger::error(LogCategory::DDL_EXPORT, "DDLFileManager", 
                    "Failed to open file for writing: " + filePath);
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLFileManager", 
                  "Error saving table DDL: " + std::string(e.what()));
  }
}

void DDLFileManager::saveIndexDDL(const std::string &cluster, const std::string &engine, 
                                 const std::string &database, const std::string &schema, 
                                 const std::string &table_name, const std::string &index_ddl) {
  try {
    std::string filePath = getFilePath("indexes", cluster, engine, database, schema, table_name + "_indexes");
    
    ensureDirectoryExists(filePath);
    
    std::ofstream file(filePath, std::ios::app);
    if (file.is_open()) {
      file << index_ddl << std::endl;
      file.close();
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLFileManager", 
                  "Error saving index DDL: " + std::string(e.what()));
  }
}

void DDLFileManager::saveConstraintDDL(const std::string &cluster, const std::string &engine, 
                                      const std::string &database, const std::string &schema, 
                                      const std::string &table_name, const std::string &constraint_ddl) {
  try {
    std::string filePath = getFilePath("constraints", cluster, engine, database, schema, table_name + "_constraints");
    
    ensureDirectoryExists(filePath);
    
    std::ofstream file(filePath, std::ios::app);
    if (file.is_open()) {
      file << constraint_ddl << std::endl;
      file.close();
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLFileManager", 
                  "Error saving constraint DDL: " + std::string(e.what()));
  }
}

void DDLFileManager::saveFunctionDDL(const std::string &cluster, const std::string &engine, 
                                    const std::string &database, const std::string &schema, 
                                    const std::string &function_name, const std::string &function_ddl) {
  try {
    std::string filePath = getFilePath("functions", cluster, engine, database, schema, function_name);
    
    ensureDirectoryExists(filePath);
    
    std::ofstream file(filePath);
    if (file.is_open()) {
      file << "-- Function DDL for " << schema << "." << function_name << std::endl;
      file << "-- Engine: " << engine << std::endl;
      file << "-- Database: " << database << std::endl;
      file << "-- Generated: " << std::time(nullptr) << std::endl;
      file << std::endl;
      file << function_ddl << std::endl;
      file.close();
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLFileManager", 
                  "Error saving function DDL: " + std::string(e.what()));
  }
}

std::string DDLFileManager::sanitizeFileName(const std::string &name) {
  std::string sanitized = name;
  std::replace(sanitized.begin(), sanitized.end(), ' ', '_');
  std::replace(sanitized.begin(), sanitized.end(), '/', '_');
  std::replace(sanitized.begin(), sanitized.end(), '\\', '_');
  std::replace(sanitized.begin(), sanitized.end(), ':', '_');
  std::replace(sanitized.begin(), sanitized.end(), '*', '_');
  std::replace(sanitized.begin(), sanitized.end(), '?', '_');
  std::replace(sanitized.begin(), sanitized.end(), '"', '_');
  std::replace(sanitized.begin(), sanitized.end(), '<', '_');
  std::replace(sanitized.begin(), sanitized.end(), '>', '_');
  std::replace(sanitized.begin(), sanitized.end(), '|', '_');
  return sanitized;
}

std::string DDLFileManager::getFilePath(const std::string &type, const std::string &cluster, 
                                       const std::string &engine, const std::string &database, 
                                       const std::string &schema, const std::string &name) {
  return exportPath + "/" + sanitizeFileName(cluster) + "/" + 
         sanitizeFileName(engine) + "/" + sanitizeFileName(database) + "/" + 
         sanitizeFileName(schema) + "/" + type + "/" + 
         sanitizeFileName(name) + ".sql";
}

bool DDLFileManager::validatePath(const std::string &path) {
  if (path.length() > 260) {
    Logger::error(LogCategory::DDL_EXPORT, "DDLFileManager", 
                  "File path too long: " + path);
    return false;
  }
  return true;
}

void DDLFileManager::ensureDirectoryExists(const std::string &path) {
  std::filesystem::path parentPath = std::filesystem::path(path).parent_path();
  if (!std::filesystem::exists(parentPath)) {
    try {
      std::filesystem::create_directories(parentPath);
    } catch (const std::filesystem::filesystem_error &e) {
      Logger::error(LogCategory::DDL_EXPORT, "DDLFileManager", 
                    "Failed to create directory: " + parentPath.string() + " - " + e.what());
    }
  }
}
