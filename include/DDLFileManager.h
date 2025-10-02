#ifndef DDLFILEMANAGER_H
#define DDLFILEMANAGER_H

#include <filesystem>
#include <string>

class DDLFileManager {
public:
  DDLFileManager(const std::string &basePath = "DDL_EXPORT");
  ~DDLFileManager() = default;

  // Directory management
  void createFolderStructure();
  void createClusterFolder(const std::string &cluster);
  void createEngineFolder(const std::string &cluster, const std::string &engine);
  void createDatabaseFolder(const std::string &cluster, const std::string &engine, const std::string &database);
  void createSchemaFolder(const std::string &cluster, const std::string &engine, const std::string &database, const std::string &schema);

  // File operations
  void saveTableDDL(const std::string &cluster, const std::string &engine, const std::string &database, 
                   const std::string &schema, const std::string &table_name, const std::string &ddl);
  void saveIndexDDL(const std::string &cluster, const std::string &engine, const std::string &database, 
                   const std::string &schema, const std::string &table_name, const std::string &index_ddl);
  void saveConstraintDDL(const std::string &cluster, const std::string &engine, const std::string &database, 
                        const std::string &schema, const std::string &table_name, const std::string &constraint_ddl);
  void saveFunctionDDL(const std::string &cluster, const std::string &engine, const std::string &database, 
                      const std::string &schema, const std::string &function_name, const std::string &function_ddl);

  // Utility functions
  std::string sanitizeFileName(const std::string &name);
  std::string getFilePath(const std::string &type, const std::string &cluster, const std::string &engine, 
                         const std::string &database, const std::string &schema, const std::string &name);

private:
  std::string exportPath;
  bool validatePath(const std::string &path);
  void ensureDirectoryExists(const std::string &path);
};

#endif // DDLFILEMANAGER_H
