#ifndef MARIADBDDLEXPORTER_H
#define MARIADBDDLEXPORTER_H

#include "DDLExporterInterface.h"
#include <mysql/mysql.h>

class MariaDBDDLExporter : public DDLExporterInterface {
public:
  MariaDBDDLExporter(DatabaseConnectionManager &connManager, DDLFileManager &fileManager);
  ~MariaDBDDLExporter() = default;

  void exportDDL(const SchemaInfo &schema) override;

private:
  void exportTables(MYSQL *conn, const SchemaInfo &schema);
  void exportViews(MYSQL *conn, const SchemaInfo &schema);
  void exportProcedures(MYSQL *conn, const SchemaInfo &schema);
  void exportFunctions(MYSQL *conn, const SchemaInfo &schema);
  void exportTriggers(MYSQL *conn, const SchemaInfo &schema);
  void exportConstraints(MYSQL *conn, const SchemaInfo &schema);
  void exportEvents(MYSQL *conn, const SchemaInfo &schema);
};

#endif // MARIADBDDLEXPORTER_H
