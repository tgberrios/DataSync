#ifndef MSSQLDDLEXPORTER_H
#define MSSQLDDLEXPORTER_H

#include "DDLExporterInterface.h"
#include <sql.h>
#include <sqlext.h>

class MSSQLDDLExporter : public DDLExporterInterface {
public:
  MSSQLDDLExporter(DatabaseConnectionManager &connManager, DDLFileManager &fileManager);
  ~MSSQLDDLExporter() override = default;

  void exportDDL(const SchemaInfo &schema) override;

private:
  void exportTables(SQLHDBC hdbc, const SchemaInfo &schema);
  void exportViews(SQLHDBC hdbc, const SchemaInfo &schema);
  void exportProcedures(SQLHDBC hdbc, const SchemaInfo &schema);
  void exportFunctions(SQLHDBC hdbc, const SchemaInfo &schema);
  void exportTriggers(SQLHDBC hdbc, const SchemaInfo &schema);
  void exportConstraints(SQLHDBC hdbc, const SchemaInfo &schema);
  void exportIndexes(SQLHDBC hdbc, const SchemaInfo &schema);
};

#endif // MSSQLDDLEXPORTER_H
