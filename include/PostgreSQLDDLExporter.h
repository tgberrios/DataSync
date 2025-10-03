#ifndef POSTGRESQLDDLEXPORTER_H
#define POSTGRESQLDDLEXPORTER_H

#include "DDLExporterInterface.h"
#include <pqxx/pqxx>

class PostgreSQLDDLExporter : public DDLExporterInterface {
public:
  PostgreSQLDDLExporter(DatabaseConnectionManager &connManager, DDLFileManager &fileManager);
  ~PostgreSQLDDLExporter() override = default;

  void exportDDL(const SchemaInfo &schema) override;

private:
  void exportTables(pqxx::connection &conn, const SchemaInfo &schema);
  void exportViews(pqxx::connection &conn, const SchemaInfo &schema);
  void exportProcedures(pqxx::connection &conn, const SchemaInfo &schema);
  void exportFunctions(pqxx::connection &conn, const SchemaInfo &schema);
  void exportTriggers(pqxx::connection &conn, const SchemaInfo &schema);
  void exportConstraints(pqxx::connection &conn, const SchemaInfo &schema);
  void exportSequences(pqxx::connection &conn, const SchemaInfo &schema);
  void exportTypes(pqxx::connection &conn, const SchemaInfo &schema);
};

#endif // POSTGRESQLDDLEXPORTER_H
