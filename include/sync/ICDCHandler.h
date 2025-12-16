#ifndef ICDCHANDLER_H
#define ICDCHANDLER_H

#include "sync/DatabaseToPostgresSync.h"
#include <string>

class ICDCHandler {
public:
  virtual ~ICDCHandler() = default;

  virtual void processTableCDC(const DatabaseToPostgresSync::TableInfo &table,
                               pqxx::connection &pgConn) = 0;

  virtual bool supportsCDC() const = 0;
  virtual std::string getCDCMechanism() const = 0;
};

#endif
