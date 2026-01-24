#ifndef BINLOG_READER_H
#define BINLOG_READER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <functional>

using json = nlohmann::json;

// BinlogReader: Lee binlog de MySQL/MariaDB para CDC nativo
class BinlogReader {
public:
  struct BinlogConfig {
    std::string host;
    int port{3306};
    std::string username;
    std::string password;
    std::string database;
    std::string binlogFile;
    int64_t binlogPosition{4};  // Posición inicial (4 = después del header)
    std::string serverId{"1"};
  };

  struct BinlogEvent {
    std::string eventType;      // INSERT, UPDATE, DELETE, etc.
    std::string database;
    std::string table;
    json before;                 // Datos antes (para UPDATE/DELETE)
    json after;                  // Datos después (para INSERT/UPDATE)
    int64_t timestamp{0};
    int64_t position{0};
    std::string binlogFile;
  };

  explicit BinlogReader(const BinlogConfig& config);
  ~BinlogReader();

  // Conectar al servidor MySQL/MariaDB
  bool connect();

  // Leer eventos del binlog
  bool readBinlog(std::function<bool(const BinlogEvent&)> eventHandler);

  // Parsear evento binlog
  BinlogEvent parseBinlogEvent(const void* eventData, size_t eventSize);

  // Obtener última posición leída
  std::pair<std::string, int64_t> getLastPosition() const;

  // Establecer posición del binlog
  bool setPosition(const std::string& binlogFile, int64_t position);

  // Desconectar
  void disconnect();

private:
  BinlogConfig config_;
  void* mysql_{nullptr};  // MYSQL*
  std::string currentBinlogFile_;
  int64_t currentPosition_{4};
  bool connected_{false};

  // Validar configuración
  bool validateConfig() const;
};

#endif // BINLOG_READER_H
