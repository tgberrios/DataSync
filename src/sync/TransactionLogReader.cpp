#include "sync/TransactionLogReader.h"
#include "core/logger.h"

TransactionLogReader::TransactionLogReader(const TransactionLogConfig& config) 
    : config_(config), lastChangeVersion_(config.lastChangeVersion) {
  Logger::info(LogCategory::SYSTEM, "TransactionLogReader", "Initializing TransactionLogReader");
}

TransactionLogReader::~TransactionLogReader() {
  // TODO: Cerrar conexión
}

bool TransactionLogReader::enableCDC() {
  // TODO: Habilitar CDC en MSSQL: EXEC sys.sp_cdc_enable_db
  return true;
}

bool TransactionLogReader::readChanges(std::function<bool(const ChangeRecord&)> recordHandler) {
  // TODO: Leer cambios usando cdc.fn_cdc_get_all_changes_* o Change Tracking
  return true;
}

TransactionLogReader::ChangeRecord TransactionLogReader::parseChangeRecord(const void* data, size_t size) {
  ChangeRecord record;
  // TODO: Parsear formato de CDC o Change Tracking
  return record;
}

bool TransactionLogReader::setLastChangeVersion(int64_t version) {
  lastChangeVersion_ = version;
  return true;
}
