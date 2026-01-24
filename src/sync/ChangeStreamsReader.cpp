#include "sync/ChangeStreamsReader.h"
#include "core/logger.h"

ChangeStreamsReader::ChangeStreamsReader(const ChangeStreamsConfig& config) 
    : config_(config), resumeToken_(config.resumeToken) {
  Logger::info(LogCategory::SYSTEM, "ChangeStreamsReader", "Initializing ChangeStreamsReader");
}

ChangeStreamsReader::~ChangeStreamsReader() {
  // TODO: Cerrar conexión MongoDB
}

bool ChangeStreamsReader::watchCollection() {
  // TODO: Crear change stream usando mongoc_collection_watch
  watching_ = true;
  return true;
}

bool ChangeStreamsReader::readChanges(std::function<bool(const ChangeDocument&)> documentHandler) {
  // TODO: Leer cambios del change stream
  return true;
}

ChangeStreamsReader::ChangeDocument ChangeStreamsReader::parseChangeDocument(const json& doc) {
  ChangeDocument changeDoc;
  // TODO: Parsear documento de change stream
  if (doc.contains("operationType")) {
    changeDoc.operationType = doc["operationType"];
  }
  if (doc.contains("resumeToken")) {
    changeDoc.resumeToken = doc["resumeToken"];
    resumeToken_ = changeDoc.resumeToken;
  }
  return changeDoc;
}
