#include "core/database_log_writer.h"

DatabaseLogWriter::DatabaseLogWriter(const std::string &connectionString)
    : connectionString_(connectionString), statementPrepared_(false),
      enabled_(true) {
  try {
    conn_ = std::make_unique<pqxx::connection>(connectionString_);
    prepareStatement();
  } catch (const std::exception &) {
    enabled_ = false;
  }
}

void DatabaseLogWriter::prepareStatement() {
  if (!conn_ || !conn_->is_open())
    return;

  try {
    pqxx::work w(*conn_);
    w.conn().prepare("log_insert",
                     "INSERT INTO metadata.logs (ts, level, category, "
                     "function, message) VALUES (NOW(), $1, $2, $3, $4)");
    w.commit();
    statementPrepared_ = true;
  } catch (const std::exception &) {
    enabled_ = false;
  }
}

bool DatabaseLogWriter::write(const std::string &formattedMessage) {
  return false;
}

bool DatabaseLogWriter::writeParsed(const std::string &levelStr,
                                    const std::string &categoryStr,
                                    const std::string &function,
                                    const std::string &message) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!enabled_ || !conn_ || !conn_->is_open())
    return false;

  try {
    if (!statementPrepared_) {
      prepareStatement();
      if (!statementPrepared_)
        return false;
    }

    pqxx::work txn(*conn_);
    txn.exec_prepared("log_insert", levelStr, categoryStr, function, message);
    txn.commit();
    return true;
  } catch (const std::exception &) {
    enabled_ = false;
    return false;
  }
}

void DatabaseLogWriter::close() {
  std::lock_guard<std::mutex> lock(mutex_);
  conn_.reset();
  enabled_ = false;
}

bool DatabaseLogWriter::isOpen() const {
  return conn_ && conn_->is_open() && enabled_;
}
