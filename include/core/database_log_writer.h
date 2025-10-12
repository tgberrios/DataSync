#ifndef DATABASE_LOG_WRITER_H
#define DATABASE_LOG_WRITER_H

#include "core/log_writer.h"
#include <memory>
#include <mutex>
#include <pqxx/pqxx>
#include <string>

class DatabaseLogWriter : public ILogWriter {
private:
  std::unique_ptr<pqxx::connection> conn_;
  std::string connectionString_;
  bool statementPrepared_;
  bool enabled_;
  std::mutex mutex_;

public:
  explicit DatabaseLogWriter(const std::string &connectionString);
  ~DatabaseLogWriter() override { close(); }

  bool write(const std::string &formattedMessage) override;
  void flush() override {}
  void close() override;
  bool isOpen() const override;
  bool isEnabled() const { return enabled_; }
  void disable() { enabled_ = false; }

  bool writeParsed(const std::string &levelStr, const std::string &categoryStr,
                   const std::string &function, const std::string &message);

private:
  void prepareStatement();
};

#endif
