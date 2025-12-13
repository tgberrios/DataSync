#include "core/database_log_writer.h"
#include <algorithm>
#include <cctype>
#include <iostream>

// Helper function to sanitize UTF-8 strings by removing invalid byte sequences
static std::string sanitizeUTF8(const std::string &input) {
  std::string result;
  result.reserve(input.size());

  for (size_t i = 0; i < input.size(); ++i) {
    unsigned char c = static_cast<unsigned char>(input[i]);

    // Keep ASCII printable characters (0x20-0x7E) and common whitespace
    if ((c >= 0x20 && c <= 0x7E) || c == '\n' || c == '\r' || c == '\t') {
      result += c;
      continue;
    }

    // Handle UTF-8 multi-byte sequences
    if ((c & 0xE0) == 0xC0) {
      // 2-byte sequence: 110xxxxx 10xxxxxx
      if (i + 1 < input.size() &&
          (static_cast<unsigned char>(input[i + 1]) & 0xC0) == 0x80) {
        result += c;
        result += input[i + 1];
        ++i;
        continue;
      }
    } else if ((c & 0xF0) == 0xE0) {
      // 3-byte sequence: 1110xxxx 10xxxxxx 10xxxxxx
      if (i + 2 < input.size() &&
          (static_cast<unsigned char>(input[i + 1]) & 0xC0) == 0x80 &&
          (static_cast<unsigned char>(input[i + 2]) & 0xC0) == 0x80) {
        result += c;
        result += input[i + 1];
        result += input[i + 2];
        i += 2;
        continue;
      }
    } else if ((c & 0xF8) == 0xF0) {
      // 4-byte sequence: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
      if (i + 3 < input.size() &&
          (static_cast<unsigned char>(input[i + 1]) & 0xC0) == 0x80 &&
          (static_cast<unsigned char>(input[i + 2]) & 0xC0) == 0x80 &&
          (static_cast<unsigned char>(input[i + 3]) & 0xC0) == 0x80) {
        result += c;
        result += input[i + 1];
        result += input[i + 2];
        result += input[i + 3];
        i += 3;
        continue;
      }
    } else if ((c & 0xC0) == 0x80) {
      // Continuation byte without a start byte - skip it
      continue;
    }

    // Invalid byte sequence - replace with '?' or skip
    // Skip invalid bytes to prevent encoding errors
  }

  return result;
}

// Constructor for DatabaseLogWriter. Initializes the log writer with a
// PostgreSQL connection string and attempts to establish a database connection.
// If the connection succeeds, a prepared statement for log insertion is
// created. If the connection fails, the writer is disabled and will silently
// fail all write operations. The connection string should be in PostgreSQL
// libpq format.
DatabaseLogWriter::DatabaseLogWriter(const std::string &connectionString)
    : connectionString_(connectionString), statementPrepared_(false),
      enabled_(true) {
  try {
    conn_ = std::make_unique<pqxx::connection>(connectionString_);
    prepareStatement();
  } catch (const std::exception &e) {
    enabled_ = false;
    std::cerr << "DatabaseLogWriter: Failed to establish connection: "
              << e.what() << std::endl;
  }
}

// Prepares the SQL statement for inserting log entries into the metadata.logs
// table. The prepared statement improves performance by avoiding SQL parsing
// overhead on each log write. The statement inserts timestamp (NOW()), log
// level, category, function name, and message. If the connection is not open
// or preparation fails, the writer is disabled. This function is called
// automatically during construction and may be called again if the statement
// preparation fails during a write operation. Note: Only a single instance of
// DatabaseLogWriter exists globally (dbWriter_ in Logger class), so there is
// no risk of connection saturation. The same connection is reused for all log
// writes throughout the application lifetime.
void DatabaseLogWriter::prepareStatement() {
  if (!conn_ || !conn_->is_open())
    return;

  if (statementPrepared_)
    return;

  try {
    pqxx::work w(*conn_);
    w.conn().prepare("log_insert",
                     "INSERT INTO metadata.logs (ts, level, category, "
                     "function, message) VALUES (NOW(), $1, $2, $3, $4)");
    w.commit();
    statementPrepared_ = true;
  } catch (const std::exception &e) {
    enabled_ = false;
    std::cerr << "DatabaseLogWriter: Failed to prepare statement: " << e.what()
              << std::endl;
  }
}

// Writes a formatted log message to the database. This method is part of the
// ILogWriter interface but is not implemented for DatabaseLogWriter, as it
// requires parsed components (level, category, function, message) rather than
// a pre-formatted string. Always returns false. This method is never called in
// practice - all logging goes through writeParsed() instead. It is kept only to
// satisfy the ILogWriter interface contract.
bool DatabaseLogWriter::write(const std::string &formattedMessage) {
  return false;
}

// Writes a log entry to the database using parsed components. This method
// inserts the log entry into metadata.logs table using the prepared statement.
// The operation is thread-safe and will automatically re-prepare the statement
// if it was not previously prepared. If the writer is disabled, the connection
// is closed, or the write fails, the function returns false and the writer is
// disabled. Returns true if the log entry was successfully written.
bool DatabaseLogWriter::writeParsed(const std::string &levelStr,
                                    const std::string &categoryStr,
                                    const std::string &function,
                                    const std::string &message) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!enabled_ || !conn_ || !conn_->is_open()) {
    if (conn_ && !conn_->is_open()) {
      enabled_ = false;
    }
    return false;
  }

  if (levelStr.length() > 50 || categoryStr.length() > 50 ||
      function.length() > 255 || message.length() > 10000) {
    return false;
  }

  try {
    if (!statementPrepared_) {
      prepareStatement();
      if (!statementPrepared_)
        return false;
    }

    // Sanitize all strings to ensure valid UTF-8 encoding
    std::string sanitizedLevel = sanitizeUTF8(levelStr);
    std::string sanitizedCategory = sanitizeUTF8(categoryStr);
    std::string sanitizedFunction = sanitizeUTF8(function);
    std::string sanitizedMessage = sanitizeUTF8(message);

    pqxx::work txn(*conn_);
    txn.exec_prepared("log_insert", sanitizedLevel, sanitizedCategory,
                      sanitizedFunction, sanitizedMessage);
    txn.commit();
    return true;
  } catch (const pqxx::broken_connection &e) {
    enabled_ = false;
    conn_.reset();
    std::cerr << "DatabaseLogWriter: Connection broken: " << e.what()
              << std::endl;
    return false;
  } catch (const pqxx::sql_error &e) {
    std::cerr << "DatabaseLogWriter: SQL error writing log entry: " << e.what()
              << std::endl;
    return false;
  } catch (const std::exception &e) {
    std::cerr << "DatabaseLogWriter: Failed to write log entry: " << e.what()
              << std::endl;
    return false;
  }
}

// Closes the database connection and disables the log writer. This function
// is thread-safe and can be called multiple times safely. After closing, all
// subsequent write operations will fail silently. The connection is released
// and the writer is marked as disabled.
void DatabaseLogWriter::close() {
  std::lock_guard<std::mutex> lock(mutex_);
  conn_.reset();
  enabled_ = false;
}

bool DatabaseLogWriter::isEnabled() const {
  std::lock_guard<std::mutex> lock(const_cast<std::mutex &>(mutex_));
  return enabled_;
}

void DatabaseLogWriter::disable() {
  std::lock_guard<std::mutex> lock(mutex_);
  enabled_ = false;
}

bool DatabaseLogWriter::isOpen() const {
  std::lock_guard<std::mutex> lock(const_cast<std::mutex &>(mutex_));
  return conn_ && conn_->is_open() && enabled_;
}
