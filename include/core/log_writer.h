#ifndef LOG_WRITER_H
#define LOG_WRITER_H

#include <string>

enum class LogLevel;
enum class LogCategory;

class ILogWriter {
public:
  virtual ~ILogWriter() = default;

  virtual bool write(const std::string &formattedMessage) = 0;
  virtual void flush() = 0;
  virtual void close() = 0;
  virtual bool isOpen() const = 0;
};

#endif
