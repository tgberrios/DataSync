#ifndef LOG_WRITER_H
#define LOG_WRITER_H

#include <string>

class ILogWriter {
public:
  virtual ~ILogWriter() = default;

  virtual void flush() = 0;
  virtual void close() = 0;
  virtual bool isOpen() const = 0;
};

#endif
