#ifndef FILE_LOG_WRITER_H
#define FILE_LOG_WRITER_H

#include "core/log_writer.h"
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>

class FileLogWriter : public ILogWriter {
private:
  std::ofstream file_;
  std::string fileName_;
  size_t maxFileSize_;
  int maxBackupFiles_;
  std::mutex mutex_;

public:
  FileLogWriter(const std::string &fileName,
                size_t maxFileSize = 10 * 1024 * 1024, int maxBackupFiles = 5);
  ~FileLogWriter() override { close(); }

  bool write(const std::string &formattedMessage) override;
  void flush() override;
  void close() override;
  bool isOpen() const override;
  void rotate();

private:
  void checkAndRotate();
};

#endif
