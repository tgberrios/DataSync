#include "core/file_log_writer.h"

FileLogWriter::FileLogWriter(const std::string &fileName, size_t maxFileSize,
                             int maxBackupFiles)
    : fileName_(fileName), maxFileSize_(maxFileSize),
      maxBackupFiles_(maxBackupFiles) {
  file_.open(fileName_, std::ios::app);
}

bool FileLogWriter::write(const std::string &formattedMessage) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!file_.is_open())
    return false;

  checkAndRotate();

  file_ << formattedMessage << std::endl;
  return file_.good();
}

void FileLogWriter::flush() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_.is_open())
    file_.flush();
}

void FileLogWriter::close() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_.is_open()) {
    file_.flush();
    file_.close();
  }
}

bool FileLogWriter::isOpen() const { return file_.is_open(); }

void FileLogWriter::checkAndRotate() {
  if (!file_.is_open())
    return;

  file_.flush();
  std::filesystem::path filePath(fileName_);

  if (std::filesystem::exists(filePath)) {
    auto fileSize = std::filesystem::file_size(filePath);
    if (fileSize >= maxFileSize_) {
      rotate();
    }
  }
}

void FileLogWriter::rotate() {
  if (file_.is_open()) {
    file_.close();
  }

  for (int i = maxBackupFiles_ - 1; i > 0; --i) {
    std::string oldFile = fileName_ + "." + std::to_string(i);
    std::string newFile = fileName_ + "." + std::to_string(i + 1);

    if (std::filesystem::exists(oldFile)) {
      if (i == maxBackupFiles_ - 1) {
        std::filesystem::remove(oldFile);
      } else {
        std::filesystem::rename(oldFile, newFile);
      }
    }
  }

  if (std::filesystem::exists(fileName_)) {
    std::filesystem::rename(fileName_, fileName_ + ".1");
  }

  file_.open(fileName_, std::ios::app);
}
