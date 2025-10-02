#ifndef LOGFILEMANAGER_H
#define LOGFILEMANAGER_H

#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>

class LogFileManager {
public:
  LogFileManager(const std::string &logFileName = "DataSync.log");
  ~LogFileManager();

  // File operations
  bool initialize();
  void shutdown();
  bool writeLog(const std::string &message);
  bool writeError(const std::string &message);

  // File management
  void rotateLogFile();
  void rotateErrorFile();
  bool checkFileSize();
  bool checkErrorFileSize();

  // Configuration
  void setLogFileName(const std::string &fileName);
  void setMaxFileSize(size_t maxSize);
  void setMaxBackupFiles(int maxFiles);
  void setMaxMessagesBeforeFlush(size_t maxMessages);

private:
  std::string logFileName;
  std::string errorFileName;
  std::ofstream logFile;
  std::ofstream errorFile;
  std::mutex logMutex;
  std::mutex errorMutex;

  // Configuration
  size_t maxFileSize;
  int maxBackupFiles;
  size_t maxMessagesBeforeFlush;
  size_t messageCount;
  size_t errorCount;

  // Helper methods
  bool openLogFile();
  bool openErrorFile();
  void closeLogFile();
  void closeErrorFile();
  bool createLogDirectory();
  void performLogRotation();
  void performErrorRotation();
};

#endif // LOGFILEMANAGER_H
