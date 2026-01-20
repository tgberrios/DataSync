#ifndef BACKUP_SCHEDULER_H
#define BACKUP_SCHEDULER_H

#include <string>
#include <thread>
#include <atomic>

class BackupScheduler {
public:
  static void start();
  static void stop();
  static bool isRunning();
  
  static bool shouldRunCron(const std::string& cron_schedule);
  static std::string calculateNextRunTime(const std::string& cron_schedule);
  static void checkAndExecuteScheduledBackups();

private:
  static std::atomic<bool> running_;
  static std::thread scheduler_thread_;
  
  static bool matchesCronField(const std::string& field, int current_value);
  static void schedulerLoop();
};

#endif
