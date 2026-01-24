#ifndef FTP_ENGINE_H
#define FTP_ENGINE_H

#include "third_party/json.hpp"
#include <curl/curl.h>
#include <string>
#include <vector>
#include <memory>

using json = nlohmann::json;

enum class FTPProtocol {
  FTP,
  SFTP
};

struct FTPConfig {
  FTPProtocol protocol = FTPProtocol::FTP;
  std::string host;
  int port = 21;
  std::string username;
  std::string password;
  std::string remote_path = "/";
  bool use_passive = true;
  int timeout_seconds = 30;
  bool use_ssl = false;
};

class FTPEngine {
  std::string connectionString_;
  FTPConfig config_;
  CURL *curl_;

  static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp);
  void parseConnectionString();
  std::string buildFTPURL();
  std::string downloadFile(const std::string &remotePath);
  std::vector<std::string> listFiles(const std::string &remotePath);

public:
  explicit FTPEngine(const std::string &connectionString, 
                    const FTPConfig &config = FTPConfig());
  ~FTPEngine();

  FTPEngine(const FTPEngine &) = delete;
  FTPEngine &operator=(const FTPEngine &) = delete;

  std::string download(const std::string &remotePath);
  std::vector<std::string> listDirectory(const std::string &remotePath = "");
  void setConfig(const FTPConfig &config);
};

#endif
