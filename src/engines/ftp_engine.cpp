#include "engines/ftp_engine.h"
#include "core/logger.h"
#include "utils/connection_utils.h"
#include <sstream>

FTPEngine::FTPEngine(const std::string &connectionString, const FTPConfig &config)
    : connectionString_(connectionString), config_(config), curl_(nullptr) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl_ = curl_easy_init();
  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "FTPEngine",
                  "Failed to initialize CURL");
  }
  
  if (config_.protocol == FTPProtocol::FTP) {
    parseConnectionString();
  }
}

FTPEngine::~FTPEngine() {
  if (curl_) {
    curl_easy_cleanup(curl_);
  }
  curl_global_cleanup();
}

void FTPEngine::parseConnectionString() {
  auto params = ConnectionStringParser::parse(connectionString_);
  if (params) {
    config_.host = params->host;
    if (!params->port.empty()) {
      try {
        config_.port = std::stoi(params->port);
      } catch (...) {
        config_.port = config_.protocol == FTPProtocol::FTP ? 21 : 22;
      }
    }
    config_.username = params->user;
    config_.password = params->password;
  }
}

std::string FTPEngine::buildFTPURL() {
  std::ostringstream url;
  
  if (config_.protocol == FTPProtocol::SFTP) {
    url << "sftp://";
  } else {
    url << "ftp://";
  }
  
  if (!config_.username.empty()) {
    url << config_.username;
    if (!config_.password.empty()) {
      url << ":" << config_.password;
    }
    url << "@";
  }
  
  url << config_.host << ":" << config_.port;
  
  if (!config_.remote_path.empty() && config_.remote_path[0] != '/') {
    url << "/";
  }
  url << config_.remote_path;
  
  return url.str();
}

size_t FTPEngine::WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
  ((std::string *)userp)->append((char *)contents, size * nmemb);
  return size * nmemb;
}

std::string FTPEngine::downloadFile(const std::string &remotePath) {
  if (!curl_) {
    throw std::runtime_error("CURL not initialized");
  }

  std::string responseBody;
  curl_easy_reset(curl_);

  std::string url = buildFTPURL();
  if (!remotePath.empty()) {
    if (url.back() != '/' && remotePath[0] != '/') {
      url += "/";
    }
    url += remotePath;
  }

  curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &responseBody);
  curl_easy_setopt(curl_, CURLOPT_TIMEOUT, static_cast<long>(config_.timeout_seconds));
  curl_easy_setopt(curl_, CURLOPT_FTP_USE_EPSV, config_.use_passive ? 1L : 0L);

  if (config_.protocol == FTPProtocol::SFTP) {
    curl_easy_setopt(curl_, CURLOPT_SSH_AUTH_TYPES, CURLSSH_AUTH_PASSWORD);
  }

  if (config_.use_ssl) {
    curl_easy_setopt(curl_, CURLOPT_USE_SSL, CURLUSESSL_ALL);
    curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 0L);
  }

  CURLcode res = curl_easy_perform(curl_);

  if (res != CURLE_OK) {
    throw std::runtime_error("FTP download failed: " + std::string(curl_easy_strerror(res)));
  }

  return responseBody;
}

std::vector<std::string> FTPEngine::listFiles(const std::string &remotePath) {
  Logger::warning(LogCategory::DATABASE, "FTPEngine",
                  "File listing requires additional CURL options - returning empty list");
  return {};
}

std::string FTPEngine::download(const std::string &remotePath) {
  return downloadFile(remotePath);
}

std::vector<std::string> FTPEngine::listDirectory(const std::string &remotePath) {
  if (remotePath.empty()) {
    return listFiles(config_.remote_path);
  }
  return listFiles(remotePath);
}

void FTPEngine::setConfig(const FTPConfig &config) {
  config_ = config;
}
