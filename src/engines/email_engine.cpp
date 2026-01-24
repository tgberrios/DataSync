#include "engines/email_engine.h"
#include "core/logger.h"
#include "utils/connection_utils.h"
#include <sstream>

EmailEngine::EmailEngine(const std::string &connectionString, const EmailConfig &config)
    : connectionString_(connectionString), config_(config), curl_(nullptr) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
  curl_ = curl_easy_init();
  if (!curl_) {
    Logger::error(LogCategory::DATABASE, "EmailEngine",
                  "Failed to initialize CURL");
  }
  
  parseConnectionString();
}

EmailEngine::~EmailEngine() {
  if (curl_) {
    curl_easy_cleanup(curl_);
  }
  curl_global_cleanup();
}

void EmailEngine::parseConnectionString() {
  auto params = ConnectionStringParser::parse(connectionString_);
  if (params) {
    config_.server = params->host;
    if (!params->port.empty()) {
      try {
        config_.port = std::stoi(params->port);
      } catch (...) {
        config_.port = config_.protocol == EmailProtocol::IMAP ? 993 : 110;
      }
    }
    config_.username = params->user;
    config_.password = params->password;
  }
}

std::string EmailEngine::buildEmailURL() {
  std::ostringstream url;
  
  if (config_.protocol == EmailProtocol::IMAP) {
    url << "imap";
  } else {
    url << "pop3";
  }
  
  if (config_.use_ssl) {
    url << "s";
  }
  
  url << "://";
  
  if (!config_.username.empty()) {
    url << config_.username;
    if (!config_.password.empty()) {
      url << ":" << config_.password;
    }
    url << "@";
  }
  
  url << config_.server << ":" << config_.port << "/" << config_.folder;
  
  return url.str();
}

size_t EmailEngine::WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
  ((std::string *)userp)->append((char *)contents, size * nmemb);
  return size * nmemb;
}

std::vector<EmailMessage> EmailEngine::fetchEmails() {
  Logger::warning(LogCategory::DATABASE, "EmailEngine",
                  "Email fetching requires IMAP/POP3 library (libcurl with IMAP support or specialized library) - returning empty list");
  return {};
}

std::vector<EmailMessage> EmailEngine::getMessages() {
  return fetchEmails();
}

std::vector<json> EmailEngine::parseEmailsToJSON() {
  std::vector<EmailMessage> messages = getMessages();
  std::vector<json> result;
  
  for (const auto &msg : messages) {
    json emailJson = json::object();
    emailJson["id"] = msg.id;
    emailJson["from"] = msg.from;
    emailJson["subject"] = msg.subject;
    emailJson["body"] = msg.body;
    emailJson["date"] = msg.date;
    emailJson["attachments"] = json::array();
    for (const auto &attachment : msg.attachments) {
      emailJson["attachments"].push_back(attachment);
    }
    result.push_back(emailJson);
  }
  
  return result;
}

void EmailEngine::setConfig(const EmailConfig &config) {
  config_ = config;
}
