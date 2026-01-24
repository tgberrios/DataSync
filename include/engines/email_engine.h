#ifndef EMAIL_ENGINE_H
#define EMAIL_ENGINE_H

#include "third_party/json.hpp"
#include <curl/curl.h>
#include <string>
#include <vector>
#include <memory>

using json = nlohmann::json;

enum class EmailProtocol {
  IMAP,
  POP3
};

struct EmailConfig {
  EmailProtocol protocol = EmailProtocol::IMAP;
  std::string server;
  int port = 993;
  std::string username;
  std::string password;
  std::string folder = "INBOX";
  bool use_ssl = true;
  int max_emails = 100;
  bool download_attachments = false;
};

struct EmailMessage {
  std::string id;
  std::string from;
  std::string subject;
  std::string body;
  std::string date;
  std::vector<std::string> attachments;
};

class EmailEngine {
  std::string connectionString_;
  EmailConfig config_;
  CURL *curl_;

  static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp);
  void parseConnectionString();
  std::string buildEmailURL();
  std::vector<EmailMessage> fetchEmails();

public:
  explicit EmailEngine(const std::string &connectionString, 
                      const EmailConfig &config = EmailConfig());
  ~EmailEngine();

  EmailEngine(const EmailEngine &) = delete;
  EmailEngine &operator=(const EmailEngine &) = delete;

  std::vector<EmailMessage> getMessages();
  std::vector<json> parseEmailsToJSON();
  void setConfig(const EmailConfig &config);
};

#endif
