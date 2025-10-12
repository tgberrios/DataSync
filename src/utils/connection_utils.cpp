#include "utils/connection_utils.h"

std::optional<ConnectionParams>
ConnectionStringParser::parse(std::string_view connStr) {
  ConnectionParams params;
  std::string connString{connStr};
  std::istringstream ss{connString};
  std::string token;

  while (std::getline(ss, token, ';')) {
    auto pos = token.find('=');
    if (pos == std::string::npos)
      continue;

    std::string key = trim(token.substr(0, pos));
    std::string value = trim(token.substr(pos + 1));

    if (key == "host" || key == "SERVER")
      params.host = value;
    else if (key == "user")
      params.user = value;
    else if (key == "password")
      params.password = value;
    else if (key == "db" || key == "DATABASE")
      params.db = value;
    else if (key == "port")
      params.port = value;
  }

  if (params.host.empty() || params.user.empty() || params.db.empty())
    return std::nullopt;

  return params;
}

std::string ConnectionStringParser::trim(const std::string &str) {
  size_t start = str.find_first_not_of(" \t\r\n");
  if (start == std::string::npos)
    return "";
  size_t end = str.find_last_not_of(" \t\r\n");
  return str.substr(start, end - start + 1);
}
