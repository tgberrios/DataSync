#include "utils/connection_utils.h"

// Parses a connection string into structured ConnectionParams. Expects a
// semicolon-separated format like "host=server;user=user;password=pass;db=database;port=3306".
// Supports both lowercase and uppercase keys (e.g., "host" or "SERVER",
// "db" or "DATABASE"). Trims whitespace from keys and values. Extracts host,
// user, password, database (db/DATABASE), and port. Validates that host, user,
// and database are not empty before returning. Returns std::nullopt if
// required parameters are missing. Port defaults to DEFAULT_MYSQL_PORT if not
// specified. This parser handles common connection string formats used across
// different database engines.
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
    else if (key == "user" || key == "USER")
      params.user = value;
    else if (key == "password" || key == "PASSWORD")
      params.password = value;
    else if (key == "db" || key == "DATABASE")
      params.db = value;
    else if (key == "port" || key == "PORT")
      params.port = value;
  }

  if (params.host.empty() || params.user.empty() || params.db.empty())
    return std::nullopt;

  return params;
}

// Trims leading and trailing whitespace (spaces, tabs, carriage returns,
// newlines) from a string. Returns an empty string if the input string
// contains only whitespace or is empty. Uses find_first_not_of and
// find_last_not_of for efficient trimming. This is a helper function used
// internally by parse to clean up connection string tokens.
std::string ConnectionStringParser::trim(const std::string &str) {
  size_t start = str.find_first_not_of(" \t\r\n");
  if (start == std::string::npos)
    return "";
  size_t end = str.find_last_not_of(" \t\r\n");
  return str.substr(start, end - start + 1);
}
