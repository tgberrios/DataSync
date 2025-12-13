#ifndef STRING_UTILS_H
#define STRING_UTILS_H

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <string>
#include <string_view>

namespace StringUtils {

inline std::string toLower(std::string_view str) {
  std::string result{str};
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return result;
}

inline std::string toUpper(std::string_view str) {
  std::string result{str};
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return result;
}

inline std::string trim(std::string_view str) {
  const auto start = std::find_if_not(
      str.begin(), str.end(), [](unsigned char c) { return std::isspace(c); });

  const auto end =
      std::find_if_not(str.rbegin(), str.rend(), [](unsigned char c) {
        return std::isspace(c);
      }).base();

  return (start < end) ? std::string(start, end) : std::string{};
}

inline std::string trimLeft(std::string_view str) {
  const auto start = std::find_if_not(
      str.begin(), str.end(), [](unsigned char c) { return std::isspace(c); });
  return std::string(start, str.end());
}

inline std::string trimRight(std::string_view str) {
  const auto end =
      std::find_if_not(str.rbegin(), str.rend(), [](unsigned char c) {
        return std::isspace(c);
      }).base();
  return std::string(str.begin(), end);
}

inline bool startsWith(std::string_view str, std::string_view prefix) {
  return str.size() >= prefix.size() &&
         str.compare(0, prefix.size(), prefix) == 0;
}

inline bool endsWith(std::string_view str, std::string_view suffix) {
  return str.size() >= suffix.size() &&
         str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

inline bool containsIgnoreCase(std::string_view haystack,
                               std::string_view needle) {
  auto it = std::search(haystack.begin(), haystack.end(), needle.begin(),
                        needle.end(), [](char ch1, char ch2) {
                          return std::tolower(ch1) == std::tolower(ch2);
                        });
  return it != haystack.end();
}

inline std::string sanitizeForSQL(const std::string &input) {
  std::string cleaned = input;

  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), ';'),
                cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\''),
                cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '"'),
                cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\n'),
                cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\r'),
                cleaned.end());
  cleaned.erase(std::remove(cleaned.begin(), cleaned.end(), '\t'),
                cleaned.end());

  cleaned = trim(cleaned);

  if (cleaned.empty()) {
    throw std::invalid_argument(
        "Input is empty or contains only invalid characters: " + input);
  }

  return toLower(cleaned);
}

inline bool isValidDatabaseIdentifier(const std::string &identifier) {
  if (identifier.empty() || identifier.length() > 128) {
    return false;
  }

  for (char c : identifier) {
    if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
          (c >= '0' && c <= '9') || c == '_' || c == '$' || c == '#')) {
      return false;
    }
  }

  if (identifier[0] >= '0' && identifier[0] <= '9') {
    return false;
  }

  return true;
}

inline std::string escapeMSSQLIdentifier(const std::string &identifier) {
  if (!isValidDatabaseIdentifier(identifier)) {
    throw std::invalid_argument("Invalid database identifier: " + identifier);
  }

  std::string escaped = identifier;
  size_t pos = 0;
  while ((pos = escaped.find(']', pos)) != std::string::npos) {
    escaped.replace(pos, 1, "]]");
    pos += 2;
  }
  return "[" + escaped + "]";
}

} // namespace StringUtils

#endif
