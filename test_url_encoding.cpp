#include <cctype>
#include <iomanip>
#include <iostream>
#include <sstream>

std::string urlEncodeRange(const std::string &range) {
  std::ostringstream encoded;
  encoded.fill('0');
  encoded << std::hex;

  for (char c : range) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      encoded << c;
    } else if (c == ' ') {
      encoded << "%20";
    } else {
      encoded << '%' << std::setw(2) << int(static_cast<unsigned char>(c));
    }
  }
  return encoded.str();
}

int main() {
  std::string range = "Class Data";
  std::string spreadsheetId = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms";
  std::string apiKey = "AIzaSyCd4AFiqUtWL2VHPHCmdn7PEStLcz85F2U";

  std::string encodedRange = urlEncodeRange(range);
  std::string url = "https://sheets.googleapis.com/v4/spreadsheets/" +
                    spreadsheetId + "/values/" + encodedRange +
                    "?key=" + apiKey;

  std::cout << "Original range: " << range << std::endl;
  std::cout << "Encoded range: " << encodedRange << std::endl;
  std::cout << "Full URL: " << url << std::endl;

  return 0;
}
