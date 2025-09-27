#include "StreamingData.h"
#include <iostream>

int main() {
  Logger::initialize();
  Logger::info(LogCategory::SYSTEM, "DataSync started");

  std::cout << "Running... :)" << std::endl;

  StreamingData sd;
  sd.initialize();
  sd.run();

  return 0;
}
